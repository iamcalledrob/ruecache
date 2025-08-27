package ruecache

import (
	"context"
	"fmt"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
	"math/rand/v2"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Tests run against a locally running redis/valkey instance, selecting database 1.
// Note: Completely erases the database before each test!

func NewTestClient(t testing.TB) rueidis.Client {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{addr},
		SelectDB:    1, // don't clobber default db (0)
	})
	require.NoError(t, err)

	// Erase the entire db to isolate tests from each other.
	err = client.Do(t.Context(), client.B().Flushdb().Build()).Error()
	require.NoError(t, err)

	return client
}

// Ensures that if keys exist, the fetch fn is not invoked
func TestCache_AllHit(t *testing.T) {
	ids := []string{"foo", "bar"}
	wantResults := map[string][]byte{
		"foo": []byte("value for foo"),
		"bar": []byte("value for bar"),
	}
	fetcher := mapFetcher(wantResults)

	var fetches atomic.Int64
	c, err := NewCache(
		NewTestClient(t),
		testOpts,
		func(ctx context.Context, ids []string) (map[string][]byte, error) {
			fetches.Add(1)
			return fetcher(ctx, ids)
		},
	)
	require.NoError(t, err)

	// First fetch: cold cache, should hit fetch fn
	var gotResults map[string][]byte
	gotResults, err = c.GetAndFill(t.Context(), ids)
	require.NoError(t, err)
	require.EqualValues(t, wantResults, gotResults)
	require.EqualValues(t, 1, fetches.Load())

	// Second fetch: warm cache, should not hit fetch fn
	gotResults, err = c.GetAndFill(t.Context(), ids)
	require.NoError(t, err)
	require.EqualValues(t, wantResults, gotResults)
	require.EqualValues(t, 1, fetches.Load())
}

func TestCache_TTL(t *testing.T) {
	opts := testOpts
	opts.DataTTL = 50 * time.Millisecond

	var fetches atomic.Int64
	c, err := NewCache(
		NewTestClient(t),
		opts,
		func(ctx context.Context, ids []string) (map[string][]byte, error) {
			fetches.Add(1)
			return map[string][]byte{"foo": []byte("hello")}, nil
		},
	)
	require.NoError(t, err)

	// First fetch: cold cache, should hit fetch fn
	_, err = c.GetAndFill(t.Context(), []string{"foo"})
	require.NoError(t, err)
	require.EqualValues(t, 1, fetches.Load())

	// Second fetch, warm cache, should not hit fetch fn
	_, err = c.GetAndFill(t.Context(), []string{"foo"})
	require.NoError(t, err)
	require.EqualValues(t, 1, fetches.Load())

	// Wait for expiry
	<-time.After(opts.DataTTL)

	// Third fetch, cached data should have expired so should have hit fetch fn again
	_, err = c.GetAndFill(t.Context(), []string{"foo"})
	require.NoError(t, err)
	require.EqualValues(t, 2, fetches.Load())
}

// Ensures that if fetch fails, the get attempt is aborted and the fetch error is returned.
// Ensures that a fetch failure doesn't result in a spin of refetching endlessly
// Ensures lock keys are released
func TestCache_FetchErrorAbortsAndUnlocks(t *testing.T) {
	client := NewTestClient(t)

	wantErr := fmt.Errorf("failure")
	c, err := NewCache(
		client,
		testOpts,
		func(ctx context.Context, ids []string) (map[string][]byte, error) {
			return nil, wantErr
		},
	)
	require.NoError(t, err)

	_, err = c.GetAndFill(t.Context(), []string{"foo"})
	require.ErrorIs(t, err, wantErr)

	// Ensure lock key is not still set
	lockKey := testOpts.LockKey("foo")
	var val int64
	var ok bool
	val, ok, err = getInt64Value(t.Context(), client, lockKey)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, int64(0), val)
}

// Ensures that key encoding works as expected -- i.e. preserving 3 states of the key
// 1. No value (not in map)
// 2. Nil value
// 3. Has value (including zero value)
//
// These are preserved and differentiated because the data source may use these different values to indicate meaning,
// e.g. requesting 5 models by id but only getting 4 back means that one of the ids doesn't exist.
//
// In any case, by preserving the exact map state, the cache won't introduce bugs in this area.
func TestCache_KeyEncoding(t *testing.T) {
	test := func(t *testing.T, wantResults map[string][]byte, testKey string) {
		var fetches atomic.Int64
		c, err := NewCache(
			NewTestClient(t),
			testOpts,
			func(ctx context.Context, ids []string) (map[string][]byte, error) {
				fetches.Add(1)
				return wantResults, nil
			},
		)
		require.NoError(t, err)

		// Fetch with cold cache, will hit fetch fn
		var gotResults map[string][]byte
		gotResults, err = c.GetAndFill(t.Context(), []string{testKey})
		require.NoError(t, err)

		// DeepEqual should compare presence of map keys as well as values
		equal := reflect.DeepEqual(wantResults, gotResults)
		if !equal {
			t.Fatalf("results not equal. want: %v, got: %v", wantResults, gotResults)
		}
		require.Equal(t, wantResults, gotResults)

		// Fetch again with warm cache, will test encoding to/from cache.
		gotResults, err = c.GetAndFill(t.Context(), []string{testKey})
		require.NoError(t, err)

		equal = reflect.DeepEqual(wantResults, gotResults)
		if !equal {
			t.Fatalf("results not equal. want: %v, got: %v", wantResults, gotResults)
		}
		require.Equal(t, wantResults, gotResults)

		// Ensure fetch fn was only invoked once (i.e. cache was used)
		require.EqualValues(t, 1, fetches.Load())
	}

	t.Run("ZeroValue", func(t *testing.T) {
		test(t, map[string][]byte{"foo": {}}, "foo")
	})
	t.Run("NilValue", func(t *testing.T) {
		test(t, map[string][]byte{"foo": nil}, "foo")
	})
	t.Run("KeyNotPresent", func(t *testing.T) {
		test(t, map[string][]byte{}, "foo")
	})
	t.Run("BytesValue", func(t *testing.T) {
		test(t, map[string][]byte{"foo": {1, 2, 3, 4, 5}}, "foo")
	})
}

// Ensures that stale data is not written to the cache if the keys are invalidated during the fetch
// (which would indicate that the fetched data may now be stale).
func TestCache_InvalidateDuringFetch(t *testing.T) {
	client := NewTestClient(t)

	fetchResultsCh := make(chan chan map[string][]byte)
	c, err := NewCache(
		client,
		testOpts,
		func(ctx context.Context, ids []string) (map[string][]byte, error) {
			ch := make(chan map[string][]byte)
			fetchResultsCh <- ch
			return <-ch, nil
		},
	)
	require.NoError(t, err)

	fetchedResults := map[string][]byte{"foo": []byte("stale data")}

	go func() {
		// Wait for fetch to be invoked
		ch := <-fetchResultsCh

		// "Fetch" results
		results := fetchedResults

		// Invalidate while fetch is still invoked
		invalidateErr := c.Invalidate(t.Context(), "foo")
		require.NoError(t, invalidateErr)

		// Unblock fetch, returning data (now stale)
		ch <- results
	}()

	var results map[string][]byte
	results, err = c.GetAndFill(t.Context(), []string{"foo"})
	require.NoError(t, err)

	// GetAndFill can return the stale data -- that's fine, as it was invoked before the invalidation.
	// Same thing would happen if you ran a db SELECT and, while it's running, ran an UPDATE.
	require.True(t, reflect.DeepEqual(results, fetchedResults))

	// Ensure that the stale data wasn't actually written to the cache
	var value []byte
	var ok bool
	value, ok, err = getBytesValue(t.Context(), client, "foo")
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, value)
}

func TestCache_tryCommitAndUnlock(t *testing.T) {

	// Ensures that a mismatched lock token (someone else has lock) does not write and does not unlock
	t.Run("LockMismatch", func(t *testing.T) {
		client := NewTestClient(t)
		c, err := NewCache(client, testOpts, nil)
		require.NoError(t, err)

		existingToken := rand.Int64()

		// Manually set lock key to something that doesn't match the request
		lockKey := c.opts.LockKey("foo")
		ttl := time.Minute
		err = setInt64Value(t.Context(), client, lockKey, existingToken, &ttl)
		require.NoError(t, err)

		// Make a commit request with a different token
		req := commitRequest{
			id:      "foo",
			version: 0,
			token:   rand.Int64(),
			data:    []byte("hello"),
		}
		var committed []string
		committed, err = c.tryCommitAndUnlock(t.Context(), []commitRequest{req})
		require.NoError(t, err)
		require.Empty(t, committed)

		// Ensure a token is still set (and unchanged)
		// i.e. we didn't unlock someone else's lock
		var token int64
		var ok bool
		token, ok, err = getInt64Value(t.Context(), client, lockKey)
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, existingToken, token)

		// Ensure no data was written to cache
		_, ok, err = getBytesValue(t.Context(), client, c.opts.DataKey("foo"))
		require.NoError(t, err)
		require.False(t, ok)

	})
	// Ensures that writes *are* possible when there's no lock (or our lock has expired).
	// As locks are used only to prevent cache stampedes, no lock indicates that we are the only interested party,
	// and so even a late value is acceptable to commit as long as it's not stale (version unchanged).
	t.Run("NoLock", func(t *testing.T) {
		client := NewTestClient(t)
		c, err := NewCache(client, testOpts, nil)
		require.NoError(t, err)

		// Make a commit request with a lock token that will not exist in the cache
		// Ensure that the data is committed
		req := commitRequest{
			id:      "foo",
			version: 0,
			token:   rand.Int64(),
			data:    []byte("hello"),
		}
		var committed []string
		committed, err = c.tryCommitAndUnlock(t.Context(), []commitRequest{req})
		require.NoError(t, err)
		require.Len(t, committed, 1)

		// Ensure data was written to cache
		var data []byte
		var ok bool
		data, ok, err = getBytesValue(t.Context(), client, c.opts.DataKey("foo"))
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, req.data, data)
	})
	// Ensures that a stale, mismatched version (but valid lock) does not write but *does* unlock
	t.Run("StaleVersion", func(t *testing.T) {
		client := NewTestClient(t)
		c, err := NewCache(client, testOpts, nil)
		require.NoError(t, err)

		// Lock key "foo" with a known token
		token := rand.Int64()
		lockKey := c.opts.LockKey("foo")
		ttl := time.Minute
		err = setInt64Value(t.Context(), client, lockKey, token, &ttl)

		require.NoError(t, err)

		// Set version key for "foo" to 2
		versionKey := c.opts.VersionKey("foo")
		err = setInt64Value(t.Context(), client, versionKey, 2, &ttl)

		require.NoError(t, err)

		// Make a commit request with the correct token, but incorrect version.
		// Version key was set above to "2".
		req := commitRequest{
			id:      "foo",
			version: 1,
			token:   token,
			data:    []byte("hello"),
		}
		var committed []string
		committed, err = c.tryCommitAndUnlock(t.Context(), []commitRequest{req})
		require.NoError(t, err)
		require.Empty(t, committed)

		// Ensure a token is not set (i.e. we released our lock)
		var ok bool
		_, ok, err = getInt64Value(t.Context(), client, lockKey)
		require.NoError(t, err)
		require.False(t, ok)

		// Ensure no data was written to cache
		_, ok, err = getBytesValue(t.Context(), client, c.opts.DataKey("foo"))
		require.NoError(t, err)
		require.False(t, ok)
	})
	// Ensures that a missing version key is treated as v0, and does not write.
	t.Run("StaleAndMissingVersion", func(t *testing.T) {
		client := NewTestClient(t)
		c, err := NewCache(client, testOpts, nil)
		require.NoError(t, err)

		// Lock key "foo" with a known token
		token := rand.Int64()
		lockKey := c.opts.LockKey("foo")
		ttl := time.Minute
		err = setInt64Value(t.Context(), client, lockKey, token, &ttl)
		require.NoError(t, err)

		// Make a commit request with the correct token, but incorrect version.
		// No version key is set, so version in cache is currently 0
		req := commitRequest{
			id:      "foo",
			version: 1,
			token:   token,
			data:    []byte("hello"),
		}
		var committed []string
		committed, err = c.tryCommitAndUnlock(t.Context(), []commitRequest{req})
		require.NoError(t, err)
		require.Empty(t, committed)

		// Ensure a token is not set (i.e. we released our lock)
		var ok bool
		_, ok, err = getInt64Value(t.Context(), client, lockKey)
		require.NoError(t, err)
		require.False(t, ok)

		// Ensure no data was written to cache
		_, ok, err = getBytesValue(t.Context(), client, c.opts.DataKey("foo"))
		require.NoError(t, err)
		require.False(t, ok)
	})
	t.Run("HappyPath", func(t *testing.T) {
		client := NewTestClient(t)
		c, err := NewCache(client, testOpts, nil)
		require.NoError(t, err)

		// Lock key "foo" with a known token
		token := rand.Int64()
		lockKey := c.opts.LockKey("foo")
		ttl := time.Minute
		err = setInt64Value(t.Context(), client, lockKey, token, &ttl)

		require.NoError(t, err)

		// Make a commit request with correct token and version
		req := commitRequest{
			id:      "foo",
			version: 0,
			token:   token,
			data:    []byte("hello"),
		}
		var committed []string
		committed, err = c.tryCommitAndUnlock(t.Context(), []commitRequest{req})
		require.NoError(t, err)

		// Ensure the commit reports as successful
		require.Len(t, committed, 1)

		// Ensure a token is not set (i.e. we released our lock)
		var ok bool
		_, ok, err = getInt64Value(t.Context(), client, lockKey)
		require.NoError(t, err)
		require.False(t, ok)

		// Ensure data was written to cache
		var data []byte
		data, ok, err = getBytesValue(t.Context(), client, c.opts.DataKey("foo"))
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, req.data, data)
	})
}

// Ensures that a bad lock value doesn't cause the cache filler to spin forever
// waiting for an unlock that is never going to happen.
//
// Defends doom loops caused by manual debugging in redis-cli
func TestCache_BadLockNoSpin(t *testing.T) {
	opts := testOpts
	opts.GetTimeout = 100 * time.Millisecond

	client := NewTestClient(t)
	c, err := NewCache(client, opts, nil)
	require.NoError(t, err)

	// Lock key "foo" *without* setting expiry on the token
	badToken := rand.Int64()
	lockKey := c.opts.LockKey("foo")
	err = setInt64Value(t.Context(), client, lockKey, badToken, nil)
	require.NoError(t, err)

	// Ensure GetAndFill returns an error and doesn't hang forever
	result := make(chan error)
	go func() {
		_, err = c.GetAndFill(t.Context(), []string{"foo"})
		result <- err
	}()

	select {
	case <-time.After(opts.GetTimeout + 50*time.Millisecond):
		t.Fatalf("GetAndFill did not return even after timeout exceeded")
	case err = <-result:
		// Don't check for ErrGetTimeout, because very rarely rueidis will return a ctx error which does not
		// inherit from the ctx we pass in. It appears, anyway.
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

// Ensures that a bad version value (e.g. not a number, so can't be INCR'd) gets reset on Invalidate,
// rather than causing the key to never be invalidated again.
//
// Guards against potential scenario where debugging in redis-cli causes a cache key to never invalidate
func TestCache_BadVersionFailsafe(t *testing.T) {
	client := NewTestClient(t)
	c, err := NewCache(client, testOpts, nil)
	require.NoError(t, err)

	// Set version key "foo" to a non-number
	versionKey := c.opts.VersionKey("foo")
	err = setValue(t.Context(), client, versionKey, "not-a-number", nil)
	require.NoError(t, err)

	// Invalidate #1 -- deletes key (treated as 0)
	err = c.Invalidate(t.Context(), "foo")
	require.NoError(t, err)

	// Invalidate #2 -- bumps key (treated as 1)
	err = c.Invalidate(t.Context(), "foo")
	require.NoError(t, err)

	var val int64
	var ok bool
	val, ok, err = getInt64Value(t.Context(), client, versionKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1, val)
}

// Ensures that a version key can't expire during a fetch, which could result in stale data being cached.
//
// Example scenario:
//  1. foo:version was read as "1" before fetching
//  2. the value "stale" is fetched from the data source
//  3. foo:version expires. The key doesn't exist, and is implicitly 0.
//  4. key 'foo' gets invalidated due to an update to the data source. INCR foo:version (missing) -> 1
//  5. version before (1) == version now (1). "stale" data is written to the cache.
//
// This is addressed internally using GETEX to bump the ttl of the version key when it's fetched the first time.
func TestCache_ExpiringVersionKey(t *testing.T) {
	fetchCh := make(chan chan map[string][]byte)

	client := NewTestClient(t)
	c, err := NewCache(client, testOpts, func(ctx context.Context, ids []string) (map[string][]byte, error) {
		resultCh := make(chan map[string][]byte)
		fetchCh <- resultCh
		return <-resultCh, nil
	})
	require.NoError(t, err)

	// Set version key "foo" to 1, but expire it soon
	versionKey := c.opts.VersionKey("foo")
	ttl := 250 * time.Millisecond
	err = setInt64Value(t.Context(), client, versionKey, 1, &ttl)

	require.NoError(t, err)

	go func() {
		resultCh := <-fetchCh

		// Wait until version key would have expired
		<-time.After(300 * time.Millisecond)

		// Fetch some data
		staleResults := map[string][]byte{"foo": []byte("stale!")}

		// Invalidate key "foo" -- stale data should not be cached in future
		// Would bump an expired/missing versionKey from 0 -> 1
		doErr := c.Invalidate(t.Context(), "foo")
		require.NoError(t, doErr)

		// Data returned is now stale due to invalidation
		resultCh <- staleResults
	}()

	// Concurrent invalidation is allowed to return the "stale" data, as invalidation during a fetch
	// is inherently racey. Same thing if you fired off a SELECT and an UPDATE to a database concurrently.
	var ok bool
	_, ok, err = c.GetAndFillOne(t.Context(), "foo")
	require.NoError(t, err)
	require.True(t, ok)

	// But the cache should *not* have been populated with stale data
	_, ok, err = getBytesValue(t.Context(), client, c.opts.DataKey("foo"))
	require.NoError(t, err)
	require.False(t, ok)
}

// Ensure Stats get updated
// Test created due to real bug where accidental value receiver prevented stats updates.
func TestCache_Stats(t *testing.T) {
	client := NewTestClient(t)
	c, err := NewCache(client, testOpts, func(ctx context.Context, ids []string) (map[string][]byte, error) {
		return map[string][]byte{
			"foo": []byte("hello"),
		}, nil
	})
	require.NoError(t, err)

	// Ensure the default stats state is empty
	statsBefore := c.stats.Snapshot()
	require.EqualValues(t, CacheStats{}, statsBefore)

	// Cache miss
	_, err = c.GetAndFill(t.Context(), []string{"foo"})
	require.NoError(t, err)
	require.EqualValues(t, 1, c.stats.Snapshot().Misses)

	// Cache hit (by requesting same key again)
	_, err = c.GetAndFill(t.Context(), []string{"foo"})
	require.NoError(t, err)
	require.EqualValues(t, 1, c.stats.Snapshot().Misses)
	require.EqualValues(t, 1, c.stats.Snapshot().Hits)

	// Ensure invalidation bumps count
	err = c.Invalidate(t.Context(), "foo")
	require.NoError(t, err)
	require.EqualValues(t, 1, c.stats.Snapshot().Invalidations)
}

// Ensures that GetAndFill will function with a nil or empty ids slice
// This is important to check because the ids slice may be output from another function, and so could end
// up being empty if other checks filter the size down to 0.
func TestCache_GetAndFillNoIds(t *testing.T) {
	c, err := NewCache(NewTestClient(t), testOpts, func(ctx context.Context, ids []string) (map[string][]byte, error) {
		t.Fatalf("fetch invoked")
		return nil, nil
	})
	require.NoError(t, err)

	t.Run("EmptySlice", func(t *testing.T) {
		var results map[string][]byte
		results, err = c.GetAndFill(t.Context(), []string{})
		require.NoError(t, err)
		require.Empty(t, results)
	})

	t.Run("NilSlice", func(t *testing.T) {
		var results map[string][]byte
		results, err = c.GetAndFill(t.Context(), nil)
		require.NoError(t, err)
		require.Empty(t, results)
	})
}

// Ensures that fetch returning no results (e.g. no items exist for these ids) is not problematic
func TestCache_FetchNoResults(t *testing.T) {
	t.Run("EmptyResultsMap", func(t *testing.T) {
		c, err := NewCache(NewTestClient(t), testOpts, func(ctx context.Context, ids []string) (map[string][]byte, error) {
			return map[string][]byte{}, nil
		})
		require.NoError(t, err)

		// Get twice to test cold vs warm cache
		_, err = c.GetAndFill(t.Context(), []string{"foo"})
		require.NoError(t, err)

		_, err = c.GetAndFill(t.Context(), []string{"foo"})
		require.NoError(t, err)
	})

	// It's reasonable for the results map to be uninitialized if there were no results
	t.Run("NilResultsMap", func(t *testing.T) {
		c, err := NewCache(NewTestClient(t), testOpts, func(ctx context.Context, ids []string) (map[string][]byte, error) {
			return nil, nil
		})
		require.NoError(t, err)

		// Get twice to test cold vs warm cache
		_, err = c.GetAndFill(t.Context(), []string{"foo"})
		require.NoError(t, err)

		_, err = c.GetAndFill(t.Context(), []string{"foo"})
		require.NoError(t, err)
	})
}

func randomString(length int) string {
	const set = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := 0; i < length; i++ {
		b[i] = set[rand.IntN(len(set))]
	}
	return string(b)
}

// With 0ms (localhost) RTT, 8 core i7:
// Hit = approx 2,100ns/op (480K op/sec)
// Miss = approx 16,000ns/op (62K op/sec)
func BenchmarkCache_GetAndFill(b *testing.B) {
	val64byte := []byte(randomString(64))

	test := func(b *testing.B, p int, hit bool) {
		client := NewTestClient(b)
		c, err := NewCache(client, testOpts, func(ctx context.Context, ids []string) (map[string][]byte, error) {
			results := make(map[string][]byte, len(ids))
			for _, id := range ids {
				results[id] = val64byte
			}
			return results, nil
		})
		require.NoError(b, err)

		// Pre-compute ids so benchmark isn't affected by rand speed
		// 36 bytes == uuid, so typical for our use case
		var ids []string
		for i := 0; i < b.N; i++ {
			ids = append(ids, randomString(36))
		}

		b.SetParallelism(p)
		b.ResetTimer()

		var n atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			var getErr error
			for pb.Next() {
				var id string

				// hit = use same id every time
				// miss = different id, will hit fetch fn and commit to cache every time
				if hit {
					id = ids[0]
				} else {
					id = ids[n.Add(1)-1]
				}

				_, getErr = c.GetAndFill(b.Context(), []string{id})
				if getErr != nil {
					b.Fatalf("GetAndFill: %s", getErr.Error())
				}
			}
		})
	}

	parallelism := []int{1, 8, 32, 128, 512}

	for _, p := range parallelism {
		b.Run(fmt.Sprintf("p=%d", p), func(b *testing.B) {
			b.Run("Hit", func(b *testing.B) {
				test(b, p, true)
			})
			b.Run("Miss", func(b *testing.B) {
				test(b, p, false)
			})
		})
	}
}

func mapFetcher(src map[string][]byte) func(ctx context.Context, ids []string) (map[string][]byte, error) {
	return func(ctx context.Context, ids []string) (map[string][]byte, error) {
		results := make(map[string][]byte)
		for _, id := range ids {
			results[id] = src[id]
		}
		return results, nil
	}
}

var testOpts = CacheOpts{
	DataKey:       func(id string) string { return "data:{" + id + "}" },
	DataTTLJitter: NoJitter, // Prevent accidentally flaky tests
}

func init() {
	_ = testOpts.Sanitize()
}

// Test helpers to cut through rueidis boilerplate

func getValue(ctx context.Context, client rueidis.Client, key string) (resp rueidis.RedisResult, exists bool, err error) {
	resp = client.Do(ctx, client.B().Get().Key(key).Build())
	if rueidis.IsRedisNil(resp.Error()) {
		// Key does not exist
		err = nil
		return
	}

	exists = true
	return
}

func getInt64Value(ctx context.Context, client rueidis.Client, key string) (val int64, exists bool, err error) {
	var resp rueidis.RedisResult
	resp, exists, err = getValue(ctx, client, key)
	if err != nil || !exists {
		return
	}
	val, err = resp.AsInt64()
	return
}

func getBytesValue(ctx context.Context, client rueidis.Client, key string) (val []byte, exists bool, err error) {
	var resp rueidis.RedisResult
	resp, exists, err = getValue(ctx, client, key)
	if err != nil || !exists {
		return
	}
	val, err = resp.AsBytes()
	return
}

func setValue(ctx context.Context, client rueidis.Client, key string, value string, ttl *time.Duration) error {
	var cmd rueidis.Completed
	if ttl != nil {
		cmd = client.B().Set().Key(key).Value(value).Px(*ttl).Build()
	} else {
		cmd = client.B().Set().Key(key).Value(value).Build()
	}

	return client.Do(ctx, cmd).Error()
}

func setInt64Value(ctx context.Context, client rueidis.Client, key string, value int64, ttl *time.Duration) error {
	return setValue(ctx, client, key, strconv.FormatInt(value, 10), ttl)
}

func setBytesValue(ctx context.Context, client rueidis.Client, key string, value []byte, ttl *time.Duration) error {
	return setValue(ctx, client, key, string(value), ttl)
}
