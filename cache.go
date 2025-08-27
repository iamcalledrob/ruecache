package ruecache

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/rueidis"
	"hash/fnv"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"
)

type CacheOpts struct {
	// DataKey is where the cached data should be stored, e.g. widgets:{WIDGET_ID}
	// Use Redis hash tags ({}) to ensure DataKey, VersionKey and LockKey use the same slot.
	DataKey func(id string) string

	// VersionKey is where the monotonically increasing version is stored. Default is DataKey:version
	VersionKey func(id string) string

	// LockKey is where a temporary lock key is stored
	LockKey func(id string) string

	// LockTTL is how long the lock should be held for when fetching data to fill the cache.
	// If the fetch operation exceeds the TTL, the result will not be written to the cache.
	// On a cache miss, concurrent gets for the same id will wait until the lock is released before returning.
	// Defaults to 5s.
	LockTTL time.Duration

	// UnlockThreshold is how much time needs to be remaining on a TTL for an early unlock to be worth sending
	// to the redis server. For example, if RTT is 10ms, and a lock has 3ms remaining, it's pointless to unlock.
	// Defaults to 20ms, should be higher than RTT. Used for unlocking after a fetch failure.
	UnlockThreshold time.Duration

	// DataTTL applies automatic expiry to the cached data. Use NoTTL constant to cache with no TTL.
	// Defaults to 300s (5 mins).
	DataTTL time.Duration

	// DataTTLJitter (0.0-1.0) is how much jitter is applied to the DataTTL to ensure that keys don't all
	// expire simultaneously, e.g. 1hr after server start time. Use NoJitter constant to disable jitter.
	// Defaults to 0.20 (+- 20% jitter)
	DataTTLJitter float64

	// VersionTTL specifies how long version keys should live for. This TTL is used to prevent stale data being
	// written into the cache, and should be longer than the maximum expected fetch time.
	// Defaults to 10 minutes.
	VersionTTL time.Duration

	// GetTimeout specifies the timeout used for a Get operation, applied alongside any timeout in the ctx.
	// Defaults to 30s, exist primarily to prevent infinite hangs when something goes wrong.
	GetTimeout time.Duration

	// Backoff function to use when polling for release of the lock key by another instance during
	// "cache stampede" situations. Defaults to DefaultBackoffFn (25-1000ms)
	Backoff func(attempt int) time.Duration

	// Logger defaults to slog.Default()
	Logger *slog.Logger

	// OnInvalidation, if set, will be invoked after the cache invalidates a key.
	// Intended for tests and debugging only.
	OnInvalidation func(id string)
}

const NoTTL = time.Duration(math.MaxInt64)
const NoJitter = -1

func (o *CacheOpts) Sanitize() error {
	if o.DataKey == nil {
		return fmt.Errorf("DataKey is required")
	}
	if o.VersionKey == nil {
		o.VersionKey = func(id string) string {
			return o.DataKey(id) + ":version"
		}
	}
	if o.LockKey == nil {
		o.LockKey = func(id string) string {
			return o.DataKey(id) + ":lock"
		}
	}
	if o.LockTTL == 0 {
		o.LockTTL = 5 * time.Second
	}
	if o.UnlockThreshold == 0 {
		o.UnlockThreshold = 20 * time.Millisecond
	}
	if o.DataTTL == 0 {
		o.DataTTL = 300 * time.Second
	}
	if o.DataTTLJitter == 0 {
		o.DataTTLJitter = 0.2
	}
	if o.VersionTTL == 0 {
		o.VersionTTL = 10 * time.Minute
	}
	if o.GetTimeout == 0 {
		o.GetTimeout = 30 * time.Second
	}
	if o.Backoff == nil {
		o.Backoff = DefaultBackoffFn()
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return nil
}

// Cache implements a redis-backed self-filling cache with per-key locking and versioning to prevent cache
// stampedes and stale writes.
//
// The cache stores values under a DataKey, tracks an increasing version per key, and uses Redis SET NX PX locks
// to prevent multiple clients from fetching the same data concurrently (stampede protection).
//
// Suitable for workloads where fetches are relatively expensive (e.g. DB queries, API calls).
//
// The cache offers consistency with the upstream data source, and will not return stale data -- the only exception
// being if Invalidate is called after a call to GetAndFill for the same key. In this scenario, the GetAndFill
// call may return the stale value, but it will not be committed to the cache. This is consistent with how
// a concurrent database read and write would behave, and should not affect application design.
//
// Does not currently use rueidis server-assisted client side caching as it's not super clear whether this would
// lead to the possibility of slightly stale data.
type Cache struct {
	client rueidis.Client
	opts   CacheOpts
	fetch  func(ctx context.Context, ids []string) (map[string][]byte, error)
	stats  CacheStats
}

func NewCache(
	client rueidis.Client,
	opts CacheOpts,
	fetch func(ctx context.Context, ids []string) (map[string][]byte, error),
) (*Cache, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	return &Cache{
		client: client,
		opts:   opts,
		fetch:  fetch,
	}, nil
}

func (f *Cache) Stats() CacheStats {
	return f.stats.Snapshot()
}

// Opts returns a pointer to the CacheOpts being used by this cache.
// Intended for testing.
func (f *Cache) Opts() *CacheOpts {
	return &f.opts
}

// GetAndFill returns cached values for the given ids, fetching missed ones and filling the cache.
// Will return a complete set of results or an error on failure.
func (f *Cache) GetAndFill(ctx context.Context, ids []string) (results map[string][]byte, err error) {
	// withCauseErr to ensure that the useful "ErrGetTimeout" is returned on timeout
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeoutCause(ctx, f.opts.GetTimeout, ErrGetTimeout)
	ctx = withCauseErr(ctx)
	defer cancel()

	// Loop runs until len(results) == len(ids)
	// Duplicate ids would loop forever
	ids = removeDuplicates(ids)

	results = make(map[string][]byte, len(ids))
	needed := append([]string{}, ids...)

	// Multiple passes may be needed to populate all results in the event of a cache stampede,
	// where a value is being filled by another node and is not ready yet.
	var got map[string][]byte
	var spin bool
	for i := 0; ; i++ {
		got, spin, err = f.getAndFillPass(ctx, needed)
		if err != nil {
			err = fmt.Errorf("pass %d: %w", i, err)
			return
		}
		maps.Copy(results, got)

		// Got everything
		if len(results) == len(ids) {
			break
		}

		// Help detect spinning in logs
		if i > 10 {
			f.opts.Logger.Warn("high number of passes", "needed", needed, "i", i)
		}

		needed = slices.DeleteFunc(needed, func(id string) bool {
			_, ok := results[id]
			return ok
		})

		// Wait to avoid spinning
		// Future optimisation: use client-side caching to be notified when lock key is deleted.
		if spin {
			wait := f.opts.Backoff(i)
			select {
			case <-time.After(wait):
				continue
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}
	}

	// May wish to consider stripping nils in the future, so if a map key exists, it's non-nil.
	// Let's see what use cases come up.
	var data []byte
	var ok bool
	for k, v := range results {
		data, ok, err = valueToData(v)
		if err != nil {
			err = fmt.Errorf("decoding data for key '%s': %w", k, err)
			return
		}
		if !ok {
			delete(results, k)
			continue
		}
		results[k] = data
	}

	return
}

// GetAndFillOne gets and fills a single id.
//
// Unlike GetAndFill, ok indicates whether a key was present in the fetched map *and* whether it is non-nil,
// so ok = true guarantees that result is a non-nil value. This aligns with expectations
func (f *Cache) GetAndFillOne(ctx context.Context, id string) (result []byte, ok bool, err error) {
	var results map[string][]byte
	results, err = f.GetAndFill(ctx, []string{id})
	result, ok = results[id]
	if result == nil {
		ok = false
	}
	return
}

// Invalidate deletes cached data for ids and bumps the versions atomically.
// By bumping the version, any fill attempt that has grabbed stale data will fail to write to the cache.
func (f *Cache) Invalidate(ctx context.Context, ids ...string) error {
	f.stats.CountInvalidations(int64(len(ids)))

	// Intended for tests and debugging
	defer func() {
		if f.opts.OnInvalidation != nil {
			for _, id := range ids {
				f.opts.OnInvalidation(id)
			}
		}
	}()

	var luaExecs []rueidis.LuaExec
	for _, id := range ids {
		luaExecs = append(luaExecs, rueidis.LuaExec{
			Keys: []string{f.opts.DataKey(id), f.opts.VersionKey(id)},
			Args: []string{strconv.FormatInt(f.opts.VersionTTL.Milliseconds(), 10)},
		})
	}

	for _, resp := range luaInvalidate.ExecMulti(ctx, f.client, luaExecs...) {
		// None of the commands in the script can throw errors, so errors will be server/connectivity related.
		err := resp.Error()
		if err != nil {
			return err
		}
	}

	return nil
}

// Performs a single best-effort pass at fetching and filling ids, returning those that could be fetched.
func (f *Cache) getAndFillPass(ctx context.Context, ids []string) (results map[string][]byte, spin bool, err error) {
	results, err = f.fetchCachedData(ctx, ids)
	if err != nil {
		err = fmt.Errorf("fetching cached data: %w", err)
		return
	}

	// Anything fetched from the cache counts as a hit, even if that's an encoded "null" or "missing" state.
	f.stats.CountHits(int64(len(results)))

	// Fast path: fetched everything from cache
	if len(results) == len(ids) {
		return
	}

	// Cache miss for one or more keys: fetch and fill cache
	var needed []string
	for _, id := range ids {
		if _, ok := results[id]; !ok {
			needed = append(needed, id)
		}
	}

	f.opts.Logger.Debug("cache miss", "ids", needed)

	// Fetching versions *before* fetching data from the underlying data source.
	// This makes it possible to skip cache writes where stale data has been fetched, as the version key
	// will be bumped when new data is written to the underlying data store.
	//
	// The TTL of each version key is set to {10 minutes} to prevent expiry during fetch. In very, very hot
	// scenarios where memory pressure is causing evictions, the version key could still get evicted, but this
	// is extremely unlikely with a 10 minute TTL. Consult docs on the redis expiry mechanism.
	var versions map[string]int64
	versions, err = f.fetchVersions(ctx, needed, f.opts.VersionTTL)
	if err != nil {
		err = fmt.Errorf("fetching versions: %w", err)
		return
	}

	// Lock the needed ids, returning tokens for successful acquisitions only.
	// Tokens are used to ensure the lock is still held when writing.
	// Prevents a cache stampede by ensuring multiple servers don't fetch the same key from the data source.
	// TTL is used to avoid locking indefinitely in the event of a bug or crash.
	var acquired map[string]int64
	acquired, err = f.tryLock(ctx, f.opts.LockTTL, needed)
	if err != nil {
		err = fmt.Errorf("acquiring locks: %w", err)
		return
	}
	lockedAt := time.Now()

	// Fast path: unable to lock any ids, they're all being filled by another node.
	// Retrying this path will spin until the other node releases the lock.
	if len(acquired) == 0 {
		spin = true
		return
	}

	// Only count misses when this instance has acquired the lock to fill the cache -- because
	// another instance could grab the lock and fill concurrently -- which would end up being a
	// cache hit for this instance on the next getAndFillPass.
	f.stats.CountMisses(int64(len(acquired)))

	acquiredIds := mapKeys(acquired)

	// Fetch data from the underlying data source for ids we have the lock for
	// Can be a subset of needed, as some ids may not exist.
	var fetched map[string][]byte
	fetched, err = f.fetch(ctx, acquiredIds)
	if err != nil {
		// Unlock if it makes sense, logging errors. Give up after 500ms so caller isn't blocked for too long.
		f.bestEffortUnlock(acquired, lockedAt, 500*time.Millisecond)
		err = fmt.Errorf("fetching data: %w", err)
		return
	}

	f.stats.CountFetches(1)

	// Encode fetched data to contain whether the value was "nil" or not.
	// This differentiates a nil and an empty/zero value, which can mean different things --
	// i.e. not exists, vs. exists but empty.
	//
	// It's important that nil (nonexistent) values are committed to the cache like any other, otherwise nonexistent
	// values from upstream would lead to hitting the upstream every time.
	//
	// This mirrors how the database would behave, e.g. SELECT * FROM table WHERE id = ANY(1, 2, 3) would only
	// return rows 1+2 if 3 did not exist.
	encodedFetched := make(map[string][]byte, len(acquiredIds))
	for _, id := range acquiredIds {
		val, ok := fetched[id]
		encodedFetched[id] = dataToValue(val, ok)
	}

	// Merge fetched data into result set, regardless of whether it's possible to commit the fetched data
	// back to the cache.
	//
	// This technically results in stale data being returned, but the staleness is acceptable because the data
	// would not have been stale at the time the function was invoked. It's what would have been returned if
	// data had been fetched from the underlying data source directly. No guarantees can be made about what data
	// is returned if a write and a get are both invoked concurrently.
	//
	// It's important not to *cache* stale data because that affects future fetches, but returning it is fine.
	maps.Copy(results, encodedFetched)

	// Try to commit fetched data to the cache, which will succeed if:
	// 1. The lock token is still ours OR the lock token has expired but nobody else holds the lock, and
	// 2. The version has not changed -- i.e. the fetched data has not become stale between fetch and commit time.
	//
	// Allowing commits even if our lock token has expired should be more resilient to the case where a very slow
	// underlying database returns a value after the lock expired, but there is no cache stampede. The value is still
	// current (version has not changed), and writing to the cache will improve subsequent calls.
	//
	// Returns ids that could be successfully committed.
	var reqs []commitRequest
	for id, data := range encodedFetched {
		reqs = append(reqs, commitRequest{
			id:      id,
			version: versions[id],
			token:   acquired[id],
			data:    data,
		})
	}

	// TODO: If we've already fetched the data, it makes sense to commit it even if the context is cancelled.
	//       Perhaps, like bestEffortUnlock, we enforce a minimum attempt duration regardless of the caller's
	//       ctx (e.g. 500ms)? A context.MaxOf essentially.

	var committed []string
	committed, err = f.tryCommitAndUnlock(ctx, reqs)
	if err != nil {
		err = fmt.Errorf("commit and unlock: %w", err)
		return
	}

	if len(committed) < len(reqs) {
		f.opts.Logger.Debug("failed to commit data for all ids", "reqs", len(reqs), "committed", committed)
	}

	return
}

func (f *Cache) fetchCachedData(ctx context.Context, ids []string) (found map[string][]byte, err error) {
	return mappingIdsToKeys[[]byte](ids, f.opts.DataKey, func(keys []string) (map[string][]byte, error) {
		var cmds []rueidis.Completed
		for _, key := range keys {
			cmds = append(cmds, f.client.B().Get().Key(key).Build())
		}
		results, getErr := getResultsByKey(keys, f.client.DoMulti(ctx, cmds...))
		if getErr != nil {
			return nil, getErr
		}

		resultsAsBytes := transformMapValues(results, func(s string) []byte { return []byte(s) })
		return resultsAsBytes, nil
	})
}

func (f *Cache) fetchVersions(ctx context.Context, ids []string, ttl time.Duration) (found map[string]int64, err error) {
	return mappingIdsToKeys[int64](ids, f.opts.VersionKey, func(keys []string) (map[string]int64, error) {
		var cmds []rueidis.Completed
		for _, key := range keys {
			// Uses GETEX to keep version around only if the cache is or has recently been filled for the id
			// Reduces stable cache size (version is nonexistent by default), and also offers better reliability
			// if keys are being evicted in ttl order.
			//
			// As long as the ttl exceeds the maximum feasible fetch duration, it's safe to expire the version.
			cmds = append(cmds, f.client.B().Getex().Key(key).Px(ttl).Build())
		}
		results, getErr := getResultsByKey(keys, f.client.DoMulti(ctx, cmds...))
		if getErr != nil {
			return nil, getErr
		}

		resultsAsInts := transformMapValues(results, func(s string) int64 {
			i, _ := strconv.ParseInt(s, 10, 64)
			return i
		})

		return resultsAsInts, nil
	})
}

func (f *Cache) tryLock(ctx context.Context, ttl time.Duration, ids []string) (acquired map[string]int64, err error) {
	acquired = make(map[string]int64, len(ids))

	var cmds []rueidis.Completed
	var tokens []int64
	for _, id := range ids {
		key := f.opts.LockKey(id)
		token := rand.Int64()
		tokens = append(tokens, token)
		cmds = append(cmds, f.client.B().Set().Key(key).Value(strconv.FormatInt(token, 10)).Nx().Px(ttl).Build())
	}

	for i, resp := range f.client.DoMulti(ctx, cmds...) {
		id := ids[i]

		err = resp.Error()
		if rueidis.IsRedisNil(err) {
			// <nil> indicates key already exists: another node has claimed
			continue
		}
		if err != nil {
			// Don't bother to unlock acquired locks: any failure to set is likely to be connectivity or server
			// health related, so issuing further commands is very likely to fail too. Rely on TTL.
			return nil, fmt.Errorf("set nx px: %s: %w", id, err)
		}

		acquired[id] = tokens[i]
	}

	return acquired, nil
}

func (f *Cache) tryUnlock(ctx context.Context, acquired map[string]int64) error {
	var luaExecs []rueidis.LuaExec
	for id, token := range acquired {
		luaExecs = append(luaExecs, rueidis.LuaExec{
			Keys: []string{f.opts.LockKey(id)},
			Args: []string{strconv.FormatInt(token, 10)},
		})
	}

	for _, resp := range luaUnlock.ExecMulti(ctx, f.client, luaExecs...) {
		// None of the commands in the script can throw errors, so errors will be server/connectivity related.
		err := resp.Error()
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *Cache) tryUnlockIfNeeded(ctx context.Context, acquired map[string]int64, lockedAt time.Time) error {
	// Skip unlock if the lock will expire soon anyway
	remaining := f.opts.LockTTL - time.Since(lockedAt)
	if remaining < f.opts.UnlockThreshold {
		return nil
	}

	// Don't wait longer than the lock's TTL
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, remaining)
	defer cancel()

	err := f.tryUnlock(ctx, acquired)
	if err != nil {
		return err
	}

	return nil
}

func (f *Cache) bestEffortUnlock(acquired map[string]int64, lockedAt time.Time, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := f.tryUnlockIfNeeded(ctx, acquired, lockedAt)
	if err != nil {
		f.opts.Logger.Error("best effort unlock failed", "err", err, "ids", mapKeys(acquired))
	}
}

func (f *Cache) tryCommitAndUnlock(ctx context.Context, reqs []commitRequest) (committed []string, err error) {
	var luaExecs []rueidis.LuaExec
	for _, req := range reqs {
		luaExecs = append(luaExecs, rueidis.LuaExec{
			Keys: []string{f.opts.LockKey(req.id), f.opts.VersionKey(req.id), f.opts.DataKey(req.id)},
			Args: []string{
				strconv.FormatInt(req.token, 10),
				strconv.FormatInt(req.version, 10),
				string(req.data),
				strconv.FormatInt(f.ttlForId(req.id).Milliseconds(), 10),
			},
		})
	}

	for i, resp := range luaCommitAndUnlock.ExecMulti(ctx, f.client, luaExecs...) {
		// None of the commands in the script can throw errors, so errors will be server/connectivity related.
		err = resp.Error()
		if err != nil {
			return
		}
		// Script returns 1 if commit was possible, 0 if commit failed due to expired lock/stale version
		if v, _ := resp.AsInt64(); v == 1 {
			committed = append(committed, reqs[i].id)
		}
	}

	return
}

func (f *Cache) ttlForId(id string) time.Duration {
	if f.opts.DataTTL == NoTTL {
		return 0
	}
	if f.opts.DataTTLJitter == NoJitter {
		return f.opts.DataTTL
	}
	return time.Duration(float64(f.opts.DataTTL) * jitterMultiplier(id, f.opts.DataTTLJitter))
}

// deterministic jitter based on seed string
func jitterMultiplier(seed string, plusMinus float64) float64 {
	h := fnv.New64()
	_, _ = h.Write([]byte(seed))

	// float64 in [0,1] range
	r := float64(h.Sum64()) / float64(^uint64(0))

	return (1 - plusMinus) + (2 * plusMinus * r)
}

type commitRequest struct {
	id      string
	version int64
	token   int64
	data    []byte
}

// getResultsByKey returns a map of key <-> non-nil redis get responses (always strings)
func getResultsByKey(keys []string, resps []rueidis.RedisResult) (found map[string]string, err error) {
	if len(keys) != len(resps) {
		return nil, fmt.Errorf("key count (%d) does not match resp count (%d)", len(keys), len(resps))
	}

	found = make(map[string]string)
	for i, resp := range resps {
		key := keys[i]

		var s string
		s, err = resp.ToString()
		if rueidis.IsRedisNil(err) {
			continue
		}
		if err != nil {
			return nil, err
		}

		found[key] = s
	}

	return found, nil
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	return slices.Collect(maps.Keys(m))
}

// encode data (a map key's value) into bytes for writing to a redis, preserving nullability and presence
func dataToValue(data []byte, ok bool) []byte {
	if !ok {
		return []byte{missingPrefix}
	}
	if data == nil {
		return []byte{nullPrefix}
	}
	return append([]byte{dataPrefix}, data...)
}

// decode a value encoded with dataToValue, converting to []byte or nil as appropriate
func valueToData(value []byte) (data []byte, ok bool, err error) {
	if len(value) == 0 {
		return nil, false, fmt.Errorf("empty value")
	}
	switch value[0] {
	case missingPrefix:
		return nil, false, nil
	case nullPrefix:
		return nil, true, nil
	case dataPrefix:
		return value[1:], true, nil
	default:
		return nil, false, fmt.Errorf("malformed value: '%s' (prefix: %x)", value, value[0])
	}
}

const (
	missingPrefix byte = '-'
	nullPrefix    byte = '!'
	dataPrefix         = 'D'
)

// removes duplicates without modifying input slice
func removeDuplicates(s []string) []string {
	s = append([]string{}, s...)
	slices.Sort(s)
	return slices.Compact(s)
}

func transformMapValues[K comparable, A any, B any](src map[K]A, fn func(A) B) map[K]B {
	dst := make(map[K]B, len(src))
	for k, v := range src {
		dst[k] = fn(v)
	}
	return dst
}

func mappingIdsToKeys[T any](
	ids []string,
	keyForId func(id string) (key string),
	do func(keys []string) (map[string]T, error),
) (resultsById map[string]T, err error) {
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = keyForId(id)
	}

	var resultsByKey map[string]T
	resultsByKey, err = do(keys)
	if err != nil {
		return
	}

	resultsById = make(map[string]T, len(resultsByKey))
	for i, key := range keys {
		if v, ok := resultsByKey[key]; ok {
			id := ids[i]
			resultsById[id] = v
		}
	}

	return
}

var (
	luaUnlock = rueidis.NewLuaScript(`
	    local lockKey = KEYS[1]
		local expectedToken = ARGV[1]
	
		if redis.call("GET", lockKey) == expectedToken then
	      redis.call("DEL", lockKey)
	    end
	
	    return 0
	`)

	// If the lock is held by someone else, do nothing and return.
	// If the lock is still held by us (our token is still the value), unlock (even if the version no longer matches)
	// If the version is unchanged, commit the data to the cache (even if the lock is held by noone)
	luaCommitAndUnlock = rueidis.NewLuaScript(`
	    local lockKey = KEYS[1]
		local versionKey = KEYS[2]
		local dataKey = KEYS[3]
	
		local expectedToken = ARGV[1]
	    local expectedVersion = ARGV[2]
		local data = ARGV[3]
		local ttlMs = ARGV[4]
	
		local currentToken = redis.call("GET", lockKey)
	
		if currentToken == expectedToken then
			redis.call("DEL", lockKey)
		elseif currentToken then
			return 0
		end
	
		if (redis.call("GET", versionKey) or "0") == expectedVersion then
			if ttlMs ~= "0" then
				redis.call("SET", dataKey, data, "PX", ttlMs)
			else
				redis.call("SET", dataKey, data)
			end
			return 1
		end
	
		return 0
	`)

	// Invalidate by deleting the data key (to cause a cache miss), and bump version (prevent write of stale data).
	// In the event that the key can't be INCR'd (e.g. value is not a number), delete the key to self-heal.
	// Version key's ttl is set via PEXPIRE, to ensure that if INCR creates the key, it's not created forever.
	luaInvalidate = rueidis.NewLuaScript(`
		local dataKey = KEYS[1]
		local versionKey = KEYS[2]
		local versionTtlMs =  ARGV[1]
	
		redis.call("DEL", dataKey)
	
		local success, err = pcall(function()
			redis.call("INCR", versionKey)
			redis.call("PEXPIRE", versionKey, versionTtlMs) 
		end)
		if not success then
			redis.call("DEL", versionKey)
		end
	
		return 0
	`)
)

var ErrGetTimeout = errors.New("get timeout exceeded")
