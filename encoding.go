package ruecache

import (
	"context"
	"fmt"
	"github.com/redis/rueidis"
)

type CacheCoder[T any] interface {
	Encode(decoded T) (encoded map[string][]byte, err error)
	Decode(encoded map[string][]byte) (decoded T, err error)
}

// EncodedCache is a convenience wrapper for Cache that handles encoding to/from []byte values for storage
// in the cache.
type EncodedCache[T any] struct {
	cache *Cache
	coder CacheCoder[T]
}

func NewEncodedCache[T any](
	client rueidis.Client,
	coder CacheCoder[T],
	opts CacheOpts,
	fetch func(ctx context.Context, ids []string) (T, error),
) (*EncodedCache[T], error) {
	cache, err := NewCache(client, opts, func(ctx context.Context, ids []string) (encoded map[string][]byte, err error) {
		var decoded T
		decoded, err = fetch(ctx, ids)
		if err != nil {
			err = fmt.Errorf("fetch: %w", err)
			return
		}
		encoded, err = coder.Encode(decoded)
		if err != nil {
			err = fmt.Errorf("encode: %w", err)
			return
		}
		return
	})
	if err != nil {
		return nil, err
	}
	return &EncodedCache[T]{cache: cache, coder: coder}, nil
}

// Raw returns the underlying, byte-based cache
func (c *EncodedCache[T]) Raw() *Cache {
	return c.cache
}

func (c *EncodedCache[T]) GetAndFill(ctx context.Context, ids []string) (decoded T, err error) {
	var results map[string][]byte
	results, err = c.cache.GetAndFill(ctx, ids)
	if err != nil {
		err = fmt.Errorf("getAndFill: %w", err)
		return
	}
	decoded, err = c.coder.Decode(results)
	if err != nil {
		err = fmt.Errorf("decode: %w", err)
		return
	}
	return
}

func (c *EncodedCache[T]) Invalidate(ctx context.Context, id string) (err error) {
	return c.cache.Invalidate(ctx, id)
}
