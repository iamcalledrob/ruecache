# ruecache
ruecache implements a fast redis-backed self-filling model cache, built on top of
[rueidis](https://github.com/redis/rueidis) with auto-pipelining.

[Godoc](http://pkg.go.dev/github.com/iamcalledrob/ruecache)

## Features
- **Self-filling:** Invokes a `fetch()` func whenever the cache needs to be filled.
- **Multi-item get and fill:**: Supports getting multiple items at a time, fetching cache misses in a single pass.
- **ID to cache key mapping:** Caller's API deals in item ids, not cache keys.
- **Cache stampede protection:** Only one instance will attempt to fill the cache any given id. Implemented
  via `SET NX PX` locks.
- **Stale write prevention:** If the cache is invalidated during a fill, fetched data - which may now be
  stale - is not written to the cache. This is carefully managed using temporary `version` keys
- **High throughput:** rueidis auto-pipelining allows for a very high op/sec.

## Goals
1. Work well as a transparent service wrapper. Wrap `GetWidgetsById(ids []ID) map[ID]Widget`-style interfaces
   with a cache, preserving and caching nil values and key presence.
2. Confidently prevent race conditions that could lead to stale data being returned, or worse, written to the cache.
3. Horizontally scalability, including working with Redis when sharded by hash slot.

## Usage example

**Instantiating the cache:**
```go
cache, err := NewCache(
    client,
    ruecache.CacheOpts{
        DataKey: func(widgetId string) string {
            return "widgets:{" + widgetId + "}"
        },
        DataTTL: 5*time.Minute,
    },

    // Fetch function, invoked when widgets are needed that are not cached
    func(ctx context.Context, widgetIds []string) (map[string][]byte, error) {
        // Fetch multiple items at a time from the database
        models, err := db.GetWidgets(ctx, widgetIds)
        if err != nil {
            return nil, err
        }

        // Serialize to bytes for persisting to the cache
        modelBytes := make(map[string][]byte)
        for id, widgetModel := range models {
            modelBytes[id] = widgetModel.MarshalBinary()
        }

        return modelBytes, nil
    },
)

x := &CachingWidgetService{cache: cache, db: db}
```

**Using the cache:**
Example shows cache wrapping an existing service interface that's implemented by a database.
Note that the `GetWidgets` signature matches the signature used in the Fetch function above.
```go
func (x *CachingWidgetService) GetWidgets(ctx context.Context, widgetIds []string) (map[string]*Widget, error)
    // Fetch model bytes from the cache. Will fill using Fetch function for cache misses.
    modelBytes, err := x.cache.GetAndFill(ctx, widgetIds)
    if err != nil {
        return nil, fmt.Errorf("cache: %w", err)
    }
    
    // Deserialize bytes back into Widget models.
    models := make(map[string]*Widget)
    for id, b := range modelBytes {
        models[id] = b.UnmarshalBinary()
    }
    return models
}

func (x *CachingWidgetService) UpdateWidget(ctx context.Context, widgetId string, updated *Widget) error {
    // Update the widget in the database, and regardless of the outcome, invalidate the cache
    // -- if db.UpdateWidget fails, the state of the db will be unknown.
    return errors.Join(
        db.UpdateWidget(ctx, widgetId, updated),
        cache.Invalidate(widgetId),
    )
}
```

## EncodedCache
A convenience wrapper, `EncodedCache[T]`, exists to reduce serializing/deserializing boilerplate.
The logic is delegated to a `CacheCoder` (in this case `WidgetCacheCoder`).

```go
coder := WidgetCacheCoder{}

cache, err := NewEncodedCache[map[string]*Widget](
    client,
    coder,
    ruecache.CacheOpts{
        DataKey: func(widgetId string) string {
            return "widgets:{" + widgetId + "}"
        },
        DataTTL: 5*time.Minute,
    },

    // NEW: Fetch function returns map[string]*Widget, not map[string][]byte
    func(ctx context.Context, widgetIds []string) (map[string]*Widget, error) {
        return db.GetWidgets(ctx, widgetIds)
    },
)
```

```go
func (x *CachingWidgetService) GetWidgets(ctx context.Context, widgetIds []string) (map[string]*Widget, error)
    // NEW: cache.GetAndFill returns map[string]*Widget, not map[string][]byte
    return x.cache.GetAndFill(ctx, widgetIds)
}
```

```go
type WidgetCacheCoder struct {}

func (WidgetCacheCoder) Encode(decoded map[string]*Widget) (encoded map[string][]byte, err error) {
    // Marshal widgets to bytes
}
func (WidgetCacheCoder) Decode(encoded map[string][]byte) (decoded map[string]*Widget, err error) {
    // Unmarshal bytes to widgets
}
```

As a further convenience, `ProtoMapCoder` implements this encoding logic for maps of protobufs. e.g.:
```go
// Implements encoding to/from map[string]*Widget <-> map[string][]byte
coder := ProtoMapCoder[*Widget]{}
```

## FAQ

### Do I have to encode from/decode to `map[ID]Model`?
You can use any data structure you like, as long as items have a unique identifier.
For example:
```go

cache, err := NewEncodedCache[[]*Widget](
    // ...
    WidgetSliceCoder{}

    // NEW: Fetch function returns a slice of widgets, not a map
    func(ctx context.Context, widgetIds []string) ([]*Widget, error) {
        return db.GetWidgets(ctx, widgetIds)
    },
)

// WidgetSliceCoder deals in slices of Widgets, which are self-identifying with a WidgetId field.
type WidgetSliceCoder struct {}

func (WidgetSliceCoder) Encode(decoded []*Widget) (encoded map[string][]byte, err error) {
    encoded = make(map[string][]byte)
    for _, w := range decoded {
        encoded[w.WidgetId] = w.MarshalBinary()
    }
    return
}

func (WidgetSliceCoder) Decode(encoded map[string][]byte) (decoded []*Widget, err error) {
    for _, b := range encoded {
        w := new(Widget)
        w.UnmarshalBinary(b)
        decoded = append(decoded, w)
    }
    return
}
```

### Can I use this for more generalised caching, e.g. not just "models"?
Yes -- `ruecache` can be used for caching any kind of data or operation as long as it can be uniquely identified
somehow.

This example uses ruecache to ensure an image is only resized by one server at a time, identifying:
the image by its `originalImageUrl`:
```go
cache, err := NewCache(
    client,
    ruecache.CacheOpts{
        DataKey: func(originalImageUrl string) string {
            return "resized-images:{" + md5(originalImageUrl) + "}:url"
        },
        DataTTL: ruecache.NoTTL,
    },

    // Uses FetchOne helper for single-item filling
    ruecache.FetchOne(func(ctx context.Context, originalImageUrl string) ([]byte, error) {
        resizedImageUrl, err := imageutils.ResizeAndUpload(originalImageUrl, "512x512px")
        if err != nil {
            return nil, fmt.Errorf("resizing image: %w", err)
        }
        return []byte(resizedImageUrl), nil
    }),
)

resizedImageUrl, _ := cache.GetAndFillOne(ctx, "https://s3.amazonaws.com/images/my-image.png")
```

## TODOs
1. Implement local optimisation for stampede protection using SingleFlight or similar.
2. Subscribe to lock releases, rather than polling with backoff.
3. Write data post-fetch even if context has been cancelled (don't throw away the fetch needlessly)

## Benchmarks
Run with high (512) parallelism in order to benchmark the cache, not the RTT.
Benchmarks run against redis 8.0 running locally.
```
goos: linux
goarch: amd64
pkg: github.com/iamcalledrob/ruecache
cpu: 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz
BenchmarkCache_GetAndFill
BenchmarkCache_GetAndFill/p=512/Hit
BenchmarkCache_GetAndFill/p=512/Hit-8             627006              1812 ns/op
BenchmarkCache_GetAndFill/p=512/Miss
BenchmarkCache_GetAndFill/p=512/Miss-8             74701             17033 ns/op
```