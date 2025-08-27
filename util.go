package ruecache

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// withCauseErr wraps a context and modifies its Err() func to join the cause (if any) with the context's default err.
func withCauseErr(ctx context.Context) context.Context {
	return &causeCtx{Context: ctx}
}

type causeCtx struct {
	context.Context
}

func (e *causeCtx) Err() error {
	orig := e.Context.Err()
	if orig == nil {
		return nil
	}

	cause := context.Cause(e.Context)
	if cause != nil {
		return fmt.Errorf("%w [%w]", cause, orig)
	}

	return orig
}

// DefaultBackoffFn backs off with jitter between 25ms-1s, for example:
// 25ms, 34ms, 95ms, 112ms, 179ms, 478ms, 1000ms, 1000ms
func DefaultBackoffFn() func(int) time.Duration {
	return BackoffFn(25*time.Millisecond, time.Second, 2)
}

// BackoffFn is a simple jittered backoff fn.
// Logic inspired by https://github.com/jpillora/backoff/blob/master/backoff.go
func BackoffFn(min time.Duration, max time.Duration, factor float64) func(int) time.Duration {
	return func(attempt int) time.Duration {
		if min > max {
			return max
		}

		fDuration := float64(min) * math.Pow(factor, float64(attempt))
		fDuration = rand.Float64()*(fDuration-float64(min)) + float64(min)

		if fDuration > float64(math.MaxInt64-512) {
			return max
		}

		duration := time.Duration(fDuration)
		if duration < min {
			return min
		}
		if duration > max {
			return max
		}
		return duration
	}
}

// FetchOne is a convenience method for fetching a single item at a time (enforced)
// Use in conjunction with GetAndFillOne
func FetchOne(
	fetch func(ctx context.Context, id string) ([]byte, error),
) func(ctx context.Context, ids []string) (map[string][]byte, error) {
	return func(ctx context.Context, ids []string) (map[string][]byte, error) {
		if len(ids) > 1 {
			return nil, fmt.Errorf("fetchOne: too many ids")
		}
		result, err := fetch(ctx, ids[0])
		if err != nil {
			return nil, fmt.Errorf("fetching %s: %w", ids[0], err)
		}
		return map[string][]byte{ids[0]: result}, nil
	}
}
