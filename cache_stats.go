package ruecache

import "sync/atomic"

type CacheStats struct {
	Hits          int64
	Misses        int64
	Fetches       int64
	Invalidations int64
}

func (s *CacheStats) CountHits(n int64) {
	atomic.AddInt64(&s.Hits, n)
}

func (s *CacheStats) CountMisses(n int64) {
	atomic.AddInt64(&s.Misses, n)
}

func (s *CacheStats) CountFetches(n int64) {
	atomic.AddInt64(&s.Fetches, n)
}

func (s *CacheStats) CountInvalidations(n int64) {
	atomic.AddInt64(&s.Invalidations, n)
}

func (s *CacheStats) Snapshot() CacheStats {
	return CacheStats{
		Hits:          atomic.LoadInt64(&s.Hits),
		Misses:        atomic.LoadInt64(&s.Misses),
		Fetches:       atomic.LoadInt64(&s.Fetches),
		Invalidations: atomic.LoadInt64(&s.Invalidations),
	}
}
