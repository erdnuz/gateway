package limiter

import (
	"context"
	"sync/atomic"
)

// Concurrency caps the requests one caller has in flight on this edge. It
// protects upstreams from a single caller's burst without any coordination:
// the cap is per edge, so N edges allow up to N times as many.
type Concurrency struct {
	inflight table[atomic.Int64]
}

// NewConcurrency returns an empty Concurrency limiter.
func NewConcurrency() *Concurrency { return &Concurrency{} }

// Acquire takes a slot for key if fewer than limit are in use. The caller
// must call release exactly once when ok is true.
func (c *Concurrency) Acquire(key string, limit int) (release func(), ok bool) {
	n := c.inflight.get(key, func() *atomic.Int64 { return new(atomic.Int64) })
	if n.Add(1) > int64(limit) {
		n.Add(-1)
		return nil, false
	}
	return func() { n.Add(-1) }, true
}

// Run forgets keys with nothing in flight until ctx ends, so memory follows
// the active callers. (A caller racing an eviction can briefly hold one
// extra slot; the cap is a guard, not an exact count.)
func (c *Concurrency) Run(ctx context.Context) {
	sweep(ctx, func(int64) {
		c.inflight.evict(func(n *atomic.Int64) bool { return n.Load() == 0 })
	})
}
