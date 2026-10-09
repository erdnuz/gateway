// Package limiter decides whether a request fits its rate limit.
//
// Admission state lives here and only in process memory, so an edge is a
// stateless replica that can be added or removed at will. Lease coordinates a
// global quota across edges by leasing blocks of tokens from the hub: the
// request path never leaves the process, and the hub sees one RPC per lease
// instead of one per request.
package limiter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Request describes one admission decision.
type Request struct {
	Scope   string // quota scope shared with the hub (the route group, e.g. "v1")
	Service string
	Key     string // caller identity
	Limit   int64  // cost admitted per window
	Cost    int64  // cost of this request; <= 0 counts as 1
	// Window is the quota period. Windows are epoch aligned (unix / window),
	// the alignment the hub uses, so every edge agrees on boundaries without
	// talking to each other. Zero means one unbounded window.
	Window time.Duration
	// LeaseSize overrides the limiter's lease size when > 0 (Lease only).
	LeaseSize int64
}

// Decision is the outcome of Allow.
type Decision struct {
	Allowed   bool
	Limit     int64
	Remaining int64     // best-effort quota left in the window across all edges
	ResetAt   time.Time // zero when the request has no window
}

// Limiter admits or denies requests.
type Limiter interface {
	Allow(ctx context.Context, req Request) (Decision, error)
}

var (
	// ErrUnavailable means the limiter cannot decide (e.g. the hub is down);
	// callers choose a fallback such as a Local safety limit.
	ErrUnavailable = errors.New("rate limit service unavailable")
	// ErrRejected means the hub refused the key or service outright. It is a
	// policy decision, not an outage, and must not trigger a fallback.
	ErrRejected = errors.New("rate limit rejected the key")
)

const (
	sweepInterval = 30 * time.Second
	idleAfter     = time.Minute
)

// window tracks the cost admitted in the current epoch-aligned window.
type window struct {
	id       atomic.Int64
	consumed atomic.Int64
	lastSeen atomic.Int64 // unix ms
}

// roll advances to the window containing now. It returns the window end
// (zero without a period) and whether this call started a new window.
func (w *window) roll(period time.Duration, now time.Time) (time.Time, bool) {
	w.lastSeen.Store(now.UnixMilli())
	sec := int64(period / time.Second)
	if sec <= 0 {
		return time.Time{}, false
	}
	id := now.Unix() / sec
	end := time.Unix((id+1)*sec, 0)
	prev := w.id.Load()
	if prev == id || !w.id.CompareAndSwap(prev, id) {
		return end, false
	}
	w.consumed.Store(0)
	return end, true
}

// table is a concurrent map of per-key state that forgets quiet keys, so
// memory tracks the active key set rather than every key ever seen.
type table[T any] struct {
	m sync.Map
}

func (t *table[T]) get(key string, init func() *T) *T {
	if v, ok := t.m.Load(key); ok {
		return v.(*T)
	}
	v, _ := t.m.LoadOrStore(key, init())
	return v.(*T)
}

func (t *table[T]) evict(drop func(*T) bool) {
	t.m.Range(func(k, v any) bool {
		if drop(v.(*T)) {
			t.m.Delete(k)
		}
		return true
	})
}

func (t *table[T]) len() int {
	n := 0
	t.m.Range(func(any, any) bool { n++; return true })
	return n
}

func sweep(ctx context.Context, evict func(cutoff int64)) {
	tick := time.NewTicker(sweepInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			evict(now.Add(-idleAfter).UnixMilli())
		}
	}
}

func stateKey(req Request) string {
	return req.Scope + "\x00" + req.Service + "\x00" + req.Key
}
