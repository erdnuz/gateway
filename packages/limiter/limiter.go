// Package limiter decides whether a request fits its rate limits.
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

// Request describes one admission decision against one limit.
type Request struct {
	Scope   string // quota scope shared with the hub (the route group, e.g. "v1")
	Service string
	// LimitID names the limit: "" is the caller's tier limit, anything else
	// one of the service's additional limits.
	LimitID string
	Key     string // the subject: a caller hash, an IP hash, or a service-wide constant
	Tier    string // the caller's tier, asserted to the hub for non-API-key callers
	Org     string // the caller's org, asserted to the hub for non-API-key callers
	Limit   int64  // cost admitted per window (or a bucket's burst)
	Cost    int64  // cost of this request; <= 0 counts as 1
	// Window is the quota period. Windows are epoch aligned (unix / window).
	// The hub's clock is authoritative: Lease tags tokens with the hub's
	// window and uses Window only to report a reset time before its first
	// grant. Zero means one unbounded window.
	Window time.Duration
	// Algo is the limit's algorithm ("" is a fixed window). Only fixed
	// windows get the per-edge guard against admitting more than Limit.
	Algo string
	// LeaseSize overrides the limiter's lease size when > 0 (Lease only).
	LeaseSize int64
	// Shadow evaluates the limit without enforcing it (see Reserve).
	Shadow bool
	// MaxDebt bounds how far post-hoc charges may overdraw this edge's
	// balance (see Reservation.Settle). Zero means the request's cost.
	MaxDebt int64
}

// Decision is the outcome of Allow.
type Decision struct {
	Allowed   bool
	Limit     int64
	Remaining int64     // best-effort quota left in the window across all edges
	ResetAt   time.Time // zero when the request has no window
	// Fallback is set when the decision came from a safety limit because
	// the primary limiter was unavailable.
	Fallback bool
	// Window is the hub window the request was charged to (0 when the
	// limiter has none, e.g. a token bucket or a local limit).
	Window int64

	// retry is when the hub expects capacity again, for a denial while it
	// says WAITING.
	retry time.Time

	// adj re-charges an admitted request (see Adjust); gen identifies the
	// window it was admitted in.
	adj     adjuster
	gen     int64
	maxDebt int64
}

type adjuster interface {
	adjust(gen, delta, maxDebt int64)
}

// RetryAt is when a denied request may succeed: when the hub expects
// capacity (a token bucket refills long before it is full), or else the
// reset.
func (d Decision) RetryAt() time.Time {
	if !d.retry.IsZero() {
		return d.retry
	}
	return d.ResetAt
}

// Adjust changes the cost charged for an admitted request by delta: a
// negative delta refunds, a positive one charges more. It never denies. A
// refund after the window has rolled is dropped (the new window owes
// nothing); an extra charge carries into the new window as debt.
func (d Decision) Adjust(delta int64) {
	if d.adj != nil && delta != 0 {
		d.adj.adjust(d.gen, delta, d.maxDebt)
	}
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
	ms := period.Milliseconds()
	if ms <= 0 {
		return time.Time{}, false
	}
	id := now.UnixMilli() / ms
	end := time.UnixMilli((id + 1) * ms)
	prev := w.id.Load()
	if prev == id || !w.id.CompareAndSwap(prev, id) {
		return end, false
	}
	w.consumed.Store(0)
	return end, true
}

// windowEnd is the end of the epoch-aligned window containing now.
func windowEnd(period time.Duration, now time.Time) time.Time {
	ms := period.Milliseconds()
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli((now.UnixMilli()/ms + 1) * ms)
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

func (t *table[T]) each(f func(*T)) {
	t.m.Range(func(_, v any) bool {
		f(v.(*T))
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
	return req.Scope + "\x00" + req.Service + "\x00" + req.LimitID + "\x00" + req.Key
}
