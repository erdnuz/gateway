package limiter

import (
	"context"
	"time"
)

// Local is a fixed-window limiter that counts in process memory. Every edge
// enforces the limit on its own, so N edges admit up to N×Limit; Gate uses it
// as the safety limit while the hub is unreachable.
type Local struct {
	counters table[window]
}

// NewLocal returns an empty Local limiter. Call Run to forget idle keys.
func NewLocal() *Local { return &Local{} }

// Allow never fails.
func (l *Local) Allow(_ context.Context, req Request) (Decision, error) {
	cost := max(req.Cost, 1)
	w := l.counters.get(stateKey(req), func() *window { return &window{} })
	end, _ := w.roll(req.Window, time.Now())
	d := Decision{Limit: req.Limit, ResetAt: end}
	used := w.consumed.Add(cost)
	if used > req.Limit {
		w.consumed.Add(-cost)
		d.Remaining = max(0, req.Limit-w.consumed.Load())
		return d, nil
	}
	d.Allowed = true
	d.Remaining = req.Limit - used
	d.adj, d.gen = w, w.id.Load()
	return d, nil
}

// adjust re-charges an admitted request; a refund after the window rolled
// is dropped.
func (w *window) adjust(gen, delta, _ int64) {
	if w.id.Load() == gen || delta > 0 {
		w.consumed.Add(delta)
	}
}

// Run evicts idle keys until ctx is done.
func (l *Local) Run(ctx context.Context) {
	sweep(ctx, func(cutoff int64) {
		l.counters.evict(func(w *window) bool { return w.lastSeen.Load() < cutoff })
	})
}
