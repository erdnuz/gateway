package limiter

import (
	"context"
	"errors"
	"sync/atomic"
)

// Reservation is the outcome of Reserve: whether every limit admitted the
// request, the decision to report, and a handle to settle its actual cost.
type Reservation struct {
	Allowed bool
	// Binding is the decision to report to the caller: the limit that
	// denied, or else the enforced limit with the least quota left.
	Binding Decision
	// BindingIndex is Binding's index in the checks (-1 with no checks).
	BindingIndex int
	Results      []Decision
	// ShadowDenied lists the shadow checks that would have denied.
	ShadowDenied []int
	// Fallback is set when any decision came from a safety limit.
	Fallback bool

	costs   []int64
	settled atomic.Bool
	// Backing arrays for the common case of a few checks, so a reservation
	// is one allocation.
	resultsBuf [3]Decision
	costsBuf   [3]int64
}

// Reserve charges a request to every check. It is admitted only if every
// enforced check admits it; shadow checks are evaluated and charged when
// they have room but never deny (a shadow limit cannot starve real
// traffic). When a check denies, the charges already made are refunded
// locally, so a denial costs no quota and no RPC.
//
// An error from an enforced check (ErrRejected, or ErrUnavailable without a
// fallback) rolls back and is returned; shadow checks' errors are ignored.
func Reserve(ctx context.Context, l Limiter, checks []Request) (*Reservation, error) {
	r := &Reservation{BindingIndex: -1}
	if len(checks) <= len(r.resultsBuf) {
		r.Results, r.costs = r.resultsBuf[:len(checks)], r.costsBuf[:len(checks)]
	} else {
		r.Results, r.costs = make([]Decision, len(checks)), make([]int64, len(checks))
	}
	for i, req := range checks {
		r.costs[i] = max(req.Cost, 1)
		d, err := l.Allow(ctx, req)
		if err != nil {
			if req.Shadow {
				continue
			}
			r.rollback(i)
			return r, err
		}
		r.Results[i] = d
		r.Fallback = r.Fallback || d.Fallback
		if d.Allowed {
			continue
		}
		if req.Shadow {
			r.ShadowDenied = append(r.ShadowDenied, i)
			continue
		}
		r.rollback(i)
		r.Binding, r.BindingIndex = d, i
		return r, nil
	}
	r.Allowed = true
	for i, d := range r.Results {
		if checks[i].Shadow {
			continue
		}
		if r.BindingIndex < 0 || remainingShare(d) < remainingShare(r.Binding) {
			r.Binding, r.BindingIndex = d, i
		}
	}
	if r.BindingIndex < 0 && len(checks) > 0 {
		r.Binding, r.BindingIndex = r.Results[0], 0
	}
	return r, nil
}

// remainingShare ranks decisions by the fraction of their limit left.
func remainingShare(d Decision) float64 {
	if d.Limit <= 0 {
		return 1
	}
	return float64(d.Remaining) / float64(d.Limit)
}

func (r *Reservation) rollback(upTo int) {
	for j := 0; j < upTo; j++ {
		if r.Results[j].Allowed {
			r.Results[j].Adjust(-r.costs[j])
			r.Results[j].Allowed = false
		}
	}
}

// Settle charges the request's actual cost in place of the cost reserved,
// on every limit that admitted it: the difference is refunded or charged
// locally, without an RPC. Only the first call has an effect.
func (r *Reservation) Settle(actual int64) {
	if r == nil || !r.Allowed || !r.settled.CompareAndSwap(false, true) {
		return
	}
	actual = max(actual, 0)
	for i, d := range r.Results {
		if d.Allowed {
			d.Adjust(actual - r.costs[i])
		}
	}
}

// Fallback wraps a primary limiter with a per-edge safety limiter used while
// the primary is unavailable. Rule returns the substitute request for a
// check, or false when the check has no safety limit (the error stands).
type Fallback struct {
	Primary Limiter
	Safety  Limiter
	Rule    func(Request) (Request, bool)
}

func (f Fallback) Allow(ctx context.Context, req Request) (Decision, error) {
	d, err := f.Primary.Allow(ctx, req)
	if err == nil || f.Safety == nil || f.Rule == nil {
		return d, err
	}
	if !errors.Is(err, ErrUnavailable) {
		return d, err
	}
	sreq, ok := f.Rule(req)
	if !ok {
		return d, err
	}
	d, _ = f.Safety.Allow(ctx, sreq)
	d.Fallback = true
	return d, nil
}
