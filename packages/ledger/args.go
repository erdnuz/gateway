package ledger

import (
	"strconv"
	"time"
)

// bankerCapPct caps a request for more than a whole quota at this share of
// it, so one edge cannot take everything in one grant.
const bankerCapPct = 25

// grantInput is a grant reduced to the numbers both implementations use, so
// Mem and the Lua cannot disagree about the inputs.
type grantInput struct {
	algo      Algo
	key       string
	limit     int64
	win       int64 // ms
	rate      float64
	target    int64
	edge      string
	activeMs  int64
	ttl       int64 // ms; token bucket lease lifetime
	parentKey string
	plimit    int64
	pwin      int64
}

func windowMs(s Spec) int64 { return max(s.Window, time.Second).Milliseconds() }

func winKey(base string, wid int64) string { return base + ":" + strconv.FormatInt(wid, 10) }

func prepare(ref Ref, spec Spec, args GrantArgs) (grantInput, error) {
	if err := spec.validate(); err != nil {
		return grantInput{}, err
	}
	in := grantInput{
		algo:     spec.Algo.Or(),
		key:      ref.Key(spec.Algo),
		limit:    spec.Limit,
		win:      windowMs(spec),
		rate:     spec.Rate,
		target:   args.target(),
		edge:     args.EdgeID,
		activeMs: max(2*args.Horizon, 2*time.Second).Milliseconds(),
		ttl:      time.Second.Milliseconds(),
	}
	if args.Horizon > 0 {
		in.ttl = args.Horizon.Milliseconds()
	}
	if in.target > spec.Limit {
		in.target = min(in.target, max(1, spec.Limit*bankerCapPct/100))
	}
	if ref.Parent != "" && args.Parent != nil {
		if args.Parent.Limit <= 0 || args.Parent.Algo.Or() != FixedWindow {
			return grantInput{}, ErrBadSpec
		}
		in.parentKey = ref.ParentKey(FixedWindow)
		in.plimit = args.Parent.Limit
		in.pwin = windowMs(*args.Parent)
	}
	return in, nil
}

// result decodes the script's reply:
// {granted, remaining, window_id, expires_ms, reset_ms, retry_ms, active_edges, parent_window_id}.
func (in grantInput) result(r []int64) Grant {
	return Grant{
		Granted:        r[0],
		Remaining:      max(0, r[1]),
		WindowID:       r[2],
		ExpiresIn:      time.Duration(r[3]) * time.Millisecond,
		ResetIn:        time.Duration(max(0, r[4])) * time.Millisecond,
		RetryAfter:     time.Duration(r[5]) * time.Millisecond,
		ActiveEdges:    r[6],
		ParentWindowID: r[7],
	}
}

type returnInput struct {
	algo      Algo
	key       string
	win       int64
	rate      float64
	wid       int64
	tokens    int64
	parentKey string
	pwin      int64
	pwid      int64
	dedupeKey string
	dedupeTTL int64
}

func prepareReturn(r Return, dedupe string) (returnInput, error) {
	if err := r.Spec.validate(); err != nil {
		return returnInput{}, err
	}
	in := returnInput{
		algo:   r.Spec.Algo.Or(),
		key:    r.Ref.Key(r.Spec.Algo),
		win:    windowMs(r.Spec),
		rate:   r.Spec.Rate,
		wid:    r.WindowID,
		tokens: r.Tokens,
	}
	period := r.Spec.Period()
	if r.Ref.Parent != "" && r.Parent != nil {
		in.parentKey = r.Ref.ParentKey(FixedWindow)
		in.pwin = windowMs(*r.Parent)
		in.pwid = r.ParentWindowID
		period = max(period, r.Parent.Period())
	}
	if dedupe != "" {
		in.dedupeKey = in.key + ":ret:" + dedupe
		in.dedupeTTL = 2 * max(period, time.Second).Milliseconds()
	}
	return in, nil
}
