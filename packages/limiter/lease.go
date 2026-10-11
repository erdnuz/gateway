package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"time"

	"gateway/packages/common/types"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LeaseClient asks the hub for a block of tokens from a key's global quota.
type LeaseClient interface {
	RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error)
}

// LeaseReturner gives unspent tokens back to the hub. Clients that cannot
// simply let tokens expire.
type LeaseReturner interface {
	ReturnLeases(ctx context.Context, in *types.ReturnLeasesRequest) (*types.ReturnLeasesResponse, error)
}

// LeaseOptions tune a Lease limiter; zero values take the defaults.
type LeaseOptions struct {
	LeaseSize   int64         // tokens requested per lease (default 100)
	LowWaterPct float64       // renew in the background below this % of the last grant (default 20)
	LeaseTTL    time.Duration // lease lifetime when the hub sends none (default 60s)
	RenewBefore time.Duration // renew this long before a lease expires (default 2s)
	// EdgeID identifies this edge to the hub, which splits the last of a
	// quota between the edges actively spending it.
	EdgeID string
}

const (
	failureBackoff = 500 * time.Millisecond
	rpcTimeout     = 2 * time.Second
	// missRetries caps refills per request when peers keep draining them;
	// it only guards against livelock.
	missRetries = 64
	// rateMinSample is the shortest interval a demand sample covers, so a
	// burst of misses does not read as an enormous rate.
	rateMinSample = 100 * time.Millisecond
)

// Lease enforces a global quota shared by every edge. Each edge spends tokens
// leased from the hub out of process memory and renews in the background
// before it runs dry, so a decision costs a few atomic operations and the hub
// handles one RPC per lease. When the hub has no capacity left it answers
// WAITING and the edge stops asking until the retry time.
//
// The hub's clock defines windows: every grant names the window its tokens
// belong to and how long they stay valid, so an edge whose clock is skewed
// can neither spend a token outside its window nor mix windows.
type Lease struct {
	client   atomic.Pointer[LeaseClient]
	opts     LeaseOptions
	counters table[leaseCounter]
	flights  singleflight.Group
	// backoffUntil (unix ms) short-circuits RPCs after a failure, so a hub
	// outage costs one RPC per backoff interval rather than one per request.
	backoffUntil atomic.Int64
	returnSeq    atomic.Uint64
	bootID       string
}

// leaseCounter is an edge's state for one quota.
type leaseCounter struct {
	// cur holds the tokens of the current window. A new window installs a
	// new held, so a take always knows which window its token came from.
	cur     atomic.Pointer[held]
	resetAt atomic.Int64 // unix ms (edge clock): when the quota is whole again
	// hubRemaining is the global quota left as of the last grant.
	hubRemaining atomic.Int64
	retryAt      atomic.Int64 // unix ms; non-zero while the hub says WAITING
	leaseSize    atomic.Int64
	lastGrant    atomic.Int64
	renewing     atomic.Bool
	lastSeen     atomic.Int64 // unix ms

	// Demand, reported to the hub for adaptive grant sizing.
	spent     atomic.Int64  // cost admitted since the last sample
	sampledAt atomic.Int64  // unix ms
	rate      atomic.Uint64 // float64 bits: EWMA of cost per second

	key string
	req Request // identity fields only, for returns
}

// held is the tokens an edge holds for one window of one quota.
type held struct {
	c         *leaseCounter
	win       int64 // the hub's window
	parentWin int64
	// tokens is the balance. It goes negative when post-hoc charges
	// overdraw it (debt), which the next grant pays off first.
	tokens    atomic.Int64
	consumed  atomic.Int64 // cost admitted in this window (fixed-window guard)
	expiresAt atomic.Int64 // unix ms (edge clock): end of the tokens' life
}

// NewLease returns a Lease limiter backed by client. Call Run to forget idle
// keys.
func NewLease(client LeaseClient, opts LeaseOptions) *Lease {
	if opts.LeaseSize <= 0 {
		opts.LeaseSize = types.DefaultRuntimePolicy().Edge.RateLeaseSize
	}
	if opts.LowWaterPct <= 0 || opts.LowWaterPct > 100 {
		opts.LowWaterPct = types.DefaultRuntimePolicy().Edge.RateLowWaterPct
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = time.Minute
	}
	if opts.RenewBefore <= 0 {
		opts.RenewBefore = 2 * time.Second
	}
	l := &Lease{opts: opts, bootID: strconv.FormatUint(rand.Uint64(), 36)}
	l.SetClient(client)
	return l
}

// SetClient swaps the hub client, e.g. to simulate an outage in tests.
func (l *Lease) SetClient(c LeaseClient) { l.client.Store(&c) }

// Allow admits or denies one request against the key's global quota. It
// returns ErrUnavailable when it needs the hub and cannot reach it.
func (l *Lease) Allow(ctx context.Context, req Request) (Decision, error) {
	req.Cost = max(req.Cost, 1)
	now := time.Now()
	nowMs := now.UnixMilli()
	c := l.counter(req)
	c.lastSeen.Store(nowMs)
	c.reset(req, nowMs)
	d := Decision{Limit: req.Limit, ResetAt: c.resetTime(req, now)}

	// While WAITING the hub has nothing to give, so do not ask before the
	// retry time; tokens already held stay spendable.
	waiting := false
	if at := c.retryAt.Load(); at > 0 {
		waiting = nowMs < at
		if !waiting {
			c.retryAt.CompareAndSwap(at, 0)
		}
	}
	h := c.cur.Load()
	// One edge can never legitimately admit more than a whole fixed-window
	// quota in one window, whatever the hub grants (e.g. after a quota cut).
	if fixedWindow(req) && h.consumed.Load()+req.Cost > req.Limit {
		return d, nil
	}

	// On a miss, fetch a lease (one RPC per key however many callers miss
	// together) and retry: peers may drain a refill before this caller gets
	// to it, so only the hub ends the loop with a denial.
	left := h.take(req.Cost, nowMs)
	for i := 0; left < 0 && !waiting && i < missRetries; i++ {
		granted, err := l.fetch(ctx, req, c)
		if err != nil {
			return d, err
		}
		if granted <= 0 {
			break
		}
		h = c.cur.Load()
		if fixedWindow(req) && h.consumed.Load()+req.Cost > req.Limit {
			break
		}
		left = h.take(req.Cost, time.Now().UnixMilli())
	}
	if left < 0 {
		d.ResetAt = c.resetTime(req, now)
		if at := c.retryAt.Load(); at > 0 {
			d.retry = time.UnixMilli(at)
		}
		return d, nil
	}
	h.consumed.Add(req.Cost)
	c.spent.Add(req.Cost)

	// Renew in the background at the low-water mark of the last grant, or
	// shortly before a lease that ends before its window does expires
	// (renewing one that ends with its window would only fetch tokens about
	// to be dropped).
	lowWater := int64(float64(max(c.lastGrant.Load(), 1)) * l.opts.LowWaterPct / 100)
	exp, reset := h.expiresAt.Load(), c.resetAt.Load()
	endsEarly := reset <= 0 || exp < reset
	if left <= lowWater || (endsEarly && exp > 0 && nowMs > exp-l.opts.RenewBefore.Milliseconds()) {
		l.renew(req, c)
	}

	d.Allowed = true
	d.Window = h.win
	d.ResetAt = c.resetTime(req, now)
	d.Remaining = max(0, min(req.Limit, c.hubRemaining.Load()+left))
	d.adj, d.maxDebt = h, req.MaxDebt
	if d.maxDebt <= 0 {
		d.maxDebt = req.Cost
	}
	return d, nil
}

func fixedWindow(req Request) bool {
	return req.Window > 0 && (req.Algo == "" || req.Algo == "fixed_window")
}

// Holding reports the unexpired tokens this edge holds for a key and when
// its lease expires (for tests and debugging).
func (l *Lease) Holding(scope, service, key string) (tokens int64, expires time.Time) {
	return l.HoldingLimit(scope, service, "", key)
}

// HoldingLimit is Holding for a named limit.
func (l *Lease) HoldingLimit(scope, service, limitID, key string) (tokens int64, expires time.Time) {
	v, ok := l.counters.m.Load(stateKey(Request{Scope: scope, Service: service, LimitID: limitID, Key: key}))
	if !ok {
		return 0, time.Time{}
	}
	h := v.(*leaseCounter).cur.Load()
	exp := h.expiresAt.Load()
	if exp > 0 && time.Now().UnixMilli() >= exp {
		return min(h.tokens.Load(), 0), time.UnixMilli(exp)
	}
	return h.tokens.Load(), time.UnixMilli(exp)
}

// Run evicts idle keys until ctx is done, returning the tokens they still
// hold so the quota is not stranded on this edge.
func (l *Lease) Run(ctx context.Context) {
	sweep(ctx, func(cutoff int64) {
		var idle []*leaseCounter
		l.counters.evict(func(c *leaseCounter) bool {
			if c.lastSeen.Load() < cutoff && !c.renewing.Load() {
				idle = append(idle, c)
				return true
			}
			return false
		})
		_ = l.giveBack(ctx, idle) // best effort: unreturned tokens expire with their window
	})
}

// Drain gives every unspent token back to the hub (and settles any debt),
// for a graceful shutdown: an edge that leaves mid-window does not take its
// share of the quota with it. Call it after the edge stops admitting.
func (l *Lease) Drain(ctx context.Context) error {
	var all []*leaseCounter
	l.counters.each(func(c *leaseCounter) { all = append(all, c) })
	return l.giveBack(ctx, all)
}

func (l *Lease) giveBack(ctx context.Context, counters []*leaseCounter) error {
	r, ok := (*l.client.Load()).(LeaseReturner)
	if !ok || len(counters) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	var returns []*types.LeaseReturn
	for _, c := range counters {
		// Swap the balance out first, so a token is never both spent and
		// returned. Expired tokens are worthless to the hub; debt is not.
		h := c.cur.Load()
		n := h.tokens.Swap(0)
		if exp := h.expiresAt.Load(); n > 0 && exp > 0 && now >= exp {
			n = 0
		}
		if n == 0 {
			continue
		}
		returns = append(returns, &types.LeaseReturn{
			Prefix: c.req.Scope, ServiceId: c.req.Service, ApiKey: c.req.Key, LimitId: c.req.LimitID,
			TierId: c.req.Tier, Org: c.req.Org, WindowId: h.win, ParentWindowId: h.parentWin, Tokens: n,
		})
	}
	if len(returns) == 0 {
		return nil
	}
	in := &types.ReturnLeasesRequest{
		EdgeId:  l.opts.EdgeID,
		Dedupe:  l.bootID + "-" + strconv.FormatUint(l.returnSeq.Add(1), 10),
		Returns: returns,
	}
	var err error
	for attempt := 0; attempt < 3; attempt++ { // the dedupe ID makes retries safe
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rpcTimeout)
		_, err = r.ReturnLeases(rctx, in)
		cancel()
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	return err
}

func (l *Lease) counter(req Request) *leaseCounter {
	c := l.counters.get(stateKey(req), func() *leaseCounter {
		c := &leaseCounter{key: stateKey(req)}
		c.req = Request{Scope: req.Scope, Service: req.Service, LimitID: req.LimitID, Key: req.Key, Tier: req.Tier, Org: req.Org}
		c.cur.Store(&held{c: c})
		c.leaseSize.Store(l.opts.LeaseSize)
		return c
	})
	if req.LeaseSize > 0 {
		c.leaseSize.Store(req.LeaseSize)
	}
	return c
}

// reset starts a new window once the hub's reset time has passed: held
// tokens are dropped (the hub already counts the new window from zero) but
// debt is carried. Without a hub reset time, it falls back to the edge's
// own epoch-aligned window.
func (c *leaseCounter) reset(req Request, nowMs int64) {
	at := c.resetAt.Load()
	if at <= 0 || nowMs < at || !c.resetAt.CompareAndSwap(at, 0) {
		return
	}
	c.retryAt.Store(0)
	if req.Algo == "token_bucket" {
		return // a bucket has no windows; it is simply full again
	}
	old := c.cur.Load()
	c.install(old, old.win, old.parentWin)
	c.hubRemaining.Store(0)
}

// install replaces old with a fresh window, carrying old's debt (never its
// tokens). It returns the window now current.
func (c *leaseCounter) install(old *held, win, parentWin int64) *held {
	next := &held{c: c, win: win, parentWin: parentWin}
	for {
		debt := min(old.tokens.Load(), 0)
		next.tokens.Store(debt)
		if c.cur.CompareAndSwap(old, next) {
			return next
		}
		old = c.cur.Load()
		if old.win > win {
			return old // a newer window won the race
		}
	}
}

func (c *leaseCounter) resetTime(req Request, now time.Time) time.Time {
	if at := c.resetAt.Load(); at > 0 {
		return time.UnixMilli(at)
	}
	return windowEnd(req.Window, now)
}

// take spends cost tokens and returns the balance left, or -1 when the edge
// holds too few (or its lease has expired).
func (h *held) take(cost int64, nowMs int64) int64 {
	if exp := h.expiresAt.Load(); exp > 0 && nowMs >= exp {
		for {
			cur := h.tokens.Load()
			if cur <= 0 || h.tokens.CompareAndSwap(cur, 0) {
				return -1
			}
		}
	}
	for {
		cur := h.tokens.Load()
		if cur < cost {
			return -1
		}
		if h.tokens.CompareAndSwap(cur, cur-cost) {
			return cur - cost
		}
	}
}

// adjust re-charges a request admitted from h. While h is the current
// window, delta moves its balance either way. Once the window has rolled, a
// refund is dropped (the new window owes nothing) and an extra charge
// becomes debt in the current window. Debt never exceeds maxDebt.
func (h *held) adjust(_, delta, maxDebt int64) {
	target := h
	if cur := h.c.cur.Load(); cur != h {
		if delta < 0 {
			return
		}
		target = cur
	}
	target.consumed.Add(delta)
	for {
		cur := target.tokens.Load()
		next := cur - delta
		if delta > 0 && next < -maxDebt {
			next = min(cur, -maxDebt)
		}
		if target.tokens.CompareAndSwap(cur, next) {
			return
		}
	}
}

// observe samples this edge's demand for the key: an EWMA of cost per second
// between fetches, reported to the hub to size the next grant.
func (c *leaseCounter) observe(nowMs int64) float64 {
	rate := math.Float64frombits(c.rate.Load())
	last := c.sampledAt.Load()
	if last == 0 {
		c.sampledAt.Store(nowMs)
		c.spent.Store(0)
		return rate
	}
	elapsed := time.Duration(nowMs-last) * time.Millisecond
	if elapsed < rateMinSample {
		return rate
	}
	inst := float64(c.spent.Swap(0)) / elapsed.Seconds()
	c.sampledAt.Store(nowMs)
	if rate == 0 {
		rate = inst
	} else {
		rate = 0.5*rate + 0.5*inst
	}
	c.rate.Store(math.Float64bits(rate))
	return rate
}

// renew tops up a key's lease in the background, at most once at a time.
func (l *Lease) renew(req Request, c *leaseCounter) {
	if c.retryAt.Load() > 0 || !c.renewing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.renewing.Store(false)
		_, _ = l.fetch(context.Background(), req, c)
	}()
}

// fetch requests a lease and adds the grant to the key's balance. Callers for
// the same key share one RPC, which runs detached from any single caller so a
// disconnecting client cannot fail its peers. It returns the tokens granted.
func (l *Lease) fetch(ctx context.Context, req Request, c *leaseCounter) (int64, error) {
	if time.Now().UnixMilli() < l.backoffUntil.Load() {
		return 0, ErrUnavailable
	}
	ch := l.flights.DoChan(c.key, func() (any, error) {
		// Lifetimes are measured from when the request left, which is no
		// later than when the hub made the grant: the edge's view of a
		// token's expiry is never later than the hub's.
		sent := time.Now()
		rpcCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rpcTimeout)
		defer cancel()
		resp, err := (*l.client.Load()).RequestQuotaLease(rpcCtx, &types.QuotaLeaseRequest{
			Prefix:          req.Scope,
			ServiceId:       req.Service,
			ApiKey:          req.Key,
			TierId:          req.Tier,
			LimitId:         req.LimitID,
			Org:             req.Org,
			RequestedTokens: c.leaseSize.Load(),
			EdgeId:          l.opts.EdgeID,
			ObservedRate:    c.observe(sent.UnixMilli()),
		})
		if err != nil {
			err = classify(err)
			if errors.Is(err, ErrUnavailable) {
				l.backoffUntil.Store(time.Now().Add(failureBackoff).UnixMilli())
			}
			return int64(0), err
		}
		return l.accept(req, c, sent, resp), nil
	})
	select {
	case res := <-ch:
		return res.Val.(int64), res.Err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// accept applies a hub answer to the key's state and returns the tokens it
// added.
func (l *Lease) accept(req Request, c *leaseCounter, sent time.Time, resp *types.QuotaLeaseResponse) int64 {
	sentMs := sent.UnixMilli()
	h := c.cur.Load()
	if resp.ExpiresInMs > 0 || resp.ResetInMs > 0 {
		// A grant for an older window than the tokens held is a late reply
		// from before a roll: its tokens are already dead.
		if resp.WindowId < h.win {
			return 0
		}
		if resp.WindowId > h.win {
			h = c.install(h, resp.WindowId, resp.ParentWindowId)
			if h.win != resp.WindowId {
				return 0
			}
		}
		if resp.ResetInMs > 0 {
			c.resetAt.Store(sentMs + resp.ResetInMs)
		}
	} else if end := windowEnd(req.Window, sent); !end.IsZero() && c.resetAt.Load() == 0 {
		c.resetAt.Store(end.UnixMilli()) // an older hub: the edge's own window
	}
	c.hubRemaining.Store(resp.RemainingGlobal)

	if resp.WaitingForCapacity {
		// Jitter the retry so edges do not return to the hub in lockstep.
		retry := resp.RetryAfterMs
		if retry <= 0 {
			retry = 500
		}
		c.retryAt.Store(time.Now().UnixMilli() + retry/2 + rand.Int64N(retry))
		return 0
	}
	if resp.GrantedTokens <= 0 {
		return 0
	}
	var exp int64
	if resp.ExpiresInMs > 0 {
		exp = sentMs + resp.ExpiresInMs
	} else {
		ttl := time.Duration(resp.LeaseTtlSeconds) * time.Second
		if ttl <= 0 {
			ttl = l.opts.LeaseTTL
		}
		exp = time.Now().Add(ttl).UnixMilli()
		if at := c.resetAt.Load(); at > 0 && at < exp {
			exp = at
		}
	}
	// Extend the lease before adding tokens so a concurrent take cannot
	// expire the new grant.
	h.expiresAt.Store(exp)
	h.tokens.Add(resp.GrantedTokens)
	c.lastGrant.Store(resp.GrantedTokens)
	return resp.GrantedTokens
}

func classify(err error) error {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied, codes.FailedPrecondition:
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}
