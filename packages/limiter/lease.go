package limiter

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
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

// LeaseOptions tune a Lease limiter; zero values take the defaults.
type LeaseOptions struct {
	LeaseSize   int64         // tokens requested per lease (default 100)
	LowWaterPct float64       // renew in the background below this % of a lease (default 20)
	LeaseTTL    time.Duration // lease lifetime when the hub sends none (default 60s)
	RenewBefore time.Duration // renew this long before a lease expires (default 2s)
}

const (
	failureBackoff = 500 * time.Millisecond
	rpcTimeout     = 2 * time.Second
	// missRetries caps refills per request when peers keep draining them;
	// it only guards against livelock.
	missRetries = 64
)

// Lease enforces a global quota shared by every edge. Each edge spends tokens
// leased from the hub out of process memory and renews in the background
// before it runs dry, so a decision costs a few atomic operations and the hub
// handles one RPC per lease. When the hub has no capacity left it answers
// WAITING and the edge stops asking until the retry time.
type Lease struct {
	client   atomic.Pointer[LeaseClient]
	opts     LeaseOptions
	counters table[leaseCounter]
	flights  singleflight.Group
	// backoffUntil (unix ms) short-circuits RPCs after a failure, so a hub
	// outage costs one RPC per backoff interval rather than one per request.
	backoffUntil atomic.Int64
}

type leaseCounter struct {
	window
	tokens       atomic.Int64 // leased tokens held by this edge
	expiresAt    atomic.Int64 // unix ms; end of the current lease
	hubRemaining atomic.Int64 // global quota left as of the last grant
	retryAt      atomic.Int64 // unix ms; non-zero while the hub says WAITING
	leaseSize    atomic.Int64
	renewing     atomic.Bool
	key          string
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
	l := &Lease{opts: opts}
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
	c := l.counter(req)
	end := l.roll(c, req.Window, now)
	d := Decision{Limit: req.Limit, ResetAt: end}

	// While WAITING the hub has nothing to give, so do not ask before the
	// retry time; tokens already held stay spendable.
	waiting := false
	if at := c.retryAt.Load(); at > 0 {
		waiting = now.UnixMilli() < at
		if !waiting {
			c.retryAt.CompareAndSwap(at, 0)
		}
	}
	// One edge can never legitimately admit more than the whole quota in a
	// window, whatever the hub grants (e.g. right after a quota cut).
	if req.Window > 0 && c.consumed.Load()+req.Cost > req.Limit {
		return d, nil
	}

	// On a miss, fetch a lease (one RPC per key however many callers miss
	// together) and retry: peers may drain a refill before this caller gets
	// to it, so only the hub ends the loop with a denial.
	left := c.take(req.Cost, now)
	for i := 0; left < 0 && !waiting && i < missRetries; i++ {
		granted, err := l.fetch(ctx, req, c, end)
		if err != nil {
			return d, err
		}
		if granted <= 0 {
			break
		}
		left = c.take(req.Cost, time.Now())
	}
	if left < 0 {
		return d, nil
	}
	c.consumed.Add(req.Cost)

	// Renew in the background at the low-water mark, or shortly before a
	// lease that ends inside the window expires (renewing a lease that ends
	// with its window would only fetch tokens about to be dropped).
	lowWater := int64(float64(c.leaseSize.Load()) * l.opts.LowWaterPct / 100)
	exp := c.expiresAt.Load()
	endsEarly := end.IsZero() || exp < end.UnixMilli()
	if left <= lowWater || (endsEarly && exp > 0 && now.UnixMilli() > exp-l.opts.RenewBefore.Milliseconds()) {
		l.renew(req, c)
	}

	d.Allowed = true
	d.Remaining = max(0, min(req.Limit, c.hubRemaining.Load()+left))
	return d, nil
}

// Holding reports the unexpired tokens this edge holds for a key and when
// its lease expires (for tests and debugging).
func (l *Lease) Holding(scope, service, key string) (tokens int64, expires time.Time) {
	v, ok := l.counters.m.Load(stateKey(Request{Scope: scope, Service: service, Key: key}))
	if !ok {
		return 0, time.Time{}
	}
	c := v.(*leaseCounter)
	exp := c.expiresAt.Load()
	if exp > 0 && time.Now().UnixMilli() >= exp {
		return 0, time.UnixMilli(exp)
	}
	return c.tokens.Load(), time.UnixMilli(exp)
}

// Run evicts idle keys until ctx is done.
func (l *Lease) Run(ctx context.Context) {
	sweep(ctx, func(cutoff int64) {
		l.counters.evict(func(c *leaseCounter) bool {
			return c.lastSeen.Load() < cutoff && !c.renewing.Load()
		})
	})
}

func (l *Lease) counter(req Request) *leaseCounter {
	c := l.counters.get(stateKey(req), func() *leaseCounter {
		c := &leaseCounter{key: stateKey(req)}
		c.leaseSize.Store(l.opts.LeaseSize)
		return c
	})
	if req.LeaseSize > 0 {
		c.leaseSize.Store(req.LeaseSize)
	}
	return c
}

// roll drops leftover tokens when a new window starts so they cannot inflate
// it; the hub has already started counting the new window from zero.
func (l *Lease) roll(c *leaseCounter, period time.Duration, now time.Time) time.Time {
	end, rolled := c.roll(period, now)
	if rolled {
		c.tokens.Store(0)
		c.expiresAt.Store(0)
		c.hubRemaining.Store(0)
		c.retryAt.Store(0)
	}
	return end
}

// take spends cost tokens and returns the balance left, or -1 when the edge
// holds too few (or its lease has expired).
func (c *leaseCounter) take(cost int64, now time.Time) int64 {
	if exp := c.expiresAt.Load(); exp > 0 && now.UnixMilli() >= exp {
		c.tokens.Store(0)
		return -1
	}
	for {
		cur := c.tokens.Load()
		if cur < cost {
			return -1
		}
		if c.tokens.CompareAndSwap(cur, cur-cost) {
			return cur - cost
		}
	}
}

// renew tops up a key's lease in the background, at most once at a time.
func (l *Lease) renew(req Request, c *leaseCounter) {
	if c.retryAt.Load() > 0 || !c.renewing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.renewing.Store(false)
		end := l.roll(c, req.Window, time.Now())
		_, _ = l.fetch(context.Background(), req, c, end)
	}()
}

// fetch requests a lease and adds the grant to the key's balance. Callers for
// the same key share one RPC, which runs detached from any single caller so a
// disconnecting client cannot fail its peers. It returns the tokens granted.
func (l *Lease) fetch(ctx context.Context, req Request, c *leaseCounter, windowEnd time.Time) (int64, error) {
	if time.Now().UnixMilli() < l.backoffUntil.Load() {
		return 0, ErrUnavailable
	}
	ch := l.flights.DoChan(c.key, func() (any, error) {
		rpcCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rpcTimeout)
		defer cancel()
		resp, err := (*l.client.Load()).RequestQuotaLease(rpcCtx, &types.QuotaLeaseRequest{
			Prefix:          req.Scope,
			ServiceId:       req.Service,
			ApiKey:          req.Key,
			TierId:          req.Tier,
			RequestedTokens: c.leaseSize.Load(),
		})
		if err != nil {
			err = classify(err)
			if errors.Is(err, ErrUnavailable) {
				l.backoffUntil.Store(time.Now().Add(failureBackoff).UnixMilli())
			}
			return int64(0), err
		}
		c.hubRemaining.Store(resp.RemainingGlobal)
		if resp.WaitingForCapacity {
			// Jitter the retry so edges do not return to the hub in lockstep.
			retry := resp.RetryAfterMs
			if retry <= 0 {
				retry = 500
			}
			c.retryAt.Store(time.Now().UnixMilli() + retry/2 + rand.Int64N(retry))
			return int64(0), nil
		}
		if resp.GrantedTokens > 0 {
			// Tokens live for the lease TTL and never past the end of the
			// window they were granted for.
			ttl := time.Duration(resp.LeaseTtlSeconds) * time.Second
			if ttl <= 0 {
				ttl = l.opts.LeaseTTL
			}
			exp := time.Now().Add(ttl)
			if !windowEnd.IsZero() && windowEnd.Before(exp) {
				exp = windowEnd
			}
			// Extend the lease before adding tokens so a concurrent take
			// cannot expire the new grant.
			c.expiresAt.Store(exp.UnixMilli())
			c.tokens.Add(resp.GrantedTokens)
		}
		return resp.GrantedTokens, nil
	})
	select {
	case res := <-ch:
		return res.Val.(int64), res.Err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func classify(err error) error {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied:
		return fmt.Errorf("%w: %v", ErrRejected, err)
	}
	return fmt.Errorf("%w: %v", ErrUnavailable, err)
}
