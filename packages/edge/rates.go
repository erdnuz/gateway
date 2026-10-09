package edge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// leaseState tracks the current lifecycle stage of a lease for a given key.
type leaseState int32

const (
	leaseStateIdle     leaseState = 0 // no active renewal in-flight
	leaseStateRenewing leaseState = 1 // async renewal request is in-flight to Hub
	leaseStateWaiting  leaseState = 2 // Hub returned 0 tokens; Edge must back off
)

// consumeLeaseTokensLua atomically decrements the local token bucket.
// Returns the remaining balance after the decrement, or -1 if insufficient.
var consumeLeaseTokensLua = redis.NewScript(`
local key = KEYS[1]
local amount = tonumber(ARGV[1])

local current = tonumber(redis.call("GET", key) or "0")
if current < amount then
  return -1
end

return tonumber(redis.call("DECRBY", key, amount))
`)

// addLeaseTokensLua atomically increments the local token bucket and sets its
// TTL in milliseconds (PEXPIRE). This implements the "lease merging / top-up"
// rule from the Distributed Leasing Strategy specification.
var addLeaseTokensLua = redis.NewScript(`
local key = KEYS[1]
local amount = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])

local updated = redis.call("INCRBY", key, amount)
if ttl_ms > 0 then
  redis.call("PEXPIRE", key, ttl_ms)
end

return tonumber(updated)
`)

// ErrLeaseUnavailable is returned while the edge is backing off after a failed
// lease RPC, so a hub outage costs one RPC per backoff interval instead of one
// per request.
var ErrLeaseUnavailable = errors.New("quota lease service unavailable")

// ErrLeaseRejected means the hub refused the key (unknown key, tier, or
// service). It is a policy decision and must not trigger safety fallback.
var ErrLeaseRejected = errors.New("quota lease rejected")

func classifyLeaseError(err error) error {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied:
		return fmt.Errorf("%w: %v", ErrLeaseRejected, err)
	}
	return fmt.Errorf("%w: %v", ErrLeaseUnavailable, err)
}

const (
	edgeLeaseRedisTimeout = 150 * time.Millisecond
	leaseFailureBackoff   = 500 * time.Millisecond
	leaseRPCTimeout       = 2 * time.Second
	leaseMissRetries      = 64
	counterSweepInterval  = 30 * time.Second
	counterMinIdle        = time.Minute
)

type leaseCounter struct {
	remaining       atomic.Int64 // local tokens (in-memory store only)
	consumed        atomic.Int64 // admitted cost in the current window on this edge
	reserveUsed     atomic.Int64
	hubRemaining    atomic.Int64 // hub's global remaining as of the last grant
	windowID        atomic.Int64 // quota window the counter currently accounts for
	expiresAtUnixMs atomic.Int64 // end of the current lease (millisecond precision)
	lastSeenUnixMs  atomic.Int64
	state           atomic.Int32 // leaseState enum: idle / renewing / waiting
	renewalQueued   atomic.Int32 // 1 while a background renewal request is queued or in-flight
	retryAfterMs    atomic.Int64 // populated from Hub WAITING response
	nextRetryUnixMs atomic.Int64 // when WAITING, earliest time to retry
	leaseSize       atomic.Int64
	localTokenKey   string
}

// RateRequest describes one admission decision.
type RateRequest struct {
	Prefix  string
	Service string
	APIKey  string
	Limit   int64
	Cost    int64
	// Period is the prefix quota period. Counters reset when the epoch-aligned
	// window (unix / period, the same alignment the hub uses) rolls over.
	// Zero means a single unbounded window.
	Period time.Duration
	// LeaseSize overrides the manager default lease quantum when > 0.
	LeaseSize int64
}

// RateDecision is the outcome of RateManager.Allow.
type RateDecision struct {
	Allowed   bool
	Limit     int64
	Remaining int64     // best-effort quota left in the window across all edges
	ResetAt   time.Time // zero when the request has no period
}

type RateManager struct {
	leaseClient   QuotaLeaseRequester
	leaseSize     int64
	lowWaterPct   float64
	minTokens     int64
	leaseTTL      time.Duration // lease lifetime when the request has no period
	renewalBuffer time.Duration // look-ahead window before expiry
	rdb           *redis.Client
	leaseMap      sync.Map // "prefix\x00service\x00apiKey" -> *leaseCounter
	safetyMap     sync.Map // same key -> *leaseCounter (only consumed/windowID used)
	refillGroup   singleflight.Group
	leaseManager  *LeaseManager
	// leaseBackoffUntilMs short-circuits lease RPCs after a failure.
	leaseBackoffUntilMs atomic.Int64
}

func withLeaseRedisTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.WithTimeout(context.Background(), edgeLeaseRedisTimeout)
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, edgeLeaseRedisTimeout)
}

type RateManagerOptions struct {
	LeaseSize     int64
	LowWaterPct   float64 // percentage 0–100; 0 → policy default (20%)
	MinTokens     int64
	LeaseTTL      time.Duration // lease lifetime for period-less requests; 0 → 60s
	RenewalBuffer time.Duration // look-ahead window before expiry; 0 → 2s
}

func DefaultRateManagerOptions() RateManagerOptions {
	edgeDefaults := types.DefaultRuntimePolicy().Edge
	return RateManagerOptions{
		LeaseSize:   edgeDefaults.RateLeaseSize,
		LowWaterPct: edgeDefaults.RateLowWaterPct,
		MinTokens:   1,
	}
}

func NewRateManager(counterStore *redis.Client) *RateManager {
	return NewRateManagerWithOptions(counterStore, DefaultRateManagerOptions())
}

// NewRateManagerWithOptions builds an edge rate manager. counterStore holds
// local lease tokens in Redis; pass nil to keep them in process memory, which
// is faster and correct whenever each edge owns its Redis.
func NewRateManagerWithOptions(counterStore *redis.Client, options RateManagerOptions) *RateManager {
	edgeDefaults := types.DefaultRuntimePolicy().Edge
	if options.LeaseSize <= 0 {
		options.LeaseSize = edgeDefaults.RateLeaseSize
	}
	if options.LowWaterPct <= 0 || options.LowWaterPct > 100 {
		options.LowWaterPct = edgeDefaults.RateLowWaterPct
	}
	if options.MinTokens < 0 {
		options.MinTokens = 0
	}
	if options.LeaseTTL <= 0 {
		options.LeaseTTL = 60 * time.Second
	}
	if options.RenewalBuffer <= 0 {
		options.RenewalBuffer = 2 * time.Second
	}
	return &RateManager{
		leaseSize:     options.LeaseSize,
		lowWaterPct:   options.LowWaterPct,
		minTokens:     options.MinTokens,
		leaseTTL:      options.LeaseTTL,
		renewalBuffer: options.RenewalBuffer,
		rdb:           counterStore,
	}
}

func (rm *RateManager) SetLeaseClient(client QuotaLeaseRequester) {
	rm.leaseClient = client
}

func (rm *RateManager) StartBackgroundWorkers(ctx context.Context) {
	if rm.leaseManager == nil {
		rm.leaseManager = NewLeaseManager(rm)
	}
	rm.leaseManager.Start(ctx)
	go rm.sweepIdleCounters(ctx)
}

// Allow admits or denies one request against the key's leased quota.
func (rm *RateManager) Allow(ctx context.Context, req RateRequest) (RateDecision, error) {
	if req.Cost <= 0 {
		req.Cost = 1
	}
	now := time.Now()
	counter := rm.getLeaseCounter(req.Prefix, req.Service, req.APIKey)
	counter.lastSeenUnixMs.Store(now.UnixMilli())
	if req.LeaseSize > 0 {
		counter.leaseSize.Store(req.LeaseSize)
	}
	windowEnd := rm.rollWindow(ctx, counter, req.Period, now)

	decision := RateDecision{Limit: req.Limit, ResetAt: windowEnd}

	if rm.leaseClient == nil {
		used := counter.consumed.Add(req.Cost)
		if used > req.Limit {
			counter.consumed.Add(-req.Cost)
			decision.Remaining = max(0, req.Limit-counter.consumed.Load())
			return decision, nil
		}
		decision.Allowed = true
		decision.Remaining = req.Limit - used
		return decision, nil
	}

	// WAITING: the hub reported zero global quota, so do not ask it again
	// before the retry deadline (then poll in the background). Tokens this
	// edge already holds remain spendable.
	waiting := leaseState(counter.state.Load()) == leaseStateWaiting
	if waiting && now.UnixMilli() >= counter.nextRetryUnixMs.Load() &&
		counter.state.CompareAndSwap(int32(leaseStateWaiting), int32(leaseStateIdle)) {
		rm.enqueueLeaseRenewal(req, counter)
	}

	// Local guard: one edge can never legitimately admit more than the whole
	// quota in a window, whatever the hub grants (e.g. after a quota cut).
	if req.Period > 0 && counter.consumed.Load()+req.Cost > req.Limit {
		return decision, nil
	}

	remaining, err := rm.consumeLocalTokens(ctx, counter, req.Cost)
	if err != nil {
		return decision, err
	}
	if remaining < 0 && waiting {
		return decision, nil
	}
	// Miss: fetch a lease synchronously (collapsed per key) and retry. Peers
	// can drain a refill before this caller consumes, so a miss after a
	// successful grant is retried rather than reported as "over quota"; only
	// the hub (WAITING or an empty grant) ends the loop with a denial. The
	// attempt cap only guards against livelock.
	for attempt := 0; remaining < 0 && attempt < leaseMissRetries; attempt++ {
		granted, err := rm.fetchLease(ctx, req, counter, windowEnd)
		if err != nil {
			return decision, err
		}
		if granted <= 0 || leaseState(counter.state.Load()) == leaseStateWaiting {
			break
		}
		if remaining, err = rm.consumeLocalTokens(ctx, counter, req.Cost); err != nil {
			return decision, err
		}
	}
	if remaining < 0 {
		if !rm.tryConsumeReserve(counter, req.Cost) {
			return decision, nil
		}
		remaining = 0
		rm.enqueueLeaseRenewal(req, counter)
	}
	counter.consumed.Add(req.Cost)

	// Volume trigger (low-water mark) and temporal trigger (lease near expiry).
	// A lease that expires with its window is not renewed early: the renewal
	// would only fetch tokens that are about to be dropped.
	lowWater := int64(float64(counter.leaseSize.Load()) * rm.lowWaterPct / 100.0)
	endsBeforeWindow := windowEnd.IsZero() || counter.expiresAtUnixMs.Load() < windowEnd.UnixMilli()
	if remaining <= lowWater || (endsBeforeWindow && rm.needsTemporalRenewal(counter, now)) {
		rm.enqueueLeaseRenewal(req, counter)
	}

	decision.Allowed = true
	decision.Remaining = max(0, min(req.Limit, counter.hubRemaining.Load()+remaining))
	return decision, nil
}

// AllowSafety enforces the service's safety tier locally (no hub involved)
// with the same epoch-aligned window as normal quotas.
func (rm *RateManager) AllowSafety(req RateRequest) RateDecision {
	if req.Cost <= 0 {
		req.Cost = 1
	}
	now := time.Now()
	key := counterKey(req.Prefix, req.Service, req.APIKey)
	v, ok := rm.safetyMap.Load(key)
	if !ok {
		v, _ = rm.safetyMap.LoadOrStore(key, &leaseCounter{})
	}
	counter := v.(*leaseCounter)
	counter.lastSeenUnixMs.Store(now.UnixMilli())
	windowEnd := rm.rollWindow(context.Background(), counter, req.Period, now)
	decision := RateDecision{Limit: req.Limit, ResetAt: windowEnd}
	used := counter.consumed.Add(req.Cost)
	if used > req.Limit {
		counter.consumed.Add(-req.Cost)
		return decision
	}
	decision.Allowed = true
	decision.Remaining = req.Limit - used
	return decision
}

// rollWindow resets per-window state when the epoch-aligned quota window has
// advanced, dropping leftover lease tokens so they cannot inflate the next
// window. It returns the end of the current window (zero when period <= 0).
func (rm *RateManager) rollWindow(ctx context.Context, counter *leaseCounter, period time.Duration, now time.Time) time.Time {
	periodSec := int64(period / time.Second)
	if periodSec <= 0 {
		return time.Time{}
	}
	wid := now.Unix() / periodSec
	windowEnd := time.Unix((wid+1)*periodSec, 0)
	prev := counter.windowID.Load()
	if prev == wid || !counter.windowID.CompareAndSwap(prev, wid) {
		return windowEnd
	}
	counter.consumed.Store(0)
	counter.reserveUsed.Store(0)
	counter.hubRemaining.Store(0)
	counter.expiresAtUnixMs.Store(0)
	counter.nextRetryUnixMs.Store(0)
	counter.state.CompareAndSwap(int32(leaseStateWaiting), int32(leaseStateIdle))
	if prev != 0 && counter.localTokenKey != "" {
		rm.dropLocalTokens(ctx, counter)
	}
	return windowEnd
}

func (rm *RateManager) enqueueLeaseRenewal(req RateRequest, counter *leaseCounter) {
	if leaseState(counter.state.Load()) != leaseStateIdle {
		return
	}
	if !counter.renewalQueued.CompareAndSwap(0, 1) {
		return
	}
	if rm.leaseManager == nil {
		go func() {
			defer counter.renewalQueued.Store(0)
			rm.renewNow(context.Background(), req, counter)
		}()
		return
	}
	if !rm.leaseManager.Enqueue(req, counter) {
		counter.renewalQueued.Store(0)
	}
}

// renewNow performs a proactive (non-blocking path) top-up.
func (rm *RateManager) renewNow(ctx context.Context, req RateRequest, counter *leaseCounter) {
	if !counter.state.CompareAndSwap(int32(leaseStateIdle), int32(leaseStateRenewing)) {
		return
	}
	defer counter.state.CompareAndSwap(int32(leaseStateRenewing), int32(leaseStateIdle))
	windowEnd := rm.rollWindow(ctx, counter, req.Period, time.Now())
	_, _ = rm.fetchLease(ctx, req, counter, windowEnd)
}

// fetchLease requests tokens from the hub. Concurrent callers for the same key
// share one RPC, so a burst of misses costs one RPC per drained lease; the RPC
// runs detached from any single caller's context so a disconnecting client
// cannot fail its peers.
// It returns the number of tokens the (possibly shared) RPC granted.
func (rm *RateManager) fetchLease(ctx context.Context, req RateRequest, counter *leaseCounter, windowEnd time.Time) (int64, error) {
	if rm.leaseClient == nil {
		return 0, fmt.Errorf("lease client is not configured")
	}
	if time.Now().UnixMilli() < rm.leaseBackoffUntilMs.Load() {
		return 0, ErrLeaseUnavailable
	}
	ch := rm.refillGroup.DoChan(counter.localTokenKey, func() (interface{}, error) {
		rpcCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseRPCTimeout)
		defer cancel()
		leaseSize := counter.leaseSize.Load()
		resp, err := rm.leaseClient.RequestQuotaLease(rpcCtx, &types.QuotaLeaseRequest{
			Prefix:          req.Prefix,
			ServiceId:       req.Service,
			ApiKey:          req.APIKey,
			RequestedTokens: leaseSize,
		})
		if err != nil {
			err = classifyLeaseError(err)
			if errors.Is(err, ErrLeaseUnavailable) {
				rm.leaseBackoffUntilMs.Store(time.Now().Add(leaseFailureBackoff).UnixMilli())
			}
			return nil, err
		}
		counter.hubRemaining.Store(resp.RemainingGlobal)
		if resp.WaitingForCapacity {
			retryMs := resp.RetryAfterMs
			if retryMs <= 0 {
				retryMs = 500
			}
			counter.retryAfterMs.Store(retryMs)
			counter.nextRetryUnixMs.Store(time.Now().UnixMilli() + retryMs)
			counter.state.Store(int32(leaseStateWaiting))
			return int64(0), nil
		}
		if resp.GrantedTokens > 0 {
			// Tokens live for the hub-provided (or default) lease TTL, and never
			// past the end of the quota window they were granted for.
			ttl := time.Duration(resp.LeaseTtlSeconds) * time.Second
			if ttl <= 0 {
				ttl = rm.leaseTTL
			}
			expiresAt := time.Now().Add(ttl)
			if !windowEnd.IsZero() && windowEnd.Before(expiresAt) {
				expiresAt = windowEnd
			}
			if err := rm.addLocalTokens(rpcCtx, counter, resp.GrantedTokens, time.Until(expiresAt)); err != nil {
				return nil, err
			}
			counter.reserveUsed.Store(0)
			counter.expiresAtUnixMs.Store(expiresAt.UnixMilli())
		}
		return resp.GrantedTokens, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return 0, res.Err
		}
		return res.Val.(int64), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (rm *RateManager) consumeLocalTokens(ctx context.Context, counter *leaseCounter, amount int64) (int64, error) {
	if rm.rdb == nil {
		// Lease expiry is enforced here for the in-memory store; Redis enforces
		// it via PEXPIRE.
		if exp := counter.expiresAtUnixMs.Load(); exp > 0 && time.Now().UnixMilli() >= exp {
			counter.remaining.Store(0)
			return -1, nil
		}
		for {
			cur := counter.remaining.Load()
			if cur < amount {
				return -1, nil
			}
			if counter.remaining.CompareAndSwap(cur, cur-amount) {
				return cur - amount, nil
			}
		}
	}

	redisCtx, cancel := withLeaseRedisTimeout(ctx)
	defer cancel()
	remaining, err := consumeLeaseTokensLua.Run(redisCtx, rm.rdb, []string{counter.localTokenKey}, amount).Int64()
	if err == redis.Nil {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	return remaining, nil
}

func (rm *RateManager) addLocalTokens(ctx context.Context, counter *leaseCounter, amount int64, ttl time.Duration) error {
	if amount <= 0 {
		return nil
	}
	if rm.rdb == nil {
		counter.remaining.Add(amount)
		return nil
	}
	if ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	redisCtx, cancel := withLeaseRedisTimeout(ctx)
	defer cancel()
	_, err := addLeaseTokensLua.Run(redisCtx, rm.rdb, []string{counter.localTokenKey}, amount, ttl.Milliseconds()).Int64()
	return err
}

func (rm *RateManager) dropLocalTokens(ctx context.Context, counter *leaseCounter) {
	counter.remaining.Store(0)
	if rm.rdb == nil {
		return
	}
	redisCtx, cancel := withLeaseRedisTimeout(ctx)
	defer cancel()
	_ = rm.rdb.Del(redisCtx, counter.localTokenKey).Err()
}

func (rm *RateManager) tryConsumeReserve(counter *leaseCounter, amount int64) bool {
	if rm.minTokens <= 0 || amount <= 0 {
		return false
	}
	expiresAtMs := counter.expiresAtUnixMs.Load()
	if expiresAtMs <= 0 || time.Now().UnixMilli() >= expiresAtMs {
		return false
	}
	for {
		used := counter.reserveUsed.Load()
		next := used + amount
		if next > rm.minTokens {
			return false
		}
		if counter.reserveUsed.CompareAndSwap(used, next) {
			return true
		}
	}
}

// needsTemporalRenewal returns true when the current lease expires within
// renewalBuffer — the temporal trigger from the Dual-Trigger Renewal Protocol.
func (rm *RateManager) needsTemporalRenewal(counter *leaseCounter, now time.Time) bool {
	expiresAtMs := counter.expiresAtUnixMs.Load()
	if expiresAtMs <= 0 {
		return false
	}
	return now.UnixMilli() > expiresAtMs-rm.renewalBuffer.Milliseconds()
}

func counterKey(prefix, service, apiKey string) string {
	return prefix + "\x00" + service + "\x00" + apiKey
}

func (rm *RateManager) getLeaseCounter(prefix, service, apiKey string) *leaseCounter {
	key := counterKey(prefix, service, apiKey)
	if v, ok := rm.leaseMap.Load(key); ok {
		return v.(*leaseCounter)
	}
	counter := &leaseCounter{
		localTokenKey: "edge:lease:tokens:" + prefix + ":" + service + ":" + apiKey,
	}
	counter.leaseSize.Store(rm.leaseSize)
	actual, _ := rm.leaseMap.LoadOrStore(key, counter)
	return actual.(*leaseCounter)
}

// sweepIdleCounters evicts counters for keys that have gone quiet so the maps
// stay proportional to the active key set.
func (rm *RateManager) sweepIdleCounters(ctx context.Context) {
	t := time.NewTicker(counterSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rm.evictIdle(time.Now().Add(-counterMinIdle))
		}
	}
}

func (rm *RateManager) evictIdle(cutoff time.Time) {
	cut := cutoff.UnixMilli()
	for _, m := range []*sync.Map{&rm.leaseMap, &rm.safetyMap} {
		m.Range(func(k, v any) bool {
			c := v.(*leaseCounter)
			if c.lastSeenUnixMs.Load() < cut && c.renewalQueued.Load() == 0 &&
				leaseState(c.state.Load()) != leaseStateRenewing {
				m.Delete(k)
			}
			return true
		})
	}
}
