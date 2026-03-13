package edge

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
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

local remaining = redis.call("DECRBY", key, amount)
if tonumber(remaining) < 0 then
  redis.call("INCRBY", key, amount)
  return -1
end

return tonumber(remaining)
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

type leaseCounter struct {
	remaining       atomic.Int64
	consumed        atomic.Int64
	reserveUsed     atomic.Int64
	expiresAtUnixMs atomic.Int64 // millisecond precision (was expiresAtUnix)
	state           atomic.Int32 // leaseState enum: idle / renewing / waiting
	renewalQueued   atomic.Int32 // 1 while a background renewal request is queued or in-flight
	retryAfterMs    atomic.Int64 // populated from Hub WAITING response
	nextRetryUnixMs atomic.Int64 // when WAITING, earliest time to retry
	leaseSize       int64
	localTokenKey   string
	renewalBuffer   time.Duration // look-ahead window before expiry that triggers temporal renewal
}

type RateManager struct {
	leaseClient   QuotaLeaseRequester
	leaseSize     int64
	lowWaterPct   float64
	minTokens     int64
	leaseTTL      time.Duration // TTL for the Redis local token key
	renewalBuffer time.Duration // look-ahead window before expiry
	rdb           *redis.Client
	leaseMap      sync.Map
	refillGroup   singleflightGroup
	leaseManager  *LeaseManager
}

const edgeLeaseRedisTimeout = 150 * time.Millisecond

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
	HardThresholdPct float64
	LeaseSize        int64
	LowWaterPct      float64 // percentage 0–100; 0 → policy default (20%)
	MinTokens        int64
	LeaseTTL         time.Duration // Redis key TTL; 0 → 60s default
	RenewalBuffer    time.Duration // look-ahead window before expiry; 0 → 2s default
}

func DefaultRateManagerOptions() RateManagerOptions {
	edgeDefaults := types.DefaultRuntimePolicy().Edge
	return RateManagerOptions{
		HardThresholdPct: edgeDefaults.RateHardThresholdPct,
		LeaseSize:        edgeDefaults.RateLeaseSize,
		LowWaterPct:      edgeDefaults.RateLowWaterPct,
		MinTokens:        1,
	}
}

var _ types.Limiter = (*RateManager)(nil)

func NewRateManager(_ string, counterStore interface{}, maxDelta int64, channels ...string) *RateManager {
	return NewRateManagerWithOptions("", counterStore, maxDelta, DefaultRateManagerOptions(), channels...)
}

func NewRateManagerWithOptions(_ string, counterStore interface{}, maxDelta int64, options RateManagerOptions, _ ...string) *RateManager {
	edgeDefaults := types.DefaultRuntimePolicy().Edge
	if options.HardThresholdPct <= 0 || options.HardThresholdPct > 1 {
		options.HardThresholdPct = edgeDefaults.RateHardThresholdPct
	}
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
	var rdb *redis.Client
	if c, ok := counterStore.(*redis.Client); ok {
		rdb = c
	}
	return &RateManager{
		// maxDelta/hard-threshold are currently enforced by lease semantics,
		// while options are preserved for future policy expansion.
		leaseSize:     options.LeaseSize,
		lowWaterPct:   options.LowWaterPct,
		minTokens:     options.MinTokens,
		leaseTTL:      options.LeaseTTL,
		renewalBuffer: options.RenewalBuffer,
		rdb:           rdb,
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
}

func (rm *RateManager) enqueueLeaseRenewal(prefix, serviceID, apiKey string, counter *leaseCounter, minimum int64) {
	if !counter.renewalQueued.CompareAndSwap(0, 1) {
		return
	}
	if rm.leaseManager == nil {
		if counter.state.CompareAndSwap(int32(leaseStateIdle), int32(leaseStateRenewing)) {
			go func() {
				defer counter.renewalQueued.Store(0)
				defer counter.state.CompareAndSwap(int32(leaseStateRenewing), int32(leaseStateIdle))
				_ = rm.ensureLease(context.Background(), prefix, serviceID, apiKey, counter, minimum)
			}()
			return
		}
		counter.renewalQueued.Store(0)
		return
	}
	if !rm.leaseManager.Enqueue(prefix, serviceID, apiKey, counter, minimum) {
		counter.renewalQueued.Store(0)
	}
}

func (rm *RateManager) Increment(ctx context.Context, prefix, apiKey string, limit, amount int64) (int64, error) {
	return rm.IncrementWithService(ctx, prefix, "", apiKey, limit, amount)
}

func (rm *RateManager) IncrementWithService(ctx context.Context, prefix, serviceID, apiKey string, limit, amount int64) (int64, error) {
	if amount <= 0 {
		amount = 1
	}
	counter := rm.getLeaseCounter(prefix, apiKey)
	if rm.leaseClient == nil {
		used := counter.consumed.Add(amount)
		return used, nil
	}

	// WAITING short-circuit: Hub reported zero global quota; return 429 until
	// the retry deadline has elapsed and a background poll succeeds.
	if leaseState(counter.state.Load()) == leaseStateWaiting {
		if time.Now().UnixMilli() < counter.nextRetryUnixMs.Load() {
			return limit + 1, nil
		}
		// Retry window elapsed — enqueue non-blocking background renewal.
		if counter.state.CompareAndSwap(int32(leaseStateWaiting), int32(leaseStateIdle)) {
			rm.enqueueLeaseRenewal(prefix, serviceID, apiKey, counter, 0)
		}
		return limit + 1, nil
	}

	if err := rm.ensureLease(ctx, prefix, serviceID, apiKey, counter, amount); err != nil {
		return 0, err
	}

	// Temporal trigger: fire an async renewal when we're within renewalBuffer
	// of the current lease expiry (independent of token balance).
	if rm.needsTemporalRenewal(counter) {
		if leaseState(counter.state.Load()) == leaseStateIdle {
			rm.enqueueLeaseRenewal(prefix, serviceID, apiKey, counter, 0)
		}
	}

	remaining, err := rm.consumeLocalTokens(ctx, counter, amount)
	if err != nil {
		return 0, err
	}
	if remaining < 0 {
		if err := rm.ensureLease(ctx, prefix, serviceID, apiKey, counter, amount); err != nil {
			return 0, err
		}
		remaining, err = rm.consumeLocalTokens(ctx, counter, amount)
		if err != nil {
			return 0, err
		}
		if remaining < 0 {
			if rm.tryConsumeReserve(counter, amount) {
				used := counter.consumed.Add(amount)
				if leaseState(counter.state.Load()) == leaseStateIdle {
					rm.enqueueLeaseRenewal(prefix, serviceID, apiKey, counter, 0)
				}
				if used > limit {
					return used, nil
				}
				return used, nil
			}
			return limit + 1, nil
		}
	}
	used := counter.consumed.Add(amount)

	// Volume trigger: fire async renewal when below low-water mark.
	if remaining <= int64(float64(counter.leaseSize)*rm.lowWaterPct/100.0) {
		if leaseState(counter.state.Load()) == leaseStateIdle {
			rm.enqueueLeaseRenewal(prefix, serviceID, apiKey, counter, 0)
		}
	}
	if used > limit {
		return used, nil
	}
	return used, nil
}

func (rm *RateManager) IncrementSafety(_ context.Context, prefix, apiKey string, limit, amount int64) (int64, error) {
	key := "safety:" + prefix
	counter := rm.getLeaseCounter(key, apiKey)
	if counter.remaining.Load() == 0 {
		counter.remaining.Store(limit)
	}
	remaining := counter.remaining.Add(-amount)
	if remaining < 0 {
		counter.remaining.Add(amount)
		return limit + 1, nil
	}
	used := counter.consumed.Add(amount)
	return used, nil
}

func (rm *RateManager) ensureLease(ctx context.Context, prefix, serviceID, apiKey string, counter *leaseCounter, minimum int64) error {
	// minimum == 0 is a sentinel meaning "top-up proactively" (temporal / LWM async
	// triggers). Positive minimum = synchronous hot-path gate.
	if minimum > 0 && rm.hasMinimumLocalTokens(ctx, counter, minimum) {
		return nil
	}
	if rm.leaseClient == nil {
		return fmt.Errorf("lease client is not configured")
	}
	_, err, _ := rm.refillGroup.Do(prefix+":"+serviceID+":"+apiKey, func() (interface{}, error) {
		if minimum > 0 && rm.hasMinimumLocalTokens(ctx, counter, minimum) {
			return nil, nil
		}
		resp, err := rm.leaseClient.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{
			Prefix:          prefix,
			ServiceId:       serviceID,
			ApiKey:          apiKey,
			RequestedTokens: rm.leaseSize,
		})
		if err != nil {
			counter.state.CompareAndSwap(int32(leaseStateRenewing), int32(leaseStateIdle))
			return nil, err
		}
		if resp.WaitingForCapacity {
			// Hub has zero global quota — enter WAITING state.
			retryMs := resp.RetryAfterMs
			if retryMs <= 0 {
				retryMs = 500
			}
			counter.retryAfterMs.Store(retryMs)
			counter.nextRetryUnixMs.Store(time.Now().UnixMilli() + retryMs)
			counter.state.Store(int32(leaseStateWaiting))
			return nil, nil
		}
		// Reset state to IDLE on a successful (possibly partial) grant.
		counter.state.CompareAndSwap(int32(leaseStateRenewing), int32(leaseStateIdle))
		if resp.GrantedTokens > 0 {
			ttl := time.Duration(resp.LeaseTtlSeconds) * time.Second
			if ttl <= 0 {
				ttl = rm.leaseTTL
			}
			if err := rm.addLocalTokens(ctx, counter, resp.GrantedTokens, ttl); err != nil {
				return nil, err
			}
			counter.reserveUsed.Store(0)
			counter.expiresAtUnixMs.Store(time.Now().Add(ttl).UnixMilli())
		}
		return nil, nil
	})
	return err
}

func (rm *RateManager) hasMinimumLocalTokens(ctx context.Context, counter *leaseCounter, minimum int64) bool {
	if minimum <= 0 {
		return true
	}
	if rm.rdb == nil {
		return counter.remaining.Load() >= minimum
	}
	redisCtx, cancel := withLeaseRedisTimeout(ctx)
	defer cancel()
	remaining, err := rm.rdb.Get(redisCtx, counter.localTokenKey).Int64()
	if err == redis.Nil {
		return false
	}
	if err != nil {
		return false
	}
	return remaining >= minimum
}

func (rm *RateManager) consumeLocalTokens(ctx context.Context, counter *leaseCounter, amount int64) (int64, error) {
	if amount <= 0 {
		amount = 1
	}
	if rm.rdb == nil {
		remaining := counter.remaining.Add(-amount)
		if remaining < 0 {
			counter.remaining.Add(amount)
			return -1, nil
		}
		return remaining, nil
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

func (rm *RateManager) addLocalTokens(ctx context.Context, counter *leaseCounter, amount int64, leaseTTL time.Duration) error {
	if amount <= 0 {
		return nil
	}
	if rm.rdb == nil {
		counter.remaining.Add(amount)
		return nil
	}

	redisCtx, cancel := withLeaseRedisTimeout(ctx)
	defer cancel()
	_, err := addLeaseTokensLua.Run(redisCtx, rm.rdb, []string{counter.localTokenKey}, amount, leaseTTL.Milliseconds()).Int64()
	return err
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
func (rm *RateManager) needsTemporalRenewal(counter *leaseCounter) bool {
	expiresAtMs := counter.expiresAtUnixMs.Load()
	if expiresAtMs <= 0 {
		return false
	}

	return time.Now().UnixMilli() > expiresAtMs-rm.renewalBuffer.Milliseconds()
}

func (rm *RateManager) getLeaseCounter(prefix, apiKey string) *leaseCounter {
	key := prefix + ":" + apiKey
	if v, ok := rm.leaseMap.Load(key); ok {
		return v.(*leaseCounter)
	}
	counter := &leaseCounter{
		leaseSize:     rm.leaseSize,
		localTokenKey: "edge:lease:tokens:" + key,
		renewalBuffer: rm.renewalBuffer,
	}
	actual, _ := rm.leaseMap.LoadOrStore(key, counter)
	return actual.(*leaseCounter)
}

type singleflightGroup struct {
	mu sync.Mutex
	m  map[string]*singleflightCall
}

type singleflightCall struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

func (g *singleflightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*singleflightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &singleflightCall{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.val, c.err, false
}
