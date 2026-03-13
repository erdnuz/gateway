package edge

import (
	"context"
	"sync"
	"testing"
	"time"

	"gateway/packages/common/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeLeaseClient struct {
	mu         sync.Mutex
	grants     []int64
	calls      int
	ttlSeconds int64
}

func (f *fakeLeaseClient) RequestQuotaLease(_ context.Context, _ *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	grant := int64(0)
	if len(f.grants) > 0 {
		grant = f.grants[0]
		f.grants = f.grants[1:]
	}
	ttl := f.ttlSeconds
	if ttl == 0 {
		ttl = 60
	}
	return &types.QuotaLeaseResponse{GrantedTokens: grant, LeaseTtlSeconds: ttl}, nil
}

func (f *fakeLeaseClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeLeaseClient) Close() error { return nil }

func TestEdgeRateManager_RequestsLeaseAndConsumesTokens(t *testing.T) {
	rm := NewRateManagerWithOptions("", nil, 100, RateManagerOptions{LeaseSize: 5, LowWaterPct: 20})
	client := &fakeLeaseClient{grants: []int64{5, 5}}
	rm.SetLeaseClient(client)

	for i := 0; i < 7; i++ {
		usage, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 100, 1)
		if err != nil {
			t.Fatalf("increment failed: %v", err)
		}
		if usage != int64(i+1) {
			t.Fatalf("unexpected usage at step %d: %d", i, usage)
		}
	}

	if client.callCount() < 2 {
		t.Fatalf("expected at least 2 lease calls, got %d", client.callCount())
	}
}

func TestEdgeRateManager_DeniesWhenNoLeaseAvailable(t *testing.T) {
	rm := NewRateManagerWithOptions("", nil, 100, RateManagerOptions{LeaseSize: 2, LowWaterPct: 20, MinTokens: 0})
	client := &fakeLeaseClient{grants: []int64{2, 0}}
	rm.SetLeaseClient(client)

	if _, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 2, 1); err != nil {
		t.Fatal(err)
	}

	usage, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if usage <= 2 {
		t.Fatalf("expected synthetic over-limit usage, got %d", usage)
	}
}

func TestEdgeRateManager_UsesMinTokensReserveUntilExpiry(t *testing.T) {
	rm := NewRateManagerWithOptions("", nil, 100, RateManagerOptions{LeaseSize: 2, LowWaterPct: 20, MinTokens: 1})
	client := &fakeLeaseClient{grants: []int64{2, 0}}
	rm.SetLeaseClient(client)

	for i := 0; i < 3; i++ {
		usage, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 100, 1)
		if err != nil {
			t.Fatalf("increment failed: %v", err)
		}
		if usage != int64(i+1) {
			t.Fatalf("expected usage %d, got %d", i+1, usage)
		}
	}

	// Fourth request should fail: reserve floor is exhausted.
	usage, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 100, 1)
	if err != nil {
		t.Fatalf("increment failed: %v", err)
	}
	if usage <= 100 {
		t.Fatalf("expected synthetic over-limit usage once reserve exhausted, got %d", usage)
	}
}

func TestEdgeRateManager_DoesNotUseReserveAfterExpiry(t *testing.T) {
	rm := NewRateManagerWithOptions("", nil, 100, RateManagerOptions{LeaseSize: 1, LowWaterPct: 20, MinTokens: 1})
	client := &fakeLeaseClient{grants: []int64{1, 0}, ttlSeconds: 1}
	rm.SetLeaseClient(client)

	if _, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 100, 1); err != nil {
		t.Fatalf("first increment failed: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	usage, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 100, 1)
	if err != nil {
		t.Fatalf("increment failed: %v", err)
	}
	if usage <= 100 {
		t.Fatalf("expected synthetic over-limit usage after lease expiry, got %d", usage)
	}
}

func TestEdgeRateManager_UsesRedisLuaLocalTokens(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	rm := NewRateManagerWithOptions("", rdb, 100, RateManagerOptions{LeaseSize: 2, LowWaterPct: 20, MinTokens: 0})
	client := &fakeLeaseClient{grants: []int64{2, 0}, ttlSeconds: 30}
	rm.SetLeaseClient(client)

	for i := 0; i < 2; i++ {
		usage, incErr := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 100, 1)
		if incErr != nil {
			t.Fatalf("increment %d failed: %v", i+1, incErr)
		}
		if usage != int64(i+1) {
			t.Fatalf("expected usage %d got %d", i+1, usage)
		}
	}

	usage, incErr := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 100, 1)
	if incErr != nil {
		t.Fatalf("increment after exhaustion failed: %v", incErr)
	}
	if usage <= 100 {
		t.Fatalf("expected synthetic over-limit usage after Lua depletion, got %d", usage)
	}
}

func TestEdgeRateManager_RedisLeaseTokensExpireWithTTL(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	rm := NewRateManagerWithOptions("", rdb, 100, RateManagerOptions{LeaseSize: 1, LowWaterPct: 20, MinTokens: 0})
	client := &fakeLeaseClient{grants: []int64{1}, ttlSeconds: 1}
	rm.SetLeaseClient(client)

	if _, incErr := rm.IncrementWithService(context.Background(), "v1", "svc", "k-ttl", 100, 1); incErr != nil {
		t.Fatalf("first increment failed: %v", incErr)
	}

	counter := rm.getLeaseCounter("v1", "k-ttl")
	if ttl := mr.TTL(counter.localTokenKey); ttl <= 0 {
		t.Fatalf("expected positive ttl on local token key, got %s", ttl)
	}

	mr.FastForward(2 * time.Second)
	if mr.Exists(counter.localTokenKey) {
		t.Fatalf("expected local token key to expire after lease ttl")
	}
}

func TestDefaultRateManagerOptions_FollowsRuntimePolicyDefaults(t *testing.T) {
	opts := DefaultRateManagerOptions()
	defaults := types.DefaultRuntimePolicy().Edge

	if opts.HardThresholdPct != defaults.RateHardThresholdPct {
		t.Fatalf("expected hard threshold pct %f, got %f", defaults.RateHardThresholdPct, opts.HardThresholdPct)
	}
	if opts.LeaseSize != defaults.RateLeaseSize {
		t.Fatalf("expected lease size %d, got %d", defaults.RateLeaseSize, opts.LeaseSize)
	}
	if opts.LowWaterPct != defaults.RateLowWaterPct {
		t.Fatalf("expected low water pct %f, got %f", defaults.RateLowWaterPct, opts.LowWaterPct)
	}
}

// fakeWaitingLeaseClient always reports zero capacity available (Hub WAITING state).
type fakeWaitingLeaseClient struct {
	mu           sync.Mutex
	calls        int
	retryAfterMs int64
}

func (f *fakeWaitingLeaseClient) RequestQuotaLease(_ context.Context, _ *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return &types.QuotaLeaseResponse{
		WaitingForCapacity: true,
		RetryAfterMs:       f.retryAfterMs,
	}, nil
}

func (f *fakeWaitingLeaseClient) Close() error { return nil }

func (f *fakeWaitingLeaseClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// T1: Temporal Overlap — a second lease is granted before the first expires, causing
// a balance top-up without losing tokens.
func TestT1_TemporalOverlap(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	ctx := context.Background()
	// LeaseTTL=10s, RenewalBuffer=2s, very low LWM so only
	// the temporal trigger causes a renewal.
	rm := NewRateManagerWithOptions("", rdb, 10000, RateManagerOptions{
		LeaseSize: 100, LowWaterPct: 1, LeaseTTL: 10 * time.Second, RenewalBuffer: 2 * time.Second,
	})
	// Two grants of 100 tokens each at 10s TTL — Lease A then Lease B.
	client := &fakeLeaseClient{grants: []int64{100, 100}, ttlSeconds: 10}
	rm.SetLeaseClient(client)

	// Fetch Lease A (100 tokens).
	if _, err := rm.IncrementWithService(ctx, "v1", "svc", "k-t1", 10000, 1); err != nil {
		t.Fatal(err)
	}

	counter := rm.getLeaseCounter("v1", "k-t1")
	balanceAfterA, _ := rdb.Get(ctx, counter.localTokenKey).Int64()
	if balanceAfterA != 99 {
		t.Fatalf("expected 99 tokens after Lease A (1 consumed), got %d", balanceAfterA)
	}

	// Simulate the 8-second mark: 2 s left on Lease A → within 2 s buffer.
	counter.expiresAtUnixMs.Store(time.Now().UnixMilli() + 1800)

	// This call should detect the temporal trigger and queue an async renewal.
	if _, err := rm.IncrementWithService(ctx, "v1", "svc", "k-t1", 10000, 1); err != nil {
		t.Fatal(err)
	}
	// Allow the async goroutine to complete.
	time.Sleep(50 * time.Millisecond)

	// Balance = (99-1 consumed in 2nd call) + 100 (Lease B) = 198.
	finalBalance, _ := rdb.Get(ctx, counter.localTokenKey).Int64()
	if finalBalance < 190 {
		t.Fatalf("expected balance ~198 after temporal top-up, got %d", finalBalance)
	}

	// TTL must have been refreshed to ~10 s by the PEXPIRE on INCRBY.
	if ttl := mr.TTL(counter.localTokenKey); ttl < 9*time.Second {
		t.Fatalf("expected TTL ~10s after Lease B, got %s", ttl)
	}

	if client.callCount() < 2 {
		t.Fatalf("expected at least 2 lease calls (A and B), got %d", client.callCount())
	}
}

// T2: LWM Top-Up Under Pressure — a burst of 850 requests triggers an async
// renewal at request 801 (when remaining == low_water_mark = 200).
func TestT2_LWMTopUpUnderPressure(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	ctx := context.Background()
	// lease_quantum = 1000, low_water_mark = 200 (LowWaterPct = 20%)
	rm := NewRateManagerWithOptions("", rdb, 100000, RateManagerOptions{
		LeaseSize: 1000, LowWaterPct: 20, LeaseTTL: 60 * time.Second, RenewalBuffer: 2 * time.Second,
	})
	client := &fakeLeaseClient{grants: []int64{1000, 1000}, ttlSeconds: 60}
	rm.SetLeaseClient(client)

	// Burst of 850 requests.
	for i := 0; i < 850; i++ {
		if _, err := rm.IncrementWithService(ctx, "v1", "svc", "k-t2", 100000, 1); err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
	}

	// Allow async LWM-triggered renewal to complete.
	time.Sleep(100 * time.Millisecond)

	counter := rm.getLeaseCounter("v1", "k-t2")
	balance, _ := rdb.Get(ctx, counter.localTokenKey).Int64()
	// After 850 consumes from the first 1000: 1000-850 = 150 remaining before
	// top-up hits; after the second 1000-grant INCRBY: balance ≥ 1000.
	if balance < 1000 {
		t.Fatalf("expected balance ≥ 1000 after LWM top-up, got %d", balance)
	}

	if client.callCount() < 2 {
		t.Fatalf("expected at least 2 lease calls (initial + LWM renewal), got %d", client.callCount())
	}
}

// T3: Global Exhaustion and Backoff — when the Hub reports zero capacity, the
// Edge enters WAITING, 429s all traffic, and retries only after retry_after_ms.
func TestT3_GlobalExhaustionAndBackoff(t *testing.T) {
	rm := NewRateManagerWithOptions("", nil, 10, RateManagerOptions{LeaseSize: 10, LowWaterPct: 20})
	client := &fakeWaitingLeaseClient{retryAfterMs: 500}
	rm.SetLeaseClient(client)

	ctx := context.Background()

	// First request: triggers lease fetch → Hub says WAITING.
	usage, err := rm.IncrementWithService(ctx, "v1", "svc", "k-t3", 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	if usage <= 10 {
		t.Fatalf("expected 429 (usage > limit) on first request, got %d", usage)
	}

	counter := rm.getLeaseCounter("v1", "k-t3")
	if leaseState(counter.state.Load()) != leaseStateWaiting {
		t.Fatalf("expected counter to be in WAITING state after Hub 202, got state %d", counter.state.Load())
	}

	// All subsequent requests within the retry window must be short-circuited.
	for i := 0; i < 5; i++ {
		usage, err = rm.IncrementWithService(ctx, "v1", "svc", "k-t3", 10, 1)
		if err != nil {
			t.Fatal(err)
		}
		if usage <= 10 {
			t.Fatalf("expected 429 while in WAITING state (req %d), got %d", i, usage)
		}
	}

	// Hub returns WAITING again after retry: Hub still exhausted.
	// Advance past the 500 ms retry window.
	time.Sleep(550 * time.Millisecond)

	// Next call: retry window has elapsed → triggers a new async renewal.
	usage, err = rm.IncrementWithService(ctx, "v1", "svc", "k-t3", 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Returns 429 because the async renewal hasn't returned capacity yet.
	if usage <= 10 {
		t.Fatalf("expected 429 during WAITING→retry transition, got %d", usage)
	}

	// The Hub was contacted for a second poll.
	calls := client.callCount()
	if calls < 2 {
		t.Fatalf("expected at least 2 Hub polls, got %d", calls)
	}
}

// T4: Stale Lease Partition (Hard Expiry) — when the Redis key TTL hits zero,
// the Edge must immediately return 429 for all traffic (as if tokens = 0).
func TestT4_StaleLeasePartitionHardExpiry(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	ctx := context.Background()
	// lease_ttl = 5s; very low LWM so only TTL-expiry causes denial.
	rm := NewRateManagerWithOptions("", rdb, 100000, RateManagerOptions{
		LeaseSize: 500, LowWaterPct: 0.1, LeaseTTL: 5 * time.Second, RenewalBuffer: 500 * time.Millisecond,
		MinTokens: 0,
	})
	// Single grant of 500 tokens. Once exhausted (partition), no further grants.
	client := &fakeLeaseClient{grants: []int64{500}, ttlSeconds: 5}
	rm.SetLeaseClient(client)

	// Fetch the initial lease.
	if _, err := rm.IncrementWithService(ctx, "v1", "svc", "k-t4", 100000, 1); err != nil {
		t.Fatal(err)
	}

	counter := rm.getLeaseCounter("v1", "k-t4")

	// Verify the key has a ~5s TTL set via PEXPIRE.
	if ttl := mr.TTL(counter.localTokenKey); ttl < 4*time.Second {
		t.Fatalf("expected ~5s TTL on token key, got %s", ttl)
	}

	// Simulate a 5.001 s passage of time — key must have expired (hard expiry).
	mr.FastForward(5001 * time.Millisecond)

	if mr.Exists(counter.localTokenKey) {
		t.Fatalf("expected local token key to expire at 5s; key still present at 5.001s")
	}

	// Network partition: no more lease grants available (grants slice empty).
	// The next request must be denied (usage > limit).
	usage, err := rm.IncrementWithService(ctx, "v1", "svc", "k-t4", 100000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if usage <= 100000 {
		t.Fatalf("expected 429 after hard expiry at 5.001s, got usage %d", usage)
	}
}
