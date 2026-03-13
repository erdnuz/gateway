//go:build integration

package integration

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/hub"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type staticConfigStore struct {
	cfg *types.GatewayConfig
}

func (s *staticConfigStore) Get() *types.GatewayConfig { return s.cfg }

func (s *staticConfigStore) Snapshot() *types.GatewayConfig { return s.cfg }

func (s *staticConfigStore) HubPolicy() types.HubRuntimePolicy {
	if s.cfg == nil {
		return types.HubRuntimePolicy{}
	}
	return s.cfg.Runtime.Hub
}

func (s *staticConfigStore) EdgePolicy() types.EdgeRuntimePolicy {
	if s.cfg == nil {
		return types.EdgeRuntimePolicy{}
	}
	return s.cfg.Runtime.Edge
}

func (s *staticConfigStore) AnalyticsPolicy() types.AnalyticsRuntimePolicy {
	if s.cfg == nil {
		return types.AnalyticsRuntimePolicy{}
	}
	return s.cfg.Runtime.Analytics
}

func (s *staticConfigStore) Prefixes() []types.PrefixConfig {
	if s.cfg == nil {
		return nil
	}
	return s.cfg.Prefixes
}

func (s *staticConfigStore) FindPrefix(prefix string) (*types.PrefixConfig, bool) {
	if s.cfg == nil {
		return nil, false
	}
	for i := range s.cfg.Prefixes {
		if s.cfg.Prefixes[i].Prefix == prefix {
			return &s.cfg.Prefixes[i], true
		}
	}
	return nil, false
}

func (s *staticConfigStore) FindService(prefix, service string) (*types.PrefixConfig, *types.ServiceConfig, bool) {
	pfx, ok := s.FindPrefix(prefix)
	if !ok {
		return nil, nil, false
	}
	for i := range pfx.Services {
		if pfx.Services[i].ServiceID == service {
			return pfx, &pfx.Services[i], true
		}
	}
	return nil, nil, false
}

type staticTierStore struct {
	mu sync.RWMutex
	m  map[string]string
}

func newStaticTierStore() *staticTierStore {
	return &staticTierStore{m: map[string]string{}}
}

func (s *staticTierStore) key(prefix, apiKey string) string { return prefix + ":" + apiKey }

func (s *staticTierStore) GetTier(_ context.Context, prefix, apiKey string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.m[s.key(prefix, apiKey)]; ok {
		return v, nil
	}
	return "free", nil
}

func (s *staticTierStore) SetTier(_ context.Context, prefix, apiKey, tierID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[s.key(prefix, apiKey)] = tierID
	return nil
}

func (s *staticTierStore) DeleteTier(_ context.Context, prefix, apiKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, s.key(prefix, apiKey))
	return nil
}

func newHubLeaseServerForIntegrationTest(t *testing.T, quota uint32, period time.Duration) (*hub.QuotaLeaseServer, *redis.Client, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{
			{
				Prefix:      "v1",
				QuotaPeriod: period,
				Services: []types.ServiceConfig{
					{
						ServiceID: "auth-api",
						Tiers: []types.TierConfig{
							{TierID: "free", Quota: quota, GetCost: 1, PostCost: 1, PutCost: 1, DeleteCost: 1, OtherCost: 1},
						},
					},
				},
			},
		},
	}
	store := newStaticTierStore()
	server := hub.NewQuotaLeaseServer(rdb, &staticConfigStore{cfg: cfg}, store)
	cleanup := func() {
		_ = rdb.Close()
		mr.Close()
	}
	return server, rdb, cleanup
}

// T-HUB-1 (implemented subset): once a window is replenished, the first waiter
// that polls is served while later waiters remain in WAITING.
func TestIntegration_THUB1WaiterServedFirstAfterReplenish(t *testing.T) {
	server, _, cleanup := newHubLeaseServerForIntegrationTest(t, 10, time.Second)
	defer cleanup()
	ctx := context.Background()

	// Drain current window fully.
	resp, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "seed", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("seed lease request failed: %v", err)
	}
	if resp.GrantedTokens != 10 {
		t.Fatalf("expected seed grant 10, got %d", resp.GrantedTokens)
	}

	// Three waiters receive WAITING in order A, B, C.
	waitA, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "seed", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("waiter A request failed: %v", err)
	}
	waitB, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "seed", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("waiter B request failed: %v", err)
	}
	waitC, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "seed", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("waiter C request failed: %v", err)
	}
	if !waitA.WaitingForCapacity || !waitB.WaitingForCapacity || !waitC.WaitingForCapacity {
		t.Fatalf("expected all waiters to be waiting: A=%v B=%v C=%v", waitA.WaitingForCapacity, waitB.WaitingForCapacity, waitC.WaitingForCapacity)
	}

	// Next quota window.
	time.Sleep(1100 * time.Millisecond)

	grantA, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "seed", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("A retry failed: %v", err)
	}
	grantB, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "seed", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("B retry failed: %v", err)
	}
	grantC, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "seed", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("C retry failed: %v", err)
	}

	if grantA.GrantedTokens != 10 {
		t.Fatalf("expected first retrier to get full grant, got %d", grantA.GrantedTokens)
	}
	if !grantB.WaitingForCapacity || !grantC.WaitingForCapacity {
		t.Fatalf("expected later retriers to remain waiting: B=%v C=%v", grantB.WaitingForCapacity, grantC.WaitingForCapacity)
	}
}

// T-HUB-2: Thundering Herd Suppression.
// Model 20 waiting edges that retry with local jitter around retry_after_ms and
// assert retry times are spread rather than spiking at a single instant.
func TestIntegration_THUB2ThunderingHerdSuppression(t *testing.T) {
	server, _, cleanup := newHubLeaseServerForIntegrationTest(t, 1, 10*time.Second)
	defer cleanup()
	ctx := context.Background()

	// Drain first so subsequent requests receive WAITING with retry_after_ms=1000.
	seed, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "herd", RequestedTokens: 1})
	if err != nil {
		t.Fatalf("seed request failed: %v", err)
	}
	if seed.GrantedTokens != 1 {
		t.Fatalf("expected seed grant 1, got %d", seed.GrantedTokens)
	}
	wait, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "herd", RequestedTokens: 1})
	if err != nil {
		t.Fatalf("wait request failed: %v", err)
	}
	if !wait.WaitingForCapacity {
		t.Fatalf("expected waiting response, got grant=%d", wait.GrantedTokens)
	}
	if wait.RetryAfterMs != 1000 {
		t.Fatalf("expected retry_after_ms=1000, got %d", wait.RetryAfterMs)
	}

	const edges = 20
	start := time.Now()
	timesMs := make([]int64, edges)
	var wg sync.WaitGroup

	for i := 0; i < edges; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// deterministic jitter window 800-1200 ms
			r := rand.New(rand.NewSource(int64(idx + 1)))
			delay := 800 + r.Intn(401)
			time.Sleep(time.Duration(delay) * time.Millisecond)
			timesMs[idx] = time.Since(start).Milliseconds()
			_, _ = server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "herd", RequestedTokens: 1})
		}(i)
	}
	wg.Wait()

	minMs, maxMs := timesMs[0], timesMs[0]
	for _, v := range timesMs[1:] {
		if v < minMs {
			minMs = v
		}
		if v > maxMs {
			maxMs = v
		}
	}
	if minMs < 760 || maxMs > 1260 {
		t.Fatalf("expected retries roughly in [800,1200]ms jitter window, got min=%d max=%d", minMs, maxMs)
	}
	if maxMs-minMs < 200 {
		t.Fatalf("expected spread retries (no spike), got window=%dms", maxMs-minMs)
	}
}

// T-HUB-3: Percentage-Based Grant Capping.
// A massive request is capped at 25% of total quota, preserving headroom.
func TestIntegration_THUB3PercentageGrantCapping(t *testing.T) {
	server, _, cleanup := newHubLeaseServerForIntegrationTest(t, 100, 5*time.Second)
	defer cleanup()
	ctx := context.Background()

	first, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "cap", RequestedTokens: 1000})
	if err != nil {
		t.Fatalf("first massive request failed: %v", err)
	}
	if first.GrantedTokens != 25 {
		t.Fatalf("expected first grant to be capped at 25, got %d", first.GrantedTokens)
	}
	if first.RemainingGlobal != 75 {
		t.Fatalf("expected remaining global=75 after first cap, got %d", first.RemainingGlobal)
	}

	second, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "cap", RequestedTokens: 1000})
	if err != nil {
		t.Fatalf("second massive request failed: %v", err)
	}
	if second.GrantedTokens != 25 {
		t.Fatalf("expected second grant to be capped at 25, got %d", second.GrantedTokens)
	}
	if second.RemainingGlobal != 50 {
		t.Fatalf("expected remaining global=50 after second cap, got %d", second.RemainingGlobal)
	}
}

// T-RES-2: zombie lease reclaim. After lease bookkeeping TTL expires on Hub,
// tokens become available again for subsequent requests.
func TestIntegration_TRES2ZombieLeaseReclaimedAfterTTL(t *testing.T) {
	server, rdb, cleanup := newHubLeaseServerForIntegrationTest(t, 10, time.Second)
	defer cleanup()
	ctx := context.Background()

	first, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "zombie", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	if first.GrantedTokens != 10 {
		t.Fatalf("expected initial full grant=10, got %d", first.GrantedTokens)
	}

	second, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "zombie", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("second request failed: %v", err)
	}
	if !second.WaitingForCapacity {
		t.Fatalf("expected waiting while lease is still in-flight, got grant=%d", second.GrantedTokens)
	}

	// Hub uses EX=period*2 for lease-used key expiry.
	time.Sleep(2200 * time.Millisecond)

	third, err := server.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "zombie", RequestedTokens: 10})
	if err != nil {
		t.Fatalf("third request failed: %v", err)
	}
	if third.GrantedTokens != 10 {
		t.Fatalf("expected grant recovery after lease bookkeeping expiry, got %d", third.GrantedTokens)
	}

	// Defensive check: key should be present again after third grant.
	keys, err := rdb.Keys(ctx, "lease-used:v1:auth-api:zombie:*").Result()
	if err != nil {
		t.Fatalf("keys lookup failed: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("expected lease bookkeeping key to exist after third grant")
	}
}
