package edge

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gateway/packages/common/types"
)

func TestTierCache_ResolvesOncePerKeyUnderConcurrency(t *testing.T) {
	var calls atomic.Int32
	tc := NewTierCache(func(context.Context, string, string) (string, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return "pro", nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tier, err := tc.Get(context.Background(), "v1", "k"); err != nil || tier != "pro" {
				t.Errorf("Get = %q, %v", tier, err)
			}
		}()
	}
	wg.Wait()
	if _, _ = tc.Get(context.Background(), "v1", "k"); calls.Load() != 1 {
		t.Fatalf("hub asked %d times, want 1", calls.Load())
	}
}

func TestTierCache_PushedChangesAndReset(t *testing.T) {
	hub := map[string]string{"k": "free"}
	var calls atomic.Int32
	tc := NewTierCache(func(_ context.Context, _, key string) (string, error) {
		calls.Add(1)
		return hub[key], nil
	})
	ctx := context.Background()
	if tier, _ := tc.Get(ctx, "v1", "k"); tier != "free" {
		t.Fatalf("tier %q", tier)
	}
	tc.Set("v1", "k", "pro") // pushed upgrade
	if tier, _ := tc.Get(ctx, "v1", "k"); tier != "pro" || calls.Load() != 1 {
		t.Fatalf("after push: tier %q, %d hub calls", tier, calls.Load())
	}
	tc.Set("v1", "k", "") // pushed revocation
	if tier, _ := tc.Get(ctx, "v1", "k"); tier != "" {
		t.Fatalf("revoked key still has tier %q", tier)
	}
	if _, ok := tc.Stale("v1", "k", time.Hour); ok {
		t.Fatal("a revoked key must not be served stale")
	}
	hub["k"] = "enterprise"
	tc.Reset() // reconnect: revalidate everything
	if tier, _ := tc.Get(ctx, "v1", "k"); tier != "enterprise" || calls.Load() != 2 {
		t.Fatalf("after reset: tier %q, %d hub calls", tier, calls.Load())
	}
}

func TestTierCache_StaleFallback(t *testing.T) {
	down := false
	tc := NewTierCache(func(context.Context, string, string) (string, error) {
		if down {
			return "", errors.New("hub down")
		}
		return "pro", nil
	})
	_, _ = tc.Get(context.Background(), "v1", "k")
	tc.Reset()
	down = true
	if _, err := tc.Get(context.Background(), "v1", "k"); err == nil {
		t.Fatal("expected an error while the hub is down")
	}
	if tier, ok := tc.Stale("v1", "k", time.Hour); !ok || tier != "pro" {
		t.Fatalf("Stale = %q, %v", tier, ok)
	}
	time.Sleep(5 * time.Millisecond) // Windows clocks are too coarse for a 1ns age
	if _, ok := tc.Stale("v1", "k", time.Millisecond); ok {
		t.Fatal("Stale ignored maxAge")
	}
}

func TestTTLCache_StaysWithinBudget(t *testing.T) {
	c := newTTLCache[int](cacheShards * 10)
	for i := 0; i < 10_000; i++ {
		c.set("k"+strconv.Itoa(i), i, time.Minute, 1)
	}
	n := 0
	for i := range c.shards {
		if c.shards[i].cost > c.shardCost {
			t.Fatalf("shard %d over budget: %d", i, c.shards[i].cost)
		}
		n += len(c.shards[i].entries)
	}
	if n > cacheShards*10 {
		t.Fatalf("%d entries kept, budget %d", n, cacheShards*10)
	}
	c.set("big", 1, time.Minute, 1000) // larger than a shard: skipped
	if _, _, _, ok := c.get("big"); ok {
		t.Fatal("cached an entry larger than a shard")
	}
}

func TestMemoryResponses_InvalidateScope(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryResponses(1 << 20)
	for _, k := range []string{"c:/v1/a/x:1", "c:/v1/b/x:2", "c:/v2/a/x:3"} {
		_ = m.Set(ctx, k, []byte("v"), time.Minute)
	}
	_ = m.Invalidate(ctx, "v1", "a")
	for k, want := range map[string]bool{"c:/v1/a/x:1": false, "c:/v1/b/x:2": true, "c:/v2/a/x:3": true} {
		if _, ok, _ := m.Get(ctx, k); ok != want {
			t.Errorf("%s present=%v, want %v", k, ok, want)
		}
	}
}

func TestApply_ConfigOpensEdgeAndTierChangesUpdateCache(t *testing.T) {
	tiers := NewTierCache(func(context.Context, string, string) (string, error) { return "", nil })
	s := NewServer(Deps{Config: NewConfigManager(nil), Tiers: tiers, Responses: NewMemoryResponses(1 << 20)}, Options{})
	s.SetStartupReady(false)

	cfg := types.GatewayConfig{Prefixes: []types.PrefixConfig{{Prefix: "v1", Services: []types.ServiceConfig{{ServiceID: "svc"}}}}}
	b, _ := json.Marshal(cfg)
	s.Apply(context.Background(), &types.ControlEvent{Event: &types.ControlEvent_Config{Config: &types.ConfigSnapshot{ConfigJson: b, ResetTiers: true}}})
	if !s.startupReady.Load() {
		t.Fatal("first snapshot did not open the edge")
	}
	if _, err := s.Config.GetServiceConfig("v1", "svc"); err != nil {
		t.Fatal(err)
	}
	s.Apply(context.Background(), &types.ControlEvent{Event: &types.ControlEvent_Tier{Tier: &types.TierChange{Prefix: "v1", ApiKey: "k", TierId: "pro"}}})
	if tier, _ := tiers.Get(context.Background(), "v1", "k"); tier != "pro" {
		t.Fatalf("pushed tier not applied: %q", tier)
	}
}
