package hub

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/config"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func testConfig(t *testing.T, target string) *types.GatewayConfig {
	t.Helper()
	cfg, err := config.ParseAndCompile([]byte(`version: 2
services:
  - name: svc
    match: { path_prefix: /v1/svc }
    upstream: `+target+`
    limits: { window: 1m, tiers: { free: { requests: 10 } } }
`), "test.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// watchHub serves cp over an in-memory gRPC connection and returns a stream
// of the events an edge watching it receives.
func watchHub(t *testing.T, ctx context.Context, cp *ControlPlane) <-chan *types.ControlEvent {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	types.RegisterControlPlaneServer(srv, cp)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///hub",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	stream, err := types.NewControlPlaneClient(conn).Watch(ctx, &types.WatchRequest{EdgeId: "e1"})
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan *types.ControlEvent, 16)
	go func() {
		for {
			ev, err := stream.Recv()
			if err != nil {
				close(events)
				return
			}
			events <- ev
		}
	}()
	return events
}

func next(t *testing.T, events <-chan *types.ControlEvent) *types.ControlEvent {
	t.Helper()
	select {
	case ev, ok := <-events:
		if !ok {
			t.Fatal("stream closed")
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("no event within 2s")
	}
	return nil
}

// Two hubs share one Redis; an edge watching hub B sees changes made through
// hub A, so hubs scale out behind a load balancer.
func TestControlPlane_ChangesReachEdgesOfEveryHub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cfgA, cfgB := &ConfigManager{}, &ConfigManager{}
	cfgA.active.Store(testConfig(t, "http://a"))
	cfgB.active.Store(testConfig(t, "http://a"))
	tiers := NewInMemoryTierStore()
	hubA, hubB := NewControlPlane(cfgA, tiers, rdb), NewControlPlane(cfgB, tiers, rdb)
	go hubA.Run(ctx)
	go hubB.Run(ctx)

	events := watchHub(t, ctx, hubB)
	first := next(t, events).GetConfig()
	if first == nil || !first.GetResetTiers() {
		t.Fatalf("stream must open with a resetting snapshot, got %v", first)
	}
	time.Sleep(50 * time.Millisecond) // let both subscriptions attach

	hubA.PublishTier("v1", "k", "free")
	if tier := next(t, events).GetTier(); tier.GetApiKey() != "k" || tier.GetTierId() != "free" {
		t.Fatalf("tier change = %v", tier)
	}

	newCfg := testConfig(t, "http://b")
	cfgA.active.Store(newCfg)
	hubA.PublishConfig(newCfg, []types.ConfigInvalidationScope{{Prefix: "v1", ServiceID: "svc"}})
	snap := next(t, events).GetConfig()
	var got types.GatewayConfig
	if err := json.Unmarshal(snap.GetConfigJson(), &got); err != nil || got.Prefixes[0].Services[0].TargetURL != "http://b" {
		t.Fatalf("snapshot = %+v, %v", got, err)
	}
	if len(snap.GetInvalidate()) != 1 || snap.GetResetTiers() {
		t.Fatalf("snapshot invalidations = %v reset=%v", snap.GetInvalidate(), snap.GetResetTiers())
	}
	if target := cfgB.Snapshot().Prefixes[0].Services[0].TargetURL; target != "http://b" {
		t.Fatalf("hub B did not adopt hub A's config: %s", target)
	}
}

func TestControlPlane_ResolveKey(t *testing.T) {
	cfg := &ConfigManager{}
	cfg.active.Store(testConfig(t, "http://a"))
	tiers := NewInMemoryTierStore()
	_ = tiers.SetTier(context.Background(), "v1", "known", "free")
	cp := NewControlPlane(cfg, tiers, nil)
	for key, want := range map[string]string{"known": "free", "unknown": ""} {
		resp, err := cp.ResolveKey(context.Background(), &types.ResolveKeyRequest{Prefix: "v1", ApiKey: key})
		if err != nil || resp.GetTierId() != want {
			t.Errorf("%s: %v %v", key, resp, err)
		}
	}
	if _, err := cp.ResolveKey(context.Background(), &types.ResolveKeyRequest{Prefix: "v1", ApiKey: "bad key"}); err == nil {
		t.Error("malformed key accepted")
	}
}
