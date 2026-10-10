package hub

import (
	"context"
	"testing"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/config"
	"gateway/packages/ledger"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Callers of jwt and none services have no tier record on the hub: the edge
// asserts the tier it resolved. API-key callers never can.
func TestQuotaLease_TrustsEdgeTierOnlyWithoutAPIKeys(t *testing.T) {
	cfg, err := config.ParseAndCompile([]byte(`version: 2
services:
  - name: open
    match: { path_prefix: /v1/open }
    upstream: http://open
    auth: { type: none }
    limits: { window: 1m, default_tier: free, tiers: { free: { requests: 10 }, pro: { requests: 100 } } }
  - name: keyed
    match: { path_prefix: /v1/keyed }
    upstream: http://keyed
    limits: { window: 1m, tiers: { free: { requests: 10 }, pro: { requests: 100 } } }
`), "t.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cm := &ConfigManager{}
	cm.active.Store(cfg)
	rdb := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer rdb.Close()
	srv := NewQuotaLeaseServer(ledger.NewRedis(rdb), cm, NewInMemoryTierStore())
	lease := func(service, tier string) (*types.QuotaLeaseResponse, error) {
		return srv.RequestQuotaLease(context.Background(), &types.QuotaLeaseRequest{
			Prefix: "v1", ServiceId: service, ApiKey: "abc123", TierId: tier, RequestedTokens: 5,
		})
	}

	if resp, err := lease("open", "pro"); err != nil || resp.GrantedTokens != 5 {
		t.Fatalf("asserted tier on an open service: %v %v", resp, err)
	}
	if _, err := lease("open", "gold"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("undefined asserted tier: %v", err)
	}
	if _, err := lease("keyed", "pro"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("an API-key service accepted the edge's tier: %v", err)
	}
}

func v3Server(t *testing.T) (*QuotaLeaseServer, *InMemoryTierStore) {
	t.Helper()
	cfg, err := config.ParseAndCompile([]byte(`version: 2
services:
  - name: search
    match: { path_prefix: /v1/search }
    upstream: http://search
    limits:
      window: 1m
      tiers:
        free: { requests: 10 }
        burst: { algorithm: token_bucket, rate: 1/s, burst: 4 }
      org: { requests: 15 }
      also:
        - { name: guard, per: service, requests: 6 }
`), "t.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cm := &ConfigManager{}
	cm.active.Store(cfg)
	mr := miniredis.RunT(t)
	mr.SetTime(time.Unix(1_760_000_030, 0)) // mid-window: a test never straddles a reset
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	tiers := NewInMemoryTierStore()
	for _, k := range []string{"key_a1", "key_a2", "key_b1"} {
		_ = tiers.SetTier(context.Background(), "v1", k, "free")
	}
	_ = tiers.SetTier(context.Background(), "v1", "key_bucket", "burst")
	_ = tiers.SetOwner(context.Background(), "v1", "key_a1", "acme")
	_ = tiers.SetOwner(context.Background(), "v1", "key_a2", "acme")
	return NewQuotaLeaseServer(ledger.NewRedis(rdb), cm, tiers), tiers
}

func leaseReq(key, limit string, n int64) *types.QuotaLeaseRequest {
	return &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "search", ApiKey: key, LimitId: limit, RequestedTokens: n}
}

func TestQuotaLease_BatchReportsEachItem(t *testing.T) {
	srv, _ := v3Server(t)
	out, err := srv.RequestQuotaLeases(context.Background(), &types.QuotaLeaseBatchRequest{Requests: []*types.QuotaLeaseRequest{
		leaseReq("key_b1", "", 4),
		leaseReq("key_nobody", "", 4), // no tier
		leaseReq("key_b1", "nope", 4), // no such limit
		leaseReq("key_b1", "guard", 4),
	}})
	if err != nil {
		t.Fatal(err)
	}
	r := out.Results
	if r[0].Code != 0 || r[0].Response.GrantedTokens != 4 || r[0].Response.WindowId == 0 || r[0].Response.ExpiresInMs <= 0 {
		t.Fatalf("item 0: %+v", r[0])
	}
	if codes.Code(r[1].Code) != codes.PermissionDenied || codes.Code(r[2].Code) != codes.NotFound {
		t.Fatalf("items 1, 2: %+v %+v", r[1], r[2])
	}
	if r[3].Response.GrantedTokens != 4 || r[3].Response.Limit != 6 {
		t.Fatalf("guard: %+v", r[3])
	}
}

func TestQuotaLease_OrgBudgetIsSharedByOwnersKeys(t *testing.T) {
	srv, _ := v3Server(t)
	ctx := context.Background()
	got := func(key string) int64 {
		resp, err := srv.RequestQuotaLease(ctx, leaseReq(key, "", 10))
		if err != nil {
			t.Fatal(err)
		}
		return resp.GrantedTokens
	}
	// Each key's tier allows 10; acme's two keys share an org budget of 15.
	if a1, a2 := got("key_a1"), got("key_a2"); a1+a2 != 15 {
		t.Fatalf("acme's keys got %d + %d, want 15 together", a1, a2)
	}
	if b := got("key_b1"); b != 10 {
		t.Fatalf("a key without an owner got %d, want its tier's 10", b)
	}
}

func TestQuotaLease_ServiceLimitIsSharedByEveryCaller(t *testing.T) {
	srv, _ := v3Server(t)
	ctx := context.Background()
	var total int64
	for i := 0; i < 5; i++ {
		resp, err := srv.RequestQuotaLease(ctx, leaseReq(types.ServiceSubject, "guard", 2))
		if err != nil {
			t.Fatal(err)
		}
		total += resp.GrantedTokens
	}
	if total != 6 {
		t.Fatalf("service-wide guard granted %d, want 6", total)
	}
}

func TestQuotaLease_ReturnCreditsTheWindow(t *testing.T) {
	srv, _ := v3Server(t)
	ctx := context.Background()
	first, _ := srv.RequestQuotaLease(ctx, leaseReq("key_b1", "", 10))
	ret := &types.ReturnLeasesRequest{EdgeId: "e1", Dedupe: "d1", Returns: []*types.LeaseReturn{
		{Prefix: "v1", ServiceId: "search", ApiKey: "key_b1", WindowId: first.WindowId, Tokens: 6},
	}}
	for i := 0; i < 2; i++ { // a retried return is applied once
		if _, err := srv.ReturnLeases(ctx, ret); err != nil {
			t.Fatal(err)
		}
	}
	again, _ := srv.RequestQuotaLease(ctx, leaseReq("key_b1", "", 10))
	if again.GrantedTokens != 6 {
		t.Fatalf("after returning 6 of 10, granted %d, want 6", again.GrantedTokens)
	}
}

func TestQuotaLease_TokenBucketTier(t *testing.T) {
	srv, _ := v3Server(t)
	resp, err := srv.RequestQuotaLease(context.Background(), leaseReq("key_bucket", "", 3))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GrantedTokens != 3 || resp.WindowId != 0 || resp.ResetInMs <= 0 {
		t.Fatalf("bucket grant: %+v", resp)
	}
	resp, _ = srv.RequestQuotaLease(context.Background(), leaseReq("key_bucket", "", 3))
	if resp.GrantedTokens != 1 {
		t.Fatalf("second grant from a burst of 4 = %d, want 1", resp.GrantedTokens)
	}
}
