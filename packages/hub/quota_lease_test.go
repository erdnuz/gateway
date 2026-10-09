package hub

import (
	"context"
	"testing"

	"gateway/packages/common/types"
	"gateway/packages/config"

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
	srv := NewQuotaLeaseServer(rdb, cm, NewInMemoryTierStore())
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
