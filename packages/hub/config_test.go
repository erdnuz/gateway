package hub

import (
	"testing"
	"time"

	"gateway/packages/common/types"
)

func validGatewayConfig() *types.GatewayConfig {
	return &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{
			{
				Prefix:      "v1",
				QuotaPeriod: time.Hour,
				Services: []types.ServiceConfig{
					{
						ServiceID: "auth-api",
						TargetURL: "https://httpbin.org/anything",
						Tiers: []types.TierConfig{
							{TierID: "free", Quota: 100, GetCost: 1, PostCost: 1, PutCost: 1, DeleteCost: 1, OtherCost: 1},
						},
						Analytics: types.AnalyticsConfig{Enabled: true, SamplingRate: 1.0},
						Cache:     &types.CacheConfig{Enabled: true, TTL: 5 * time.Minute, CacheKey: "$method:$path:$key"},
						Failure: types.FailureConfig{
							Hub:      types.HubFailurePolicy{TierLookupStrategy: "stale-or-default", DefaultTier: "free", StaleTierMaxAge: time.Hour},
							Upstream: types.UpstreamFailurePolicy{Mode: "fail-closed", RetryOnStatuses: []int{502, 503, 504}},
						},
					},
				},
			},
		},
	}
}

func TestValidateGatewayConfig_Valid(t *testing.T) {
	cfg := validGatewayConfig()
	if err := ValidateGatewayConfig(cfg); err != nil {
		t.Fatalf("expected valid config, got err: %v", err)
	}
}

func TestValidateGatewayConfig_DuplicatePrefix(t *testing.T) {
	cfg := validGatewayConfig()
	cfg.Prefixes = append(cfg.Prefixes, cfg.Prefixes[0])

	if err := ValidateGatewayConfig(cfg); err == nil {
		t.Fatal("expected error for duplicate prefix")
	}
}

func TestValidateGatewayConfig_InvalidTargetURL(t *testing.T) {
	cfg := validGatewayConfig()
	cfg.Prefixes[0].Services[0].TargetURL = "not-a-url"

	if err := ValidateGatewayConfig(cfg); err == nil {
		t.Fatal("expected error for invalid target_url")
	}
}

func TestValidateGatewayConfig_InvalidHubTierStrategy(t *testing.T) {
	cfg := validGatewayConfig()
	cfg.Prefixes[0].Services[0].Failure.Hub.TierLookupStrategy = "maybe-open"

	if err := ValidateGatewayConfig(cfg); err == nil {
		t.Fatal("expected error for invalid tier_lookup_strategy")
	}
}

func TestValidateGatewayConfig_InvalidRetryStatusCode(t *testing.T) {
	cfg := validGatewayConfig()
	cfg.Prefixes[0].Services[0].Failure.Upstream.RetryOnStatuses = []int{42}

	if err := ValidateGatewayConfig(cfg); err == nil {
		t.Fatal("expected error for invalid retry status code")
	}
}
