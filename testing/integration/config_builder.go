//go:build integration

package integration

import (
	"gateway/packages/common/types"
	"gateway/packages/config"
	"os"
	"path/filepath"
	"time"
)

// writeConfigFile stores cfg as a version 2 config file, so the hub and edges
// load it through the same path as production configs.
func writeConfigFile(path string, cfg *types.GatewayConfig) error {
	out, err := config.Marshal(config.FromInternal(cfg))
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o600)
}

func readConfigFile(path string) (*types.GatewayConfig, error) {
	return config.Load(path)
}

func writeTestConfig(dir, upstreamURL string) (string, *types.GatewayConfig, error) {
	runtime := types.DefaultRuntimePolicy()
	// Smaller lease chunks keep shared quota fair across multiple edges in integration tests.
	runtime.Edge.RateLeaseSize = 10

	cfg := &types.GatewayConfig{
		Runtime: runtime,
		Prefixes: []types.PrefixConfig{
			{
				Prefix:      "v1",
				QuotaPeriod: time.Minute,
				Services: []types.ServiceConfig{
					{
						ServiceID: "auth-api",
						TargetURL: upstreamURL,
						Tiers: []types.TierConfig{
							{TierID: "free", Quota: 20, GetCost: 1, PostCost: 1, PutCost: 1, DeleteCost: 1, OtherCost: 1},
						},
						Transform: types.TransformConfig{StripPrefix: true},
						Analytics: types.AnalyticsConfig{Enabled: true, Mode: types.AnalyticsModeHeavy, SamplingRate: 1.0},
						Cache:     &types.CacheConfig{Enabled: true, TTL: 2 * time.Second, CacheKey: "$method:$path"},
						Failure: types.FailureConfig{
							FailOpen: false,
							Hub: types.HubFailurePolicy{
								TierLookupStrategy:      "default-tier",
								DefaultTier:             "free",
								AllowOnRateServiceError: false,
								StaleTierMaxAge:         5 * time.Minute,
							},
							Upstream: types.UpstreamFailurePolicy{Mode: "fail-closed"},
						},
					},
				},
			},
		},
	}

	if err := cfg.Validate(); err != nil {
		return "", nil, err
	}
	path := filepath.Join(dir, "gateway.yaml")
	if err := writeConfigFile(path, cfg); err != nil {
		return "", nil, err
	}
	return path, cfg, nil
}
