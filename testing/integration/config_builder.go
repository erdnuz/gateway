//go:build integration

package integration

import (
	"encoding/json"
	"gateway/packages/common/types"
	"os"
	"path/filepath"
	"time"
)

func writeConfigFile(path string, cfg *types.GatewayConfig) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(cfg)
}

func readConfigFile(path string) (*types.GatewayConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg types.GatewayConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func writeTestConfig(dir, upstreamURL string) (string, *types.GatewayConfig, error) {
	cfg := &types.GatewayConfig{
		UpdatedAt: time.Now().UTC(),
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
	path := filepath.Join(dir, "config.json")
	if err := writeConfigFile(path, cfg); err != nil {
		return "", nil, err
	}
	return path, cfg, nil
}
