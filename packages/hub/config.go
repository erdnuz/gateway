package hub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"

	"gateway/packages/common/types"
)

type ConfigManager struct {
	filePath string
	active   atomic.Pointer[types.GatewayConfig]
}

// NewConfigManager initializes the L1 cache from a JSON file.
func NewConfigManager(filePath string) (*ConfigManager, error) {
	cm := &ConfigManager{filePath: filePath}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	var cfg types.GatewayConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		// This is the most important change for debugging
		return nil, fmt.Errorf("failed to decode %s: %w", filePath, err)
	}
	if err := ValidateGatewayConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid gateway config %s: %w", filePath, err)
	}

	cm.active.Store(&cfg)
	return cm, nil
}

// ValidateGatewayConfig performs structural and semantic validation before the
// config is allowed into the in-memory source of truth.
func ValidateGatewayConfig(cfg *types.GatewayConfig) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if len(cfg.Prefixes) == 0 {
		return fmt.Errorf("config must include at least one prefix")
	}

	prefixSeen := map[string]struct{}{}
	for pi, prefix := range cfg.Prefixes {
		path := fmt.Sprintf("prefixes[%d]", pi)
		pfx := strings.TrimSpace(prefix.Prefix)
		if pfx == "" {
			return fmt.Errorf("%s.prefix is required", path)
		}
		if _, exists := prefixSeen[pfx]; exists {
			return fmt.Errorf("%s.prefix duplicates value %q", path, pfx)
		}
		prefixSeen[pfx] = struct{}{}

		if prefix.QuotaPeriod <= 0 {
			return fmt.Errorf("%s.quota_period must be > 0", path)
		}
		if len(prefix.Services) == 0 {
			return fmt.Errorf("%s.services must include at least one service", path)
		}

		serviceSeen := map[string]struct{}{}
		for si, service := range prefix.Services {
			svcPath := fmt.Sprintf("%s.services[%d]", path, si)
			svcID := strings.TrimSpace(service.ServiceID)
			if svcID == "" {
				return fmt.Errorf("%s.service_id is required", svcPath)
			}
			if _, exists := serviceSeen[svcID]; exists {
				return fmt.Errorf("%s.service_id duplicates value %q within prefix %q", svcPath, svcID, pfx)
			}
			serviceSeen[svcID] = struct{}{}

			u, err := url.ParseRequestURI(service.TargetURL)
			if err != nil {
				return fmt.Errorf("%s.target_url is invalid: %w", svcPath, err)
			}
			if u.Scheme != "http" && u.Scheme != "https" {
				return fmt.Errorf("%s.target_url must use http or https", svcPath)
			}
			if strings.TrimSpace(u.Host) == "" {
				return fmt.Errorf("%s.target_url host is required", svcPath)
			}

			if len(service.Tiers) == 0 {
				return fmt.Errorf("%s.tiers must include at least one tier", svcPath)
			}
			tierSeen := map[string]struct{}{}
			for ti, tier := range service.Tiers {
				tierPath := fmt.Sprintf("%s.tiers[%d]", svcPath, ti)
				tierID := strings.TrimSpace(tier.TierID)
				if tierID == "" {
					return fmt.Errorf("%s.tier_id is required", tierPath)
				}
				if _, exists := tierSeen[tierID]; exists {
					return fmt.Errorf("%s.tier_id duplicates value %q", tierPath, tierID)
				}
				tierSeen[tierID] = struct{}{}
				if tier.Quota == 0 {
					return fmt.Errorf("%s.quota must be > 0", tierPath)
				}
			}

			if service.Cache != nil && service.Cache.Enabled {
				if service.Cache.TTL <= 0 {
					return fmt.Errorf("%s.cache.ttl must be > 0 when cache is enabled", svcPath)
				}
			}
			if service.Analytics.SamplingRate < 0 || service.Analytics.SamplingRate > 1 {
				return fmt.Errorf("%s.analytics.sampling_rate must be between 0 and 1", svcPath)
			}

			hp := service.Failure.EffectiveHubPolicy()
			switch hp.TierLookupStrategy {
			case "fail-closed", "default-tier", "stale-or-default":
			default:
				return fmt.Errorf("%s.failure.hub.tier_lookup_strategy has invalid value %q", svcPath, hp.TierLookupStrategy)
			}
			if (hp.TierLookupStrategy == "default-tier" || hp.TierLookupStrategy == "stale-or-default") && strings.TrimSpace(hp.DefaultTier) == "" {
				return fmt.Errorf("%s.failure.hub.default_tier is required for strategy %q", svcPath, hp.TierLookupStrategy)
			}

			up := service.Failure.EffectiveUpstreamPolicy()
			switch up.Mode {
			case "fail-closed", "fail-open":
			default:
				return fmt.Errorf("%s.failure.upstream.mode has invalid value %q", svcPath, up.Mode)
			}
			for ri, code := range up.RetryOnStatuses {
				if code < http.StatusContinue || code > 599 {
					return fmt.Errorf("%s.failure.upstream.retry_on_statuses[%d] must be between 100 and 599", svcPath, ri)
				}
			}
		}
	}

	return nil
}

// Get returns the read-only configuration.
// Highly efficient O(1) with no mutex contention.
func (cm *ConfigManager) Get() *types.GatewayConfig {
	return cm.active.Load()
}
