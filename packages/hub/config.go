package hub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
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
	if err := cm.ReloadFromFile(); err != nil {
		return nil, err
	}
	return cm, nil
}

func MustNewConfigManager(filePath string) *ConfigManager {
	cm, err := NewConfigManager(filePath)
	if err != nil {
		panic(fmt.Sprintf("gate: config boot failure: %v", err))
	}
	return cm
}

func (cm *ConfigManager) ReloadFromFile() error {
	data, err := os.ReadFile(cm.filePath)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %w", cm.filePath, err)
	}

	var cfg types.GatewayConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("failed to decode %s: %w", cm.filePath, err)
	}
	if err := validateRuntimePolicyProvided(cfg.Runtime); err != nil {
		return fmt.Errorf("invalid gateway config %s: %w", cm.filePath, err)
	}
	cfg.Runtime = cfg.Runtime.Effective()
	if err := ValidateGatewayConfig(&cfg); err != nil {
		return fmt.Errorf("invalid gateway config %s: %w", cm.filePath, err)
	}

	cm.active.Store(&cfg)
	return nil
}

// ValidateGatewayConfig performs structural and semantic validation before the
// config is allowed into the in-memory source of truth.
func ValidateGatewayConfig(cfg *types.GatewayConfig) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}
	if err := validateEffectiveRuntime(cfg); err != nil {
		return err
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
			if err := service.Validate(); err != nil {
				return fmt.Errorf("%s is invalid: %w", svcPath, err)
			}
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

			if service.SafetyTier != nil {
				if _, exists := tierSeen[strings.TrimSpace(service.SafetyTier.TierID)]; exists {
					return fmt.Errorf("%s.safety_tier.tier_id must not duplicate any regular tier id", svcPath)
				}
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

func validateRuntimePolicyProvided(runtime types.RuntimePolicy) error {
	return requireNonZeroStructFields("runtime", reflect.ValueOf(runtime))
}

func validateEffectiveRuntime(cfg *types.GatewayConfig) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if err := requireNonZeroStructFields("runtime", reflect.ValueOf(cfg.Runtime)); err != nil {
		return fmt.Errorf("config runtime validation failed: %w", err)
	}
	return nil
}

func requireNonZeroStructFields(path string, v reflect.Value) error {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return fmt.Errorf("%s is required", path)
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		ft := t.Field(i)
		if !ft.IsExported() {
			continue
		}
		tag := ft.Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name == "" || name == "-" {
			name = strings.ToLower(ft.Name)
		}
		next := path + "." + name

		if f.Kind() == reflect.Pointer {
			if f.IsNil() {
				return fmt.Errorf("%s is required", next)
			}
			f = f.Elem()
		}

		switch f.Kind() {
		case reflect.Struct:
			if err := requireNonZeroStructFields(next, f); err != nil {
				return err
			}
		case reflect.String:
			if strings.TrimSpace(f.String()) == "" {
				return fmt.Errorf("%s is required", next)
			}
		case reflect.Slice, reflect.Array, reflect.Map:
			if f.Len() == 0 {
				return fmt.Errorf("%s is required", next)
			}
		case reflect.Bool:
			// false can be a valid explicit value
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if f.Int() == 0 {
				return fmt.Errorf("%s is required", next)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			if f.Uint() == 0 {
				return fmt.Errorf("%s is required", next)
			}
		case reflect.Float32, reflect.Float64:
			if f.Float() == 0 {
				return fmt.Errorf("%s is required", next)
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

func (cm *ConfigManager) Snapshot() *types.GatewayConfig {
	return cm.Get()
}

func (cm *ConfigManager) HubPolicy() types.HubRuntimePolicy {
	if cfg := cm.Get(); cfg != nil {
		return cfg.Runtime.Hub
	}
	return types.HubRuntimePolicy{}
}

func (cm *ConfigManager) EdgePolicy() types.EdgeRuntimePolicy {
	if cfg := cm.Get(); cfg != nil {
		return cfg.Runtime.Edge
	}
	return types.EdgeRuntimePolicy{}
}

func (cm *ConfigManager) AnalyticsPolicy() types.AnalyticsRuntimePolicy {
	if cfg := cm.Get(); cfg != nil {
		return cfg.Runtime.Analytics
	}
	return types.AnalyticsRuntimePolicy{}
}

func (cm *ConfigManager) Prefixes() []types.PrefixConfig {
	if cfg := cm.Get(); cfg != nil {
		return cfg.Prefixes
	}
	return nil
}

func (cm *ConfigManager) FindPrefix(prefix string) (*types.PrefixConfig, bool) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return nil, false
	}
	cfg := cm.Get()
	if cfg == nil {
		return nil, false
	}
	for i := range cfg.Prefixes {
		if cfg.Prefixes[i].Prefix == prefix {
			return &cfg.Prefixes[i], true
		}
	}
	return nil, false
}

func (cm *ConfigManager) FindService(prefix, service string) (*types.PrefixConfig, *types.ServiceConfig, bool) {
	prefixCfg, ok := cm.FindPrefix(prefix)
	if !ok {
		return nil, nil, false
	}
	service = strings.TrimSpace(service)
	for i := range prefixCfg.Services {
		if prefixCfg.Services[i].ServiceID == service {
			return prefixCfg, &prefixCfg.Services[i], true
		}
	}
	return nil, nil, false
}
