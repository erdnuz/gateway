package types

import "time"

// GatewayConfig is the root object held by the Source of Truth.
type GatewayConfig struct {
	Prefixes []PrefixConfig `bson:"prefixes" json:"prefixes"`
	Runtime  RuntimePolicy  `bson:"runtime" json:"runtime,omitempty"`
}

func (cfg *GatewayConfig) Validate() error {
	if cfg == nil {
		return errConfig("config is nil")
	}
	for i := range cfg.Prefixes {
		if err := cfg.Prefixes[i].Validate(); err != nil {
			return errConfig("prefixes[%d]: %v", i, err)
		}
	}
	if err := cfg.Runtime.Validate(); err != nil {
		return errConfig("runtime: %v", err)
	}
	return nil
}

// LeasingConfig holds per-prefix lease parameters. Zero-value fields fall back
// to the global EdgeRuntimePolicy defaults via LeasingConfig.Effective().
type LeasingConfig struct {
	// LeaseQuantum is the number of tokens requested per lease grant.
	// Zero → falls back to EdgeRuntimePolicy.RateLeaseSize.
	LeaseQuantum int64 `bson:"lease_quantum" json:"lease_quantum,omitempty"`
	// LowWaterPct is the percentage of LeaseQuantum remaining that triggers an
	// async renewal (0–100). Zero → falls back to EdgeRuntimePolicy.RateLowWaterPct.
	LowWaterPct float64 `bson:"low_water_pct" json:"low_water_pct,omitempty"`
	// LeaseTTL is the Redis key TTL for the local token bucket.
	// Zero → computed from the prefix QuotaPeriod; falls back to 60s.
	LeaseTTL time.Duration `bson:"lease_ttl" json:"lease_ttl,omitempty"`
	// RenewalBuffer is the look-ahead window before expiry at which the temporal
	// trigger fires. Zero → 2s.
	RenewalBuffer time.Duration `bson:"renewal_buffer" json:"renewal_buffer,omitempty"`
}

// Effective merges per-prefix overrides with global EdgeRuntimePolicy defaults.
// quotaPeriod is the prefix quota period, used to derive a sensible LeaseTTL
// when the field is zero.
func (lc LeasingConfig) Effective(global EdgeRuntimePolicy, quotaPeriod time.Duration) LeasingConfig {
	if lc.LeaseQuantum <= 0 {
		lc.LeaseQuantum = global.RateLeaseSize
	}
	if lc.LowWaterPct <= 0 {
		lc.LowWaterPct = global.RateLowWaterPct
	}
	if lc.LeaseTTL <= 0 {
		if quotaPeriod > 0 {
			lc.LeaseTTL = quotaPeriod
		} else {
			lc.LeaseTTL = 60 * time.Second // 1-minute safe default
		}
	}
	if lc.RenewalBuffer <= 0 {
		lc.RenewalBuffer = 2 * time.Second
	}
	return lc
}

// PrefixConfig groups services under a common path (e.g., /v1).
type PrefixConfig struct {
	Prefix      string          `bson:"prefix" json:"prefix"`
	QuotaPeriod time.Duration   `bson:"quota_period" json:"quota_period"` // Global reset window
	Leasing     LeasingConfig   `bson:"leasing" json:"leasing,omitempty"`
	Services    []ServiceConfig `bson:"services" json:"services"`
}

func (p *PrefixConfig) Validate() error {
	if p == nil {
		return errConfig("prefix is nil")
	}
	if p.Leasing.LeaseQuantum < 0 {
		return errConfig("leasing.lease_quantum must be >= 0")
	}
	if p.Leasing.LowWaterPct < 0 {
		return errConfig("leasing.low_water_pct must be >= 0")
	}
	if p.Leasing.LeaseTTL < 0 {
		return errConfig("leasing.lease_ttl must be >= 0")
	}
	if p.Leasing.RenewalBuffer < 0 {
		return errConfig("leasing.renewal_buffer must be >= 0")
	}
	for i := range p.Services {
		if err := p.Services[i].Validate(); err != nil {
			return errConfig("services[%d]: %v", i, err)
		}
	}
	return nil
}

// ServiceConfig defines the specific behavior for an individual upstream service.
type ServiceConfig struct {
	ServiceID  string       `bson:"_id" json:"service_id" validate:"required,kebab-case"`
	TargetURL  string       `bson:"target_url" json:"target_url"`
	Tiers      []TierConfig `bson:"tiers" json:"tiers"`
	SafetyTier *TierConfig  `bson:"safety_tier" json:"safety_tier,omitempty"`

	// Middleware Policy Blocks
	Transform TransformConfig `bson:"transform" json:"transform"`
	CORS      *CORSConfig     `bson:"cors" json:"cors,omitempty"`
	Analytics AnalyticsConfig `bson:"analytics" json:"analytics"`
	Cache     *CacheConfig    `bson:"cache" json:"cache,omitempty"`

	// Reliability Strategy
	Failure FailureConfig `bson:"failure" json:"failure"`
}

func (s *ServiceConfig) Validate() error {
	if s == nil {
		return errConfig("service is nil")
	}
	if err := s.Analytics.Validate(); err != nil {
		return errConfig("analytics: %v", err)
	}
	if s.SafetyTier != nil {
		if s.SafetyTier.TierID == "" {
			return errConfig("safety_tier.tier_id is required")
		}
		if s.SafetyTier.Quota == 0 {
			return errConfig("safety_tier.quota must be > 0")
		}
	}
	return nil
}
