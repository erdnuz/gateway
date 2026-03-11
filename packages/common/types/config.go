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

// PrefixConfig groups services under a common path (e.g., /v1).
type PrefixConfig struct {
	Prefix      string          `bson:"prefix" json:"prefix"`
	QuotaPeriod time.Duration   `bson:"quota_period" json:"quota_period"` // Global reset window
	Services    []ServiceConfig `bson:"services" json:"services"`
}

func (p *PrefixConfig) Validate() error {
	if p == nil {
		return errConfig("prefix is nil")
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
