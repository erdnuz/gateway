package types

import "time"

// GatewayConfig is the root object held by the Source of Truth.
type GatewayConfig struct {
	Prefixes []PrefixConfig `bson:"prefixes" json:"prefixes"`

	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

// PrefixConfig groups services under a common path (e.g., /v1).
type PrefixConfig struct {
	Prefix      string          `bson:"prefix" json:"prefix"`
	QuotaPeriod time.Duration   `bson:"quota_period" json:"quota_period"` // Global reset window
	Services    []ServiceConfig `bson:"services" json:"services"`
}

// ServiceConfig defines the specific behavior for an individual upstream service.
type ServiceConfig struct {
	ServiceID string `bson:"_id" json:"service_id" validate:"required,kebab-case"`
	TargetURL string `bson:"target_url" json:"target_url"`
	Status    string `bson:"status" json:"status"` // "enabled", "disabled", "deprecated"

	Tiers map[string]TierConfig `bson:"tiers" json:"tiers"`

	// Middleware Policy Blocks
	Transform  TransformConfig   `bson:"transform" json:"transform"`
	Security   SecurityConfig    `bson:"security" json:"security"`
	Analytics  AnalyticsConfig   `bson:"analytics" json:"analytics"`
	Cache      *CacheConfig      `bson:"cache" json:"cache,omitempty"`
	Resilience *ResilienceConfig `bson:"resilience" json:"resilience,omitempty"`

	// Reliability Strategy
	FailOpen bool `bson:"fail_open" json:"fail_open"` // Allow traffic if Redis/Hub is unreachable?
}
