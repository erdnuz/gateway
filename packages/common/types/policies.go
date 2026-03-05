package types

import "time"

// TransformConfig modifies the request before it hits upstream or the client.
type TransformConfig struct {
	StripPrefix bool              `bson:"strip_prefix" json:"strip_prefix"`
	AddHeaders  map[string]string `bson:"add_headers" json:"add_headers"`   // Injected for Upstream
	HideHeaders []string          `bson:"hide_headers" json:"hide_headers"` // Removed from Client response
}

// SecurityConfig handles perimeter defense.
type SecurityConfig struct {
	MaxBodySize int64       `bson:"max_body_size" json:"max_body_size"`
	AllowedIPs  []string    `bson:"allowed_ips" json:"allowed_ips"`
	BlockedIPs  []string    `bson:"blocked_ips" json:"blocked_ips"`
	CORS        *CORSConfig `bson:"cors" json:"cors,omitempty"`
}

type CORSConfig struct {
	AllowedOrigins []string `bson:"allowed_origins" json:"allowed_origins"`
	AllowedMethods []string `bson:"allowed_methods" json:"allowed_methods"`
	MaxAge         int      `bson:"max_age" json:"max_age"`
}

// TierConfig defines usage quotas and billing weights.
type TierConfig struct {
	TierID     string            `bson:"tier_id" json:"tier_id"`
	Quota      uint64            `bson:"quota" json:"quota"`
	TokenCosts map[string]uint64 `bson:"token_costs" json:"token_costs"` // Weight-based billing
}

// ResilienceConfig handles retries and circuit breaking.
type ResilienceConfig struct {
	MaxRetries    int           `bson:"max_retries" json:"max_retries"`
	RetryInterval time.Duration `bson:"retry_interval" json:"retry_interval"`
	ReadTimeout   time.Duration `bson:"read_timeout" json:"read_timeout"`

	HealthCheck *HealthCheck `bson:"health_check" json:"health_check,omitempty"`
}

// CacheConfig manages Edge-level response caching.
type CacheConfig struct {
	Enabled      bool          `bson:"enabled" json:"enabled"`
	TTL          time.Duration `bson:"ttl" json:"ttl"`
	CacheKey     string        `bson:"cache_key" json:"cache_key"`         // e.g., "$method:$path"
	InvalidateOn []string      `bson:"invalidate_on" json:"invalidate_on"` // e.g., ["POST", "PUT"]
}

// HealthCheck defines how the Edge monitors the Upstream.
type HealthCheck struct {
	Enabled  bool          `bson:"enabled" json:"enabled"`
	Path     string        `bson:"path" json:"path"`
	Interval time.Duration `bson:"interval" json:"interval"`
	Timeout  time.Duration `bson:"timeout" json:"timeout"`
}

// AnalyticsConfig controls the sampling and shipping of data.
type AnalyticsConfig struct {
	Enabled          bool          `bson:"enabled" json:"enabled"`
	FlushingInterval time.Duration `bson:"flushing_interval" json:"flushing_interval"`
	SamplingRate     float64       `bson:"sampling_rate" json:"sampling_rate"` // 0.0 to 1.0
}
