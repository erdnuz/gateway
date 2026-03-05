package types

import "time"

// TransformConfig modifies the request before it hits upstream or the client.
type TransformConfig struct {
	AddHeaders  map[string]string `bson:"add_headers" json:"add_headers"`   // Injected for Upstream
	HideHeaders []string          `bson:"hide_headers" json:"hide_headers"` // Removed from Client response
}

type CORSConfig struct {
	AllowedOrigins []string      `bson:"allowed_origins" json:"allowed_origins"`
	AllowedMethods []string      `bson:"allowed_methods" json:"allowed_methods"`
	MaxAge         time.Duration `bson:"max_age" json:"max_age"`
}

// TierConfig defines usage quotas and billing weights.
type TierConfig struct {
	TierID string `bson:"tier_id" json:"tier_id"`
	Quota  uint32 `bson:"quota" json:"quota"`

	// Cost per request type (used for billing or quota deduction)
	GetCost    uint8 `bson:"get_cost" json:"get_cost"`       // Cost per GET request
	PostCost   uint8 `bson:"post_cost" json:"post_cost"`     // Cost per POST request
	PutCost    uint8 `bson:"put_cost" json:"put_cost"`       // Cost per PUT request
	DeleteCost uint8 `bson:"delete_cost" json:"delete_cost"` // Cost per DELETE request
	OtherCost  uint8 `bson:"other_cost" json:"other_cost"`   // Cost per other request types
}

// ResilienceConfig handles retries and circuit breaking.
type ResilienceConfig struct {
	MaxRetries    uint8         `bson:"max_retries" json:"max_retries"`
	RetryInterval time.Duration `bson:"retry_interval" json:"retry_interval"`
	ReadTimeout   time.Duration `bson:"read_timeout" json:"read_timeout"`
}

// CacheConfig manages Edge-level response caching.
type CacheConfig struct {
	Enabled  bool          `bson:"enabled" json:"enabled"`
	TTL      time.Duration `bson:"ttl" json:"ttl"`
	CacheKey string        `bson:"cache_key" json:"cache_key"` // e.g., "$method:$path"
}

// AnalyticsConfig controls the sampling and shipping of data.
type AnalyticsConfig struct {
	Enabled          bool          `bson:"enabled" json:"enabled"`
	FlushingInterval time.Duration `bson:"flushing_interval" json:"flushing_interval"`
	SamplingRate     float64       `bson:"sampling_rate" json:"sampling_rate"` // 0.0 to 1.0
}
