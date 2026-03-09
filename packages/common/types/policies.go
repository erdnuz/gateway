package types

import "time"

// TransformConfig modifies the request before it hits upstream or the client.
type TransformConfig struct {
	StripPrefix bool              `bson:"strip_prefix" json:"strip_prefix"`
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

type FailureConfig struct {
	// Strategy: "fail-open" (allow) or "fail-closed" (block 503)
	FailOpen bool `bson:"fail_open" json:"fail_open"`

	FallbackTier string `bson:"fallback_tier" json:"fallback_tier"`

	// Hub resilience policy controls behavior when control-plane dependencies fail.
	Hub HubFailurePolicy `bson:"hub" json:"hub"`

	// Upstream resilience policy controls retries/fallback behavior for proxy calls.
	Upstream UpstreamFailurePolicy `bson:"upstream" json:"upstream"`
}

type HubFailurePolicy struct {
	// TierLookupStrategy determines behavior when tier lookup fails:
	// - "fail-closed" (default): return 500
	// - "default-tier": use DefaultTier (or FailureConfig.FallbackTier)
	// - "stale-or-default": try stale tier first, then default tier
	TierLookupStrategy string `bson:"tier_lookup_strategy" json:"tier_lookup_strategy"`

	// DefaultTier used when TierLookupStrategy permits fallback.
	DefaultTier string `bson:"default_tier" json:"default_tier"`

	// StaleTierMaxAge bounds how old a stale tier entry can be when used.
	StaleTierMaxAge time.Duration `bson:"stale_tier_max_age" json:"stale_tier_max_age"`

	// AllowOnRateServiceError allows request pass-through when local rate service fails.
	AllowOnRateServiceError bool `bson:"allow_on_rate_service_error" json:"allow_on_rate_service_error"`
}

type UpstreamFailurePolicy struct {
	// Mode controls terminal behavior after retries:
	// - "fail-closed" (default): return service unavailable
	// - "fail-open": return configured fallback response
	Mode string `bson:"mode" json:"mode"`

	// MaxRetries is the number of additional attempts after the first proxy attempt.
	MaxRetries int `bson:"max_retries" json:"max_retries"`

	// RetryBackoff applies between retry attempts.
	RetryBackoff time.Duration `bson:"retry_backoff" json:"retry_backoff"`

	// RetryOnStatuses contains upstream status codes that should trigger retries.
	RetryOnStatuses []int `bson:"retry_on_statuses" json:"retry_on_statuses"`

	// RetryNonIdempotentMethods enables retries for methods like POST/PATCH.
	RetryNonIdempotentMethods bool `bson:"retry_non_idempotent_methods" json:"retry_non_idempotent_methods"`

	// AttemptTimeout applies a per-attempt timeout (0 uses edge server default client timeout).
	AttemptTimeout time.Duration `bson:"attempt_timeout" json:"attempt_timeout"`

	// Optional fallback response used when Mode is "fail-open".
	FallbackStatusCode int               `bson:"fallback_status_code" json:"fallback_status_code"`
	FallbackBody       string            `bson:"fallback_body" json:"fallback_body"`
	FallbackHeaders    map[string]string `bson:"fallback_headers" json:"fallback_headers"`
}

func (fc FailureConfig) EffectiveHubPolicy() HubFailurePolicy {
	hp := fc.Hub
	if hp.TierLookupStrategy == "" {
		hp.TierLookupStrategy = "fail-closed"
	}
	if hp.DefaultTier == "" {
		hp.DefaultTier = fc.FallbackTier
	}
	if hp.StaleTierMaxAge <= 0 {
		hp.StaleTierMaxAge = 6 * time.Hour
	}
	return hp
}

func (fc FailureConfig) EffectiveUpstreamPolicy() UpstreamFailurePolicy {
	up := fc.Upstream
	if up.Mode == "" {
		if fc.FailOpen {
			up.Mode = "fail-open"
		} else {
			up.Mode = "fail-closed"
		}
	}
	if up.MaxRetries < 0 {
		up.MaxRetries = 0
	}
	if up.RetryBackoff <= 0 {
		up.RetryBackoff = 50 * time.Millisecond
	}
	if up.FallbackStatusCode <= 0 {
		up.FallbackStatusCode = 503
	}
	if len(up.FallbackHeaders) == 0 {
		up.FallbackHeaders = map[string]string{}
	}
	if _, ok := up.FallbackHeaders["X-Upstream-Error"]; !ok {
		up.FallbackHeaders["X-Upstream-Error"] = "true"
	}
	if up.FallbackBody == "" {
		up.FallbackBody = "Service temporarily degraded"
	}
	return up
}
