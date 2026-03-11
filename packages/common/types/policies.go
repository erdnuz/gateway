package types

import (
	"fmt"
	"strings"
	"time"
)

const (
	AnalyticsModeLite  = "lite"
	AnalyticsModeHeavy = "heavy"
)

func errConfig(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

// TransformConfig modifies the request before it hits upstream or the client.
type TransformConfig struct {
	StripPrefix bool              `bson:"strip_prefix" json:"strip_prefix"`
	AddHeaders  map[string]string `bson:"add_headers" json:"add_headers"`   // Injected for Upstream
	HideHeaders []string          `bson:"hide_headers" json:"hide_headers"` // Removed from Client response
}

type CORSConfig struct {
	AllowedOrigins []string      `bson:"allowed_origins" json:"allowed_origins"`
	AllowedMethods []string      `bson:"allowed_methods" json:"allowed_methods"`
	AllowedHeaders []string      `bson:"allowed_headers" json:"allowed_headers,omitempty"`
	MaxAge         time.Duration `bson:"max_age" json:"max_age"`
}

type RuntimePolicy struct {
	Hub       HubRuntimePolicy       `bson:"hub" json:"hub,omitempty"`
	Edge      EdgeRuntimePolicy      `bson:"edge" json:"edge,omitempty"`
	Analytics AnalyticsRuntimePolicy `bson:"analytics" json:"analytics,omitempty"`
}

type HubRuntimePolicy struct {
	CORSAllowedHeaders  []string      `bson:"cors_allowed_headers" json:"cors_allowed_headers,omitempty"`
	CORSAllowedMethods  []string      `bson:"cors_allowed_methods" json:"cors_allowed_methods,omitempty"`
	CORSPreflightMaxAge time.Duration `bson:"cors_preflight_max_age" json:"cors_preflight_max_age,omitempty"`
	APIKeyPattern       string        `bson:"api_key_pattern" json:"api_key_pattern,omitempty"`
	MaxDelta            int64         `bson:"max_delta" json:"max_delta,omitempty"`
	QueueWorkers        int           `bson:"queue_workers" json:"queue_workers,omitempty"`
	QueueSubmitTimeout  time.Duration `bson:"queue_submit_timeout" json:"queue_submit_timeout,omitempty"`
	QueueRetryMax       int           `bson:"queue_retry_max" json:"queue_retry_max,omitempty"`
	QueueRetryBackoff   time.Duration `bson:"queue_retry_backoff" json:"queue_retry_backoff,omitempty"`
	TierUpdatesSubject  string        `bson:"tier_updates_subject" json:"tier_updates_subject,omitempty"`
	HTTPReadTimeout     time.Duration `bson:"http_read_timeout" json:"http_read_timeout,omitempty"`
	HTTPWriteTimeout    time.Duration `bson:"http_write_timeout" json:"http_write_timeout,omitempty"`
	HTTPShutdownTimeout time.Duration `bson:"http_shutdown_timeout" json:"http_shutdown_timeout,omitempty"`
}

type EdgeRuntimePolicy struct {
	AnalyticsBufferSize     int           `bson:"analytics_buffer_size" json:"analytics_buffer_size,omitempty"`
	AnalyticsPublishTimeout time.Duration `bson:"analytics_publish_timeout" json:"analytics_publish_timeout,omitempty"`
	RateHardThresholdPct    float64       `bson:"rate_hard_threshold_pct" json:"rate_hard_threshold_pct,omitempty"`
	RateLeaseSize           int64         `bson:"rate_lease_size" json:"rate_lease_size,omitempty"`
	RateLowWaterPct         float64       `bson:"rate_low_water_pct" json:"rate_low_water_pct,omitempty"`
	CacheMaxObjectBytes     int64         `bson:"cache_max_object_bytes" json:"cache_max_object_bytes,omitempty"`
	UpstreamClientTimeout   time.Duration `bson:"upstream_client_timeout" json:"upstream_client_timeout,omitempty"`
	UpstreamMaxIdleConns    int           `bson:"upstream_max_idle_conns" json:"upstream_max_idle_conns,omitempty"`
	UpstreamIdleConnTimeout time.Duration `bson:"upstream_idle_conn_timeout" json:"upstream_idle_conn_timeout,omitempty"`
	HubHTTPTimeout          time.Duration `bson:"hub_http_timeout" json:"hub_http_timeout,omitempty"`
	HTTPReadTimeout         time.Duration `bson:"http_read_timeout" json:"http_read_timeout,omitempty"`
	HTTPWriteTimeout        time.Duration `bson:"http_write_timeout" json:"http_write_timeout,omitempty"`
	HTTPIdleTimeout         time.Duration `bson:"http_idle_timeout" json:"http_idle_timeout,omitempty"`
}

type AnalyticsRuntimePolicy struct {
	ReadTimeout         time.Duration `bson:"read_timeout" json:"read_timeout,omitempty"`
	WriteTimeout        time.Duration `bson:"write_timeout" json:"write_timeout,omitempty"`
	IdleTimeout         time.Duration `bson:"idle_timeout" json:"idle_timeout,omitempty"`
	DefaultEventsLimit  int64         `bson:"default_events_limit" json:"default_events_limit,omitempty"`
	MaxEventsLimit      int64         `bson:"max_events_limit" json:"max_events_limit,omitempty"`
	MaxEventsOffset     int64         `bson:"max_events_offset" json:"max_events_offset,omitempty"`
	DefaultSummaryLimit int64         `bson:"default_summary_limit" json:"default_summary_limit,omitempty"`
	MaxSummaryLimit     int64         `bson:"max_summary_limit" json:"max_summary_limit,omitempty"`
	NATSSubject         string        `bson:"nats_subject" json:"nats_subject,omitempty"`
	NATSQueue           string        `bson:"nats_queue" json:"nats_queue,omitempty"`
	ConfigFetchTimeout  time.Duration `bson:"config_fetch_timeout" json:"config_fetch_timeout,omitempty"`
	ShutdownTimeout     time.Duration `bson:"shutdown_timeout" json:"shutdown_timeout,omitempty"`
}

func DefaultRuntimePolicy() RuntimePolicy {
	return RuntimePolicy{
		Hub: HubRuntimePolicy{
			CORSAllowedHeaders:  []string{"Authorization", "Content-Type", "X-API-Key"},
			CORSAllowedMethods:  []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			CORSPreflightMaxAge: 5 * time.Minute,
			APIKeyPattern:       "^[A-Za-z0-9]{3,128}$",
			MaxDelta:            10000,
			QueueWorkers:        2,
			QueueSubmitTimeout:  25 * time.Millisecond,
			QueueRetryMax:       1,
			QueueRetryBackoff:   10 * time.Millisecond,
			TierUpdatesSubject:  "tier.updates",
			HTTPReadTimeout:     5 * time.Second,
			HTTPWriteTimeout:    10 * time.Second,
			HTTPShutdownTimeout: 15 * time.Second,
		},
		Edge: EdgeRuntimePolicy{
			AnalyticsBufferSize:     1000,
			AnalyticsPublishTimeout: 2 * time.Second,
			RateHardThresholdPct:    0.9,
			RateLeaseSize:           100,
			RateLowWaterPct:         0.2,
			CacheMaxObjectBytes:     1 << 20,
			UpstreamClientTimeout:   30 * time.Second,
			UpstreamMaxIdleConns:    100,
			UpstreamIdleConnTimeout: 90 * time.Second,
			HubHTTPTimeout:          5 * time.Second,
			HTTPReadTimeout:         15 * time.Second,
			HTTPWriteTimeout:        15 * time.Second,
			HTTPIdleTimeout:         60 * time.Second,
		},
		Analytics: AnalyticsRuntimePolicy{
			ReadTimeout:         10 * time.Second,
			WriteTimeout:        10 * time.Second,
			IdleTimeout:         30 * time.Second,
			DefaultEventsLimit:  100,
			MaxEventsLimit:      5000,
			MaxEventsOffset:     1_000_000,
			DefaultSummaryLimit: 1000,
			MaxSummaryLimit:     10000,
			NATSSubject:         "analytics.events",
			NATSQueue:           "analytics-subscribers",
			ConfigFetchTimeout:  5 * time.Second,
			ShutdownTimeout:     5 * time.Second,
		},
	}
}

func (rp RuntimePolicy) Effective() RuntimePolicy {
	defaults := DefaultRuntimePolicy()
	if len(rp.Hub.CORSAllowedHeaders) == 0 {
		rp.Hub.CORSAllowedHeaders = defaults.Hub.CORSAllowedHeaders
	}
	if len(rp.Hub.CORSAllowedMethods) == 0 {
		rp.Hub.CORSAllowedMethods = defaults.Hub.CORSAllowedMethods
	}
	if rp.Hub.CORSPreflightMaxAge <= 0 {
		rp.Hub.CORSPreflightMaxAge = defaults.Hub.CORSPreflightMaxAge
	}
	if strings.TrimSpace(rp.Hub.APIKeyPattern) == "" {
		rp.Hub.APIKeyPattern = defaults.Hub.APIKeyPattern
	}
	if rp.Hub.MaxDelta <= 0 {
		rp.Hub.MaxDelta = defaults.Hub.MaxDelta
	}
	if rp.Hub.QueueWorkers <= 0 {
		rp.Hub.QueueWorkers = defaults.Hub.QueueWorkers
	}
	if rp.Hub.QueueSubmitTimeout <= 0 {
		rp.Hub.QueueSubmitTimeout = defaults.Hub.QueueSubmitTimeout
	}
	if rp.Hub.QueueRetryMax < 0 {
		rp.Hub.QueueRetryMax = defaults.Hub.QueueRetryMax
	}
	if rp.Hub.QueueRetryBackoff <= 0 {
		rp.Hub.QueueRetryBackoff = defaults.Hub.QueueRetryBackoff
	}
	if strings.TrimSpace(rp.Hub.TierUpdatesSubject) == "" {
		rp.Hub.TierUpdatesSubject = defaults.Hub.TierUpdatesSubject
	}
	if rp.Hub.HTTPReadTimeout <= 0 {
		rp.Hub.HTTPReadTimeout = defaults.Hub.HTTPReadTimeout
	}
	if rp.Hub.HTTPWriteTimeout <= 0 {
		rp.Hub.HTTPWriteTimeout = defaults.Hub.HTTPWriteTimeout
	}
	if rp.Hub.HTTPShutdownTimeout <= 0 {
		rp.Hub.HTTPShutdownTimeout = defaults.Hub.HTTPShutdownTimeout
	}

	if rp.Edge.AnalyticsBufferSize <= 0 {
		rp.Edge.AnalyticsBufferSize = defaults.Edge.AnalyticsBufferSize
	}
	if rp.Edge.AnalyticsPublishTimeout <= 0 {
		rp.Edge.AnalyticsPublishTimeout = defaults.Edge.AnalyticsPublishTimeout
	}
	if rp.Edge.RateHardThresholdPct <= 0 || rp.Edge.RateHardThresholdPct > 1 {
		rp.Edge.RateHardThresholdPct = defaults.Edge.RateHardThresholdPct
	}
	if rp.Edge.RateLeaseSize <= 0 {
		rp.Edge.RateLeaseSize = defaults.Edge.RateLeaseSize
	}
	if rp.Edge.RateLowWaterPct <= 0 || rp.Edge.RateLowWaterPct >= 1 {
		rp.Edge.RateLowWaterPct = defaults.Edge.RateLowWaterPct
	}
	if rp.Edge.CacheMaxObjectBytes <= 0 {
		rp.Edge.CacheMaxObjectBytes = defaults.Edge.CacheMaxObjectBytes
	}
	if rp.Edge.UpstreamClientTimeout <= 0 {
		rp.Edge.UpstreamClientTimeout = defaults.Edge.UpstreamClientTimeout
	}
	if rp.Edge.UpstreamMaxIdleConns <= 0 {
		rp.Edge.UpstreamMaxIdleConns = defaults.Edge.UpstreamMaxIdleConns
	}
	if rp.Edge.UpstreamIdleConnTimeout <= 0 {
		rp.Edge.UpstreamIdleConnTimeout = defaults.Edge.UpstreamIdleConnTimeout
	}
	if rp.Edge.HubHTTPTimeout <= 0 {
		rp.Edge.HubHTTPTimeout = defaults.Edge.HubHTTPTimeout
	}
	if rp.Edge.HTTPReadTimeout <= 0 {
		rp.Edge.HTTPReadTimeout = defaults.Edge.HTTPReadTimeout
	}
	if rp.Edge.HTTPWriteTimeout <= 0 {
		rp.Edge.HTTPWriteTimeout = defaults.Edge.HTTPWriteTimeout
	}
	if rp.Edge.HTTPIdleTimeout <= 0 {
		rp.Edge.HTTPIdleTimeout = defaults.Edge.HTTPIdleTimeout
	}

	if rp.Analytics.ReadTimeout <= 0 {
		rp.Analytics.ReadTimeout = defaults.Analytics.ReadTimeout
	}
	if rp.Analytics.WriteTimeout <= 0 {
		rp.Analytics.WriteTimeout = defaults.Analytics.WriteTimeout
	}
	if rp.Analytics.IdleTimeout <= 0 {
		rp.Analytics.IdleTimeout = defaults.Analytics.IdleTimeout
	}
	if rp.Analytics.DefaultEventsLimit <= 0 {
		rp.Analytics.DefaultEventsLimit = defaults.Analytics.DefaultEventsLimit
	}
	if rp.Analytics.MaxEventsLimit <= 0 {
		rp.Analytics.MaxEventsLimit = defaults.Analytics.MaxEventsLimit
	}
	if rp.Analytics.MaxEventsOffset <= 0 {
		rp.Analytics.MaxEventsOffset = defaults.Analytics.MaxEventsOffset
	}
	if rp.Analytics.DefaultSummaryLimit <= 0 {
		rp.Analytics.DefaultSummaryLimit = defaults.Analytics.DefaultSummaryLimit
	}
	if rp.Analytics.MaxSummaryLimit <= 0 {
		rp.Analytics.MaxSummaryLimit = defaults.Analytics.MaxSummaryLimit
	}
	if strings.TrimSpace(rp.Analytics.NATSSubject) == "" {
		rp.Analytics.NATSSubject = defaults.Analytics.NATSSubject
	}
	if strings.TrimSpace(rp.Analytics.NATSQueue) == "" {
		rp.Analytics.NATSQueue = defaults.Analytics.NATSQueue
	}
	if rp.Analytics.ConfigFetchTimeout <= 0 {
		rp.Analytics.ConfigFetchTimeout = defaults.Analytics.ConfigFetchTimeout
	}
	if rp.Analytics.ShutdownTimeout <= 0 {
		rp.Analytics.ShutdownTimeout = defaults.Analytics.ShutdownTimeout
	}

	return rp
}

func (rp RuntimePolicy) Validate() error {
	if err := rp.Hub.Validate(); err != nil {
		return errConfig("hub: %v", err)
	}
	if err := rp.Edge.Validate(); err != nil {
		return errConfig("edge: %v", err)
	}
	if err := rp.Analytics.Validate(); err != nil {
		return errConfig("analytics: %v", err)
	}
	return nil
}

func (p HubRuntimePolicy) Validate() error {
	if p.CORSPreflightMaxAge < 0 {
		return errConfig("cors_preflight_max_age must be >= 0")
	}
	if p.MaxDelta < 0 {
		return errConfig("max_delta must be >= 0")
	}
	if p.QueueWorkers < 0 {
		return errConfig("queue_workers must be >= 0")
	}
	if p.QueueSubmitTimeout < 0 || p.QueueRetryBackoff < 0 || p.HTTPReadTimeout < 0 || p.HTTPWriteTimeout < 0 || p.HTTPShutdownTimeout < 0 {
		return errConfig("timeouts must be >= 0")
	}
	if p.QueueRetryMax < 0 {
		return errConfig("queue_retry_max must be >= 0")
	}
	return nil
}

func (p EdgeRuntimePolicy) Validate() error {
	if p.AnalyticsBufferSize < 0 {
		return errConfig("analytics_buffer_size must be >= 0")
	}
	if p.AnalyticsPublishTimeout < 0 {
		return errConfig("analytics_publish_timeout must be >= 0")
	}
	if p.RateHardThresholdPct < 0 || p.RateHardThresholdPct > 1 {
		return errConfig("rate_hard_threshold_pct must be between 0 and 1")
	}
	if p.RateLeaseSize < 0 {
		return errConfig("rate_lease_size must be >= 0")
	}
	if p.RateLowWaterPct < 0 || p.RateLowWaterPct >= 1 {
		return errConfig("rate_low_water_pct must be >= 0 and < 1")
	}
	if p.CacheMaxObjectBytes < 0 {
		return errConfig("cache_max_object_bytes must be >= 0")
	}
	if p.UpstreamClientTimeout < 0 || p.UpstreamIdleConnTimeout < 0 || p.HubHTTPTimeout < 0 || p.HTTPReadTimeout < 0 || p.HTTPWriteTimeout < 0 || p.HTTPIdleTimeout < 0 {
		return errConfig("edge timeouts must be >= 0")
	}
	if p.UpstreamMaxIdleConns < 0 {
		return errConfig("upstream_max_idle_conns must be >= 0")
	}
	return nil
}

func (p AnalyticsRuntimePolicy) Validate() error {
	if p.ReadTimeout < 0 || p.WriteTimeout < 0 || p.IdleTimeout < 0 || p.ConfigFetchTimeout < 0 || p.ShutdownTimeout < 0 {
		return errConfig("timeouts must be >= 0")
	}
	if p.DefaultEventsLimit < 0 || p.MaxEventsLimit < 0 || p.MaxEventsOffset < 0 || p.DefaultSummaryLimit < 0 || p.MaxSummaryLimit < 0 {
		return errConfig("limits/offsets must be >= 0")
	}
	if p.MaxEventsLimit > 0 && p.DefaultEventsLimit > p.MaxEventsLimit {
		return errConfig("default_events_limit must be <= max_events_limit")
	}
	if p.MaxSummaryLimit > 0 && p.DefaultSummaryLimit > p.MaxSummaryLimit {
		return errConfig("default_summary_limit must be <= max_summary_limit")
	}
	return nil
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
	Mode             string        `bson:"mode" json:"mode,omitempty"`
	FlushingInterval time.Duration `bson:"flushing_interval" json:"flushing_interval"`
	SamplingRate     float64       `bson:"sampling_rate" json:"sampling_rate"` // 0.0 to 1.0
}

func (a AnalyticsConfig) EffectiveMode() string {
	if !a.Enabled {
		return AnalyticsModeLite
	}
	if a.Mode == "" {
		return AnalyticsModeHeavy
	}
	return a.Mode
}

func (a AnalyticsConfig) Validate() error {
	mode := a.EffectiveMode()
	if mode != AnalyticsModeLite && mode != AnalyticsModeHeavy {
		return errConfig("mode must be one of %q or %q", AnalyticsModeLite, AnalyticsModeHeavy)
	}
	if a.SamplingRate < 0 || a.SamplingRate > 1 {
		return errConfig("sampling_rate must be between 0 and 1")
	}
	return nil
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
