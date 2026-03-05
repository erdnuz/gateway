package types

import "time"

// AnalyticsEntry represents a single request log captured at the Edge.
type AnalyticsEntry struct {
	ServicePrefix string    `json:"service_prefix"` // The matched route prefix (e.g., "/v1/users")
	ServiceID     string    `json:"service_id"`     // The specific service handling the request (e.g., "get-users")
	Timestamp     time.Time `json:"timestamp"`      // Unix timestamp of the request

	// --- Client Context ---
	Method   string `json:"method"`    // GET, POST, etc.
	UserTier string `json:"user_tier"` // Tier active at time of request

	// --- Performance Metrics ---
	TotalLatency    time.Duration `json:"total_latency_ms"`    // Full round-trip time
	UpstreamLatency time.Duration `json:"upstream_latency_ms"` // Time spent waiting for upstream
	CacheHit        bool          `json:"cache_hit"`           // Served from local Redis?

	// --- Quota & Limits ---
	LimitUsed        uint64  `json:"limit_used"`          // Usage count after this request
	LimitUsedOfTotal float64 `json:"limit_used_of_total"` // Total allowed for this window

	// --- Data Volume & Status ---
	RequestSize  uint64 `json:"request_size_bytes"`
	ResponseSize uint64 `json:"response_size_bytes"`
	ResponseCode uint16 `json:"response_code"`
}
