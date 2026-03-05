package types

// api.go contains shared data structures for API communication between Edge and Hub, as well as any other services that need to exchange structured data. This includes request/response formats, synchronization payloads, and any other contract definitions that ensure consistent communication across the system.

type BatchRequest struct {
	EdgeID         string            `json:"edge_id"`         // Regional identifier (e.g., "us-east-1")
	ServicePrefix  string            `json:"service_prefix"`  // The matched route prefix (e.g., "/v1/users")
	ServiceID      string            `json:"service_id"`      // Target service identifier
	UsageDeltas    map[string]uint64 `json:"usage_deltas"`    // e.g., {"user_tier:free": 10}
	AnalyticsBatch []AnalyticsEntry  `json:"analytics_batch"` // List of analytics entries
}

type BatchResponse struct {
	TrueUsage map[string]uint64 `json:"true_usage"` // e.g., {"user_tier:free": 100} - Updated counts after applying this batch
}
