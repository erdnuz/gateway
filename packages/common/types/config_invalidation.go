package types

// ConfigInvalidationScope names cached responses a config change made stale.
type ConfigInvalidationScope struct {
	Prefix    string `json:"prefix"`
	ServiceID string `json:"service_id,omitempty"`
	Mode      string `json:"mode,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type ConfigInvalidations struct {
	ResponseCache []ConfigInvalidationScope `json:"response_cache,omitempty"`
}
