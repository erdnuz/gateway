package types

const ConfigReloadEventVersion = 1

type ConfigInvalidationScope struct {
	Prefix    string `json:"prefix"`
	ServiceID string `json:"service_id,omitempty"`
	Mode      string `json:"mode,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type ConfigInvalidations struct {
	ResponseCache []ConfigInvalidationScope `json:"response_cache,omitempty"`
}

type ConfigReloadEvent struct {
	Version       int                 `json:"version"`
	Epoch         uint64              `json:"epoch"`
	ReloadedAtUTC string              `json:"reloaded_at_utc"`
	Invalidations ConfigInvalidations `json:"invalidations,omitempty"`
}
