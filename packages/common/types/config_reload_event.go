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

// DefaultConfigReloadSubject is the NATS subject carrying ConfigReloadEvent.
// NATS is the bus every service shares; Redis pub/sub is not, because each
// edge owns a private Redis.
const DefaultConfigReloadSubject = "gate.config.reload"

type ConfigReloadEvent struct {
	Version int `json:"version"`
	// BootID identifies the hub process that assigned Epoch. Epochs restart
	// at 1 when the hub restarts, so ordering only holds within one BootID.
	BootID        string              `json:"boot_id,omitempty"`
	Epoch         uint64              `json:"epoch"`
	ReloadedAtUTC string              `json:"reloaded_at_utc"`
	Invalidations ConfigInvalidations `json:"invalidations,omitempty"`
}
