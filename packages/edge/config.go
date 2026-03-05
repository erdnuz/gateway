package edge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"

	"gateway/packages/common/types"
)

type ConfigManager struct {
	active atomic.Pointer[types.GatewayConfig]
}

// NewConfigManager performs a one-time hydration from the Hub on startup.
func NewConfigManager(hubAddr string) (*ConfigManager, error) {
	cm := &ConfigManager{}

	// Initial (and only) Hydration
	cfg, err := fetchConfig(hubAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to hydrate config on startup: %w", err)
	}

	cm.active.Store(cfg)
	return cm, nil
}

// Get returns the immutable GatewayConfig.
func (cm *ConfigManager) Get() *types.GatewayConfig {
	return cm.active.Load()
}

// Helper to find a specific prefix from the immutable state.
func (cm *ConfigManager) GetPrefix(path string) (*types.PrefixConfig, bool) {
	cfg := cm.Get()
	if cfg == nil {
		return nil, false
	}

	for i := range cfg.Prefixes {
		if cfg.Prefixes[i].Prefix == path {
			return &cfg.Prefixes[i], true
		}
	}
	return nil, false
}

func fetchConfig(hubAddr string) (*types.GatewayConfig, error) {
	resp, err := http.Get(hubAddr + "/config")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hub returned status: %d", resp.StatusCode)
	}

	var cfg types.GatewayConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
