package hub

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"

	"gateway/packages/common/types"
)

type ConfigManager struct {
	filePath string
	active   atomic.Pointer[types.GatewayConfig]
}

// NewConfigManager initializes the L1 cache from a JSON file.
func NewConfigManager(filePath string) (*ConfigManager, error) {
	cm := &ConfigManager{filePath: filePath}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	var cfg types.GatewayConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		// This is the most important change for debugging
		return nil, fmt.Errorf("failed to decode %s: %w", filePath, err)
	}

	cm.active.Store(&cfg)
	return cm, nil
}

// Get returns the read-only configuration.
// Highly efficient O(1) with no mutex contention.
func (cm *ConfigManager) Get() *types.GatewayConfig {
	return cm.active.Load()
}
