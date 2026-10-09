package edge

import (
	"fmt"
	"sync/atomic"

	"gateway/packages/common/types"
)

// ConfigManager holds the edge's current config. The hub pushes snapshots
// (Apply); requests read them lock-free.
type ConfigManager struct {
	active atomic.Pointer[configIndex]
}

type configIndex struct {
	cfg      *types.GatewayConfig
	prefixes map[string]*types.PrefixConfig
	services map[string]*types.ServiceConfig
}

// NewConfigManager starts with cfg, or with no routes when cfg is nil.
func NewConfigManager(cfg *types.GatewayConfig) *ConfigManager {
	cm := &ConfigManager{}
	if cfg == nil {
		cfg = &types.GatewayConfig{}
	}
	cm.Apply(cfg)
	return cm
}

// Apply swaps in a new config atomically.
func (cm *ConfigManager) Apply(cfg *types.GatewayConfig) {
	idx := &configIndex{cfg: cfg, prefixes: map[string]*types.PrefixConfig{}, services: map[string]*types.ServiceConfig{}}
	for i := range cfg.Prefixes {
		p := &cfg.Prefixes[i]
		idx.prefixes[p.Prefix] = p
		for j := range p.Services {
			idx.services[p.Prefix+"\x00"+p.Services[j].ServiceID] = &p.Services[j]
		}
	}
	cm.active.Store(idx)
}

// Get returns the current config.
func (cm *ConfigManager) Get() *types.GatewayConfig {
	if idx := cm.active.Load(); idx != nil {
		return idx.cfg
	}
	return nil
}

func (cm *ConfigManager) EdgePolicy() types.EdgeRuntimePolicy {
	if cfg := cm.Get(); cfg != nil {
		return cfg.Runtime.Edge
	}
	return types.EdgeRuntimePolicy{}
}

// GetPrefix finds a route group.
func (cm *ConfigManager) GetPrefix(prefix string) (*types.PrefixConfig, bool) {
	idx := cm.active.Load()
	if idx == nil {
		return nil, false
	}
	p, ok := idx.prefixes[prefix]
	return p, ok
}

// GetServiceConfig finds a service of a route group.
func (cm *ConfigManager) GetServiceConfig(prefix, service string) (*types.ServiceConfig, error) {
	idx := cm.active.Load()
	if idx == nil {
		return nil, fmt.Errorf("prefix not found: %s", prefix)
	}
	if _, ok := idx.prefixes[prefix]; !ok {
		return nil, fmt.Errorf("prefix not found: %s", prefix)
	}
	if svc, ok := idx.services[prefix+"\x00"+service]; ok {
		return svc, nil
	}
	return nil, fmt.Errorf("service not found: %s", service)
}

// GetTier finds a tier of a service by ID.
func GetTier(svc *types.ServiceConfig, tierID string) (*types.TierConfig, bool) {
	for i := range svc.Tiers {
		if svc.Tiers[i].TierID == tierID {
			return &svc.Tiers[i], true
		}
	}
	return nil, false
}
