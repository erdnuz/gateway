package testutils

import (
	"context"
	"gateway/packages/common/types"
	"sync"
)

// MockConfigManager is a configurable mock for config management
type MockConfigManager struct {
	config *types.GatewayConfig
	getErr error
	mu     sync.RWMutex
}

// NewMockConfigManager creates a new mock config manager
func NewMockConfigManager(cfg *types.GatewayConfig) *MockConfigManager {
	return &MockConfigManager{
		config: cfg,
	}
}

// Get returns the config
func (m *MockConfigManager) Get() *types.GatewayConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// GetPrefix returns a prefix config
func (m *MockConfigManager) GetPrefix(prefix string) (*types.PrefixConfig, bool) {
	cfg := m.Get()
	if cfg == nil {
		return nil, false
	}
	for i := range cfg.Prefixes {
		if cfg.Prefixes[i].Prefix == prefix {
			return &cfg.Prefixes[i], true
		}
	}
	return nil, false
}

// GetServiceConfig returns a service config
func (m *MockConfigManager) GetServiceConfig(prefix, service string) (*types.ServiceConfig, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	prefixCfg, found := m.GetPrefix(prefix)
	if !found {
		return nil, ErrNotFound
	}
	for i := range prefixCfg.Services {
		if prefixCfg.Services[i].ServiceID == service {
			return &prefixCfg.Services[i], nil
		}
	}
	return nil, ErrNotFound
}

// SetError sets the error to return on Get
func (m *MockConfigManager) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getErr = err
}

// UpdateConfig updates the configuration
func (m *MockConfigManager) UpdateConfig(cfg *types.GatewayConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = cfg
}

// MockTierManager is a configurable mock for tier management
type MockTierManager struct {
	tiers map[string]string // key: "prefix:apiKey", value: tierID
	err   error
	mu    sync.RWMutex
}

// NewMockTierManager creates a new mock tier manager
func NewMockTierManager() *MockTierManager {
	return &MockTierManager{
		tiers: make(map[string]string),
	}
}

// GetUserTier returns the tier for a user
func (m *MockTierManager) GetUserTier(ctx context.Context, prefix, apiKey string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.err != nil {
		return "", m.err
	}
	key := prefix + ":" + apiKey
	if tier, ok := m.tiers[key]; ok {
		return tier, nil
	}
	return "free", nil // default tier
}

// SetTier sets the tier for a user
func (m *MockTierManager) SetTier(prefix, apiKey, tierID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := prefix + ":" + apiKey
	m.tiers[key] = tierID
}

// SetError sets the error to return
func (m *MockTierManager) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// MockRateManager is a configurable mock for rate limiting
type MockRateManager struct {
	limits map[string]int64 // key: "prefix:apiKey", value: current usage
	err    error
	mu     sync.RWMutex
}

// NewMockRateManager creates a new mock rate manager
func NewMockRateManager() *MockRateManager {
	return &MockRateManager{
		limits: make(map[string]int64),
	}
}

// Increment increments the rate limit counter
func (m *MockRateManager) Increment(ctx context.Context, prefix, apiKey string, limit, amount int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return 0, m.err
	}
	key := prefix + ":" + apiKey
	current := m.limits[key]
	current += amount
	m.limits[key] = current
	return current, nil
}

// GetUsage returns the current usage
func (m *MockRateManager) GetUsage(prefix, apiKey string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := prefix + ":" + apiKey
	return m.limits[key]
}

// SetError sets the error to return
func (m *MockRateManager) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Reset resets all usage counters
func (m *MockRateManager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.limits = make(map[string]int64)
}

// MockAnalyticsManager captures analytics entries for testing
type MockAnalyticsManager struct {
	entries []*types.AnalyticsEntry
	mu      sync.RWMutex
}

// NewMockAnalyticsManager creates a new mock analytics manager
func NewMockAnalyticsManager() *MockAnalyticsManager {
	return &MockAnalyticsManager{
		entries: make([]*types.AnalyticsEntry, 0),
	}
}

// Capture records an analytics entry
func (m *MockAnalyticsManager) Capture(entry *types.AnalyticsEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entry)
}

// ShouldSample returns whether to sample based on rate
func (m *MockAnalyticsManager) ShouldSample(rate float64) bool {
	return rate >= 1.0
}

// GetEntries returns all captured entries
func (m *MockAnalyticsManager) GetEntries() []*types.AnalyticsEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entries
}

// GetEntriesCount returns the number of entries
func (m *MockAnalyticsManager) GetEntriesCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

// Clear clears all entries
func (m *MockAnalyticsManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make([]*types.AnalyticsEntry, 0)
}
