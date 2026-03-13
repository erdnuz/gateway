package edge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
)

type ConfigManager struct {
	active    atomic.Pointer[types.GatewayConfig]
	indexes   atomic.Pointer[configIndexes]
	hubAddr   string
	authToken string
	client    *http.Client
}

type configIndexes struct {
	cfg          *types.GatewayConfig
	prefixByPath map[string]*types.PrefixConfig
	serviceByKey map[string]*types.ServiceConfig
}

func buildConfigIndexes(cfg *types.GatewayConfig) *configIndexes {
	if cfg == nil {
		return &configIndexes{
			prefixByPath: map[string]*types.PrefixConfig{},
			serviceByKey: map[string]*types.ServiceConfig{},
		}
	}
	prefixByPath := make(map[string]*types.PrefixConfig, len(cfg.Prefixes))
	serviceCount := 0
	for i := range cfg.Prefixes {
		serviceCount += len(cfg.Prefixes[i].Services)
	}
	serviceByKey := make(map[string]*types.ServiceConfig, serviceCount)
	for i := range cfg.Prefixes {
		p := &cfg.Prefixes[i]
		prefixByPath[p.Prefix] = p
		for j := range p.Services {
			svc := &p.Services[j]
			serviceByKey[serviceLookupKey(p.Prefix, svc.ServiceID)] = svc
		}
	}
	return &configIndexes{cfg: cfg, prefixByPath: prefixByPath, serviceByKey: serviceByKey}
}

func serviceLookupKey(prefix, service string) string {
	return prefix + "\x00" + service
}

func (cm *ConfigManager) ensureIndexes(cfg *types.GatewayConfig) *configIndexes {
	idx := cm.indexes.Load()
	if idx != nil && idx.cfg == cfg {
		return idx
	}
	rebuilt := buildConfigIndexes(cfg)
	cm.indexes.Store(rebuilt)
	return rebuilt
}

// NewConfigManager performs a one-time hydration from the Hub on startup.
func NewConfigManager(hubAddr, authToken string) (*ConfigManager, error) {
	return NewConfigManagerWithClient(hubAddr, authToken, nil)
}

func NewConfigManagerWithClient(hubAddr, authToken string, client *http.Client) (*ConfigManager, error) {
	return NewConfigManagerWithFallback(hubAddr, authToken, client, "")
}

func NewConfigManagerWithFallback(hubAddr, authToken string, client *http.Client, bootstrapFile string) (*ConfigManager, error) {
	if client == nil {
		client = newHubHTTPClient(0)
	}
	cm := &ConfigManager{hubAddr: hubAddr, authToken: authToken, client: client}

	// Initial hydration from hub, with optional bootstrap fallback.
	cfg, err := cm.fetchConfig()
	if err != nil {
		if bootstrapFile == "" {
			return nil, fmt.Errorf("failed to hydrate config on startup: %w", err)
		}
		fallbackCfg, fallbackErr := loadConfigFromFile(bootstrapFile)
		if fallbackErr != nil {
			return nil, fmt.Errorf("failed to hydrate config on startup from hub (%v) and bootstrap file (%v)", err, fallbackErr)
		}
		cfg = fallbackCfg
		log.Printf("edge config bootstrap fallback activated: using %s", bootstrapFile)
	}

	cm.active.Store(cfg)
	cm.indexes.Store(buildConfigIndexes(cfg))
	return cm, nil
}

func loadConfigFromFile(path string) (*types.GatewayConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	bytes, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var cfg types.GatewayConfig
	if err := json.Unmarshal(bytes, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Get returns the immutable GatewayConfig.
func (cm *ConfigManager) Get() *types.GatewayConfig {
	return cm.active.Load()
}

func (cm *ConfigManager) EdgePolicy() types.EdgeRuntimePolicy {
	if cfg := cm.Get(); cfg != nil {
		return cfg.Runtime.Edge
	}
	return types.EdgeRuntimePolicy{}
}

func (cm *ConfigManager) AnalyticsPolicy() types.AnalyticsRuntimePolicy {
	if cfg := cm.Get(); cfg != nil {
		return cfg.Runtime.Analytics
	}
	return types.AnalyticsRuntimePolicy{}
}

func (cm *ConfigManager) Prefixes() []types.PrefixConfig {
	if cfg := cm.Get(); cfg != nil {
		return cfg.Prefixes
	}
	return nil
}

// TierUpdatesSubject is the only hub runtime setting needed by edge for tier
// invalidation subscriptions.
func (cm *ConfigManager) TierUpdatesSubject() string {
	if cfg := cm.Get(); cfg != nil {
		return cfg.Runtime.Hub.TierUpdatesSubject
	}
	return ""
}

// Helper to find a specific prefix from the immutable state.
func (cm *ConfigManager) GetPrefix(path string) (*types.PrefixConfig, bool) {
	cfg := cm.Get()
	if cfg == nil {
		return nil, false
	}
	idx := cm.ensureIndexes(cfg)
	prefix, ok := idx.prefixByPath[path]
	return prefix, ok
}

// Helper to find a specific service config from the immutable state.
func (cm *ConfigManager) GetServiceConfig(prefix, service string) (*types.ServiceConfig, error) {
	cfg := cm.Get()
	if cfg == nil {
		return nil, fmt.Errorf("prefix not found: %s", prefix)
	}
	idx := cm.ensureIndexes(cfg)
	if _, found := idx.prefixByPath[prefix]; !found {
		return nil, fmt.Errorf("prefix not found: %s", prefix)
	}
	if svc, ok := idx.serviceByKey[serviceLookupKey(prefix, service)]; ok {
		return svc, nil
	}
	return nil, fmt.Errorf("service not found: %s", service)
}

func GetTier(svc *types.ServiceConfig, tierID string) (*types.TierConfig, bool) {
	for i := range svc.Tiers {
		if svc.Tiers[i].TierID == tierID {
			return &svc.Tiers[i], true
		}
	}
	return nil, false
}

func (cm *ConfigManager) fetchConfig() (*types.GatewayConfig, error) {
	req, err := http.NewRequest(http.MethodGet, cm.hubAddr+"/config", nil)
	if err != nil {
		return nil, err
	}
	if cm.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+cm.authToken)
	}

	resp, err := cm.client.Do(req)
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

// StartAutoRefresh periodically refreshes config from hub and atomically swaps
// the active snapshot on successful fetches.
func (cm *ConfigManager) StartAutoRefresh(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				cfg, err := cm.fetchConfig()
				if err != nil {
					log.Printf("edge config refresh failed: %v", err)
					continue
				}
				cm.active.Store(cfg)
				cm.indexes.Store(buildConfigIndexes(cfg))
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (cm *ConfigManager) RefreshConfig() error {
	cfg, err := cm.fetchConfig()
	if err != nil {
		return err
	}
	cm.active.Store(cfg)
	cm.indexes.Store(buildConfigIndexes(cfg))
	return nil
}

func (cm *ConfigManager) SetHTTPClient(client *http.Client) {
	if client == nil {
		return
	}
	cm.client = client
}

func (cm *ConfigManager) StartConfigReloadSubscriber(ctx context.Context, rdb *redis.Client, channel string) {
	if rdb == nil {
		return
	}
	if channel == "" {
		channel = types.DefaultConfigReloadChannel
	}
	go func() {
		sub := rdb.Subscribe(ctx, channel)
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					return
				}
				if err := cm.RefreshConfig(); err != nil {
					log.Printf("edge config pubsub refresh failed: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
