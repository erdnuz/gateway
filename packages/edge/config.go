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
)

type ConfigManager struct {
	active    atomic.Pointer[types.GatewayConfig]
	hubAddr   string
	authToken string
	client    *http.Client
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

// Helper to find a specific service config from the immutable state.
func (cm *ConfigManager) GetServiceConfig(prefix, service string) (*types.ServiceConfig, error) {
	prefixCfg, found := cm.GetPrefix(prefix)
	if !found {
		return nil, fmt.Errorf("prefix not found: %s", prefix)
	}

	for i := range prefixCfg.Services {
		if prefixCfg.Services[i].ServiceID == service {
			return &prefixCfg.Services[i], nil
		}
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
			case <-ctx.Done():
				return
			}
		}
	}()
}
