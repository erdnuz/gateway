package hub

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfigManager(t *testing.T) {
	filePath := "../../cmd/config.json"
	cm, err := NewConfigManager(filePath)
	assert.NoError(t, err, "should load config without error")

	cfg := cm.Get()
	// Print field names and values
	fmt.Printf("Config Data: %+v\n", cfg)
	assert.NotNil(t, cfg, "config should not be nil")
	assert.Equal(t, 1, len(cfg.Prefixes), "should have 1 prefix")
	assert.Equal(t, "v1", cfg.Prefixes[0].Prefix, "prefix should be 'v1'")
	assert.Equal(t, 10*time.Second, cfg.Prefixes[0].QuotaPeriod, "quota period should be 10 seconds")
	assert.Equal(t, 1, len(cfg.Prefixes[0].Services), "should have 1 service")
	assert.Equal(t, "auth-api", cfg.Prefixes[0].Services[0].ServiceID, "service ID should be 'auth-api'")
}

// for now just test config manager
