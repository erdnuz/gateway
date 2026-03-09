package hub

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"gateway/packages/common/types"
	testutils "gateway/testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// helper to create a simple ConfigManager with default config
func makeTestConfigManager() *ConfigManager {
	cfg := testutils.NewTestGatewayConfig()
	cm := &ConfigManager{}
	cm.active.Store(cfg)
	return cm
}

func TestHubRateManager_DeltaListenerAndSOT(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	cfgMgr := makeTestConfigManager()
	rm := NewRateManager(rdb, cfgMgr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rm.StartDeltaListener(ctx); err != nil {
		t.Fatalf("StartDeltaListener: %v", err)
	}

	// subscribe to SOT channel so we can verify message
	sub := rdb.Subscribe(ctx, RateSOTChannel)
	defer sub.Close()

	// publish a delta message
	ud := types.UsageDelta{Prefix: "/api/v1", APIKey: "user1", Delta: 7}
	b, _ := json.Marshal(ud)
	if err := rdb.Publish(ctx, RateUpdateChannel, b).Err(); err != nil {
		t.Fatal(err)
	}

	// wait for sot message or timeout
	select {
	case msg := <-sub.Channel():
		var sot types.SOTMsg
		json.Unmarshal([]byte(msg.Payload), &sot)
		if sot.Prefix != ud.Prefix || sot.APIKey != ud.APIKey {
			t.Fatalf("unexpected SOT payload: %+v", sot)
		}
		if sot.Total <= 0 {
			t.Fatalf("expected positive total, got %d", sot.Total)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("did not receive SOT message in time")
	}
}
