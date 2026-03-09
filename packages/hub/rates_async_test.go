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

type fakeDeltaConsumer struct {
	rdb           *redis.Client
	updateChannel string
	apply         func(ctx context.Context, prefix, apiKey string, delta int64) error
}

func (c *fakeDeltaConsumer) Start(ctx context.Context) error {
	sub := c.rdb.Subscribe(ctx, c.updateChannel)
	ch := sub.Channel()
	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var ud types.UsageDelta
				if err := json.Unmarshal([]byte(msg.Payload), &ud); err != nil {
					continue
				}
				if ud.Delta <= 0 {
					continue
				}
				lastKey := "rate-last-seq:" + ud.Prefix + ":" + ud.APIKey
				last, err := c.rdb.Get(ctx, lastKey).Int64()
				if err != nil && err != redis.Nil {
					continue
				}
				if ud.Seq > 0 {
					if ud.Seq <= last {
						continue
					}
					_ = c.rdb.Set(ctx, lastKey, ud.Seq, 0).Err()
				}
				_ = c.apply(ctx, ud.Prefix, ud.APIKey, ud.Delta)
			case <-ctx.Done():
				_ = sub.Close()
				return
			}
		}
	}()
	return nil
}

// helper to create a simple ConfigManager with default config
func makeTestConfigManager() *ConfigManager {
	cfg := testutils.NewTestGatewayConfig()
	cm := &ConfigManager{}
	cm.active.Store(cfg)
	return cm
}

func TestHubRateManager_DeltaListenerAppliesDelta(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	cfgMgr := makeTestConfigManager()
	rm := NewRateManager(rdb, cfgMgr)
	rm.consumer = &fakeDeltaConsumer{rdb: rdb, updateChannel: types.DefaultRateUpdateChannel, apply: func(ctx context.Context, prefix, apiKey string, delta int64) error {
		_, err := rm.Increment(ctx, prefix, apiKey, delta)
		return err
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rm.StartDeltaListener(ctx); err != nil {
		t.Fatalf("StartDeltaListener: %v", err)
	}

	// publish a delta message
	ud := types.UsageDelta{Prefix: "/api/v1", APIKey: "user1", Delta: 7, Seq: 1}
	b, _ := json.Marshal(ud)
	if err := rdb.Publish(ctx, types.DefaultRateUpdateChannel, b).Err(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	total, err := rm.Increment(ctx, ud.Prefix, ud.APIKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 7 {
		t.Fatalf("expected total=7 after applied delta, got %d", total)
	}
}

func TestHubRateManager_DeduplicatesBySeq(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	rm := NewRateManager(rdb, makeTestConfigManager())
	rm.consumer = &fakeDeltaConsumer{rdb: rdb, updateChannel: types.DefaultRateUpdateChannel, apply: func(ctx context.Context, prefix, apiKey string, delta int64) error {
		_, err := rm.Increment(ctx, prefix, apiKey, delta)
		return err
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rm.StartDeltaListener(ctx); err != nil {
		t.Fatalf("StartDeltaListener: %v", err)
	}

	ud := types.UsageDelta{Prefix: "/api/v1", APIKey: "user1", Delta: 5, Seq: 10}
	b, _ := json.Marshal(ud)
	if err := rdb.Publish(ctx, types.DefaultRateUpdateChannel, b).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Publish(ctx, types.DefaultRateUpdateChannel, b).Err(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	total, err := rm.Increment(ctx, "/api/v1", "user1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 {
		t.Fatalf("expected deduped total=5, got %d", total)
	}
}
