package edge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"gateway/packages/common/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestEdgeRateManager_SOTSubscription(t *testing.T) {
	// start in-memory redis
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	rm := NewRateManager("", rdb, 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rm.StartSOTSubscriber(ctx); err != nil {
		t.Fatalf("StartSOTSubscriber: %v", err)
	}

	// publish a SOT message and ensure the sync key is updated
	sot := types.SOTMsg{Prefix: "/api/v1", APIKey: "user1", Total: 42}
	b, _ := json.Marshal(sot)
	if err := rdb.Publish(ctx, RateSOTChannel, b).Err(); err != nil {
		t.Fatal(err)
	}

	// tiny wait for goroutine to process
	time.Sleep(50 * time.Millisecond)

	syncKey := rm.getSyncKey(sot.Prefix, sot.APIKey)
	val, err := rdb.Get(ctx, syncKey).Int64()
	if err != nil {
		t.Fatal(err)
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}

	// also verify that calling Increment will publish UsageDelta when threshold reached
	sub := rdb.Subscribe(ctx, RateUpdateChannel)
	defer sub.Close()

	// perform an increment which should exceed maxDelta (5) and trigger publish
	_, err = rm.Increment(ctx, "/api/v1", "user1", 100, 6)
	if err != nil {
		t.Fatal(err)
	}

	// verify analytics entry was pushed
	list, err := rdb.LRange(ctx, "rate-analytics", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) == 0 {
		t.Fatal("expected analytics entry, got none")
	}

	select {
	case msg := <-sub.Channel():
		var ud types.UsageDelta
		json.Unmarshal([]byte(msg.Payload), &ud)
		if ud.Prefix != "/api/v1" || ud.APIKey != "user1" || ud.Delta != 6 {
			t.Fatalf("unexpected delta message: %+v", ud)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected delta message but none received")
	}
}
