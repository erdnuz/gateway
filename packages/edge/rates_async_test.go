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

type testRateSync struct {
	rdb           *redis.Client
	updateChannel string
	sotChannel    string
}

func (s *testRateSync) FlushPendingDelta(ctx context.Context, prefix, apiKey string) {
	pendingKey := ratePendingKey(prefix, apiKey)
	delta, err := s.rdb.GetSet(ctx, pendingKey, 0).Int64()
	if err == redis.Nil || delta <= 0 {
		return
	}
	if err != nil {
		return
	}
	seq, err := s.rdb.Incr(ctx, rateSeqKey(prefix, apiKey)).Result()
	if err != nil {
		return
	}
	b, _ := json.Marshal(types.UsageDelta{Prefix: prefix, APIKey: apiKey, Delta: delta, Seq: seq})
	_ = s.rdb.Publish(ctx, s.updateChannel, b).Err()
}

func (s *testRateSync) StartSOTSubscriber(ctx context.Context) error {
	sub := s.rdb.Subscribe(ctx, s.sotChannel)
	ch := sub.Channel()
	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var sot types.SOTMsg
				if err := json.Unmarshal([]byte(msg.Payload), &sot); err != nil {
					continue
				}
				_ = s.rdb.Set(ctx, rateSyncKey(sot.Prefix, sot.APIKey), sot.Total, 0).Err()
			case <-ctx.Done():
				_ = sub.Close()
				return
			}
		}
	}()
	return nil
}

func TestEdgeRateManager_SOTSubscription(t *testing.T) {
	// start in-memory redis
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	rm := NewRateManager("", rdb, 5)
	rm.sync = &testRateSync{rdb: rdb, updateChannel: types.DefaultRateUpdateChannel, sotChannel: types.DefaultRateSOTChannel}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rm.StartBackgroundWorkers(ctx)
	if err := rm.StartSOTSubscriber(ctx); err != nil {
		t.Fatalf("StartSOTSubscriber: %v", err)
	}

	// publish a SOT message and ensure the sync key is updated
	sot := types.SOTMsg{Prefix: "/api/v1", APIKey: "user1", Total: 42}
	b, _ := json.Marshal(sot)
	if err := rdb.Publish(ctx, types.DefaultRateSOTChannel, b).Err(); err != nil {
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
	sub := rdb.Subscribe(ctx, types.DefaultRateUpdateChannel)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}

	// perform an increment which should exceed maxDelta (5) and trigger publish
	_, err = rm.Increment(ctx, "/api/v1", "user1", 100, 6)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-sub.Channel():
		var ud types.UsageDelta
		json.Unmarshal([]byte(msg.Payload), &ud)
		if ud.Prefix != "/api/v1" || ud.APIKey != "user1" || ud.Delta != 6 {
			t.Fatalf("unexpected delta message: %+v", ud)
		}
		if ud.Seq <= 0 {
			t.Fatalf("expected positive seq, got %+v", ud)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected delta message but none received")
	}
}

func TestEdgeRateManager_PublishesOnlyUnsentDelta(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	rm := NewRateManager("", rdb, 5)
	rm.sync = &testRateSync{rdb: rdb, updateChannel: types.DefaultRateUpdateChannel, sotChannel: types.DefaultRateSOTChannel}

	ctx := context.Background()
	rm.StartBackgroundWorkers(ctx)
	sub := rdb.Subscribe(ctx, types.DefaultRateUpdateChannel)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}

	if _, err := rm.Increment(ctx, "/api/v1", "user1", 100, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := rm.Increment(ctx, "/api/v1", "user1", 100, 3); err != nil {
		t.Fatal(err)
	}

	var first types.UsageDelta
	select {
	case msg := <-sub.Channel():
		if err := json.Unmarshal([]byte(msg.Payload), &first); err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected first delta message")
	}

	if first.Delta != 6 {
		t.Fatalf("expected first delta=6, got %d", first.Delta)
	}

	if _, err := rm.Increment(ctx, "/api/v1", "user1", 100, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := rm.Increment(ctx, "/api/v1", "user1", 100, 3); err != nil {
		t.Fatal(err)
	}

	var followups []types.UsageDelta
	deadline := time.After(350 * time.Millisecond)
	for {
		select {
		case msg := <-sub.Channel():
			var next types.UsageDelta
			if err := json.Unmarshal([]byte(msg.Payload), &next); err != nil {
				t.Fatal(err)
			}
			followups = append(followups, next)
		case <-deadline:
			goto done
		}
	}

done:
	if len(followups) == 0 {
		t.Fatal("expected at least one follow-up delta message")
	}

	total := int64(0)
	lastSeq := first.Seq
	for _, msg := range followups {
		total += msg.Delta
		if msg.Seq <= lastSeq {
			t.Fatalf("expected increasing seqs, previous=%d current=%d", lastSeq, msg.Seq)
		}
		lastSeq = msg.Seq
	}
	if total != 6 {
		t.Fatalf("expected unsent deltas to total 6, got %d", total)
	}
}
