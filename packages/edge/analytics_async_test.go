package edge

import (
	"context"
	"sync"
	"testing"
	"time"

	"gateway/packages/common/types"
)

func TestAnalyticsManager_PublisherSendsImmediately(t *testing.T) {
	var mu sync.Mutex
	var received []types.AnalyticsEntry

	mgr := NewAnalyticsManager(100)
	ack := make(chan struct{}, 2)
	mgr.write = func(_ context.Context, entry *types.AnalyticsEntry) error {
		mu.Lock()
		received = append(received, *entry)
		mu.Unlock()
		ack <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.StartPublisher(ctx)

	entry := &types.AnalyticsEntry{Prefix: "/api/v1", Service: "users", Method: "GET"}
	mgr.Capture(entry)
	mgr.Capture(entry)

	for i := 0; i < 2; i++ {
		select {
		case <-ack:
		case <-time.After(300 * time.Millisecond):
			t.Fatal("timed out waiting for analytics publish")
		}
	}

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count == 0 {
		t.Fatal("expected analytics entries to be flushed to hub, got none")
	}
	if count != 2 {
		t.Errorf("expected 2 analytics entries published, got %d", count)
	}
}

func TestAnalyticsManager_PublisherStopsOnCancel(t *testing.T) {
	var mu sync.Mutex
	var received []types.AnalyticsEntry

	mgr := NewAnalyticsManager(100)
	mgr.write = func(_ context.Context, entry *types.AnalyticsEntry) error {
		mu.Lock()
		received = append(received, *entry)
		mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	mgr.StartPublisher(ctx)

	entry := &types.AnalyticsEntry{Prefix: "/api/v1", Service: "orders", Method: "POST"}
	mgr.Capture(entry)
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 1 {
		t.Errorf("expected 1 entry before shutdown, got %d", count)
	}
}

func TestAnalyticsManager_PublisherNoWritesWhenEmpty(t *testing.T) {
	calls := 0
	mgr := NewAnalyticsManager(100)
	mgr.write = func(_ context.Context, _ *types.AnalyticsEntry) error {
		calls++
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.StartPublisher(ctx)
	time.Sleep(60 * time.Millisecond)

	if calls != 0 {
		t.Errorf("expected 0 publishes when buffer is empty, got %d", calls)
	}
}
