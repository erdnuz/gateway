package edge

import (
	"context"
	"net/http"
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

func TestAnalyticsManager_UsesConfiguredPublishTimeout(t *testing.T) {
	mgr := NewAnalyticsManagerWithNATSOptions(10, "", "", AnalyticsManagerOptions{PublishTimeout: 125 * time.Millisecond})
	if mgr.timeout != 125*time.Millisecond {
		t.Fatalf("expected publish timeout from options, got %s", mgr.timeout)
	}
}

func TestAnalyticsManager_UsesDefaultPublishTimeoutWhenMissing(t *testing.T) {
	mgr := NewAnalyticsManagerWithNATSOptions(10, "", "", AnalyticsManagerOptions{})
	if mgr.timeout != types.DefaultRuntimePolicy().Edge.AnalyticsPublishTimeout {
		t.Fatalf("expected default publish timeout %s, got %s", types.DefaultRuntimePolicy().Edge.AnalyticsPublishTimeout, mgr.timeout)
	}
}

func TestAnalyticsManager_StatsTrackDrops(t *testing.T) {
	mgr := NewAnalyticsManager(1)
	entry := &types.AnalyticsEntry{Prefix: "v1", Service: "svc", Method: http.MethodGet}

	mgr.Capture(entry)
	mgr.Capture(entry)

	stats := mgr.Stats()
	if stats.Captured != 2 {
		t.Fatalf("expected captured=2, got %+v", stats)
	}
	if stats.Dropped != 1 {
		t.Fatalf("expected dropped=1, got %+v", stats)
	}
	if stats.BufferDepth != 1 || stats.BufferCapacity != 1 {
		t.Fatalf("unexpected buffer stats: %+v", stats)
	}
}

func TestAnalyticsManager_StatsTrackPublishFailures(t *testing.T) {
	mgr := NewAnalyticsManager(2)
	mgr.write = func(_ context.Context, _ *types.AnalyticsEntry) error {
		return context.DeadlineExceeded
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.StartPublisher(ctx)
	mgr.Capture(&types.AnalyticsEntry{Prefix: "v1", Service: "svc", Method: http.MethodPost})

	deadline := time.After(300 * time.Millisecond)
	for {
		stats := mgr.Stats()
		if stats.PublishFailures > 0 {
			if stats.Published != 0 {
				t.Fatalf("expected no successful publishes, got %+v", stats)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for publish failure metric")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}
