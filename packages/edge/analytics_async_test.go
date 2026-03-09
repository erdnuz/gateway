package edge

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"gateway/packages/common/types"
)

func TestAnalyticsManager_Flusher(t *testing.T) {
	var mu sync.Mutex
	var received []types.AnalyticsEntry

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/analytics" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		b, _ := io.ReadAll(r.Body)
		var batch []types.AnalyticsEntry
		if err := json.Unmarshal(b, &batch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, batch...)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"count":  len(batch),
		})
	}))
	defer srv.Close()

	mgr := NewAnalyticsManager(100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// use a short flush interval so the test completes quickly
	mgr.StartFlusher(ctx, srv.URL, 20*time.Millisecond)

	// capture a couple of entries
	entry := &types.AnalyticsEntry{Prefix: "/api/v1", Service: "users", Method: "GET"}
	mgr.Capture(entry)
	mgr.Capture(entry)

	// wait for at least one flush interval to pass
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count == 0 {
		t.Fatal("expected analytics entries to be flushed to hub, got none")
	}
	if count != 2 {
		t.Errorf("expected 2 analytics entries flushed, got %d", count)
	}
}

func TestAnalyticsManager_FlusherFinalFlushOnCancel(t *testing.T) {
	var mu sync.Mutex
	var received []types.AnalyticsEntry

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var batch []types.AnalyticsEntry
		json.Unmarshal(b, &batch)
		mu.Lock()
		received = append(received, batch...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mgr := NewAnalyticsManager(100)
	ctx, cancel := context.WithCancel(context.Background())

	// long flush interval so the periodic flush won't fire before cancel
	mgr.StartFlusher(ctx, srv.URL, 10*time.Second)

	entry := &types.AnalyticsEntry{Prefix: "/api/v1", Service: "orders", Method: "POST"}
	mgr.Capture(entry)

	// cancel immediately to trigger the final flush path
	cancel()

	// give the goroutine time to complete the final flush
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 1 {
		t.Errorf("expected 1 entry in final flush, got %d", count)
	}
}

func TestAnalyticsManager_FlusherNoOpWhenEmpty(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mgr := NewAnalyticsManager(100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// short interval — multiple ticks with no entries must produce no HTTP calls
	mgr.StartFlusher(ctx, srv.URL, 10*time.Millisecond)
	time.Sleep(80 * time.Millisecond)

	if calls != 0 {
		t.Errorf("expected 0 HTTP calls when buffer is empty, got %d", calls)
	}
}
