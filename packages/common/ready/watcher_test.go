package ready

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatchAllEventuallyReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	var isReady atomic.Bool
	watcher := NewReadyWatcher("test", 5*time.Millisecond, 20*time.Millisecond)
	go watcher.WatchAll(ctx, []Check{{
		Name: "dep",
		URL:  "dep://local",
		Probe: func(context.Context) error {
			if attempts.Add(1) < 3 {
				return errors.New("not yet")
			}
			return nil
		},
	}}, func(ready bool) {
		isReady.Store(ready)
	})

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if isReady.Load() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected watcher to eventually report ready=true")
}

func TestWatchAllHandlesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	watcher := NewReadyWatcher("test", 5*time.Millisecond, 20*time.Millisecond)
	done := make(chan struct{})
	go func() {
		watcher.WatchAll(ctx, []Check{{
			Name: "dep",
			URL:  "dep://local",
			Probe: func(context.Context) error {
				return errors.New("down")
			},
		}}, nil)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("watcher did not stop after context cancellation")
	}
}
