package limiter

import (
	"context"
	"gate/internal/state"
	"time"
)

type SyncWorker struct {
	state   *state.LocalState
	limiter *RedisLimiter
}

func NewSyncWorker(state *state.LocalState, limiter *RedisLimiter) *SyncWorker {
	return &SyncWorker{
		state:   state,
		limiter: limiter,
	}
}

func (w *SyncWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			w.performSync(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (w *SyncWorker) performSync(ctx context.Context) {
	w.state.Lock()
	// Extract current buffer to sync and reset it
	batch := w.state.LocalBuffer
	w.state.LocalBuffer = make(map[string]map[string]int64)
	w.state.Unlock()

	for svcID, users := range batch {
		for apiKey, count := range users {
			// Pull actual TTL from config or use default
			newGlobal, err := w.limiter.PushPull(ctx, svcID, apiKey, count, 3600)
			if err == nil {
				w.state.Lock()
				if w.state.GlobalCounts[svcID] == nil {
					w.state.GlobalCounts[svcID] = make(map[string]int64)
				}
				w.state.GlobalCounts[svcID][apiKey] = newGlobal
				w.state.Unlock()
			}
		}
	}
}
