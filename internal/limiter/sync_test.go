package limiter

import (
	"context"
	"gate/internal/state"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestSyncWorker_Cycle(t *testing.T) {
	// 1. Setup infrastructure
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// 2. Setup Shared State
	ls := state.NewLocalState()
	rl := NewRedisLimiter(rdb)
	worker := &SyncWorker{
		state:   ls,
		limiter: rl,
	}

	svcID := "svc-sync"
	apiKey := "user-sync"

	// 3. Scenario: Local pod has processed 7 requests
	// Use a block to ensure the lock is released before performSync
	{
		ls.Lock()
		if ls.LocalBuffer[svcID] == nil {
			ls.LocalBuffer[svcID] = make(map[string]int64)
		}
		ls.LocalBuffer[svcID][apiKey] = 7
		ls.Unlock()
	}

	// 4. Run the sync logic
	worker.performSync(context.Background())

	// 5. Assertions: Snapshot after first sync
	{
		ls.RLock()
		if count := ls.LocalBuffer[svcID][apiKey]; count != 0 {
			t.Errorf("LocalBuffer should be empty after sync, got %d", count)
		}
		if truth := ls.GlobalCounts[svcID][apiKey]; truth != 7 {
			t.Errorf("GlobalCounts should now be 7, got %d", truth)
		}
		ls.RUnlock()
	}

	// 6. Scenario: Cumulative update (Simulate external Redis traffic)
	mr.Set("usage:svc-sync:user-sync", "17") // 7 from us + 10 from elsewhere

	{
		ls.Lock()
		// FIX: Ensure the inner map exists for this ServiceID
		if ls.LocalBuffer[svcID] == nil {
			ls.LocalBuffer[svcID] = make(map[string]int64)
		}
		ls.LocalBuffer[svcID][apiKey] = 3
		ls.Unlock()
	}

	worker.performSync(context.Background())

	// 7. Final Verification
	{
		ls.RLock()
		if truth := ls.GlobalCounts[svcID][apiKey]; truth != 20 {
			t.Errorf("Expected cumulative total 20 (17 in redis + 3 local), got %d", truth)
		}
		ls.RUnlock()
	}
}
