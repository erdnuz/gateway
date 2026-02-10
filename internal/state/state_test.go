package state

import (
	"sync"
	"testing"
)

func TestLocalState_ThreadSafety(t *testing.T) {
	ls := NewLocalState()
	svcID := "bench-svc"

	// Use a WaitGroup to synchronize goroutines
	var wg sync.WaitGroup
	iterations := 1000

	// Test concurrent Increments to the LocalBuffer
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			ls.IncrementLocal(svcID, "user-1", 1)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			ls.IncrementLocal(svcID, "user-2", 1)
		}
	}()

	wg.Wait()

	// Verify counts
	if ls.LocalBuffer[svcID]["user-1"] != int64(iterations) {
		t.Errorf("Concurrent increment failed for user-1, got %d", ls.LocalBuffer[svcID]["user-1"])
	}
}

func TestLocalState_ProjectedUsage(t *testing.T) {
	ls := NewLocalState()
	svcID := "api-v1"
	apiKey := "key-red"

	// 1. Manually set global state (what we think Redis says)
	ls.Lock()
	ls.GlobalCounts[svcID] = map[string]int64{apiKey: 50}
	ls.Unlock()

	// 2. Add some local buffer (requests that haven't hit Redis yet)
	ls.IncrementLocal(svcID, apiKey, 1)
	ls.IncrementLocal(svcID, apiKey, 1)

	// 3. Projected should be 50 + 2 = 52
	projected := ls.GetProjectedUsage(svcID, apiKey)
	if projected != 52 {
		t.Errorf("Expected projected usage 52, got %d", projected)
	}
}

func TestLocalState_InitializationSafety(t *testing.T) {
	ls := NewLocalState()
	svcID := "new-service"
	apiKey := "new-user"

	// This should NOT panic even though the nested maps don't exist yet
	// because IncrementLocal should handle initialization internally.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncrementLocal panicked on unitialized nested map: %v", r)
		}
	}()

	ls.IncrementLocal(svcID, apiKey, 1)

	if ls.LocalBuffer[svcID][apiKey] != 1 {
		t.Errorf("Expected 1, got %d", ls.LocalBuffer[svcID][apiKey])
	}
}
