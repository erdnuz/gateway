package middleware

import (
	"gate/internal/config"
	"gate/internal/state"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRateLimitMiddleware_HandleStrategies(t *testing.T) {
	// 1. Setup State
	ls := state.NewLocalState()
	svcID := "test-api"
	apiKey := "user-token-123"

	// 2. Setup Config in LocalState RAM
	ls.Lock()
	ls.Configs[svcID] = config.ServiceConfig{
		ServiceID: svcID,
		Tiers: []config.TierConfig{
			{
				Quota: 5,
			},
			{
				Quota: 50,
			},
		},
	}
	// Map user to the FREE tier
	if ls.UserTiers[svcID] == nil {
		ls.UserTiers[svcID] = make(map[string]int64)
	}
	ls.UserTiers[svcID][apiKey] = 0
	ls.Unlock()

	mw := &RateLimitMiddleware{State: ls}

	// Create a dummy next handler
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})

	t.Run("Allow Request Under Quota", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-API-Key", apiKey)
		req.Header.Set("X-Service-ID", svcID)
		rr := httptest.NewRecorder()

		mw.Handler(nextHandler).ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rr.Code)
		}
	})

	t.Run("Block Request Over Quota", func(t *testing.T) {
		// Simulate being at the limit (5 hits)
		ls.Lock()
		if ls.GlobalCounts[svcID] == nil {
			ls.GlobalCounts[svcID] = make(map[string]int64)
		}
		ls.GlobalCounts[svcID][apiKey] = 5
		ls.Unlock()

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-API-Key", apiKey)
		req.Header.Set("X-Service-ID", svcID)
		rr := httptest.NewRecorder()

		mw.Handler(nextHandler).ServeHTTP(rr, req)

		if rr.Code != http.StatusTooManyRequests {
			t.Errorf("Expected 429 for BLOCK strategy, got %d", rr.Code)
		}
	})

}
