package limiter

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisLimiter_PushPull(t *testing.T) {
	// 1. Setup MiniRedis
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	l := NewRedisLimiter(rdb)
	ctx := context.Background()

	svcID := "service-1"
	apiKey := "user-A"
	ttl := 60 // 1 minute

	t.Run("Initial Increment", func(t *testing.T) {
		// First push of 5 requests
		newTotal, err := l.PushPull(ctx, svcID, apiKey, 5, ttl)
		if err != nil {
			t.Fatalf("PushPull failed: %v", err)
		}
		if newTotal != 5 {
			t.Errorf("Expected 5, got %d", newTotal)
		}
	})

	t.Run("Subsequent Increment", func(t *testing.T) {
		// Second push of 10 requests (total should be 15)
		newTotal, _ := l.PushPull(ctx, svcID, apiKey, 10, ttl)
		if newTotal != 15 {
			t.Errorf("Expected 15, got %d", newTotal)
		}
	})

	t.Run("TTL Verification", func(t *testing.T) {
		// Verify miniredis actually set the TTL
		key := "usage:service-1:user-A"
		s.CheckGet(t, key, "15")
		if s.TTL(key) <= 0 {
			t.Error("Key should have a positive TTL")
		}
	})
}
