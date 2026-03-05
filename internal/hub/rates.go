package hub

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RateManager struct {
	rdb *redis.Client
}

func NewRateManager(rdb *redis.Client) *RateManager {
	return &RateManager{
		rdb: rdb,
	}
}

// Get retrieves the current true count from Redis.
// With persistence enabled, this is your source of truth.
func (rm *RateManager) Get(ctx context.Context, key string) (int64, error) {
	val, err := rm.rdb.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// Increment handles atomic Redis updates.
// Native TTL handles the "expiry on increments" requirement.
func (rm *RateManager) Increment(ctx context.Context, key string, amount int64, expiry time.Duration) (int64, error) {
	// 1. Atomic Hot Increment
	newTotal, err := rm.rdb.IncrBy(ctx, key, amount).Result()
	if err != nil {
		return 0, err
	}

	// 2. Set/Refresh Expiry
	// If the key is new (newTotal == amount), or if you want sliding windows,
	// we set the TTL. Redis handles the deletion automatically.
	if expiry > 0 {
		rm.rdb.Expire(ctx, key, expiry)
	}

	// 3. Optional: Tracking (Registry)
	// We use a Set to track active/dirty keys if you want to monitor
	// which users are currently active.
	rm.rdb.SAdd(ctx, "registry:active_keys", key)

	return newTotal, nil
}

// Reset clears a specific rate limit immediately
func (rm *RateManager) Reset(ctx context.Context, key string) error {
	return rm.rdb.Del(ctx, key).Err()
}
