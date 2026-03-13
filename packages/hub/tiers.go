package hub

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisTierKeyPrefix = "hub:tier:"
const hubTierRedisTimeout = 200 * time.Millisecond

func withHubTierTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.WithTimeout(context.Background(), hubTierRedisTimeout)
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, hubTierRedisTimeout)
}

type TierManager struct {
	rdb *redis.Client
}

func NewTierManager(rdb *redis.Client) *TierManager {
	return &TierManager{rdb: rdb}
}

func (tm *TierManager) getCacheKey(prefixID, apiKey string) string {
	return redisTierKeyPrefix + strings.TrimSpace(prefixID) + ":" + strings.TrimSpace(apiKey)
}

// SetTier persists user's tier assignment in Redis with no TTL.
func (tm *TierManager) SetTier(ctx context.Context, prefixID, apiKey, tierID string) error {
	redisCtx, cancel := withHubTierTimeout(ctx)
	defer cancel()
	return tm.rdb.Set(redisCtx, tm.getCacheKey(prefixID, apiKey), strings.TrimSpace(tierID), 0).Err()
}

// GetTier reads the persistent tier assignment from Redis.
func (tm *TierManager) GetTier(ctx context.Context, prefixID, apiKey string) (string, error) {
	redisCtx, cancel := withHubTierTimeout(ctx)
	defer cancel()
	val, err := tm.rdb.Get(redisCtx, tm.getCacheKey(prefixID, apiKey)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(val), nil
}

// DeleteTier removes the persistent mapping from Redis.
func (tm *TierManager) DeleteTier(ctx context.Context, prefixID, apiKey string) error {
	redisCtx, cancel := withHubTierTimeout(ctx)
	defer cancel()
	return tm.rdb.Del(redisCtx, tm.getCacheKey(prefixID, apiKey)).Err()
}
