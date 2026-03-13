package edge

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type CounterStore interface {
	IncrBy(ctx context.Context, key string, value int64) (int64, error)
	Get(ctx context.Context, key string) (int64, error)
}

type RedisCounterAdapter struct {
	rdb *redis.Client
}

const edgeCounterRedisTimeout = 150 * time.Millisecond

func withCounterRedisTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.WithTimeout(context.Background(), edgeCounterRedisTimeout)
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, edgeCounterRedisTimeout)
}

func NewRedisCounterAdapter(rdb *redis.Client) *RedisCounterAdapter {
	return &RedisCounterAdapter{rdb: rdb}
}

func (a *RedisCounterAdapter) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	redisCtx, cancel := withCounterRedisTimeout(ctx)
	defer cancel()
	return a.rdb.IncrBy(redisCtx, key, value).Result()
}

func (a *RedisCounterAdapter) Get(ctx context.Context, key string) (int64, error) {
	redisCtx, cancel := withCounterRedisTimeout(ctx)
	defer cancel()
	val, err := a.rdb.Get(redisCtx, key).Result()
	if err != nil {
		return 0, err
	}
	parsed, parseErr := strconv.ParseInt(val, 10, 64)
	if parseErr != nil {
		return 0, parseErr
	}
	return parsed, nil
}
