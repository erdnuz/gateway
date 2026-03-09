package edge

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type CounterStore interface {
	IncrBy(ctx context.Context, key string, value int64) (int64, error)
	Get(ctx context.Context, key string) (int64, error)
}

type RedisCounterAdapter struct {
	rdb *redis.Client
}

func NewRedisCounterAdapter(rdb *redis.Client) *RedisCounterAdapter {
	return &RedisCounterAdapter{rdb: rdb}
}

func (a *RedisCounterAdapter) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return a.rdb.IncrBy(ctx, key, value).Result()
}

func (a *RedisCounterAdapter) Get(ctx context.Context, key string) (int64, error) {
	val, err := a.rdb.Get(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	parsed, parseErr := strconv.ParseInt(val, 10, 64)
	if parseErr != nil {
		return 0, parseErr
	}
	return parsed, nil
}
