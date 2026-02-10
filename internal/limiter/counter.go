package limiter

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Lua script to atomically increment and return the new total
var syncLua = redis.NewScript(`
    local key = KEYS[1]
    local increment = tonumber(ARGV[1])
    local ttl = tonumber(ARGV[2])

    local current = redis.call("INCRBY", key, increment)
    if current == increment then
        redis.call("EXPIRE", key, ttl)
    end
    return current
`)

type RedisLimiter struct {
	client *redis.Client
}

func NewRedisLimiter(rdb *redis.Client) *RedisLimiter {
	return &RedisLimiter{client: rdb}
}

func (r *RedisLimiter) PushPull(ctx context.Context, svcID, apiKey string, count int64, ttlSeconds int) (int64, error) {
	key := fmt.Sprintf("usage:%s:%s", svcID, apiKey)

	val, err := syncLua.Run(ctx, r.client, []string{key}, count, ttlSeconds).Int64()
	if err != nil {
		return 0, err
	}
	return val, nil
}
