package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCounter is the textbook central limiter: every decision is an atomic
// increment of a shared Redis counter for the key's window. It is exact, and
// it puts a Redis round trip on every request. Gate keeps it only as the
// baseline the benchmark compares leasing against (EDGE_LIMITER=redis-counter).
type RedisCounter struct {
	rdb redis.UniversalClient
}

// NewRedisCounter returns a central-counter limiter over rdb.
func NewRedisCounter(rdb redis.UniversalClient) *RedisCounter { return &RedisCounter{rdb: rdb} }

var counterLua = redis.NewScript(`
local used = redis.call('INCRBY', KEYS[1], ARGV[1])
if used == tonumber(ARGV[1]) then redis.call('PEXPIRE', KEYS[1], ARGV[3]) end
if used > tonumber(ARGV[2]) then
  redis.call('DECRBY', KEYS[1], ARGV[1])
  return {0, used - tonumber(ARGV[1])}
end
return {1, used}
`)

func (r *RedisCounter) Allow(ctx context.Context, req Request) (Decision, error) {
	cost := max(req.Cost, 1)
	now := time.Now()
	window := max(req.Window, time.Second)
	id := now.UnixMilli() / window.Milliseconds()
	d := Decision{Limit: req.Limit, ResetAt: windowEnd(window, now)}
	key := fmt.Sprintf("counter:{%s}:%d", stateKey(req), id)
	res, err := counterLua.Run(ctx, r.rdb, []string{key}, cost, req.Limit, 2*window.Milliseconds()).Int64Slice()
	if err != nil {
		return d, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	d.Allowed, d.Remaining, d.Window = res[0] == 1, max(0, req.Limit-res[1]), id
	return d, nil
}
