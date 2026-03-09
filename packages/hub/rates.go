package hub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"gateway/packages/common/types"
)

var hybridWindowLua = redis.NewScript(`
    local key_base = KEYS[1]
    local current_id = ARGV[1]
    local previous_id = ARGV[2]
    local weight = tonumber(ARGV[3])
    local amount = tonumber(ARGV[4])
    local expiry = tonumber(ARGV[5])

    local current_key = key_base .. ":" .. current_id
    local previous_key = key_base .. ":" .. previous_id

    local current_count = redis.call("INCRBY", current_key, amount)
    if tonumber(current_count) == tonumber(amount) then
        redis.call("EXPIRE", current_key, expiry)
    end

    local previous_count = tonumber(redis.call("GET", previous_key) or "0")
    local total = current_count + (previous_count * (1 - weight))

    return math.floor(total)
`)

type RateManager struct {
	rdb    *redis.Client
	config *ConfigManager
}

const (
	RateUpdateChannel = "rate-updates"
	RateSOTChannel    = "rate-sot"
)

// UsageDelta and SOTMsg exist in common/types/messaging.go

func NewRateManager(rdb *redis.Client, config *ConfigManager) *RateManager {
	return &RateManager{
		rdb:    rdb,
		config: config,
	}
}

// Increment adds to the quota and returns the new weighted total.
func (rm *RateManager) Increment(ctx context.Context, prefixStr, key string, amount int64) (int64, error) {
	// core logic unchanged – this is the hub's authoritative counter
	size, curr, prev, weight, err := rm.calculateWindow(prefixStr)
	if err != nil {
		return 0, err
	}

	baseKey := fmt.Sprintf("rate:%s:%s", prefixStr, key)
	total, err := hybridWindowLua.Run(ctx, rm.rdb, []string{baseKey},
		curr, prev, weight, amount, size*2).Int64()
	if err != nil {
		return 0, err
	}

	// after increment, publish state‑of‑truth message so edges can sync
	sot := types.SOTMsg{Prefix: prefixStr, APIKey: key, Total: total}
	b, _ := json.Marshal(sot)
	_ = rm.rdb.Publish(ctx, RateSOTChannel, b).Err()

	return total, nil
}

// StartDeltaListener subscribes to edge delta messages and updates the hub's
// counters accordingly. Each received UsageDelta is applied via Increment,
// which in turn publishes a state-of-truth message.
//
// The listener spawns a goroutine; cancel the provided context to stop.
func (rm *RateManager) StartDeltaListener(ctx context.Context) error {
	sub := rm.rdb.Subscribe(ctx, RateUpdateChannel)
	ch := sub.Channel()
	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var ud types.UsageDelta
				if err := json.Unmarshal([]byte(msg.Payload), &ud); err != nil {
					continue
				}
				_, _ = rm.Increment(ctx, ud.Prefix, ud.APIKey, ud.Delta)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// calculateWindow retrieves the quota period from the read-only L1 config and performs window math.
func (rm *RateManager) calculateWindow(prefixStr string) (size, curr, prev uint64, weight float64, err error) {
	cfg := rm.config.Get() // O(1) atomic load
	var quotaPeriod time.Duration
	for _, p := range cfg.Prefixes {
		if p.Prefix == prefixStr {
			quotaPeriod = p.QuotaPeriod
			break
		}
	}

	if quotaPeriod <= 0 {
		return 0, 0, 0, 0, errors.New("quota period not found for prefix: " + prefixStr)
	}

	now := uint64(time.Now().Unix())
	size = uint64(quotaPeriod.Seconds())

	curr = now / size
	prev = curr - 1
	weight = float64(now%size) / float64(size)

	return size, curr, prev, weight, nil
}