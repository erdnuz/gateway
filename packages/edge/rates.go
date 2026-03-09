package edge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type RateManager struct {
	rdb      *redis.Client
	hubAddr  string
	maxDelta int64
	group    singleflight.Group // Prevents multiple concurrent syncs for the same key
}

const (
	RateUpdateChannel = "rate-updates"
	RateSOTChannel    = "rate-sot"
)

// helper message types are defined in common/types/messaging.go

func NewRateManager(hubAddr string, rdb *redis.Client, maxDelta int64) *RateManager {
	return &RateManager{
		rdb:      rdb,
		hubAddr:  hubAddr,
		maxDelta: maxDelta,
	}
}

func (rm *RateManager) Increment(ctx context.Context, prefix, apiKey string, limit, amount int64) (int64, error) {
	localKey := rm.getLocalKey(prefix, apiKey)
	// increment delta locally
	delta, err := rm.rdb.IncrBy(ctx, localKey, amount).Result()
	if err != nil {
		return 0, err
	}

	// record analytic entry for dashboard
	if amount > 0 {
		entry := types.RateAnalyticsEntry{
			Timestamp: time.Now(),
			Prefix:    prefix,
			APIKey:    apiKey,
			Delta:     amount,
		}
		if b, err := json.Marshal(entry); err == nil {
			_ = rm.rdb.RPush(ctx, "rate-analytics", b).Err()
		}
	}

	// read last global total (may not exist yet)
	syncKey := rm.getSyncKey(prefix, apiKey)
	lastGlobal := int64(0)
	if val, err := rm.rdb.Get(ctx, syncKey).Result(); err == nil {
		lastGlobal, _ = strconv.ParseInt(val, 10, 64)
	}

	projected := lastGlobal + delta

	// send updates asynchronously when threshold hit or close to limit
	if projected > int64(float64(limit)*0.9) {
		// hard threshold, publish immediately
		rm.publishDelta(ctx, prefix, apiKey, delta)
	} else if delta >= rm.maxDelta {
		// soft threshold, fire-and-forget
		go rm.publishDelta(context.Background(), prefix, apiKey, delta)
	}

	return projected, nil
}

// publishDelta sends usage information to the hub asynchronously via Redis pub/sub.
func (rm *RateManager) publishDelta(ctx context.Context, prefix, apiKey string, delta int64) {
	if delta == 0 {
		return
	}
	ud := types.UsageDelta{
		Prefix: prefix,
		APIKey: apiKey,
		Delta:  delta,
	}
	b, _ := json.Marshal(ud)
	_ = rm.rdb.Publish(ctx, RateUpdateChannel, b).Err()
}

// StartSOTSubscriber listens for hub "state of truth" messages and updates
// the local sync key accordingly.  It returns immediately and spins up a
// goroutine; caller should provide a context for cancellation.
func (rm *RateManager) StartSOTSubscriber(ctx context.Context) error {
	sub := rm.rdb.Subscribe(ctx, RateSOTChannel)
	// subscription errors show up when using sub.Channel()
	ch := sub.Channel()
	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var sot types.SOTMsg
				if err := json.Unmarshal([]byte(msg.Payload), &sot); err != nil {
					continue
				}
				syncKey := rm.getSyncKey(sot.Prefix, sot.APIKey)
				rm.rdb.Set(ctx, syncKey, sot.Total, 0)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// the old http-based syncWithHub is kept for backwards compatibility but is
// no longer called by Increment.
func (rm *RateManager) syncWithHub(ctx context.Context, prefix, apiKey string, delta int64) (int64, error) {
	cacheKey := rm.getSyncKey(prefix, apiKey)

	// Use singleflight so multiple goroutines for the same user wait for one HTTP call
	result, err, _ := rm.group.Do(cacheKey, func() (interface{}, error) {
		url := fmt.Sprintf("%s/rate/%s/%s?delta=%d", rm.hubAddr, prefix, apiKey, delta)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if err != nil {
			return int64(0), err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return int64(0), err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return int64(0), fmt.Errorf("hub error: %d", resp.StatusCode)
		}

		var newGlobal int64
		if err := json.NewDecoder(resp.Body).Decode(&newGlobal); err != nil {
			return int64(0), err
		}

		// Update Redis state atomically
		pipe := rm.rdb.Pipeline()
		pipe.Set(ctx, cacheKey, newGlobal, 0)
		pipe.DecrBy(ctx, rm.getLocalKey(prefix, apiKey), delta)
		if _, err := pipe.Exec(ctx); err != nil {
			return int64(0), err
		}

		return newGlobal, nil
	})

	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (rm *RateManager) getLocalKey(p, k string) string { return "rate-local:" + p + ":" + k }
func (rm *RateManager) getSyncKey(p, k string) string  { return "rate-sync:" + p + ":" + k }
