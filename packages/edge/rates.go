package edge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type RateManager struct {
	rdb      *redis.Client
	hubAddr  string
	maxDelta int64
	group    singleflight.Group // Prevents multiple concurrent syncs for the same key
}

func NewRateManager(hubAddr string, rdb *redis.Client, maxDelta int64) *RateManager {
	return &RateManager{
		rdb:      rdb,
		hubAddr:  hubAddr,
		maxDelta: maxDelta,
	}
}

func (rm *RateManager) Increment(ctx context.Context, prefix, apiKey string, limit, amount int64) (int64, error) {
	localKey := rm.getLocalKey(prefix, apiKey)
	syncKey := rm.getSyncKey(prefix, apiKey)

	// 1. Atomically increment local delta
	delta, err := rm.rdb.IncrBy(ctx, localKey, amount).Result()
	if err != nil {
		return 0, err
	}

	// 2. Get last known global total
	lastGlobalStr, err := rm.rdb.Get(ctx, syncKey).Result()

	// CRITICAL: If syncKey is missing, we MUST sync immediately to find the truth.
	if err == redis.Nil {
		return rm.syncWithHub(ctx, prefix, apiKey, delta)
	}

	lastGlobal, _ := strconv.ParseInt(lastGlobalStr, 10, 64)
	projected := lastGlobal + delta

	// 3. Hard Check: > 90% limit or projected overflow
	if projected > int64(float64(limit)*0.9) {
		return rm.syncWithHub(ctx, prefix, apiKey, delta)
	}

	// 4. Soft Check: Async flush using singleflight to prevent redundant HTTP calls
	if delta >= rm.maxDelta {
		go func() {
			// Use background context for async flush
			_, _ = rm.syncWithHub(context.Background(), prefix, apiKey, delta)
		}()
	}

	return projected, nil
}

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
