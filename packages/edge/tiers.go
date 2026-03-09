package edge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	tierCacheTTL = 1 * time.Hour
)

type TierManager struct {
	rdb     *redis.Client
	hubAddr string
	group   singleflight.Group
}

func NewTierManager(hubAddr string, rdb *redis.Client) *TierManager {
	return &TierManager{
		hubAddr: hubAddr,
		rdb:     rdb,
	}
}

// GetUserTier retrieves the tier for an API key.
// It checks Redis first, then falls back to the Hub via singleflight.
func (tm *TierManager) GetUserTier(ctx context.Context, prefix, apiKey string) (string, error) {
	cacheKey := tm.getCacheKey(prefix, apiKey)

	// 1. L1: Fast-path (Redis)
	val, err := tm.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		return val, nil
	}

	// 2. L2: Slow-path (Hub) with thundering herd protection
	result, err, _ := tm.group.Do(cacheKey, func() (interface{}, error) {
		tier, err := tm.fetchFromHub(ctx, prefix, apiKey)
		if err != nil {
			return "", err
		}

		// 3. Backfill Redis so the next request is O(1)
		// We use a background context here so the client request doesn't
		// wait for the Redis write to finish.
		_ = tm.rdb.Set(context.Background(), cacheKey, tier, tierCacheTTL).Err()

		return tier, nil
	})

	if err != nil {
		return "", fmt.Errorf("tier lookup failed: %w", err)
	}

	return result.(string), nil
}

func (tm *TierManager) fetchFromHub(ctx context.Context, prefix, apiKey string) (string, error) {
	url := fmt.Sprintf("%s/tiers/%s/%s", tm.hubAddr, prefix, apiKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("hub returned status %d", resp.StatusCode)
	}

	var body struct {
		Tier string `json:"tier"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}

	return body.Tier, nil
}

func (tm *TierManager) getCacheKey(prefix, apiKey string) string {
	return fmt.Sprintf("tier:%s:%s", prefix, apiKey)
}

// StartHubUpdateListener subscribes to the hub_updates channel and invalidates
// the local Redis tier cache whenever the hub publishes an INVALIDATE message.
// The listener spawns a goroutine; cancel the provided context to stop it.
func (tm *TierManager) StartHubUpdateListener(ctx context.Context) error {
	sub := tm.rdb.Subscribe(ctx, "hub_updates")
	ch := sub.Channel()
	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				// Hub publishes: INVALIDATE:{prefix}:{apiKey}
				if !strings.HasPrefix(msg.Payload, "INVALIDATE:") {
					continue
				}
				// Split into at most 3 parts so prefixes that contain ":" are preserved
				parts := strings.SplitN(msg.Payload, ":", 3)
				if len(parts) != 3 {
					continue
				}
				cacheKey := tm.getCacheKey(parts[1], parts[2])
				tm.rdb.Del(ctx, cacheKey)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}
