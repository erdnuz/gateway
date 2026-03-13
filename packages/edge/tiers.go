package edge

import (
	"context"
	"encoding/json"
	"fmt"
	"gateway/packages/common/types"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	tierCacheTTL          = 1 * time.Hour
	defaultTierRedisOpTTL = 150 * time.Millisecond
	defaultTierHubOpTTL   = 500 * time.Millisecond
)

type TierManager struct {
	rdb         *redis.Client
	hubAddr     string
	authTok     string
	client      *http.Client
	stale       sync.Map
	group       singleflight.Group
	natsConn    *nats.Conn
	natsSubject string
	natsQueue   string
}

type TierManagerOptions struct {
	NATSURL   string
	NATSSubj  string
	NATSQueue string
}

type tierSnapshot struct {
	tier      string
	updatedAt time.Time
}

func NewTierManager(hubAddr string, rdb *redis.Client, hubUpdatesChannel string, authTok ...string) *TierManager {
	token := ""
	if len(authTok) > 0 {
		token = authTok[0]
	}
	return NewTierManagerWithClient(hubAddr, rdb, hubUpdatesChannel, token, nil)
}

func NewTierManagerWithClient(hubAddr string, rdb *redis.Client, hubUpdatesChannel string, authTok string, client *http.Client) *TierManager {
	return NewTierManagerWithOptions(hubAddr, rdb, hubUpdatesChannel, authTok, client, TierManagerOptions{})
}

func NewTierManagerWithOptions(hubAddr string, rdb *redis.Client, hubUpdatesChannel string, authTok string, client *http.Client, options TierManagerOptions) *TierManager {
	token := ""
	if authTok != "" {
		token = authTok
	}
	if client == nil {
		client = newHubHTTPClient(0)
	}
	if hubUpdatesChannel == "" {
		hubUpdatesChannel = types.DefaultHubUpdatesChannel
	}
	natsURL := strings.TrimSpace(options.NATSURL)
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	subject := strings.TrimSpace(options.NATSSubj)
	if subject == "" {
		subject = "tier.updates"
	}
	queue := strings.TrimSpace(options.NATSQueue)
	if queue == "" {
		queue = "edge-tier-updates"
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("edge tier nats connect failed url=%s err=%v", natsURL, err)
	}
	return &TierManager{
		hubAddr:     hubAddr,
		rdb:         rdb,
		authTok:     token,
		client:      client,
		natsConn:    nc,
		natsSubject: subject,
		natsQueue:   queue,
	}
}

// GetUserTier retrieves the tier for an API key.
// It checks Redis first, then falls back to the Hub via singleflight.
func (tm *TierManager) GetUserTier(ctx context.Context, prefix, apiKey string) (string, error) {
	cacheKey := tm.getCacheKey(prefix, apiKey)

	// 1. L1: Fast-path (Redis)
	redisCtx, redisCancel := context.WithTimeout(ctx, defaultTierRedisOpTTL)
	val, err := tm.rdb.Get(redisCtx, cacheKey).Result()
	redisCancel()
	if err == nil {
		tm.stale.Store(cacheKey, tierSnapshot{tier: val, updatedAt: time.Now()})
		return val, nil
	}

	// 2. L2: Slow-path (Hub) with thundering herd protection
	result, err, _ := tm.group.Do(cacheKey, func() (interface{}, error) {
		hubCtx, hubCancel := context.WithTimeout(ctx, defaultTierHubOpTTL)
		defer hubCancel()
		tier, err := tm.fetchFromHub(hubCtx, prefix, apiKey)
		if err != nil {
			return "", err
		}

		// 3. Backfill Redis so the next request is O(1)
		// We use a background context here so the client request doesn't
		// wait for the Redis write to finish.
		backfillCtx, backfillCancel := context.WithTimeout(context.Background(), defaultTierRedisOpTTL)
		defer backfillCancel()
		if err := tm.rdb.Set(backfillCtx, cacheKey, tier, tierCacheTTL).Err(); err != nil {
			log.Printf("edge tier cache set failed key=%s err=%v", cacheKey, err)
		}
		tm.stale.Store(cacheKey, tierSnapshot{tier: tier, updatedAt: time.Now()})

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
	if tm.authTok != "" {
		req.Header.Set("Authorization", "Bearer "+tm.authTok)
	}

	resp, err := tm.client.Do(req)
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

func (tm *TierManager) GetStaleTier(prefix, apiKey string, maxAge time.Duration) (string, bool) {
	cacheKey := tm.getCacheKey(prefix, apiKey)
	v, ok := tm.stale.Load(cacheKey)
	if !ok {
		return "", false
	}
	snapshot, ok := v.(tierSnapshot)
	if !ok {
		return "", false
	}
	if maxAge > 0 && time.Since(snapshot.updatedAt) > maxAge {
		return "", false
	}
	if snapshot.tier == "" {
		return "", false
	}
	return snapshot.tier, true
}

// StartHubUpdateListener subscribes to NATS tier updates and applies
// the tier snapshot directly to local Redis cache.
// The listener spawns a goroutine; cancel the provided context to stop it.
func (tm *TierManager) StartHubUpdateListener(ctx context.Context) error {
	if tm.natsConn == nil {
		return nil
	}
	sub, err := tm.natsConn.QueueSubscribe(tm.natsSubject, tm.natsQueue, func(msg *nats.Msg) {
		tm.applyTierUpdatePayload(ctx, string(msg.Data))
	})
	if err != nil {
		return err
	}
	if err := tm.natsConn.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return err
	}
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
		tm.natsConn.Close()
	}()
	return nil
}

func (tm *TierManager) applyTierUpdatePayload(ctx context.Context, payload string) {
	if !strings.HasPrefix(payload, "TIER_UPDATE:") {
		return
	}
	parts := strings.SplitN(payload, ":", 4)
	if len(parts) != 4 {
		return
	}
	prefix := parts[1]
	apiKey := parts[2]
	tierID := parts[3]
	cacheKey := tm.getCacheKey(prefix, apiKey)
	if tierID == "" {
		redisCtx, redisCancel := context.WithTimeout(ctx, defaultTierRedisOpTTL)
		if err := tm.rdb.Del(redisCtx, cacheKey).Err(); err != nil {
			log.Printf("edge tier cache delete failed key=%s err=%v", cacheKey, err)
		}
		redisCancel()
		tm.stale.Delete(cacheKey)
		return
	}
	redisCtx, redisCancel := context.WithTimeout(ctx, defaultTierRedisOpTTL)
	if err := tm.rdb.Set(redisCtx, cacheKey, tierID, tierCacheTTL).Err(); err != nil {
		log.Printf("edge tier cache update failed key=%s tier=%s err=%v", cacheKey, tierID, err)
		redisCancel()
		return
	}
	redisCancel()
	tm.stale.Store(cacheKey, tierSnapshot{tier: tierID, updatedAt: time.Now()})
}
