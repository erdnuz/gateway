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

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/singleflight"
)

const (
	tierCacheTTL = 1 * time.Hour
)

type TierManager struct {
	rdb         *redis.Client
	hubAddr     string
	authTok     string
	client      *http.Client
	stale       sync.Map
	group       singleflight.Group
	kafkaReader *kafka.Reader
}

type TierManagerOptions struct {
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
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
	brokers := make([]string, 0, len(options.KafkaBrokers))
	for _, b := range options.KafkaBrokers {
		v := strings.TrimSpace(b)
		if v != "" {
			brokers = append(brokers, v)
		}
	}
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	topic := strings.TrimSpace(options.KafkaTopic)
	if topic == "" {
		topic = "tier-updates"
	}
	groupID := strings.TrimSpace(options.KafkaGroupID)
	if groupID == "" {
		groupID = "edge-tier-updates"
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	return &TierManager{
		hubAddr:     hubAddr,
		rdb:         rdb,
		authTok:     token,
		client:      client,
		kafkaReader: reader,
	}
}

// GetUserTier retrieves the tier for an API key.
// It checks Redis first, then falls back to the Hub via singleflight.
func (tm *TierManager) GetUserTier(ctx context.Context, prefix, apiKey string) (string, error) {
	cacheKey := tm.getCacheKey(prefix, apiKey)

	// 1. L1: Fast-path (Redis)
	val, err := tm.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		tm.stale.Store(cacheKey, tierSnapshot{tier: val, updatedAt: time.Now()})
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
		if err := tm.rdb.Set(context.Background(), cacheKey, tier, tierCacheTTL).Err(); err != nil {
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

// StartHubUpdateListener subscribes to Kafka tier updates and applies
// the tier snapshot directly to local Redis cache.
// The listener spawns a goroutine; cancel the provided context to stop it.
func (tm *TierManager) StartHubUpdateListener(ctx context.Context) error {
	go func() {
		defer func() { _ = tm.kafkaReader.Close() }()
		for {
			msg, err := tm.kafkaReader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("edge tier kafka read failed: %v", err)
				continue
			}
			tm.applyTierUpdatePayload(ctx, string(msg.Value))
			if ctx.Err() != nil {
				return
			}
		}
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
		if err := tm.rdb.Del(ctx, cacheKey).Err(); err != nil {
			log.Printf("edge tier cache delete failed key=%s err=%v", cacheKey, err)
		}
		tm.stale.Delete(cacheKey)
		return
	}
	if err := tm.rdb.Set(ctx, cacheKey, tierID, tierCacheTTL).Err(); err != nil {
		log.Printf("edge tier cache update failed key=%s tier=%s err=%v", cacheKey, tierID, err)
		return
	}
	tm.stale.Store(cacheKey, tierSnapshot{tier: tierID, updatedAt: time.Now()})
}
