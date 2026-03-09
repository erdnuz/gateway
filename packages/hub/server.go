package hub

import (
	"context"
	"encoding/json"
	"gateway/packages/common/types"
	"gateway/packages/common/workers"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9" // Updated to v9
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
)

type HubServer struct {
	rdb              *redis.Client
	cfgManager       ConfigStore
	tierManager      TierStore
	rateManager      RateLimiter
	authToken        string
	maxDelta         int64
	hubUpdatesChan   string
	asyncQueue       *workers.BoundedQueue
	queueWorkers     int
	submitTimeout    time.Duration
	retryMax         int
	retryBackoff     time.Duration
	tierUpdateWriter *kafka.Writer
	tierUpdateTopic  string
}

var apiKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{3,128}$`)

func NewHubServer(mdb *mongo.Database, rdb *redis.Client, configFilePath, authToken string, maxDelta int64, rateUpdateChannel, rateSOTChannel, hubUpdatesChannel string) *HubServer {
	cfg, err := NewConfigManager(configFilePath) // Load initial config from file
	if err != nil {
		panic("Failed to load config: " + err.Error())
	}
	if maxDelta <= 0 {
		maxDelta = 10000
	}
	if rateUpdateChannel == "" {
		rateUpdateChannel = types.DefaultRateUpdateChannel
	}
	if rateSOTChannel == "" {
		rateSOTChannel = types.DefaultRateSOTChannel
	}
	if hubUpdatesChannel == "" {
		hubUpdatesChannel = types.DefaultHubUpdatesChannel
	}

	return NewHubServerWithManagers(
		rdb,
		cfg,
		NewTierManager(mdb, rdb),
		NewRateManager(rdb, cfg, rateUpdateChannel, rateSOTChannel),
		authToken,
		maxDelta,
		hubUpdatesChannel,
	)
}

func NewHubServerWithManagers(
	rdb *redis.Client,
	cfg ConfigStore,
	tierStore TierStore,
	rateLimiter RateLimiter,
	authToken string,
	maxDelta int64,
	hubUpdatesChannel string,
) *HubServer {
	if maxDelta <= 0 {
		maxDelta = 10000
	}
	if hubUpdatesChannel == "" {
		hubUpdatesChannel = types.DefaultHubUpdatesChannel
	}
	queue := workers.NewBoundedQueue(512)
	return &HubServer{
		rdb:            rdb,
		cfgManager:     cfg,
		tierManager:    tierStore,
		rateManager:    rateLimiter,
		authToken:      authToken,
		maxDelta:       maxDelta,
		hubUpdatesChan: hubUpdatesChannel,
		asyncQueue:     queue,
		queueWorkers:   2,
		submitTimeout:  25 * time.Millisecond,
		retryMax:       1,
		retryBackoff:   10 * time.Millisecond,
	}
}

func (s *HubServer) SetTierUpdateMessaging(brokers []string, topic string) {
	clean := make([]string, 0, len(brokers))
	for _, b := range brokers {
		v := strings.TrimSpace(b)
		if v != "" {
			clean = append(clean, v)
		}
	}
	if len(clean) == 0 {
		clean = []string{"localhost:9092"}
	}
	if strings.TrimSpace(topic) == "" {
		topic = "tier-updates"
	}
	s.tierUpdateTopic = topic
	s.tierUpdateWriter = &kafka.Writer{
		Addr:         kafka.TCP(clean...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 20 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
}

func (s *HubServer) SetAsyncQueueConfig(workersCount int, submitTimeout time.Duration, retryMax int, retryBackoff time.Duration) {
	if workersCount > 0 {
		s.queueWorkers = workersCount
	}
	if submitTimeout > 0 {
		s.submitTimeout = submitTimeout
	}
	if retryMax >= 0 {
		s.retryMax = retryMax
	}
	if retryBackoff > 0 {
		s.retryBackoff = retryBackoff
	}
}

func (s *HubServer) StartBackgroundWorkers(ctx context.Context) {
	if s.asyncQueue != nil {
		s.asyncQueue.Start(ctx, s.queueWorkers)
	}
	if s.rateManager != nil {
		if err := s.rateManager.StartDeltaListener(ctx); err != nil {
			log.Printf("hub delta listener start failed: %v", err)
		}
	}
}

func (s *HubServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := r.URL.Path

	// Standardize: Remove leading/trailing slashes for easier splitting
	trimmedPath := strings.Trim(path, "/")
	parts := strings.Split(trimmedPath, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	switch parts[0] {
	case "health":
		s.handleHealth(w, r)
		return
	}

	if !s.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	switch parts[0] {
	case "config":
		s.handleConfig(w, r)
	case "tiers":
		s.handleTiers(w, r, ctx, parts[1:])
	case "rate":
		s.handleRate(w, r, ctx, parts[1:])
	default:
		http.NotFound(w, r)
	}
}

func (s *HubServer) handleRate(w http.ResponseWriter, r *http.Request, ctx context.Context, params []string) {
	// Expecting: {prefix}/{api_key}
	if len(params) < 2 {
		http.Error(w, "invalid path: expected /rate/{prefix}/{api_key}", http.StatusBadRequest)
		return
	}

	prefix := params[0]
	apiKey := params[1]
	if !s.isAllowedPrefix(prefix) || !apiKeyPattern.MatchString(apiKey) {
		http.Error(w, "invalid prefix or api key", http.StatusBadRequest)
		return
	}

	var total int64
	var err error

	switch r.Method {
	case http.MethodPost:
		// 1. Parse Delta for Increment
		delta := int64(1)
		maxDelta := s.maxDelta
		if maxDelta <= 0 {
			maxDelta = 10000
		}
		if dStr := r.URL.Query().Get("delta"); dStr != "" {
			d, err := strconv.ParseInt(dStr, 10, 64)
			if err != nil || d <= 0 || d > maxDelta {
				http.Error(w, "invalid delta", http.StatusBadRequest)
				return
			}
			delta = d
		}
		total, err = s.rateManager.Increment(ctx, prefix, apiKey, delta)

	case http.MethodGet:
		// 2. Pure retrieval (Weighted Calculation only)
		total, err = s.rateManager.Increment(ctx, prefix, apiKey, 0)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err != nil {
		s.writeInternalError(w, "handleRate", err)
		return
	}

	// 3. Clean integer response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(total)
}

// --- Handler Implementations ---

func (s *HubServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *HubServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	config := s.cfgManager.Get()
	if err := json.NewEncoder(w).Encode(config); err != nil {
		s.writeInternalError(w, "handleConfig", err)
	}

}

func (s *HubServer) handleTiers(w http.ResponseWriter, r *http.Request, ctx context.Context, params []string) {
	// Expecting: {prefix}/{api_key}
	if len(params) < 2 {
		http.Error(w, "invalid path: expected /tiers/{prefix}/{api_key}", http.StatusBadRequest)
		return
	}

	prefix := params[0]
	apiKey := params[1]
	if !s.isAllowedPrefix(prefix) || !apiKeyPattern.MatchString(apiKey) {
		http.Error(w, "invalid prefix or api key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		tier, err := s.tierManager.GetTier(ctx, prefix, apiKey)
		if err != nil {
			s.writeInternalError(w, "handleTiers.GetTier", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"tier": tier})

	case http.MethodPost, http.MethodPut:
		var req struct {
			TierID string `json:"tier_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := s.tierManager.SetTier(ctx, prefix, apiKey, req.TierID); err != nil {
			s.writeInternalError(w, "handleTiers.SetTier", err)
			return
		}

		// Broadcast tier update so Edges can update cache directly.
		s.publishTierUpdate(prefix, apiKey, req.TierID)
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		if err := s.tierManager.DeleteTier(ctx, prefix, apiKey); err != nil {
			s.writeInternalError(w, "handleTiers.DeleteTier", err)
			return
		}

		// Empty tier indicates deletion/eviction on edge caches.
		s.publishTierUpdate(prefix, apiKey, "")
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HubServer) writeInternalError(w http.ResponseWriter, op string, err error) {
	log.Printf("hub internal error op=%s err=%v", op, err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func (s *HubServer) authorize(r *http.Request) bool {
	if s.authToken == "" {
		return true
	}
	authz := r.Header.Get("Authorization")
	const bearer = "Bearer "
	if !strings.HasPrefix(authz, bearer) {
		return false
	}
	return strings.TrimPrefix(authz, bearer) == s.authToken
}

func (s *HubServer) isAllowedPrefix(prefix string) bool {
	cfg := s.cfgManager.Get()
	if cfg == nil {
		return false
	}
	for _, p := range cfg.Prefixes {
		if p.Prefix == prefix {
			return true
		}
	}
	return false
}

func (s *HubServer) publishTierUpdate(prefix, apiKey, tierID string) {
	task := &tierUpdateTask{
		prefix:   prefix,
		apiKey:   apiKey,
		tierID:   tierID,
		retryMax: s.retryMax,
		backoff:  s.retryBackoff,
		writer:   s.tierUpdateWriter,
		topic:    s.tierUpdateTopic,
	}
	if err := s.asyncQueue.Submit(task, s.submitTimeout); err != nil {
		log.Printf("hub tier update queue submit failed prefix=%s api_key=%s tier=%s err=%v", prefix, apiKey, tierID, err)
	}
}

type tierUpdateTask struct {
	prefix   string
	apiKey   string
	tierID   string
	retryMax int
	backoff  time.Duration
	writer   *kafka.Writer
	topic    string
}

func (t *tierUpdateTask) Execute(ctx context.Context) error {
	payload := "TIER_UPDATE:" + t.prefix + ":" + t.apiKey + ":" + t.tierID
	publishCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if t.writer == nil {
		return nil
	}
	msg := kafka.Message{Key: []byte(t.prefix + ":" + t.apiKey), Value: []byte(payload)}
	if err := t.writer.WriteMessages(publishCtx, msg); err != nil {
		log.Printf("hub tier update kafka publish failed prefix=%s api_key=%s tier=%s topic=%s err=%v", t.prefix, t.apiKey, t.tierID, t.topic, err)
		return err
	}
	return nil
}

func (t *tierUpdateTask) RetryPolicy() *workers.RetryPolicy {
	return &workers.RetryPolicy{MaxRetries: t.retryMax, Backoff: t.backoff}
}
