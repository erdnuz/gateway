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

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9" // Updated to v9
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
	tierUpdateConn   *nats.Conn
	tierUpdateSubj   string
	configReloadChan string
	corsAllowed      map[string]struct{}
	corsAllowHeaders []string
	corsAllowMethods []string
	corsMaxAge       time.Duration
	apiKeyPattern    *regexp.Regexp
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
		rdb:              rdb,
		cfgManager:       cfg,
		tierManager:      tierStore,
		rateManager:      rateLimiter,
		authToken:        authToken,
		maxDelta:         maxDelta,
		hubUpdatesChan:   hubUpdatesChannel,
		asyncQueue:       queue,
		queueWorkers:     2,
		submitTimeout:    25 * time.Millisecond,
		retryMax:         1,
		retryBackoff:     10 * time.Millisecond,
		configReloadChan: types.DefaultConfigReloadChannel,
		corsAllowed:      map[string]struct{}{},
		corsAllowHeaders: []string{"Authorization", "Content-Type", "X-API-Key"},
		corsAllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		corsMaxAge:       5 * time.Minute,
		apiKeyPattern:    regexp.MustCompile(types.DefaultRuntimePolicy().Hub.APIKeyPattern),
	}
}

func (s *HubServer) SetCORSAllowedOrigins(origins []string) {
	allowed := make(map[string]struct{}, len(origins))
	for _, origin := range origins {
		v := strings.TrimSpace(strings.TrimSuffix(origin, "/"))
		if v != "" {
			allowed[v] = struct{}{}
		}
	}
	s.corsAllowed = allowed
}

func (s *HubServer) SetConfigReloadChannel(channel string) {
	if strings.TrimSpace(channel) == "" {
		return
	}
	s.configReloadChan = channel
}

func (s *HubServer) SetCORSPreflightPolicy(allowedHeaders, allowedMethods []string, maxAge time.Duration) {
	if len(allowedHeaders) > 0 {
		s.corsAllowHeaders = allowedHeaders
	}
	if len(allowedMethods) > 0 {
		s.corsAllowMethods = allowedMethods
	}
	if maxAge > 0 {
		s.corsMaxAge = maxAge
	}
}

func (s *HubServer) SetAPIKeyPattern(pattern string) error {
	v := strings.TrimSpace(pattern)
	if v == "" {
		return nil
	}
	re, err := regexp.Compile(v)
	if err != nil {
		return err
	}
	s.apiKeyPattern = re
	return nil
}

func (s *HubServer) SetTierUpdateMessaging(natsURL, subject string) {
	if strings.TrimSpace(natsURL) == "" {
		natsURL = nats.DefaultURL
	}
	if strings.TrimSpace(subject) == "" {
		subject = "tier.updates"
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("hub nats connect failed url=%s err=%v", natsURL, err)
		return
	}
	s.tierUpdateConn = nc
	s.tierUpdateSubj = subject
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
}

func (s *HubServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := r.URL.Path
	if s.handleCORS(w, r) {
		return
	}

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
	case "healthz":
		s.handleHealth(w, r)
		return
	case "readyz":
		s.handleReady(w, r)
		return
	}

	if !s.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	switch parts[0] {
	case "config":
		s.handleConfig(w, r)
	case "config-reload":
		s.handleConfigReload(w, r)
	case "tiers":
		s.handleTiers(w, r, ctx, parts[1:])
	case "rate":
		s.handleRate(w, r, ctx, parts[1:])
	default:
		http.NotFound(w, r)
	}
}

func (s *HubServer) handleCORS(w http.ResponseWriter, r *http.Request) bool {
	origin := strings.TrimSpace(strings.TrimSuffix(r.Header.Get("Origin"), "/"))
	if origin == "" {
		return false
	}
	if !s.isCORSOriginAllowed(origin) {
		http.Error(w, "forbidden origin", http.StatusForbidden)
		return true
	}
	w.Header().Set("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(s.corsAllowMethods, ", "))
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(s.corsAllowHeaders, ", "))
		w.Header().Set("Access-Control-Max-Age", strconv.FormatInt(int64(s.corsMaxAge.Seconds()), 10))
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

func (s *HubServer) isCORSOriginAllowed(origin string) bool {
	if len(s.corsAllowed) == 0 {
		return false
	}
	_, ok := s.corsAllowed[origin]
	return ok
}

func (s *HubServer) handleRate(w http.ResponseWriter, r *http.Request, ctx context.Context, params []string) {
	// Expecting: {prefix}/{api_key}
	if len(params) < 2 {
		http.Error(w, "invalid path: expected /rate/{prefix}/{api_key}", http.StatusBadRequest)
		return
	}

	prefix := params[0]
	apiKey := params[1]
	if !s.isAllowedPrefix(prefix) || !s.matchesAPIKey(apiKey) {
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

func (s *HubServer) handleConfigReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cfgMgr, ok := s.cfgManager.(*ConfigManager)
	if !ok {
		http.Error(w, "config reload unsupported", http.StatusNotImplemented)
		return
	}
	if err := cfgMgr.ReloadFromFile(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.rdb != nil {
		_ = s.rdb.Publish(r.Context(), s.configReloadChan, time.Now().UTC().Format(time.RFC3339Nano)).Err()
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *HubServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.rdb == nil || s.rdb.Ping(r.Context()).Err() != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (s *HubServer) handleTiers(w http.ResponseWriter, r *http.Request, ctx context.Context, params []string) {
	// Expecting: {prefix}/{api_key}
	if len(params) < 2 {
		http.Error(w, "invalid path: expected /tiers/{prefix}/{api_key}", http.StatusBadRequest)
		return
	}

	prefix := params[0]
	apiKey := params[1]
	if !s.isAllowedPrefix(prefix) || !s.matchesAPIKey(apiKey) {
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

func (s *HubServer) matchesAPIKey(apiKey string) bool {
	if s.apiKeyPattern == nil {
		s.apiKeyPattern = regexp.MustCompile(types.DefaultRuntimePolicy().Hub.APIKeyPattern)
	}
	return s.apiKeyPattern.MatchString(apiKey)
}

func (s *HubServer) publishTierUpdate(prefix, apiKey, tierID string) {
	task := &tierUpdateTask{
		prefix:   prefix,
		apiKey:   apiKey,
		tierID:   tierID,
		retryMax: s.retryMax,
		backoff:  s.retryBackoff,
		conn:     s.tierUpdateConn,
		subject:  s.tierUpdateSubj,
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
	conn     *nats.Conn
	subject  string
}

func (t *tierUpdateTask) Execute(ctx context.Context) error {
	_ = ctx
	payload := "TIER_UPDATE:" + t.prefix + ":" + t.apiKey + ":" + t.tierID
	if t.conn == nil {
		return nil
	}
	if err := t.conn.Publish(t.subject, []byte(payload)); err != nil {
		log.Printf("hub tier update nats publish failed prefix=%s api_key=%s tier=%s subject=%s err=%v", t.prefix, t.apiKey, t.tierID, t.subject, err)
		return err
	}
	return nil
}

func (t *tierUpdateTask) RetryPolicy() *workers.RetryPolicy {
	return &workers.RetryPolicy{MaxRetries: t.retryMax, Backoff: t.backoff}
}
