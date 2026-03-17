package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"gateway/packages/common/types"
	"gateway/packages/common/workers"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9" // Updated to v9
)

type corsPolicy struct {
	allowed           map[string]struct{}
	allowHeaders      []string
	allowMethods      []string
	maxAge            time.Duration
	allowHeadersValue string
	allowMethodsValue string
	maxAgeValue       string
}

const hubReadyRedisTimeout = 200 * time.Millisecond

func newCORSPolicy(origins map[string]struct{}, allowedHeaders, allowedMethods []string, maxAge time.Duration) *corsPolicy {
	if origins == nil {
		origins = map[string]struct{}{}
	}
	originsCopy := make(map[string]struct{}, len(origins))
	for k, v := range origins {
		originsCopy[k] = v
	}
	headersCopy := append([]string(nil), allowedHeaders...)
	methodsCopy := append([]string(nil), allowedMethods...)

	return &corsPolicy{
		allowed:           originsCopy,
		allowHeaders:      headersCopy,
		allowMethods:      methodsCopy,
		maxAge:            maxAge,
		allowHeadersValue: strings.Join(headersCopy, ", "),
		allowMethodsValue: strings.Join(methodsCopy, ", "),
		maxAgeValue:       strconv.FormatInt(int64(maxAge.Seconds()), 10),
	}
}

type HubServer struct {
	rdb               *redis.Client
	cfgManager        ConfigRegistry
	tierManager       TierStore
	rateManager       RateLimiter
	authToken         string
	maxDelta          int64
	hubUpdatesChan    string
	asyncQueue        *workers.BoundedQueue
	queueWorkers      int
	submitTimeout     time.Duration
	retryMax          int
	retryBackoff      time.Duration
	tierUpdateConn    *nats.Conn
	tierUpdateSubj    string
	configReloadChan  string
	corsPolicy        atomic.Pointer[corsPolicy]
	apiKeyPattern     atomic.Pointer[regexp.Regexp]
	readyHealthy      atomic.Bool
	pendingDependency atomic.Value
	readyProbeStarted atomic.Bool
	tierPublishErrors atomic.Uint64
	configEpoch       atomic.Uint64
}

type configFileLoader interface {
	LoadFromFile() (*types.GatewayConfig, error)
}

type configPayloadReloader interface {
	ReloadFromPayload(cfg *types.GatewayConfig) error
}

type configUpdateReport struct {
	Mode      string               `json:"mode"`
	Valid     bool                 `json:"valid"`
	Analysis  configUpdateAnalysis `json:"analysis"`
	NextEpoch uint64               `json:"next_epoch"`
}

func NewHubServerWithManagers(
	rdb *redis.Client,
	cfg ConfigRegistry,
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
	server := &HubServer{
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
	}
	server.readyHealthy.Store(false)
	server.corsPolicy.Store(newCORSPolicy(
		map[string]struct{}{},
		[]string{"Authorization", "Content-Type", "X-API-Key"},
		[]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		5*time.Minute,
	))
	server.apiKeyPattern.Store(regexp.MustCompile(types.DefaultRuntimePolicy().Hub.APIKeyPattern))
	server.pendingDependency.Store("redis,nats")
	return server
}

func (s *HubServer) SetStartupReady(ready bool) {
	s.readyHealthy.Store(ready)
}

func (s *HubServer) SetPendingDependency(dependency string) {
	v := strings.TrimSpace(dependency)
	if v == "" {
		v = "unknown"
	}
	s.pendingDependency.Store(v)
}

func (s *HubServer) pendingDependencyValue() string {
	v := s.pendingDependency.Load()
	dep, _ := v.(string)
	dep = strings.TrimSpace(dep)
	if dep == "" {
		return "unknown"
	}
	return dep
}

func (s *HubServer) currentCORSPolicy() *corsPolicy {
	policy := s.corsPolicy.Load()
	if policy != nil {
		return policy
	}
	fallback := newCORSPolicy(
		map[string]struct{}{},
		[]string{"Authorization", "Content-Type", "X-API-Key"},
		[]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		5*time.Minute,
	)
	s.corsPolicy.Store(fallback)
	return fallback
}

func (s *HubServer) SetCORSAllowedOrigins(origins []string) {
	current := s.currentCORSPolicy()
	allowed := make(map[string]struct{}, len(origins))
	for _, origin := range origins {
		v := strings.TrimSpace(strings.TrimSuffix(origin, "/"))
		if v != "" {
			allowed[v] = struct{}{}
		}
	}
	s.corsPolicy.Store(newCORSPolicy(allowed, current.allowHeaders, current.allowMethods, current.maxAge))
}

func (s *HubServer) SetConfigReloadChannel(channel string) {
	if strings.TrimSpace(channel) == "" {
		return
	}
	s.configReloadChan = channel
}

func (s *HubServer) SetCORSPreflightPolicy(allowedHeaders, allowedMethods []string, maxAge time.Duration) {
	current := s.currentCORSPolicy()
	nextHeaders := current.allowHeaders
	nextMethods := current.allowMethods
	nextMaxAge := current.maxAge
	if len(allowedHeaders) > 0 {
		nextHeaders = allowedHeaders
	}
	if len(allowedMethods) > 0 {
		nextMethods = allowedMethods
	}
	if maxAge > 0 {
		nextMaxAge = maxAge
	}
	s.corsPolicy.Store(newCORSPolicy(current.allowed, nextHeaders, nextMethods, nextMaxAge))
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
	s.apiKeyPattern.Store(re)
	return nil
}

func (s *HubServer) SetTierUpdateMessaging(natsURL, subject string) error {
	if strings.TrimSpace(natsURL) == "" {
		natsURL = nats.DefaultURL
	}
	if strings.TrimSpace(subject) == "" {
		subject = "tier.updates"
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return err
	}
	s.tierUpdateConn = nc
	s.tierUpdateSubj = subject
	return nil
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

func (s *HubServer) startReadinessProbe(ctx context.Context) {
	if s.rdb == nil {
		s.readyHealthy.Store(true)
		return
	}
	if !s.readyProbeStarted.CompareAndSwap(false, true) {
		return
	}
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			probeCtx, cancel := context.WithTimeout(ctx, hubReadyRedisTimeout)
			err := s.rdb.Ping(probeCtx).Err()
			cancel()
			s.readyHealthy.Store(err == nil)

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
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
		if len(parts) > 1 && parts[1] == "dry-run" {
			s.handleConfigReloadDryRun(w, r)
			return
		}
		s.handleConfigReload(w, r)
	case "queue-metrics":
		s.handleQueueMetrics(w, r)
	case "tiers":
		s.handleTiers(w, r, ctx, parts[1:])
	case "rate":
		s.handleRate(w, r, ctx, parts[1:])
	default:
		http.NotFound(w, r)
	}
}

func (s *HubServer) handleCORS(w http.ResponseWriter, r *http.Request) bool {
	policy := s.currentCORSPolicy()
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
		w.Header().Set("Access-Control-Allow-Methods", policy.allowMethodsValue)
		w.Header().Set("Access-Control-Allow-Headers", policy.allowHeadersValue)
		w.Header().Set("Access-Control-Max-Age", policy.maxAgeValue)
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

func (s *HubServer) isCORSOriginAllowed(origin string) bool {
	policy := s.currentCORSPolicy()
	if len(policy.allowed) == 0 {
		return false
	}
	_, ok := policy.allowed[origin]
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
	config := s.cfgManager.Snapshot()
	if err := json.NewEncoder(w).Encode(config); err != nil {
		s.writeInternalError(w, "handleConfig", err)
	}

}

func (s *HubServer) handleConfigReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	nextCfg, mode, err := s.resolveReloadTargetConfig(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	current := s.cfgManager.Snapshot()
	analysis := analyzeConfigUpdate(current, nextCfg)

	if mode == "payload" {
		reloader, ok := s.cfgManager.(configPayloadReloader)
		if !ok {
			http.Error(w, "config payload reload unsupported", http.StatusNotImplemented)
			return
		}
		if err := reloader.ReloadFromPayload(nextCfg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		reloader, ok := s.cfgManager.(interface{ ReloadFromFile() error })
		if !ok {
			http.Error(w, "config reload unsupported", http.StatusNotImplemented)
			return
		}
		if err := reloader.ReloadFromFile(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	event := types.ConfigReloadEvent{
		Version:       types.ConfigReloadEventVersion,
		Epoch:         s.configEpoch.Add(1),
		ReloadedAtUTC: time.Now().UTC().Format(time.RFC3339Nano),
		Invalidations: analysis.Invalidations,
	}
	s.publishConfigReloadEvent(event)
	w.WriteHeader(http.StatusNoContent)
}

func (s *HubServer) handleConfigReloadDryRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	nextCfg, mode, err := s.resolveReloadTargetConfig(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	analysis := analyzeConfigUpdate(s.cfgManager.Snapshot(), nextCfg)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(configUpdateReport{
		Mode:      mode,
		Valid:     true,
		Analysis:  analysis,
		NextEpoch: s.configEpoch.Load() + 1,
	})
}

func (s *HubServer) resolveReloadTargetConfig(r *http.Request) (*types.GatewayConfig, string, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		return nil, "", err
	}
	payloadBody := strings.TrimSpace(string(body))
	if payloadBody != "" {
		cfg, err := decodeAndValidateConfig(body)
		if err != nil {
			return nil, "", err
		}
		return cfg, "payload", nil
	}
	loader, ok := s.cfgManager.(configFileLoader)
	if !ok {
		return nil, "", fmt.Errorf("config file reload unsupported")
	}
	cfg, err := loader.LoadFromFile()
	if err != nil {
		return nil, "", err
	}
	return cfg, "file", nil
}

func (s *HubServer) publishConfigReloadEvent(event types.ConfigReloadEvent) {
	if s.rdb == nil {
		return
	}
	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("hub config reload event marshal failed: %v", err)
		return
	}
	go func(channel string, data []byte) {
		pubCtx, cancel := context.WithTimeout(context.Background(), hubReadyRedisTimeout)
		defer cancel()
		_ = s.rdb.Publish(pubCtx, channel, data).Err()
	}(s.configReloadChan, payload)
}

func (s *HubServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.readyHealthy.Load() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "pending", "missing_dependency": s.pendingDependencyValue()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (s *HubServer) handleQueueMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snapshot := workers.QueueSnapshot{}
	if s.asyncQueue != nil {
		snapshot = s.asyncQueue.Snapshot()
	}
	write := map[string]interface{}{
		"tier_update_queue":                    snapshot,
		"tier_update_publish_failures":         s.tierPublishErrors.Load(),
		"tier_update_submit_non_blocking_mode": true,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(write)
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
	_, ok := s.cfgManager.FindPrefix(prefix)
	return ok
}

func (s *HubServer) matchesAPIKey(apiKey string) bool {
	pattern := s.apiKeyPattern.Load()
	if pattern == nil {
		pattern = regexp.MustCompile(types.DefaultRuntimePolicy().Hub.APIKeyPattern)
		s.apiKeyPattern.Store(pattern)
	}
	return pattern.MatchString(apiKey)
}

func (s *HubServer) publishTierUpdate(prefix, apiKey, tierID string) {
	if s.asyncQueue == nil {
		return
	}
	task := &tierUpdateTask{
		prefix:   prefix,
		apiKey:   apiKey,
		tierID:   tierID,
		retryMax: s.retryMax,
		backoff:  s.retryBackoff,
		conn:     s.tierUpdateConn,
		subject:  s.tierUpdateSubj,
		errorCnt: &s.tierPublishErrors,
	}
	if err := s.asyncQueue.Submit(task, 0); err != nil {
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
	errorCnt *atomic.Uint64
}

func (t *tierUpdateTask) Execute(ctx context.Context) error {
	_ = ctx
	payload := "TIER_UPDATE:" + t.prefix + ":" + t.apiKey + ":" + t.tierID
	if t.conn == nil {
		return nil
	}
	if err := t.conn.Publish(t.subject, []byte(payload)); err != nil {
		if t.errorCnt != nil {
			t.errorCnt.Add(1)
		}
		log.Printf("hub tier update nats publish failed prefix=%s api_key=%s tier=%s subject=%s err=%v", t.prefix, t.apiKey, t.tierID, t.subject, err)
		return err
	}
	return nil
}

func (t *tierUpdateTask) RetryPolicy() *workers.RetryPolicy {
	return &workers.RetryPolicy{MaxRetries: t.retryMax, Backoff: t.backoff}
}
