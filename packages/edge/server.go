package edge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"gateway/packages/common/types"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	edgeReadyProbeInterval = 2 * time.Second
	edgeReadyProbeTimeout  = 500 * time.Millisecond
	// edgeReadyProbeFailures consecutive failed probes mark the edge unready;
	// one slow PING under load must not flip readiness.
	edgeReadyProbeFailures = 3
)

type contextKey int

const (
	requestStateCtxKey contextKey = iota
)

type requestState struct {
	svcCfg       *types.ServiceConfig
	prefixCfg    *types.PrefixConfig
	tierPolicy   *types.TierConfig
	analytics    *types.AnalyticsEntry
	cacheManager *CacheManager
	prefixID     string
	serviceID    string
	apiKey       string
}

func (s *requestState) reset() {
	s.svcCfg = nil
	s.prefixCfg = nil
	s.tierPolicy = nil
	s.analytics = nil
	s.cacheManager = nil
	s.prefixID = ""
	s.serviceID = ""
	s.apiKey = ""
}

// apiKeyMatcher caches the compiled runtime.hub.api_key_pattern so the edge
// rejects malformed keys with the same rule the hub applies.
type apiKeyMatcher struct {
	src string
	re  *regexp.Regexp
}

// redactKey returns a log-safe fingerprint of an API key.
func redactKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return "sha256:" + hex.EncodeToString(sum[:6])
}

var requestStatePool = sync.Pool{New: func() interface{} { return &requestState{} }}
var responseWriterPool = sync.Pool{New: func() interface{} { return &responseWriterWrapper{} }}

func getRequestState(ctx context.Context) (*requestState, bool) {
	state, ok := ctx.Value(requestStateCtxKey).(*requestState)
	if !ok || state == nil {
		return nil, false
	}
	return state, true
}

// EdgeServer acts as an HTTP gateway with a pipeline of middleware
type EdgeServer struct {
	configManager             *ConfigManager
	tierManager               *TierManager
	rateManager               *RateManager
	analyticsSink             AnalyticsSink
	edgeID                    string
	rdb                       *redis.Client
	client                    *http.Client
	maxCacheableResponseBytes int64
	maxRequestBodyBytes       int64
	adminToken                string
	readyHealthy              atomic.Bool // startup gate: false blocks all traffic
	localHealthy              atomic.Bool // edge-local deps; reported by /readyz only
	pendingDependency         atomic.Value
	keyMatcher                atomic.Pointer[apiKeyMatcher]
	pipeline                  http.HandlerFunc
}

type EdgeServerOptions struct {
	UpstreamClientTimeout     time.Duration
	MaxCacheableResponseBytes int64
	UpstreamMaxIdleConns      int
	UpstreamIdleConnTimeout   time.Duration
	EdgeID                    string
	// MaxRequestBodyBytes caps proxied request bodies; 0 → 10 MiB.
	MaxRequestBodyBytes int64
	// AdminToken guards internal endpoints (/analytics/metrics). Empty
	// disables them.
	AdminToken string
}

const defaultMaxRequestBodyBytes = 10 << 20

// NewEdgeServer creates a new edge gateway server with necessary managers
func NewEdgeServer(configMgr *ConfigManager, tierMgr *TierManager, rateMgr *RateManager, analyticsSink AnalyticsSink, rdb *redis.Client) *EdgeServer {
	return NewEdgeServerWithOptions(configMgr, tierMgr, rateMgr, analyticsSink, rdb, EdgeServerOptions{})
}

func NewEdgeServerWithOptions(configMgr *ConfigManager, tierMgr *TierManager, rateMgr *RateManager, analyticsSink AnalyticsSink, rdb *redis.Client, options EdgeServerOptions) *EdgeServer {
	if options.UpstreamClientTimeout <= 0 {
		options.UpstreamClientTimeout = types.DefaultRuntimePolicy().Edge.UpstreamClientTimeout
	}
	if options.MaxCacheableResponseBytes <= 0 {
		options.MaxCacheableResponseBytes = types.DefaultRuntimePolicy().Edge.CacheMaxObjectBytes
	}
	if options.UpstreamMaxIdleConns <= 0 {
		options.UpstreamMaxIdleConns = types.DefaultRuntimePolicy().Edge.UpstreamMaxIdleConns
	}
	if options.UpstreamIdleConnTimeout <= 0 {
		options.UpstreamIdleConnTimeout = types.DefaultRuntimePolicy().Edge.UpstreamIdleConnTimeout
	}
	if options.MaxRequestBodyBytes <= 0 {
		options.MaxRequestBodyBytes = defaultMaxRequestBodyBytes
	}
	es := &EdgeServer{
		configManager:             configMgr,
		tierManager:               tierMgr,
		rateManager:               rateMgr,
		analyticsSink:             analyticsSink,
		edgeID:                    strings.TrimSpace(options.EdgeID),
		rdb:                       rdb,
		maxCacheableResponseBytes: options.MaxCacheableResponseBytes,
		maxRequestBodyBytes:       options.MaxRequestBodyBytes,
		adminToken:                options.AdminToken,
		client: &http.Client{
			Timeout: options.UpstreamClientTimeout,
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				MaxIdleConns:        options.UpstreamMaxIdleConns,
				MaxIdleConnsPerHost: options.UpstreamMaxIdleConns,
				IdleConnTimeout:     options.UpstreamIdleConnTimeout,
			},
			// The gateway relays redirects to the client instead of following them.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	if es.edgeID == "" {
		es.edgeID = "unknown-edge"
	}
	es.readyHealthy.Store(true)
	es.localHealthy.Store(true)
	es.pendingDependency.Store("hub_readyz")

	// Pipeline: Analytics -> CORS -> Auth -> Cache -> RateLimit -> Proxy.
	// Built once; per-request state travels in the context.
	es.pipeline = es.analyticsMiddleware(
		es.corsMiddleware(
			es.authMiddleware(
				es.cacheMiddleware(
					es.rateLimitMiddleware(
						http.HandlerFunc(es.proxyHandler),
					),
				),
			),
		),
	)
	return es
}

// StartReadinessProbe keeps /readyz tied to edge-local dependencies (Redis)
// once startup is complete. Hub or analytics outages must not take the data
// plane down: the failure policies handle them per request.
func (s *EdgeServer) StartReadinessProbe(ctx context.Context) {
	s.startReadinessProbe(ctx)
}

func (s *EdgeServer) startReadinessProbe(ctx context.Context) {
	if s.rdb == nil {
		s.localHealthy.Store(false)
		return
	}
	go func() {
		ticker := time.NewTicker(edgeReadyProbeInterval)
		defer ticker.Stop()
		failures := 0
		for {
			probeCtx, cancel := context.WithTimeout(ctx, edgeReadyProbeTimeout)
			err := s.rdb.Ping(probeCtx).Err()
			cancel()
			if err == nil {
				failures = 0
				s.localHealthy.Store(true)
			} else if failures++; failures >= edgeReadyProbeFailures {
				s.SetPendingDependency("edge_redis")
				s.localHealthy.Store(false)
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *EdgeServer) StartBackgroundWorkers(ctx context.Context) {
	if s.rateManager != nil {
		s.rateManager.StartBackgroundWorkers(ctx)
	}
	if s.tierManager != nil {
		if err := s.tierManager.StartHubUpdateListener(ctx); err != nil {
			log.Printf("edge tier invalidation listener start failed: %v", err)
		}
	}
}

func (s *EdgeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}
	if r.URL.Path == "/analytics/metrics" {
		if !s.authorizeAdmin(r) {
			http.NotFound(w, r)
			return
		}
		if m, ok := s.analyticsSink.(interface{ Stats() AnalyticsManagerStats }); ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(m.Stats())
			return
		}
		http.Error(w, "analytics metrics unavailable", http.StatusNotFound)
		return
	}
	if r.URL.Path == "/readyz" {
		if !s.readyHealthy.Load() || !s.localHealthy.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "pending", "missing_dependency": s.pendingDependencyValue()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
		return
	}
	if !s.readyHealthy.Load() {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "upstream dependencies not ready", "status": "pending", "missing_dependency": s.pendingDependencyValue()})
		return
	}

	state := requestStatePool.Get().(*requestState)
	state.reset()
	defer func() {
		state.reset()
		requestStatePool.Put(state)
	}()
	if s.analyticsSink != nil {
		state.analytics = &types.AnalyticsEntry{Timestamp: time.Now(), Method: r.Method, EdgeID: s.edgeID}
	}
	ctx := context.WithValue(r.Context(), requestStateCtxKey, state)
	s.pipeline(w, r.WithContext(ctx))
}

func (s *EdgeServer) authorizeAdmin(r *http.Request) bool {
	if s.adminToken == "" {
		return false
	}
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return ok && subtle.ConstantTimeCompare([]byte(got), []byte(s.adminToken)) == 1
}

// validAPIKey applies the hub's runtime.hub.api_key_pattern at the edge so
// keys that the hub would reject (or mis-route, e.g. "key/suffix") never
// reach tier lookup, leasing, or Redis.
func (s *EdgeServer) validAPIKey(apiKey string) bool {
	src := ""
	if s.configManager != nil {
		if cfg := s.configManager.Get(); cfg != nil {
			src = cfg.Runtime.Hub.APIKeyPattern
		}
	}
	if src == "" {
		src = types.DefaultRuntimePolicy().Hub.APIKeyPattern
	}
	m := s.keyMatcher.Load()
	if m == nil || m.src != src {
		re, err := regexp.Compile(src)
		if err != nil {
			re = regexp.MustCompile(types.DefaultRuntimePolicy().Hub.APIKeyPattern)
		}
		m = &apiKeyMatcher{src: src, re: re}
		s.keyMatcher.Store(m)
	}
	return m.re.MatchString(apiKey)
}

func (s *EdgeServer) SetStartupReady(ready bool) {
	s.readyHealthy.Store(ready)
}

func (s *EdgeServer) SetPendingDependency(dependency string) {
	v := strings.TrimSpace(dependency)
	if v == "" {
		v = "unknown"
	}
	s.pendingDependency.Store(v)
}

func (s *EdgeServer) pendingDependencyValue() string {
	v := s.pendingDependency.Load()
	dep, _ := v.(string)
	dep = strings.TrimSpace(dep)
	if dep == "" {
		return "unknown"
	}
	return dep
}

func (s *EdgeServer) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state, ok := getRequestState(r.Context())
		if !ok {
			state = requestStatePool.Get().(*requestState)
			state.reset()
			defer func() {
				state.reset()
				requestStatePool.Put(state)
			}()
			r = r.WithContext(context.WithValue(r.Context(), requestStateCtxKey, state))
		}

		prefixID, serviceID := s.parsePath(r.URL.Path)
		if prefixID == "" || serviceID == "" {
			http.Error(w, "Invalid path format", http.StatusBadRequest)
			return
		}

		cfg, err := s.configManager.GetServiceConfig(prefixID, serviceID)
		if err != nil {
			http.Error(w, "Service not found", http.StatusNotFound)
			return
		}

		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			http.Error(w, "X-API-Key header required", http.StatusUnauthorized)
			return
		}
		if !s.validAPIKey(apiKey) {
			http.Error(w, "Invalid API key", http.StatusUnauthorized)
			return
		}
		prefixCfg, _ := s.configManager.GetPrefix(prefixID)

		tierID, err := s.tierManager.GetUserTier(r.Context(), prefixID, apiKey)
		if err == nil && tierID == "" {
			// The hub knows no tier for this key: it was never issued or was revoked.
			http.Error(w, "Invalid API key", http.StatusUnauthorized)
			return
		}
		if err != nil {
			hubPolicy := cfg.Failure.EffectiveHubPolicy()
			switch hubPolicy.TierLookupStrategy {
			case "default-tier":
				tierID = hubPolicy.DefaultTier
			case "stale-or-default":
				if staleTier, ok := s.tierManager.GetStaleTier(prefixID, apiKey, hubPolicy.StaleTierMaxAge); ok {
					tierID = staleTier
				} else {
					tierID = hubPolicy.DefaultTier
				}
			default:
				http.Error(w, "Tier lookup failed", http.StatusInternalServerError)
				return
			}
			if tierID == "" {
				http.Error(w, "Tier lookup failed", http.StatusInternalServerError)
				return
			}
		}

		tierPolicy, found := GetTier(cfg, tierID)
		if !found {
			http.Error(w, "Tier not defined", http.StatusForbidden)
			return
		}

		state.svcCfg = cfg
		state.prefixCfg = prefixCfg
		state.tierPolicy = tierPolicy
		state.prefixID = prefixID
		state.serviceID = serviceID
		state.apiKey = apiKey

		if state.analytics != nil {
			state.analytics.Prefix = prefixID
			state.analytics.Service = serviceID
			state.analytics.Tier = tierID
		}

		next(w, r)
	}
}

func (s *EdgeServer) proxyHandler(w http.ResponseWriter, r *http.Request) {
	state, ok := getRequestState(r.Context())
	if !ok || state.svcCfg == nil {
		http.Error(w, "Service config not found", http.StatusInternalServerError)
		return
	}
	entry := state.analytics
	if entry == nil {
		entry = &types.AnalyticsEntry{}
	}

	s.executeProxy(w, r, state.svcCfg, entry)
}

func (s *EdgeServer) analyticsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.analyticsSink == nil {
			next(w, r)
			return
		}
		state, ok := getRequestState(r.Context())
		if !ok || state.analytics == nil {
			next(w, r)
			return
		}
		rw := responseWriterPool.Get().(*responseWriterWrapper)
		rw.reset(w)
		defer responseWriterPool.Put(rw)
		entry := state.analytics

		defer func() {
			entry.TotalLatency = time.Since(entry.Timestamp)
			entry.ResponseCode = uint16(rw.statusCode)
			if shouldCaptureAnalytics(state.svcCfg) {
				s.analyticsSink.Capture(entry)
			}
		}()
		next(rw, r)
	}
}

func shouldCaptureAnalytics(cfg *types.ServiceConfig) bool {
	if cfg == nil {
		return false
	}
	if !cfg.Analytics.Enabled {
		return false
	}
	rate := cfg.Analytics.SamplingRate
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	return rand.Float64() < rate
}

func (s *EdgeServer) corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state, stateOK := getRequestState(r.Context())
		var cfg *types.ServiceConfig
		ok := stateOK && state != nil && state.svcCfg != nil
		if ok {
			cfg = state.svcCfg
		} else if s.configManager != nil {
			prefixID, serviceID := s.parsePath(r.URL.Path)
			if prefixID != "" && serviceID != "" {
				if resolvedCfg, err := s.configManager.GetServiceConfig(prefixID, serviceID); err == nil {
					cfg = resolvedCfg
					ok = true
				}
			}
		}
		if ok && cfg != nil && cfg.CORS != nil {
			origin := r.Header.Get("Origin")
			allowedOrigin, originAllowed := resolveAllowedCORSOrigin(origin, cfg.CORS.AllowedOrigins)
			// Handle CORS preflight
			if r.Method == http.MethodOptions {
				if originAllowed {
					w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
				}
				w.Header().Set("Access-Control-Allow-Methods", strings.Join(cfg.CORS.AllowedMethods, ", "))
				w.Header().Set("Access-Control-Max-Age", fmt.Sprintf("%d", int(cfg.CORS.MaxAge.Seconds())))
				if len(cfg.CORS.AllowedHeaders) > 0 {
					w.Header().Set("Access-Control-Allow-Headers", strings.Join(cfg.CORS.AllowedHeaders, ", "))
				} else {
					w.Header().Set("Access-Control-Allow-Headers", strings.Join(types.DefaultRuntimePolicy().Hub.CORSAllowedHeaders, ", "))
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			// Set CORS headers for actual request
			if originAllowed {
				w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			}
		}
		next(w, r)
	}
}

func resolveAllowedCORSOrigin(origin string, allowedOrigins []string) (string, bool) {
	trimmed := strings.TrimSpace(origin)
	if trimmed == "" {
		return "", false
	}
	for _, allowed := range allowedOrigins {
		candidate := strings.TrimSpace(allowed)
		if candidate == "" {
			continue
		}
		if candidate == "*" {
			return trimmed, true
		}
		if candidate == trimmed {
			return trimmed, true
		}
	}
	return "", false
}

func (s *EdgeServer) cacheMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Only cache GET requests
		if r.Method != http.MethodGet {
			next(w, r)
			return
		}

		state, ok := getRequestState(r.Context())
		if !ok || state.svcCfg == nil || state.svcCfg.Cache == nil || !state.svcCfg.Cache.Enabled {
			next(w, r)
			return
		}
		cfg := state.svcCfg

		apiKey := state.apiKey
		if apiKey == "" {
			apiKey = r.Header.Get("X-API-Key")
		}
		cm := NewCacheManager(s.rdb, *cfg.Cache)

		// Check if response is cached
		cached, found, err := cm.CheckCache(r.Context(), r, apiKey)
		if err == nil && found {
			if state.analytics != nil {
				state.analytics.CacheHit = true
			}
			w.Header().Set("X-Cache", "HIT")
			if cached.ContentType != "" {
				w.Header().Set("Content-Type", cached.ContentType)
			}
			w.WriteHeader(cached.StatusCode)
			_, _ = w.Write(cached.Body)
			return
		}

		// Wrap response writer to capture response for caching
		rw := responseWriterPool.Get().(*responseWriterWrapper)
		rw.reset(w)
		defer responseWriterPool.Put(rw)
		if state.analytics != nil {
			state.analytics.CacheHit = false
		}

		state.cacheManager = cm
		next(rw, r)
	}
}

func (s *EdgeServer) rateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state, ok := getRequestState(r.Context())
		if !ok || state.tierPolicy == nil {
			http.Error(w, "Tier policy not found", http.StatusInternalServerError)
			return
		}
		tierPolicy := state.tierPolicy

		if state.prefixID == "" {
			http.Error(w, "Prefix not found in context", http.StatusInternalServerError)
			return
		}

		apiKey := state.apiKey
		if apiKey == "" {
			apiKey = r.Header.Get("X-API-Key")
		}
		if apiKey == "" {
			http.Error(w, "X-API-Key header required", http.StatusUnauthorized)
			return
		}

		req := RateRequest{
			Prefix:  state.prefixID,
			Service: state.serviceID,
			APIKey:  apiKey,
			Limit:   int64(tierPolicy.Quota),
			Cost:    int64(s.getMethodCost(r.Method, tierPolicy)),
		}
		if p := state.prefixCfg; p != nil {
			// Mirror the hub: a missing quota period means 1s windows.
			req.Period = max(p.QuotaPeriod, time.Second)
			req.LeaseSize = p.Leasing.Effective(s.configManager.EdgePolicy(), p.QuotaPeriod).LeaseQuantum
		}
		decision, err := s.rateManager.Allow(r.Context(), req)

		if errors.Is(err, ErrLeaseRejected) {
			http.Error(w, "API key not permitted for this service", http.StatusForbidden)
			return
		}
		if err != nil {
			if !errors.Is(err, ErrLeaseUnavailable) || rateErrLog.Allow() {
				log.Printf("edge rate check failed prefix=%s service=%s key=%s err=%v", state.prefixID, state.serviceID, redactKey(apiKey), err)
			}
			if state.svcCfg != nil {
				hubPolicy := state.svcCfg.Failure.EffectiveHubPolicy()
				if safety := state.svcCfg.SafetyTier; hubPolicy.AllowOnRateServiceError && safety != nil {
					safetyReq := req
					safetyReq.Limit = int64(safety.Quota)
					safetyReq.Cost = int64(s.getMethodCost(r.Method, safety))
					sd := s.rateManager.AllowSafety(safetyReq)
					writeRateLimitHeaders(w, sd)
					if sd.Allowed {
						next(w, r)
						return
					}
					http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
					return
				}
			}
			http.Error(w, "Rate limit service unavailable", http.StatusServiceUnavailable)
			return
		}

		writeRateLimitHeaders(w, decision)
		if !decision.Allowed {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		if state.analytics != nil && decision.Limit > 0 {
			used := decision.Limit - decision.Remaining
			state.analytics.LimitUsed = uint64(used)
			state.analytics.LimitUsedOfTotal = float64(used) / float64(decision.Limit)
		}

		next(w, r)
	}
}

// rateErrLog throttles lease-unavailable logging during hub outages.
var rateErrLog = newLogThrottle(time.Second)

type logThrottle struct {
	every  int64
	nextNs atomic.Int64
}

func newLogThrottle(every time.Duration) *logThrottle {
	return &logThrottle{every: int64(every)}
}

func (l *logThrottle) Allow() bool {
	now := time.Now().UnixNano()
	next := l.nextNs.Load()
	return now >= next && l.nextNs.CompareAndSwap(next, now+l.every)
}

// writeRateLimitHeaders emits the IETF RateLimit header fields
// (draft-ietf-httpapi-ratelimit-headers) plus Retry-After on denial.
func writeRateLimitHeaders(w http.ResponseWriter, d RateDecision) {
	h := w.Header()
	h.Set("RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
	h.Set("RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
	if !d.ResetAt.IsZero() {
		reset := int64(math.Ceil(time.Until(d.ResetAt).Seconds()))
		if reset < 0 {
			reset = 0
		}
		h.Set("RateLimit-Reset", strconv.FormatInt(reset, 10))
		if !d.Allowed {
			h.Set("Retry-After", strconv.FormatInt(max(reset, 1), 10))
		}
	} else if !d.Allowed {
		h.Set("Retry-After", "1")
	}
}

// Helper Methods

// parsePath extracts prefix and service ID from URL path
// Expected format: /{prefix}/{service}/...
func (s *EdgeServer) parsePath(path string) (string, string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

// getMethodCost returns the cost for a given HTTP method based on tier configuration
func (s *EdgeServer) getMethodCost(method string, tier *types.TierConfig) uint8 {
	switch method {
	case http.MethodGet:
		return tier.GetCost
	case http.MethodPost:
		return tier.PostCost
	case http.MethodPut:
		return tier.PutCost
	case http.MethodDelete:
		return tier.DeleteCost
	default:
		return tier.OtherCost
	}
}

// executeProxy handles the HTTP proxying to upstream service
func (s *EdgeServer) executeProxy(w http.ResponseWriter, r *http.Request, cfg *types.ServiceConfig, entry *types.AnalyticsEntry) {
	policy := cfg.Failure.EffectiveUpstreamPolicy()

	// Parse target URL
	targetURL, err := url.Parse(cfg.TargetURL)
	if err != nil {
		http.Error(w, "Invalid target URL", http.StatusBadGateway)
		return
	}

	// Build request path - handle prefix stripping if configured, and keep
	// any base path of the target URL (e.g. http://svc/api/v2).
	targetURL.Path = joinURLPath(targetURL.Path, s.buildProxyPath(r.URL.Path, cfg))
	targetURL.RawPath = ""
	targetURL.RawQuery = r.URL.RawQuery

	body := http.MaxBytesReader(w, r.Body, s.maxRequestBodyBytes)
	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), body)
	if err != nil {
		http.Error(w, "Failed to create proxy request", http.StatusBadGateway)
		return
	}
	proxyReq.ContentLength = r.ContentLength
	// Retries need a rewindable body; buffer (bounded) only when a retry is possible.
	if policy.MaxRetries > 0 && s.canRetryMethod(r.Method, policy) && r.Body != nil && r.Body != http.NoBody {
		buf, readErr := io.ReadAll(body)
		if readErr != nil {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		proxyReq.Body = io.NopCloser(bytes.NewReader(buf))
		proxyReq.ContentLength = int64(len(buf))
		proxyReq.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf)), nil }
	}

	proxyReq.Header = r.Header.Clone()
	removeHopByHopHeaders(proxyReq.Header)
	// The gateway credential is for the gateway; upstreams must not see it.
	proxyReq.Header.Del("X-API-Key")
	setForwardedHeaders(proxyReq, r)

	// Apply request transforms (add headers, hide headers, etc.)
	s.applyRequestTransforms(proxyReq, cfg.Transform)

	// Record upstream start time
	startTime := time.Now()

	// Execute proxy request with resilience policy.
	resp, err := s.doProxyWithPolicy(r.Context(), proxyReq, policy)
	entry.UpstreamLatency = time.Since(startTime)

	if err != nil {
		// Handle failure strategy
		s.handleProxyError(w, cfg.Failure, err, entry)
		return
	}
	defer resp.Body.Close()

	// Copy response headers from upstream
	removeHopByHopHeaders(resp.Header)
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Apply response transforms (hide headers, etc.)
	s.applyResponseTransforms(w, cfg.Transform)

	requestSize := uint64(0)
	if r.ContentLength > 0 {
		requestSize = uint64(r.ContentLength)
	}

	// Stream by default; only buffer when we need to make a bounded cache decision.
	if r.Method != http.MethodGet || cfg.Cache == nil || !cfg.Cache.Enabled {
		w.Header().Set("X-Cache", "MISS")
		w.WriteHeader(resp.StatusCode)
		written, copyErr := io.Copy(w, resp.Body)
		if copyErr != nil {
			http.Error(w, "Failed to stream response", http.StatusBadGateway)
			return
		}
		entry.ResponseSize = uint64(written)
		entry.RequestSize = requestSize
		return
	}

	// Cache-enabled GET path: read a bounded prefix first.
	limitedBody, readErr := io.ReadAll(io.LimitReader(resp.Body, s.maxCacheableResponseBytes+1))
	if readErr != nil {
		http.Error(w, "Failed to read response", http.StatusBadGateway)
		return
	}

	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(resp.StatusCode)

	if int64(len(limitedBody)) <= s.maxCacheableResponseBytes {
		// Entire body fits in cache bound; safe to cache and write once.
		s.cacheResponse(r, cfg, resp.StatusCode, resp.Header.Get("Content-Type"), limitedBody)
		if _, writeErr := w.Write(limitedBody); writeErr != nil {
			return
		}
		entry.ResponseSize = uint64(len(limitedBody))
		entry.RequestSize = requestSize
		return
	}

	// Body exceeded cache bound: stream remaining bytes and skip cache write.
	written, writeErr := w.Write(limitedBody)
	if writeErr != nil {
		return
	}
	streamedTail, tailErr := io.Copy(w, resp.Body)
	if tailErr != nil && !errors.Is(tailErr, io.EOF) {
		http.Error(w, "Failed to stream response", http.StatusBadGateway)
		return
	}
	entry.ResponseSize = uint64(written) + uint64(streamedTail)
	entry.RequestSize = requestSize
}

func (s *EdgeServer) doProxyWithPolicy(ctx context.Context, req *http.Request, policy types.UpstreamFailurePolicy) (*http.Response, error) {
	attempts := 1 + policy.MaxRetries
	if attempts < 1 {
		attempts = 1
	}
	lastErr := error(nil)
	for attempt := 0; attempt < attempts; attempt++ {
		attemptReq := req.Clone(ctx)
		if attempt > 0 && req.GetBody != nil {
			b, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			attemptReq.Body = b
		}
		cancel := context.CancelFunc(func() {})
		if policy.AttemptTimeout > 0 {
			var attemptCtx context.Context
			attemptCtx, cancel = context.WithTimeout(ctx, policy.AttemptTimeout)
			attemptReq = attemptReq.WithContext(attemptCtx)
		}
		resp, err := s.client.Do(attemptReq)
		if err != nil {
			cancel()
			lastErr = err
			if attempt < attempts-1 && s.canRetryMethod(req.Method, policy) {
				if !sleepCtx(ctx, policy.RetryBackoff) {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, err
		}
		// The attempt context must outlive Do: it also governs reading the body.
		resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}

		if s.shouldRetryStatus(resp.StatusCode, policy) && attempt < attempts-1 && s.canRetryMethod(req.Method, policy) {
			_ = resp.Body.Close()
			if !sleepCtx(ctx, policy.RetryBackoff) {
				return nil, ctx.Err()
			}
			continue
		}

		if s.shouldRetryStatus(resp.StatusCode, policy) {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("upstream returned status %d", resp.StatusCode)
		}
		return resp, nil
	}
	if lastErr == nil {
		lastErr = errors.New("proxy retries exhausted")
	}
	return nil, lastErr
}

func (s *EdgeServer) shouldRetryStatus(statusCode int, policy types.UpstreamFailurePolicy) bool {
	for _, retryCode := range policy.RetryOnStatuses {
		if retryCode == statusCode {
			return true
		}
	}
	return false
}

func (s *EdgeServer) canRetryMethod(method string, policy types.UpstreamFailurePolicy) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	default:
		return policy.RetryNonIdempotentMethods
	}
}

// buildProxyPath constructs the path for the proxy request
func (s *EdgeServer) buildProxyPath(originalPath string, cfg *types.ServiceConfig) string {
	if cfg.Transform.StripPrefix {
		// Remove first two path segments (prefix and service)
		parts := strings.Split(strings.Trim(originalPath, "/"), "/")
		if len(parts) > 2 {
			return "/" + strings.Join(parts[2:], "/")
		}
		return "/"
	}
	return originalPath
}

// applyRequestTransforms modifies the proxy request based on configuration
func (s *EdgeServer) applyRequestTransforms(req *http.Request, transform types.TransformConfig) {
	for key := range req.Header {
		if strings.HasPrefix(http.CanonicalHeaderKey(key), "X-Gate-") {
			req.Header.Del(key)
		}
	}

	// Add headers
	for key, value := range transform.AddHeaders {
		req.Header.Set(key, value)
	}

	// Remove specific headers
	for _, header := range transform.HideHeaders {
		req.Header.Del(header)
	}
}

// applyResponseTransforms modifies the response headers before sending to client
func (s *EdgeServer) applyResponseTransforms(w http.ResponseWriter, transform types.TransformConfig) {
	// Remove headers that should be hidden from client
	for _, header := range transform.HideHeaders {
		w.Header().Del(header)
	}
}

// cacheResponse attempts to store the response in cache if configured
func (s *EdgeServer) cacheResponse(r *http.Request, cfg *types.ServiceConfig, statusCode int, contentType string, body []byte) {
	if r.Method != http.MethodGet || cfg.Cache == nil || !cfg.Cache.Enabled {
		return
	}
	cached := &CachedResponse{StatusCode: statusCode, ContentType: contentType, Body: body}

	state, ok := getRequestState(r.Context())
	if !ok || state.cacheManager == nil {
		// Fall back to a temporary manager if middleware state is unavailable.
		cm := NewCacheManager(s.rdb, *cfg.Cache)
		apiKey := r.Header.Get("X-API-Key")
		_ = cm.SetCache(r.Context(), r, apiKey, cached)
		return
	}

	apiKey := state.apiKey
	if apiKey == "" {
		apiKey = r.Header.Get("X-API-Key")
	}
	_ = state.cacheManager.SetCache(r.Context(), r, apiKey, cached)
}

// handleProxyError handles upstream service failures
func (s *EdgeServer) handleProxyError(w http.ResponseWriter, failure types.FailureConfig, err error, entry *types.AnalyticsEntry) {
	// log error if present so operators can diagnose
	if err != nil {
		// using the standard library log package avoids additional deps
		// we do not want to crash the handler, just record the cause
		log.Printf("proxy error: %v", err)
	}

	// if analytics entry exists, note that the request failed upstream
	if entry != nil {
		entry.UpstreamError = true
	}

	upstreamPolicy := failure.EffectiveUpstreamPolicy()
	if upstreamPolicy.Mode == "fail-open" {
		for key, value := range upstreamPolicy.FallbackHeaders {
			w.Header().Set(key, value)
		}
		if failure.FallbackTier != "" {
			w.Header().Set("X-Fallback-Tier", failure.FallbackTier)
		}
		w.WriteHeader(upstreamPolicy.FallbackStatusCode)
		_, _ = w.Write([]byte(upstreamPolicy.FallbackBody))
		return
	}

	// On fail-closed, deny the request
	http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
}

// responseWriterWrapper wraps http.ResponseWriter to capture the status code.
// It never buffers the body, so responses stream straight through.
type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (rw *responseWriterWrapper) reset(w http.ResponseWriter) {
	rw.ResponseWriter = w
	rw.statusCode = http.StatusOK
	rw.wroteHeader = false
}

func (rw *responseWriterWrapper) WriteHeader(statusCode int) {
	if rw.wroteHeader {
		return
	}
	rw.wroteHeader = true
	rw.statusCode = statusCode
	rw.ResponseWriter.WriteHeader(statusCode)
}

func (rw *responseWriterWrapper) Write(b []byte) (int, error) {
	rw.wroteHeader = true
	return rw.ResponseWriter.Write(b)
}

// Flush lets streaming responses (SSE, chunked) reach the client promptly.
func (rw *responseWriterWrapper) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (rw *responseWriterWrapper) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// cancelOnClose releases a per-attempt context once the body is consumed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// hopByHopHeaders are connection-scoped (RFC 9110 §7.6.1) and must not be
// forwarded by a proxy.
var hopByHopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

func removeHopByHopHeaders(h http.Header) {
	for _, f := range h.Values("Connection") {
		for _, name := range strings.Split(f, ",") {
			if name = strings.TrimSpace(name); name != "" {
				h.Del(name)
			}
		}
	}
	for _, name := range hopByHopHeaders {
		h.Del(name)
	}
}

func setForwardedHeaders(out, in *http.Request) {
	if ip, _, err := net.SplitHostPort(in.RemoteAddr); err == nil {
		if prior := in.Header.Values("X-Forwarded-For"); len(prior) > 0 {
			ip = strings.Join(prior, ", ") + ", " + ip
		}
		out.Header.Set("X-Forwarded-For", ip)
	}
	proto := "http"
	if in.TLS != nil {
		proto = "https"
	}
	out.Header.Set("X-Forwarded-Proto", proto)
	out.Header.Set("X-Forwarded-Host", in.Host)
}

// joinURLPath appends a request path to a target base path without
// doubling or dropping the slash between them.
func joinURLPath(base, p string) string {
	if base == "" || base == "/" {
		return p
	}
	return strings.TrimSuffix(base, "/") + "/" + strings.TrimPrefix(p, "/")
}

// Hijack support for websockets, etc.
func (rw *responseWriterWrapper) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}
