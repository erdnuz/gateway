package edge

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"gateway/packages/common/types"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxCacheableResponseBytes = 1 << 20 // 1 MiB

type contextKey int

const (
	requestStateCtxKey contextKey = iota
)

type requestState struct {
	svcCfg       *types.ServiceConfig
	tierPolicy   *types.TierConfig
	analytics    *types.AnalyticsEntry
	cacheManager *CacheManager
	prefixID     string
	serviceID    string
	apiKey       string
}

func getRequestState(ctx context.Context) (*requestState, bool) {
	state, ok := ctx.Value(requestStateCtxKey).(*requestState)
	if !ok || state == nil {
		return nil, false
	}
	return state, true
}

// EdgeServer acts as an HTTP gateway with a pipeline of middleware
type EdgeServer struct {
	configManager *ConfigManager
	tierManager   *TierManager
	rateManager   *RateManager
	analyticsSink AnalyticsSink
	rdb           *redis.Client
	client        *http.Client
}

// NewEdgeServer creates a new edge gateway server with necessary managers
func NewEdgeServer(configMgr *ConfigManager, tierMgr *TierManager, rateMgr *RateManager, analyticsSink AnalyticsSink, rdb *redis.Client) *EdgeServer {
	es := &EdgeServer{
		configManager: configMgr,
		tierManager:   tierMgr,
		rateManager:   rateMgr,
		analyticsSink: analyticsSink,
		rdb:           rdb,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    90 * time.Second,
				DisableKeepAlives:  false,
				DisableCompression: false,
			},
		},
	}

	return es
}

func (s *EdgeServer) StartBackgroundWorkers(ctx context.Context) {
	if s.rateManager != nil {
		s.rateManager.StartBackgroundWorkers(ctx)
		if err := s.rateManager.StartSOTSubscriber(ctx); err != nil {
			log.Printf("edge SOT subscriber start failed: %v", err)
		}
	}
	if s.tierManager != nil {
		if err := s.tierManager.StartHubUpdateListener(ctx); err != nil {
			log.Printf("edge tier invalidation listener start failed: %v", err)
		}
	}
}

func (s *EdgeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	state := &requestState{}
	if s.analyticsSink != nil {
		state.analytics = &types.AnalyticsEntry{Timestamp: time.Now(), Method: r.Method}
	}
	ctx := context.WithValue(r.Context(), requestStateCtxKey, state)

	// Pipeline: Analytics -> CORS -> Cache -> Auth -> RateLimit -> Proxy
	pipeline := s.analyticsMiddleware(
		s.corsMiddleware(
			s.authMiddleware(
				s.cacheMiddleware(
					s.rateLimitMiddleware(
						http.HandlerFunc(s.proxyHandler),
					),
				),
			),
		),
	)
	pipeline(w, r.WithContext(ctx))
}

func (s *EdgeServer) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state, ok := getRequestState(r.Context())
		if !ok {
			state = &requestState{}
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

		tierID, err := s.tierManager.GetUserTier(r.Context(), prefixID, apiKey)
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
		rw := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}
		entry := state.analytics

		defer func() {
			entry.TotalLatency = time.Since(entry.Timestamp)
			entry.ResponseCode = uint16(rw.statusCode)
			s.analyticsSink.Capture(entry)
		}()
		next(rw, r)
	}
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
			// Handle CORS preflight
			if r.Method == http.MethodOptions {
				w.Header().Set("Access-Control-Allow-Origin", strings.Join(cfg.CORS.AllowedOrigins, ", "))
				w.Header().Set("Access-Control-Allow-Methods", strings.Join(cfg.CORS.AllowedMethods, ", "))
				w.Header().Set("Access-Control-Max-Age", fmt.Sprintf("%d", int(cfg.CORS.MaxAge.Seconds())))
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Authorization")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			// Set CORS headers for actual request
			origin := r.Header.Get("Origin")
			for _, allowed := range cfg.CORS.AllowedOrigins {
				if allowed == "*" || allowed == origin {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					break
				}
			}
		}
		next(w, r)
	}
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
		rw := &responseWriterWrapper{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
			body:           []byte{},
		}
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

		cost := s.getMethodCost(r.Method, tierPolicy)
		usage, err := s.rateManager.Increment(r.Context(), state.prefixID, apiKey, int64(tierPolicy.Quota), int64(cost))

		if err != nil {
			if state.svcCfg != nil && state.svcCfg.Failure.EffectiveHubPolicy().AllowOnRateServiceError {
				next(w, r)
				return
			}
			http.Error(w, "Rate limit service unavailable", http.StatusServiceUnavailable)
			return
		}

		if usage > int64(tierPolicy.Quota) {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		if state.analytics != nil {
			state.analytics.LimitUsed = uint64(usage)
			state.analytics.LimitUsedOfTotal = float64(usage) / float64(tierPolicy.Quota)
		}

		next(w, r)
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

	// Build request path - handle prefix stripping if configured
	requestPath := s.buildProxyPath(r.URL.Path, cfg)
	targetURL.Path = requestPath
	targetURL.RawQuery = r.URL.RawQuery

	// Create proxy request
	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), r.Body)
	if err != nil {
		http.Error(w, "Failed to create proxy request", http.StatusBadGateway)
		return
	}

	// Copy headers from original request
	proxyReq.Header = r.Header.Clone()

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
	limitedBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxCacheableResponseBytes+1))
	if readErr != nil {
		http.Error(w, "Failed to read response", http.StatusBadGateway)
		return
	}

	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(resp.StatusCode)

	if len(limitedBody) <= maxCacheableResponseBytes {
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
		attemptCtx := ctx
		cancel := func() {}
		if policy.AttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, policy.AttemptTimeout)
			attemptReq = attemptReq.WithContext(attemptCtx)
		}
		resp, err := s.client.Do(attemptReq)
		cancel()
		if err != nil {
			lastErr = err
			if attempt < attempts-1 && s.canRetryMethod(req.Method, policy) {
				time.Sleep(policy.RetryBackoff)
				continue
			}
			return nil, err
		}

		if s.shouldRetryStatus(resp.StatusCode, policy) && attempt < attempts-1 && s.canRetryMethod(req.Method, policy) {
			_ = resp.Body.Close()
			time.Sleep(policy.RetryBackoff)
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

// responseWriterWrapper wraps http.ResponseWriter to capture status codes and response body
type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
	body       []byte
}

func (rw *responseWriterWrapper) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
	rw.ResponseWriter.WriteHeader(statusCode)
}

func (rw *responseWriterWrapper) Write(b []byte) (int, error) {
	rw.body = append(rw.body, b...)
	return rw.ResponseWriter.Write(b)
}

// Hijack support for websockets, etc.
func (rw *responseWriterWrapper) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}
