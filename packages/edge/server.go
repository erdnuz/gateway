package edge

import (
	"bufio"
	"context"
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

type contextKey int

const (
	svcCfgKey contextKey = iota
	tierKey
	analyticsKey
	cacheKeyCtx
	prefixIDCtx
	serviceIDCtx
	apiKeyCtx
)

// EdgeServer acts as an HTTP gateway with a pipeline of middleware
type EdgeServer struct {
	configManager    *ConfigManager
	tierManager      *TierManager
	rateManager      *RateManager
	analyticsManager *AnalyticsManager
	rdb              *redis.Client
	client           *http.Client
}

// NewEdgeServer creates a new edge gateway server with necessary managers
func NewEdgeServer(configMgr *ConfigManager, tierMgr *TierManager, rateMgr *RateManager, analyticsMgr *AnalyticsManager, rdb *redis.Client) *EdgeServer {
	es := &EdgeServer{
		configManager:    configMgr,
		tierManager:      tierMgr,
		rateManager:      rateMgr,
		analyticsManager: analyticsMgr,
		rdb:              rdb,
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

	// if the rate manager is present, start its SOT listener so the edge can
	// keep the local sync keys up to date.
	if rateMgr != nil {
		// background context to run indefinitely
		_ = rateMgr.StartSOTSubscriber(context.Background())
	}

	return es
}

func (s *EdgeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	entry := &types.AnalyticsEntry{Timestamp: time.Now(), Method: r.Method}
	ctx := context.WithValue(r.Context(), analyticsKey, entry)

	// Pipeline: Analytics -> CORS -> Cache -> Auth -> RateLimit -> Proxy
	pipeline := s.analyticsMiddleware(
		s.corsMiddleware(
			s.cacheMiddleware(
				s.authMiddleware(
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
			http.Error(w, "Tier lookup failed", http.StatusInternalServerError)
			return
		}

		tierPolicy, found := GetTier(cfg, tierID)
		if !found {
			http.Error(w, "Tier not defined", http.StatusForbidden)
			return
		}

		// Store request context
		entry := r.Context().Value(analyticsKey).(*types.AnalyticsEntry)
		if entry != nil {
			entry.Prefix = prefixID
			entry.Service = serviceID
			entry.Tier = tierID
		}

		// Build new context with all extracted values
		ctx := context.WithValue(r.Context(), svcCfgKey, cfg)
		ctx = context.WithValue(ctx, tierKey, tierPolicy)
		ctx = context.WithValue(ctx, prefixIDCtx, prefixID)
		ctx = context.WithValue(ctx, serviceIDCtx, serviceID)
		ctx = context.WithValue(ctx, apiKeyCtx, apiKey)

		next(w, r.WithContext(ctx))
	}
}

func (s *EdgeServer) proxyHandler(w http.ResponseWriter, r *http.Request) {
	cfg, ok := r.Context().Value(svcCfgKey).(*types.ServiceConfig)
	if !ok {
		http.Error(w, "Service config not found", http.StatusInternalServerError)
		return
	}

	entry, ok := r.Context().Value(analyticsKey).(*types.AnalyticsEntry)
	if !ok {
		entry = &types.AnalyticsEntry{}
	}

	s.executeProxy(w, r, cfg, entry)
}

func (s *EdgeServer) analyticsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rw := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}
		entry := r.Context().Value(analyticsKey).(*types.AnalyticsEntry)

		defer func() {
			entry.TotalLatency = time.Since(entry.Timestamp)
			entry.ResponseCode = uint16(rw.statusCode)
			s.analyticsManager.Capture(entry)
		}()
		next(rw, r)
	}
}

func (s *EdgeServer) corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Try to get config from context (set by authMiddleware)
		cfg, ok := r.Context().Value(svcCfgKey).(*types.ServiceConfig)
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

		// Retrieve service config to check if caching is enabled
		cfg, ok := r.Context().Value(svcCfgKey).(*types.ServiceConfig)
		if !ok || cfg == nil || cfg.Cache == nil || !cfg.Cache.Enabled {
			next(w, r)
			return
		}

		apiKey := r.Header.Get("X-API-Key")
		cm := NewCacheManager(s.rdb, *cfg.Cache)

		// Check if response is cached
		val, found, err := cm.CheckCache(r.Context(), r, apiKey)
		if err == nil && found {
			w.Header().Set("X-Cache", "HIT")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(val)
			return
		}

		// Wrap response writer to capture response for caching
		rw := &responseWriterWrapper{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
			body:           []byte{},
		}

		// Add cache manager and key to context
		ctx := context.WithValue(r.Context(), cacheKeyCtx, cm)
		next(rw, r.WithContext(ctx))
	}
}

func (s *EdgeServer) rateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tierPolicy, ok := r.Context().Value(tierKey).(*types.TierConfig)
		if !ok || tierPolicy == nil {
			http.Error(w, "Tier policy not found", http.StatusInternalServerError)
			return
		}

		prefixID, ok := r.Context().Value(prefixIDCtx).(string)
		if !ok {
			http.Error(w, "Prefix not found in context", http.StatusInternalServerError)
			return
		}

		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			http.Error(w, "X-API-Key header required", http.StatusUnauthorized)
			return
		}

		cost := s.getMethodCost(r.Method, tierPolicy)
		usage, err := s.rateManager.Increment(r.Context(), prefixID, apiKey, int64(tierPolicy.Quota), int64(cost))

		if err != nil {
			http.Error(w, "Rate limit service unavailable", http.StatusServiceUnavailable)
			return
		}

		if usage > int64(tierPolicy.Quota) {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		// Store usage in context for analytics
		entry := r.Context().Value(analyticsKey).(*types.AnalyticsEntry)
		if entry != nil {
			entry.LimitUsed = uint64(usage)
			entry.LimitUsedOfTotal = float64(usage) / float64(tierPolicy.Quota)
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

	// Execute proxy request
	resp, err := s.client.Do(proxyReq)
	entry.UpstreamLatency = time.Since(startTime)

	if err != nil {
		// Handle failure strategy
		s.handleProxyError(w, cfg.Failure, err, entry)
		return
	}
	defer resp.Body.Close()

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Failed to read response", http.StatusBadGateway)
		return
	}

	// Copy response headers from upstream
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Apply response transforms (hide headers, etc.)
	s.applyResponseTransforms(w, cfg.Transform)

	// Try to cache the response if it's cacheable
	s.cacheResponse(r, cfg, responseBody)

	// Record response size
	entry.ResponseSize = uint64(len(responseBody))
	entry.RequestSize = uint64(r.ContentLength)

	// Write response
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(resp.StatusCode)
	w.Write(responseBody)
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
func (s *EdgeServer) cacheResponse(r *http.Request, cfg *types.ServiceConfig, body []byte) {
	if r.Method != http.MethodGet || cfg.Cache == nil || !cfg.Cache.Enabled {
		return
	}

	// Get cache manager from context if available
	cm, ok := r.Context().Value(cacheKeyCtx).(*CacheManager)
	if !ok {
		cm = NewCacheManager(s.rdb, *cfg.Cache)
	}

	apiKey := r.Header.Get("X-API-Key")
	_ = cm.SetCache(r.Context(), r, apiKey, body)
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

	if failure.FailOpen {
		// On fail-open, allow the request to pass through with a warning header
		w.Header().Set("X-Upstream-Error", "true")
		w.Header().Set("X-Fallback-Tier", failure.FallbackTier)
		http.Error(w, "Upstream service unavailable (fail-open)", http.StatusGatewayTimeout)
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
