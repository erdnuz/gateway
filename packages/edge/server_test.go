package edge

import (
	"context"
	"encoding/json"
	"gateway/packages/common/types"
	testutils "gateway/testing"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type captureAnalyticsSink struct {
	mu      sync.Mutex
	entries []*types.AnalyticsEntry
}

func (s *captureAnalyticsSink) Capture(entry *types.AnalyticsEntry) {
	if entry == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copyEntry := *entry
	s.entries = append(s.entries, &copyEntry)
}

func (s *captureAnalyticsSink) Last() *types.AnalyticsEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return nil
	}
	return s.entries[len(s.entries)-1]
}

func (s *captureAnalyticsSink) Close() error { return nil }

// TestParsePathValid tests path parsing with valid paths
func TestParsePathValid(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantPre string
		wantSvc string
	}{
		{
			name:    "simple path",
			path:    "/api/v1/users",
			wantPre: "api",
			wantSvc: "v1",
		},
		{
			name:    "path with trailing slash",
			path:    "/api/v1/users/",
			wantPre: "api",
			wantSvc: "v1",
		},
	}

	config := testutils.NewTestGatewayConfig()
	configMgr := &ConfigManager{}
	configMgr.active.Store(config)
	server := NewEdgeServer(configMgr, nil, nil, nil, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pre, svc := server.parsePath(tt.path)
			testutils.AssertEqual(t, tt.wantPre, pre, "prefix")
			testutils.AssertEqual(t, tt.wantSvc, svc, "service")
		})
	}
}

// TestParsePathInvalid tests path parsing with invalid paths
func TestParsePathInvalid(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "empty path", path: ""},
		{name: "single segment", path: "/api"},
		{name: "root path", path: "/"},
	}

	config := testutils.NewTestGatewayConfig()
	configMgr := &ConfigManager{}
	configMgr.active.Store(config)
	server := NewEdgeServer(configMgr, nil, nil, nil, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pre, svc := server.parsePath(tt.path)
			testutils.AssertEqual(t, "", pre, "prefix should be empty")
			testutils.AssertEqual(t, "", svc, "service should be empty")
		})
	}
}

// TestGetMethodCost tests method cost calculation
func TestGetMethodCost(t *testing.T) {
	tests := []struct {
		method   string
		expected uint8
	}{
		{method: http.MethodGet, expected: 1},
		{method: http.MethodPost, expected: 2},
		{method: http.MethodPut, expected: 2},
		{method: http.MethodDelete, expected: 3},
		{method: "PATCH", expected: 1}, // OtherCost
	}

	config := testutils.NewTestGatewayConfig()
	tier := config.Prefixes[0].Services[0].Tiers[0]

	configMgr := &ConfigManager{}
	configMgr.active.Store(config)
	server := NewEdgeServer(configMgr, nil, nil, nil, nil)

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			cost := server.getMethodCost(tt.method, &tier)
			testutils.AssertEqual(t, tt.expected, cost, "method cost")
		})
	}
}

// TestBuildProxyPath tests proxy path building
func TestBuildProxyPath(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		stripPrefix bool
		expected    string
	}{
		{
			name:        "no stripping",
			path:        "/api/v1/users/123",
			stripPrefix: false,
			expected:    "/api/v1/users/123",
		},
		{
			name:        "with stripping",
			path:        "/api/v1/users/123",
			stripPrefix: true,
			expected:    "/users/123",
		},
		{
			name:        "with stripping root only",
			path:        "/api/v1",
			stripPrefix: true,
			expected:    "/",
		},
	}

	config := testutils.NewTestGatewayConfig()
	configMgr := &ConfigManager{}
	configMgr.active.Store(config)
	server := NewEdgeServer(configMgr, nil, nil, nil, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testutils.NewTestServiceConfig("test-service")
			cfg.Transform.StripPrefix = tt.stripPrefix
			result := server.buildProxyPath(tt.path, &cfg)
			testutils.AssertEqual(t, tt.expected, result, "proxy path")
		})
	}
}

// TestGetTier tests tier retrieval function
func TestGetTier(t *testing.T) {
	service := testutils.NewTestServiceConfig("test-service")

	tests := []struct {
		name    string
		tierID  string
		wantOk  bool
		wantQty uint32
	}{
		{name: "free tier", tierID: "free", wantOk: true, wantQty: 1000},
		{name: "premium tier", tierID: "premium", wantOk: true, wantQty: 10000},
		{name: "non-existing tier", tierID: "enterprise", wantOk: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tier, ok := GetTier(&service, tt.tierID)
			testutils.AssertEqual(t, tt.wantOk, ok, "tier found")
			if ok {
				testutils.AssertEqual(t, tt.wantQty, tier.Quota, "quota")
			}
		})
	}
}

// TestConfigManagerGetPrefix tests prefix retrieval
func TestConfigManagerGetPrefix(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	configMgr := testutils.NewMockConfigManager(config)

	tests := []struct {
		name   string
		prefix string
		wantOk bool
	}{
		{name: "existing prefix", prefix: "/api/v1", wantOk: true},
		{name: "another existing prefix", prefix: "/api/v2", wantOk: true},
		{name: "non-existing prefix", prefix: "/api/v3", wantOk: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefixCfg, ok := configMgr.GetPrefix(tt.prefix)
			testutils.AssertEqual(t, tt.wantOk, ok, "prefix found")
			if ok {
				testutils.AssertEqual(t, tt.prefix, prefixCfg.Prefix, "prefix value")
			}
		})
	}
}

// TestConfigManagerGetServiceConfig tests service configuration retrieval
func TestConfigManagerGetServiceConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	configMgr := testutils.NewMockConfigManager(config)

	tests := []struct {
		name       string
		prefix     string
		service    string
		wantErr    bool
		wantTarget string
	}{
		{
			name:       "existing service",
			prefix:     "/api/v1",
			service:    "users-service",
			wantErr:    false,
			wantTarget: "http://upstream:3000",
		},
		{
			name:    "non-existing service",
			prefix:  "/api/v1",
			service: "unknown-service",
			wantErr: true,
		},
		{
			name:    "non-existing prefix",
			prefix:  "/api/v3",
			service: "any-service",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := configMgr.GetServiceConfig(tt.prefix, tt.service)
			if tt.wantErr {
				testutils.AssertError(t, err, "should return error")
			} else {
				testutils.AssertNoError(t, err, "should not return error")
				testutils.AssertEqual(t, tt.service, svc.ServiceID, "service ID")
				testutils.AssertEqual(t, tt.wantTarget, svc.TargetURL, "target URL")
			}
		})
	}
}

// TestTierManager tests tier lookup
func TestTierManager(t *testing.T) {
	tierMgr := testutils.NewMockTierManager()

	// Set a tier
	tierMgr.SetTier("api/v1", "user-123", "premium")

	// Get the tier
	tier, err := tierMgr.GetUserTier(context.TODO(), "api/v1", "user-123")
	testutils.AssertNoError(t, err, "should not error")
	testutils.AssertEqual(t, "premium", tier, "tier should match")

	// Get unknown tier (should default)
	tier, err = tierMgr.GetUserTier(context.TODO(), "api/v1", "unknown-user")
	testutils.AssertNoError(t, err, "should not error")
	testutils.AssertEqual(t, "free", tier, "should default to free")
}

// TestRateManager tests rate limiting
func TestRateManager(t *testing.T) {
	rateMgr := testutils.NewMockRateManager()

	// First increment
	usage, err := rateMgr.Increment(context.TODO(), "api/v1", "user-123", 100, 10)
	testutils.AssertNoError(t, err, "should increment")
	testutils.AssertEqual(t, int64(10), usage, "usage should be 10")

	// Second increment
	usage, err = rateMgr.Increment(context.TODO(), "api/v1", "user-123", 100, 20)
	testutils.AssertNoError(t, err, "should increment")
	testutils.AssertEqual(t, int64(30), usage, "usage should be 30")

	// Get usage
	current := rateMgr.GetUsage("api/v1", "user-123")
	testutils.AssertEqual(t, int64(30), current, "usage should match")

	// Reset
	rateMgr.Reset()
	current = rateMgr.GetUsage("api/v1", "user-123")
	testutils.AssertEqual(t, int64(0), current, "usage should be 0 after reset")
}

// TestAnalyticsManager tests analytics capture
func TestAnalyticsManager(t *testing.T) {
	analyticsMgr := testutils.NewMockAnalyticsManager()

	entry := testutils.NewTestAnalyticsEntry("api/v1", "users-service", "free", "GET")
	analyticsMgr.Capture(entry)

	testutils.AssertEqual(t, 1, analyticsMgr.GetEntriesCount(), "should have 1 entry")
	entries := analyticsMgr.GetEntries()
	testutils.AssertEqual(t, "api/v1", entries[0].Prefix, "prefix should match")
	testutils.AssertEqual(t, "GET", entries[0].Method, "method should match")

	// Capture more
	analyticsMgr.Capture(testutils.NewTestAnalyticsEntry("api/v1", "posts-service", "premium", "POST"))
	testutils.AssertEqual(t, 2, analyticsMgr.GetEntriesCount(), "should have 2 entries")

	// Clear
	analyticsMgr.Clear()
	testutils.AssertEqual(t, 0, analyticsMgr.GetEntriesCount(), "should have 0 after clear")
}

// TestApplyRequestTransforms tests request transformation
func TestApplyRequestTransforms(t *testing.T) {
	server := NewEdgeServer(nil, nil, nil, nil, nil)

	// Create a request
	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Internal", "secret")

	transform := types.TransformConfig{
		AddHeaders:  map[string]string{"X-Service": "test-service"},
		HideHeaders: []string{"X-Internal"},
	}

	server.applyRequestTransforms(req, transform)

	testutils.AssertEqual(t, "test-service", req.Header.Get("X-Service"), "should have added header")
	testutils.AssertEqual(t, "", req.Header.Get("X-Internal"), "should have removed header")
}

// TestCacheConfig tests cache configuration validation
func TestCacheConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertNotNil(t, service.Cache, "cache should exist")
	testutils.AssertTrue(t, service.Cache.Enabled, "cache should be enabled")
	testutils.AssertTrue(t, service.Cache.TTL > 0, "TTL should be positive")
	testutils.AssertContains(t, service.Cache.CacheKey, "$", "cache key should have template")
}

// TestAnalyticsConfig tests analytics configuration
func TestAnalyticsConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertTrue(t, service.Analytics.Enabled, "analytics should be enabled")
	testutils.AssertTrue(t, service.Analytics.FlushingInterval > 0, "interval should be positive")
	testutils.AssertTrue(t, service.Analytics.SamplingRate >= 0 && service.Analytics.SamplingRate <= 1.0, "rate should be 0-1")
}

func TestEdgeServer_AnalyticsPolicyDisabledSkipsCapture(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	svc := testutils.NewTestServiceConfig("svc")
	svc.TargetURL = upstream.URL
	svc.Analytics.Enabled = false

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{{
			Prefix:      "v1",
			QuotaPeriod: time.Hour,
			Services:    []types.ServiceConfig{svc},
		}},
	}

	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)
	tierMgr := NewTierManager("http://hub.invalid", rdb, types.DefaultHubUpdatesChannel)
	if err := rdb.Set(context.Background(), "tier:v1:test-key", "free", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}

	rateMgr := NewRateManager("", rdb, 100)
	sink := &captureAnalyticsSink{}
	edgeServer := NewEdgeServer(configMgr, tierMgr, rateMgr, sink, rdb)

	req := httptest.NewRequest(http.MethodGet, "/v1/svc/resource", nil)
	req.Header.Set("X-API-Key", "test-key")
	rr := httptest.NewRecorder()
	edgeServer.ServeHTTP(rr, req)

	testutils.AssertEqual(t, http.StatusOK, rr.Code, "request should succeed")
	if sink.Last() != nil {
		t.Fatalf("analytics capture should be skipped when disabled")
	}
}

func TestEdgeServer_AnalyticsPolicySamplingZeroSkipsCapture(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	svc := testutils.NewTestServiceConfig("svc")
	svc.TargetURL = upstream.URL
	svc.Analytics.Enabled = true
	svc.Analytics.SamplingRate = 0

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{{
			Prefix:      "v1",
			QuotaPeriod: time.Hour,
			Services:    []types.ServiceConfig{svc},
		}},
	}

	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)
	tierMgr := NewTierManager("http://hub.invalid", rdb, types.DefaultHubUpdatesChannel)
	if err := rdb.Set(context.Background(), "tier:v1:test-key", "free", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}

	rateMgr := NewRateManager("", rdb, 100)
	sink := &captureAnalyticsSink{}
	edgeServer := NewEdgeServer(configMgr, tierMgr, rateMgr, sink, rdb)

	req := httptest.NewRequest(http.MethodGet, "/v1/svc/resource", nil)
	req.Header.Set("X-API-Key", "test-key")
	rr := httptest.NewRecorder()
	edgeServer.ServeHTTP(rr, req)

	testutils.AssertEqual(t, http.StatusOK, rr.Code, "request should succeed")
	if sink.Last() != nil {
		t.Fatalf("analytics capture should be skipped when sampling rate is zero")
	}
}

// TestCORSConfig tests CORS configuration
func TestCORSConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertNotNil(t, service.CORS, "CORS should exist")
	testutils.AssertTrue(t, len(service.CORS.AllowedOrigins) > 0, "should have origins")
	testutils.AssertTrue(t, len(service.CORS.AllowedMethods) > 0, "should have methods")
	testutils.AssertTrue(t, service.CORS.MaxAge > 0, "should have max age")
}

// TestTransformConfig tests transform configuration
func TestTransformConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertTrue(t, service.Transform.StripPrefix, "should strip prefix")
	testutils.AssertTrue(t, len(service.Transform.AddHeaders) > 0, "should add headers")
	testutils.AssertTrue(t, len(service.Transform.HideHeaders) > 0, "should hide headers")
}

// TestGatewayConfigStructure tests overall config structure
func TestGatewayConfigStructure(t *testing.T) {
	config := testutils.NewTestGatewayConfig()

	testutils.AssertTrue(t, len(config.Prefixes) > 0, "should have prefixes")

	for _, prefix := range config.Prefixes {
		testutils.AssertTrue(t, len(prefix.Prefix) > 0, "prefix name should be set")
		testutils.AssertTrue(t, prefix.QuotaPeriod > 0, "quota period should be set")
		testutils.AssertTrue(t, len(prefix.Services) > 0, "should have services")

		for _, svc := range prefix.Services {
			testutils.AssertTrue(t, len(svc.ServiceID) > 0, "service ID should be set")
			testutils.AssertTrue(t, len(svc.TargetURL) > 0, "target URL should be set")
			testutils.AssertTrue(t, len(svc.Tiers) > 0, "should have tiers")

			for _, tier := range svc.Tiers {
				testutils.AssertTrue(t, len(tier.TierID) > 0, "tier ID should be set")
				testutils.AssertTrue(t, tier.Quota > 0, "quota should be positive")
			}
		}
	}
}

func TestEdgeServer_CacheHitPathReachable(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()

	upstreamCalls := atomic.Int32{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"source":"upstream"}`))
	}))
	defer upstream.Close()

	svc := testutils.NewTestServiceConfig("svc")
	svc.TargetURL = upstream.URL
	svc.Cache = &types.CacheConfig{Enabled: true, TTL: time.Minute, CacheKey: "$PATH:$QUERY:$KEY:$ENC"}
	svc.CORS = &types.CORSConfig{AllowedOrigins: []string{"https://client.example"}, AllowedMethods: []string{"GET"}, MaxAge: time.Hour}

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{{
			Prefix:      "v1",
			QuotaPeriod: time.Hour,
			Services:    []types.ServiceConfig{svc},
		}},
	}

	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)
	tierMgr := NewTierManager("http://hub.invalid", rdb, types.DefaultHubUpdatesChannel)
	if err := rdb.Set(context.Background(), "tier:v1:test-key", "free", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	rateMgr := NewRateManager("", rdb, 100)

	sink := &captureAnalyticsSink{}
	edgeServer := NewEdgeServer(configMgr, tierMgr, rateMgr, sink, rdb)

	req := httptest.NewRequest(http.MethodGet, "/v1/svc/resource?a=1", nil)
	req.Header.Set("X-API-Key", "test-key")
	req.Header.Set("Accept-Encoding", "gzip")

	cm := NewCacheManager(rdb, *svc.Cache)
	cacheKey := cm.generateCacheKey(req, "test-key")
	if err := rdb.Set(context.Background(), cacheKey, []byte(`{"source":"cache"}`), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	rw := httptest.NewRecorder()
	edgeServer.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	if got := rw.Header().Get("X-Cache"); got != "HIT" {
		t.Fatalf("expected X-Cache HIT, got %q", got)
	}
	if rw.Body.String() != `{"source":"cache"}` {
		t.Fatalf("expected cached body, got %q", rw.Body.String())
	}
	if upstreamCalls.Load() != 0 {
		t.Fatalf("expected upstream to not be called on cache hit, got %d calls", upstreamCalls.Load())
	}
	if last := sink.Last(); last == nil || !last.CacheHit {
		t.Fatalf("expected analytics entry with cache_hit=true, got %+v", last)
	}
}

func TestEdgeServer_CacheHitPreservesStatusCode(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()

	upstreamCalls := atomic.Int32{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		http.NotFound(w, r)
	}))
	defer upstream.Close()

	svc := testutils.NewTestServiceConfig("svc")
	svc.TargetURL = upstream.URL
	svc.Cache = &types.CacheConfig{Enabled: true, TTL: time.Minute, CacheKey: "$PATH:$QUERY:$KEY:$ENC"}

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{{
			Prefix:      "v1",
			QuotaPeriod: time.Hour,
			Services:    []types.ServiceConfig{svc},
		}},
	}

	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)
	tierMgr := NewTierManager("http://hub.invalid", rdb, types.DefaultHubUpdatesChannel)
	if err := rdb.Set(context.Background(), "tier:v1:test-key", "free", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	rateMgr := NewRateManager("", rdb, 100)

	edgeServer := NewEdgeServer(configMgr, tierMgr, rateMgr, NoOpAnalyticsSink{}, rdb)

	req1 := httptest.NewRequest(http.MethodGet, "/v1/svc/not-found", nil)
	req1.Header.Set("X-API-Key", "test-key")
	res1 := httptest.NewRecorder()
	edgeServer.ServeHTTP(res1, req1)
	if res1.Code != http.StatusNotFound {
		t.Fatalf("expected first response 404, got %d", res1.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/v1/svc/not-found", nil)
	req2.Header.Set("X-API-Key", "test-key")
	res2 := httptest.NewRecorder()
	edgeServer.ServeHTTP(res2, req2)
	if res2.Code != http.StatusNotFound {
		t.Fatalf("expected cached response 404, got %d", res2.Code)
	}
	if got := res2.Header().Get("X-Cache"); got != "HIT" {
		t.Fatalf("expected second response to be cache HIT, got %q", got)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("expected upstream to be called only once, got %d", upstreamCalls.Load())
	}
}

func TestEdgeServer_CORSPreflightWithoutAPIKey(t *testing.T) {
	svc := testutils.NewTestServiceConfig("svc")
	svc.CORS = &types.CORSConfig{
		AllowedOrigins: []string{"https://client.example"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"Content-Type", "X-Edge-Token"},
		MaxAge:         time.Hour,
	}

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{{
			Prefix:      "v1",
			QuotaPeriod: time.Hour,
			Services:    []types.ServiceConfig{svc},
		}},
	}

	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)

	edgeServer := NewEdgeServer(configMgr, nil, nil, NoOpAnalyticsSink{}, nil)

	req := httptest.NewRequest(http.MethodOptions, "/v1/svc/resource", nil)
	req.Header.Set("Origin", "https://client.example")

	rw := httptest.NewRecorder()
	edgeServer.ServeHTTP(rw, req)

	if rw.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for preflight, got %d body=%s", rw.Code, rw.Body.String())
	}
	if got := rw.Header().Get("Access-Control-Allow-Origin"); got != "https://client.example" {
		t.Fatalf("expected allow-origin header, got %q", got)
	}
	if got := rw.Header().Get("Access-Control-Allow-Methods"); got == "" {
		t.Fatal("expected Access-Control-Allow-Methods header")
	}
	if got := rw.Header().Get("Access-Control-Allow-Headers"); got != "Content-Type, X-Edge-Token" {
		t.Fatalf("expected policy-driven allow-headers, got %q", got)
	}
}

func TestEdgeServer_BoundedCachingBehavior(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()

	var smallCalls atomic.Int32
	var largeCalls atomic.Int32
	largeBody := strings.Repeat("a", int(types.DefaultRuntimePolicy().Edge.CacheMaxObjectBytes)+128)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/small":
			smallCalls.Add(1)
			_, _ = w.Write([]byte(`{"kind":"small"}`))
		case "/large":
			largeCalls.Add(1)
			_, _ = w.Write([]byte(largeBody))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	svc := testutils.NewTestServiceConfig("svc")
	svc.TargetURL = upstream.URL
	svc.Cache = &types.CacheConfig{Enabled: true, TTL: time.Minute, CacheKey: "$PATH:$QUERY:$KEY:$ENC"}

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{{
			Prefix:      "v1",
			QuotaPeriod: time.Hour,
			Services:    []types.ServiceConfig{svc},
		}},
	}

	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)
	tierMgr := NewTierManager("http://hub.invalid", rdb, types.DefaultHubUpdatesChannel)
	if err := rdb.Set(context.Background(), "tier:v1:test-key", "free", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	rateMgr := NewRateManager("", rdb, 100)
	edgeServer := NewEdgeServer(configMgr, tierMgr, rateMgr, NoOpAnalyticsSink{}, rdb)

	// Small response should be cached after first MISS, then served as HIT.
	smallReq1 := httptest.NewRequest(http.MethodGet, "/v1/svc/small", nil)
	smallReq1.Header.Set("X-API-Key", "test-key")
	smallRes1 := httptest.NewRecorder()
	edgeServer.ServeHTTP(smallRes1, smallReq1)
	if smallRes1.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("expected first small response MISS, got %q", smallRes1.Header().Get("X-Cache"))
	}

	smallReq2 := httptest.NewRequest(http.MethodGet, "/v1/svc/small", nil)
	smallReq2.Header.Set("X-API-Key", "test-key")
	smallRes2 := httptest.NewRecorder()
	edgeServer.ServeHTTP(smallRes2, smallReq2)
	if smallRes2.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("expected second small response HIT, got %q", smallRes2.Header().Get("X-Cache"))
	}
	if smallCalls.Load() != 1 {
		t.Fatalf("expected small upstream called once, got %d", smallCalls.Load())
	}

	// Large response should never be cached.
	largeReq1 := httptest.NewRequest(http.MethodGet, "/v1/svc/large", nil)
	largeReq1.Header.Set("X-API-Key", "test-key")
	largeRes1 := httptest.NewRecorder()
	edgeServer.ServeHTTP(largeRes1, largeReq1)
	if largeRes1.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("expected first large response MISS, got %q", largeRes1.Header().Get("X-Cache"))
	}
	if largeRes1.Body.Len() != len(largeBody) {
		t.Fatalf("expected full large body length %d, got %d", len(largeBody), largeRes1.Body.Len())
	}

	largeReq2 := httptest.NewRequest(http.MethodGet, "/v1/svc/large", nil)
	largeReq2.Header.Set("X-API-Key", "test-key")
	largeRes2 := httptest.NewRecorder()
	edgeServer.ServeHTTP(largeRes2, largeReq2)
	if largeRes2.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("expected second large response MISS, got %q", largeRes2.Header().Get("X-Cache"))
	}
	if largeCalls.Load() != 2 {
		t.Fatalf("expected large upstream called twice (not cached), got %d", largeCalls.Load())
	}
}

func TestEdgeServer_HubTierLookupFallbackToDefaultTier(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	svc := testutils.NewTestServiceConfig("svc")
	svc.TargetURL = upstream.URL
	svc.Failure.Hub.TierLookupStrategy = "default-tier"
	svc.Failure.Hub.DefaultTier = "free"

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{{
			Prefix:      "v1",
			QuotaPeriod: time.Hour,
			Services:    []types.ServiceConfig{svc},
		}},
	}

	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)
	// Hub address intentionally unreachable to force tier lookup fallback path.
	tierMgr := NewTierManager("http://127.0.0.1:1", rdb, types.DefaultHubUpdatesChannel)
	rateMgr := NewRateManager("", rdb, 100)

	edgeServer := NewEdgeServer(configMgr, tierMgr, rateMgr, NoOpAnalyticsSink{}, rdb)

	req := httptest.NewRequest(http.MethodGet, "/v1/svc/resource", nil)
	req.Header.Set("X-API-Key", "fallback-user")
	rw := httptest.NewRecorder()
	edgeServer.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200 with default-tier fallback, got %d body=%s", rw.Code, rw.Body.String())
	}
}

func TestEdgeServer_UpstreamRetriesOnConfiguredStatus(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()

	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := calls.Add(1)
		if attempt <= 2 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("transient"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("recovered"))
	}))
	defer upstream.Close()

	svc := testutils.NewTestServiceConfig("svc")
	svc.TargetURL = upstream.URL
	svc.Failure.Upstream.MaxRetries = 2
	svc.Failure.Upstream.RetryBackoff = 1 * time.Millisecond
	svc.Failure.Upstream.RetryOnStatuses = []int{http.StatusBadGateway}

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{{
			Prefix:      "v1",
			QuotaPeriod: time.Hour,
			Services:    []types.ServiceConfig{svc},
		}},
	}

	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)
	tierMgr := NewTierManager("http://hub.invalid", rdb, types.DefaultHubUpdatesChannel)
	if err := rdb.Set(context.Background(), "tier:v1:test-key", "free", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	rateMgr := NewRateManager("", rdb, 100)

	edgeServer := NewEdgeServer(configMgr, tierMgr, rateMgr, NoOpAnalyticsSink{}, rdb)
	req := httptest.NewRequest(http.MethodGet, "/v1/svc/retry", nil)
	req.Header.Set("X-API-Key", "test-key")
	rw := httptest.NewRecorder()
	edgeServer.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200 after retries, got %d body=%s", rw.Code, rw.Body.String())
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3 upstream attempts, got %d", calls.Load())
	}
}

func TestEdgeServer_UpstreamFailOpenFallbackResponse(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()

	svc := testutils.NewTestServiceConfig("svc")
	svc.TargetURL = "http://127.0.0.1:1"
	svc.Failure.Upstream.Mode = "fail-open"
	svc.Failure.Upstream.FallbackStatusCode = http.StatusAccepted
	svc.Failure.Upstream.FallbackBody = "degraded-mode"
	svc.Failure.Upstream.FallbackHeaders = map[string]string{"Content-Type": "text/plain"}

	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{{
			Prefix:      "v1",
			QuotaPeriod: time.Hour,
			Services:    []types.ServiceConfig{svc},
		}},
	}

	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)
	tierMgr := NewTierManager("http://hub.invalid", rdb, types.DefaultHubUpdatesChannel)
	if err := rdb.Set(context.Background(), "tier:v1:test-key", "free", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	rateMgr := NewRateManager("", rdb, 100)

	edgeServer := NewEdgeServer(configMgr, tierMgr, rateMgr, NoOpAnalyticsSink{}, rdb)
	req := httptest.NewRequest(http.MethodGet, "/v1/svc/fallback", nil)
	req.Header.Set("X-API-Key", "test-key")
	rw := httptest.NewRecorder()
	edgeServer.ServeHTTP(rw, req)

	if rw.Code != http.StatusAccepted {
		t.Fatalf("expected 202 fallback status, got %d", rw.Code)
	}
	if rw.Body.String() != "degraded-mode" {
		t.Fatalf("expected fallback body, got %q", rw.Body.String())
	}
	if rw.Header().Get("X-Upstream-Error") != "true" {
		t.Fatalf("expected X-Upstream-Error header, got %q", rw.Header().Get("X-Upstream-Error"))
	}
}

func TestEdgeServer_UsesConfiguredMaxCacheableResponseBytes(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()

	body := strings.Repeat("b", 80)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer upstream.Close()

	svc := testutils.NewTestServiceConfig("svc")
	svc.TargetURL = upstream.URL
	svc.Cache = &types.CacheConfig{Enabled: true, TTL: time.Minute, CacheKey: "$PATH:$QUERY:$KEY:$ENC"}

	cfg := &types.GatewayConfig{Prefixes: []types.PrefixConfig{{Prefix: "v1", QuotaPeriod: time.Hour, Services: []types.ServiceConfig{svc}}}}
	configMgr := &ConfigManager{}
	configMgr.active.Store(cfg)
	tierMgr := NewTierManager("http://hub.invalid", rdb, types.DefaultHubUpdatesChannel)
	if err := rdb.Set(context.Background(), "tier:v1:test-key", "free", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	rateMgr := NewRateManager("", rdb, 100)

	edgeServer := NewEdgeServerWithOptions(configMgr, tierMgr, rateMgr, NoOpAnalyticsSink{}, rdb, EdgeServerOptions{MaxCacheableResponseBytes: 64})

	req1 := httptest.NewRequest(http.MethodGet, "/v1/svc/large", nil)
	req1.Header.Set("X-API-Key", "test-key")
	res1 := httptest.NewRecorder()
	edgeServer.ServeHTTP(res1, req1)
	if res1.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("expected first response MISS, got %q", res1.Header().Get("X-Cache"))
	}

	req2 := httptest.NewRequest(http.MethodGet, "/v1/svc/large", nil)
	req2.Header.Set("X-API-Key", "test-key")
	res2 := httptest.NewRecorder()
	edgeServer.ServeHTTP(res2, req2)
	if res2.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("expected uncached response due to configured size cap, got %q", res2.Header().Get("X-Cache"))
	}
}

func TestEdgeServer_UsesDefaultUpstreamClientTimeoutFromPolicy(t *testing.T) {
	edgeServer := NewEdgeServer(nil, nil, nil, NoOpAnalyticsSink{}, nil)
	if edgeServer.client.Timeout != types.DefaultRuntimePolicy().Edge.UpstreamClientTimeout {
		t.Fatalf("expected default upstream timeout %s, got %s", types.DefaultRuntimePolicy().Edge.UpstreamClientTimeout, edgeServer.client.Timeout)
	}
}

func TestEdgeServer_UsesDefaultTransportPoolSettingsFromPolicy(t *testing.T) {
	edgeServer := NewEdgeServer(nil, nil, nil, NoOpAnalyticsSink{}, nil)
	transport, ok := edgeServer.client.Transport.(*http.Transport)
	if !ok {
		t.Fatal("expected *http.Transport")
	}
	if transport.MaxIdleConns != types.DefaultRuntimePolicy().Edge.UpstreamMaxIdleConns {
		t.Fatalf("expected MaxIdleConns=%d, got %d", types.DefaultRuntimePolicy().Edge.UpstreamMaxIdleConns, transport.MaxIdleConns)
	}
	if transport.IdleConnTimeout != types.DefaultRuntimePolicy().Edge.UpstreamIdleConnTimeout {
		t.Fatalf("expected IdleConnTimeout=%s, got %s", types.DefaultRuntimePolicy().Edge.UpstreamIdleConnTimeout, transport.IdleConnTimeout)
	}
}

func TestEdgeServer_AnalyticsMetricsEndpoint(t *testing.T) {
	mgr := NewAnalyticsManager(4)
	mgr.Capture(&types.AnalyticsEntry{Prefix: "v1", Service: "svc", Method: http.MethodGet})

	edgeServer := NewEdgeServer(nil, nil, nil, mgr, nil)
	req := httptest.NewRequest(http.MethodGet, "/analytics/metrics", nil)
	rw := httptest.NewRecorder()
	edgeServer.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var stats AnalyticsManagerStats
	if err := json.NewDecoder(rw.Body).Decode(&stats); err != nil {
		t.Fatalf("decode analytics stats: %v", err)
	}
	if stats.Captured != 1 || stats.BufferDepth != 1 {
		t.Fatalf("unexpected analytics stats payload: %+v", stats)
	}
}
