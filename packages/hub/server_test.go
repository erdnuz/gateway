package hub

import (
	"encoding/json"
	"gateway/packages/common/types"
	"gateway/packages/edge"
	testutils "gateway/testing"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestHubConfigStructure tests hub configuration structure
func TestHubConfigStructure(t *testing.T) {
	config := testutils.NewTestGatewayConfig()

	testutils.AssertTrue(t, len(config.Prefixes) > 0, "should have prefixes")

	for _, prefix := range config.Prefixes {
		testutils.AssertTrue(t, len(prefix.Prefix) > 0, "prefix should not be empty")
		testutils.AssertTrue(t, prefix.QuotaPeriod > 0, "quota period should be positive")
		testutils.AssertTrue(t, len(prefix.Services) > 0, "should have services")

		for _, svc := range prefix.Services {
			testutils.AssertTrue(t, len(svc.ServiceID) > 0, "service ID should not be empty")
			testutils.AssertTrue(t, len(svc.TargetURL) > 0, "target URL should not be empty")
			testutils.AssertTrue(t, len(svc.Tiers) > 0, "should have tiers")

			for _, tier := range svc.Tiers {
				testutils.AssertTrue(t, len(tier.TierID) > 0, "tier ID should not be empty")
				testutils.AssertTrue(t, tier.Quota > 0, "quota should be positive")
			}
		}
	}
}

// TestGet TierMultipleTiers tests tier retrieval
func TestGetTierMultipleTiers(t *testing.T) {
	service := testutils.NewTestServiceConfig("test-service")

	freeTier, ok := edge.GetTier(&service, "free")
	testutils.AssertTrue(t, ok, "should find free tier")
	testutils.AssertEqual(t, "free", freeTier.TierID, "tier ID should match")

	premiumTier, ok := edge.GetTier(&service, "premium")
	testutils.AssertTrue(t, ok, "should find premium tier")
	testutils.AssertEqual(t, "premium", premiumTier.TierID, "tier ID should match")
	testutils.AssertTrue(t, premiumTier.Quota > freeTier.Quota, "premium should have higher quota")
}

// TestGetTierNotFound tests tier not found case
func TestGetTierNotFound(t *testing.T) {
	service := testutils.NewTestServiceConfig("test-service")

	tier, ok := edge.GetTier(&service, "nonexistent")
	testutils.AssertFalse(t, ok, "should not find nonexistent tier")
	if tier != nil {
		t.Errorf("tier should be nil when not found: got %v", tier)
	}
}

// TestHubTierStructure tests tier structure validity
func TestHubTierStructure(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	// Test free tier
	freeTier, found := edge.GetTier(&service, "free")
	testutils.AssertTrue(t, found, "free tier should exist")
	testutils.AssertTrue(t, freeTier.Quota > 0, "free tier should have quota")
	testutils.AssertTrue(t, freeTier.GetCost > 0, "free tier should have GET cost")

	// Test premium tier
	premiumTier, found := edge.GetTier(&service, "premium")
	testutils.AssertTrue(t, found, "premium tier should exist")
	testutils.AssertTrue(t, premiumTier.Quota > 0, "premium tier should have quota")
	testutils.AssertTrue(t, premiumTier.Quota > freeTier.Quota, "premium should have higher quota than free")
}

// TestHubPrefixQuotaPeriod tests quota period configuration
func TestHubPrefixQuotaPeriod(t *testing.T) {
	config := testutils.NewTestGatewayConfig()

	for _, prefix := range config.Prefixes {
		testutils.AssertTrue(t, prefix.QuotaPeriod == 1*time.Hour, "quota period should be 1 hour")
	}
}

// TestHubServiceTargetURLValidation tests service target URL
func TestHubServiceTargetURLValidation(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertTrue(t, len(service.TargetURL) > 0, "target URL should not be empty")
	testutils.AssertContains(t, service.TargetURL, "://", "target URL should have scheme")
	testutils.AssertContains(t, service.TargetURL, ":", "target URL should have port")
}

// TestHubCORSConfig tests CORS configuration
func TestHubCORSConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertNotNil(t, service.CORS, "CORS config should exist")
	testutils.AssertTrue(t, len(service.CORS.AllowedOrigins) > 0, "should have allowed origins")
	testutils.AssertTrue(t, len(service.CORS.AllowedMethods) > 0, "should have allowed methods")
	testutils.AssertTrue(t, service.CORS.MaxAge > 0, "max age should be positive")
}

// TestHubCacheConfig tests cache configuration
func TestHubCacheConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertNotNil(t, service.Cache, "cache config should exist")
	testutils.AssertTrue(t, service.Cache.Enabled, "cache should be enabled")
	testutils.AssertTrue(t, service.Cache.TTL > 0, "TTL should be positive")
	testutils.AssertTrue(t, len(service.Cache.CacheKey) > 0, "cache key template should not be empty")
}

// TestHubAnalyticsConfig tests analytics configuration
func TestHubAnalyticsConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertTrue(t, service.Analytics.Enabled, "analytics should be enabled")
	testutils.AssertTrue(t, service.Analytics.FlushingInterval > 0, "flushing interval should be positive")
	testutils.AssertTrue(t, service.Analytics.SamplingRate >= 0 && service.Analytics.SamplingRate <= 1.0, "sampling rate should be 0-1")
}

// TestHubFailureConfig tests failure configuration
func TestHubFailureConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertFalse(t, service.Failure.FailOpen, "should fail closed by default")
	testutils.AssertTrue(t, len(service.Failure.FallbackTier) > 0, "fallback tier should be set")
}

// TestHubTransformConfig tests transform configuration
func TestHubTransformConfig(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]

	testutils.AssertTrue(t, service.Transform.StripPrefix, "should strip prefix")
	testutils.AssertTrue(t, len(service.Transform.AddHeaders) > 0, "should have headers to add")
	testutils.AssertTrue(t, len(service.Transform.HideHeaders) > 0, "should have headers to hide")
}

// TestMethodCosts tests cost configuration for different methods
func TestMethodCosts(t *testing.T) {
	config := testutils.NewTestGatewayConfig()
	service := config.Prefixes[0].Services[0]
	tier := service.Tiers[0]

	testutils.AssertTrue(t, tier.GetCost > 0, "GET cost should be positive")
	testutils.AssertTrue(t, tier.PostCost > 0, "POST cost should be positive")
	testutils.AssertTrue(t, tier.PutCost > 0, "PUT cost should be positive")
	testutils.AssertTrue(t, tier.DeleteCost > 0, "DELETE cost should be positive")
	testutils.AssertTrue(t, tier.OtherCost > 0, "OTHER cost should be positive")
}

// TestTierComparison tests quota tiers are properly differentiated
func TestTierComparison(t *testing.T) {
	service := testutils.NewTestServiceConfig("test-service")

	tier1, _ := edge.GetTier(&service, "free")
	tier2, _ := edge.GetTier(&service, "premium")

	testutils.AssertTrue(t, tier1.Quota < tier2.Quota, "premium should have more quota than free")
	testutils.AssertTrue(t, tier1.Quota == 1000, "free tier should have 1000 quota")
	testutils.AssertTrue(t, tier2.Quota == 10000, "premium tier should have 10000 quota")
}

// TestMultiplePrefixes tests configuration with multiple API versions
func TestMultiplePrefixes(t *testing.T) {
	config := testutils.NewTestGatewayConfig()

	testutils.AssertTrue(t, len(config.Prefixes) >= 2, "should have at least 2 prefixes")

	prefix1 := config.Prefixes[0]
	prefix2 := config.Prefixes[1]

	testutils.AssertNotEqual(t, prefix1.Prefix, prefix2.Prefix, "prefixes should be different")
	testutils.AssertTrue(t, len(prefix1.Services) > 0, "first prefix should have services")
	testutils.AssertTrue(t, len(prefix2.Services) > 0, "second prefix should have services")
}

// TestServiceConfiguration tests individual service configuration
func TestServiceConfiguration(t *testing.T) {
	svc := testutils.NewTestServiceConfig("my-service")

	testutils.AssertEqual(t, "my-service", svc.ServiceID, "service ID should match")
	testutils.AssertContains(t, svc.TargetURL, "http", "target URL should have http scheme")
	testutils.AssertTrue(t, len(svc.Tiers) > 0, "should have tiers")
	testutils.AssertNotNil(t, svc.CORS, "should have CORS config")
	testutils.AssertNotNil(t, svc.Cache, "should have cache config")
	testutils.AssertTrue(t, svc.Analytics.Enabled, "analytics should be enabled")
}

// --- HTTP Handler Integration Tests ---

// newTestHubServer builds a HubServer backed by an in-memory Redis instance.
// It skips MongoDB-dependent components (TierManager) so tests remain
// fully self-contained. The returned cleanup func must be deferred.
func newTestHubServer(t *testing.T) (*HubServer, func()) {
	t.Helper()

	m, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})

	// Use a single-segment prefix so it maps cleanly to a URL path segment,
	// e.g. /rate/v1/user1.
	cfg := &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{
			{
				Prefix:      "v1",
				QuotaPeriod: time.Hour,
				Services:    []types.ServiceConfig{testutils.NewTestServiceConfig("auth-api")},
			},
		},
	}

	cfgMgr := &ConfigManager{}
	cfgMgr.active.Store(cfg)

	hs := &HubServer{
		rdb:         rdb,
		cfgManager:  cfgMgr,
		rateManager: NewRateManager(rdb, cfgMgr),
	}

	cleanup := func() {
		_ = rdb.Close()
		m.Close()
	}
	return hs, cleanup
}

// TestHandleConfig_OK verifies that GET /config returns the gateway configuration.
func TestHandleConfig_OK(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	hs.SetCORSAllowedOrigins([]string{"http://edge-a:8082"})

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rw.Code, rw.Body.String())
	}

	var cfg types.GatewayConfig
	if err := json.NewDecoder(rw.Body).Decode(&cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if len(cfg.Prefixes) == 0 {
		t.Fatal("expected at least one prefix in config response")
	}
}

func TestHubCORSPreflightAllowed(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	hs.SetCORSAllowedOrigins([]string{"http://edge-a:8082"})
	hs.SetCORSPreflightPolicy([]string{"Authorization", "X-Edge-Token"}, []string{"GET", "POST"}, 12*time.Second)

	req := httptest.NewRequest(http.MethodOptions, "/config", nil)
	req.Header.Set("Origin", "http://edge-a:8082")
	req.Header.Set("Access-Control-Request-Method", "GET")
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rw.Code, rw.Body.String())
	}
	if got := rw.Header().Get("Access-Control-Allow-Origin"); got != "http://edge-a:8082" {
		t.Fatalf("expected allow-origin header for edge, got %q", got)
	}
	if got := rw.Header().Get("Access-Control-Allow-Headers"); got != "Authorization, X-Edge-Token" {
		t.Fatalf("expected configured allow headers, got %q", got)
	}
	if got := rw.Header().Get("Access-Control-Max-Age"); got != "12" {
		t.Fatalf("expected configured max age=12, got %q", got)
	}
}

func TestHubCORSPreflightRejectedForUnknownOrigin(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	hs.SetCORSAllowedOrigins([]string{"http://edge-a:8082"})

	req := httptest.NewRequest(http.MethodOptions, "/config", nil)
	req.Header.Set("Origin", "http://evil.example")
	req.Header.Set("Access-Control-Request-Method", "GET")
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rw.Code, rw.Body.String())
	}
}

func TestHubCORSActualRequestRejectedForUnknownOrigin(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	hs.SetCORSAllowedOrigins([]string{"http://edge-a:8082"})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "http://evil.example")
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rw.Code, rw.Body.String())
	}
}

// TestHandleConfig_MethodNotAllowed verifies that non-GET methods are rejected.
func TestHandleConfig_MethodNotAllowed(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/config", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rw.Code)
	}
}

// TestHandleRate_Get verifies that GET /rate/{prefix}/{apiKey} returns the current total.
func TestHandleRate_Get(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/rate/v1/user1", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rw.Code, rw.Body.String())
	}

	var total int64
	if err := json.NewDecoder(rw.Body).Decode(&total); err != nil {
		t.Fatalf("decode total: %v", err)
	}
}

// TestHandleRate_Post verifies that POST /rate/{prefix}/{apiKey}?delta=N increments the counter.
func TestHandleRate_Post(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/rate/v1/user1?delta=10", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rw.Code, rw.Body.String())
	}

	var total int64
	if err := json.NewDecoder(rw.Body).Decode(&total); err != nil {
		t.Fatalf("decode total: %v", err)
	}
	if total != 10 {
		t.Errorf("expected total=10 after incrementing by 10, got %d", total)
	}
}

// TestHandleRate_PostCumulative verifies that subsequent increments accumulate.
func TestHandleRate_PostCumulative(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	for _, delta := range []string{"5", "3"} {
		req := httptest.NewRequest(http.MethodPost, "/rate/v1/user2?delta="+delta, nil)
		rw := httptest.NewRecorder()
		hs.ServeHTTP(rw, req)
		if rw.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rw.Code, rw.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/rate/v1/user2", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	var total int64
	if err := json.NewDecoder(rw.Body).Decode(&total); err != nil {
		t.Fatalf("decode total: %v", err)
	}
	if total != 8 {
		t.Errorf("expected cumulative total=8, got %d", total)
	}
}

// TestHandleRate_InvalidPath verifies that a missing apiKey segment returns 400.
func TestHandleRate_InvalidPath(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/rate/v1", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
}

// TestHandleRate_InvalidDelta verifies that a non-numeric delta returns 400.
func TestHandleRate_InvalidDelta(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/rate/v1/user1?delta=notanumber", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rw.Code, rw.Body.String())
	}
}

// TestHandleRate_MethodNotAllowed verifies that unsupported methods return 405.
func TestHandleRate_MethodNotAllowed(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodDelete, "/rate/v1/user1", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rw.Code)
	}
}

// TestServeHTTP_NotFound verifies that unrecognised routes return 404.
func TestServeHTTP_NotFound(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/unknown-route", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rw.Code)
	}
}

// TestHandleHealth_OK verifies that GET /health returns {"status":"ok"}.
func TestHandleHealth_OK(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rw.Code, rw.Body.String())
	}

	var body map[string]string
	if err := json.NewDecoder(rw.Body).Decode(&body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", body["status"])
	}
}

// TestHandleHealth_MethodNotAllowed verifies that non-GET methods return 405.
func TestHandleHealth_MethodNotAllowed(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rw.Code)
	}
}

func TestServeHTTP_AuthRequired(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	hs.authToken = "secret-token"

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rw.Code)
	}
}

func TestServeHTTP_HealthBypassesAuth(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	hs.authToken = "secret-token"

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rw := httptest.NewRecorder()
	hs.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
}
