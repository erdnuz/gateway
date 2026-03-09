package edge

import (
	"context"
	"gateway/packages/common/types"
	"gateway/testutils"
	"net/http"
	"testing"
)

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
	testutils.AssertFalse(t, config.UpdatedAt.IsZero(), "should have update time")

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
