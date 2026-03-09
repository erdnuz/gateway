package hub

import (
	"gateway/packages/edge"
	testutils "gateway/testing"
	"testing"
	"time"
)

// TestHubConfigStructure tests hub configuration structure
func TestHubConfigStructure(t *testing.T) {
	config := testutils.NewTestGatewayConfig()

	testutils.AssertTrue(t, len(config.Prefixes) > 0, "should have prefixes")
	testutils.AssertFalse(t, config.UpdatedAt.IsZero(), "should have update time")

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
