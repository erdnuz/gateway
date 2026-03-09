package testing

import (
	"context"
	"gateway/packages/common/types"
	"strings"
	"testing"
	"time"
)

// Generator functions for test data

// NewTestGatewayConfig creates a default test gateway configuration
func NewTestGatewayConfig() *types.GatewayConfig {
	return &types.GatewayConfig{
		Prefixes: []types.PrefixConfig{
			{
				Prefix:      "/api/v1",
				QuotaPeriod: 1 * time.Hour,
				Services: []types.ServiceConfig{
					NewTestServiceConfig("users-service"),
					NewTestServiceConfig("posts-service"),
				},
			},
			{
				Prefix:      "/api/v2",
				QuotaPeriod: 1 * time.Hour,
				Services: []types.ServiceConfig{
					NewTestServiceConfig("users-v2-service"),
				},
			},
		},
		UpdatedAt: time.Now(),
	}
}

// NewTestServiceConfig creates a default test service configuration
func NewTestServiceConfig(serviceID string) types.ServiceConfig {
	return types.ServiceConfig{
		ServiceID: serviceID,
		TargetURL: "http://upstream:3000",
		Tiers: []types.TierConfig{
			{
				TierID:     "free",
				Quota:      1000,
				GetCost:    1,
				PostCost:   2,
				PutCost:    2,
				DeleteCost: 3,
				OtherCost:  1,
			},
			{
				TierID:     "premium",
				Quota:      10000,
				GetCost:    1,
				PostCost:   1,
				PutCost:    1,
				DeleteCost: 1,
				OtherCost:  1,
			},
		},
		Transform: types.TransformConfig{
			StripPrefix: true,
			AddHeaders: map[string]string{
				"X-Service": serviceID,
			},
			HideHeaders: []string{"X-Internal"},
		},
		CORS: &types.CORSConfig{
			AllowedOrigins: []string{"*"},
			AllowedMethods: []string{"GET", "POST", "PUT", "DELETE"},
			MaxAge:         24 * time.Hour,
		},
		Cache: &types.CacheConfig{
			Enabled:  true,
			TTL:      5 * time.Minute,
			CacheKey: "$method:$path:$key",
		},
		Analytics: types.AnalyticsConfig{
			Enabled:          true,
			FlushingInterval: 1 * time.Second,
			SamplingRate:     1.0,
		},
		Failure: types.FailureConfig{
			FailOpen:     false,
			FallbackTier: "free",
		},
	}
}

// NewTestAnalyticsEntry creates a test analytics entry
func NewTestAnalyticsEntry(prefix, service, tier, method string) *types.AnalyticsEntry {
	return &types.AnalyticsEntry{
		Prefix:           prefix,
		Service:          service,
		Timestamp:        time.Now(),
		Method:           method,
		Tier:             tier,
		TotalLatency:     100 * time.Millisecond,
		UpstreamLatency:  50 * time.Millisecond,
		CacheHit:         false,
		LimitUsed:        500,
		LimitUsedOfTotal: 0.5,
		RequestSize:      256,
		ResponseSize:     1024,
		ResponseCode:     200,
	}
}

// AssertEqual checks if two values are equal
func AssertEqual(t *testing.T, expected, actual interface{}, message string) {
	if expected != actual {
		t.Errorf("%s: expected %v, got %v", message, expected, actual)
	}
}

// AssertNotEqual checks if two values are not equal
func AssertNotEqual(t *testing.T, expected, actual interface{}, message string) {
	if expected == actual {
		t.Errorf("%s: expected %v to not equal %v", message, expected, actual)
	}
}

// AssertNil checks if a value is nil
func AssertNil(t *testing.T, val interface{}, message string) {
	if val != nil {
		t.Errorf("%s: expected nil, got %v", message, val)
	}
}

// AssertNotNil checks if a value is not nil
func AssertNotNil(t *testing.T, val interface{}, message string) {
	if val == nil {
		t.Errorf("%s: expected non-nil value", message)
	}
}

// AssertTrue checks if a condition is true
func AssertTrue(t *testing.T, condition bool, message string) {
	if !condition {
		t.Errorf("%s: expected true", message)
	}
}

// AssertFalse checks if a condition is false
func AssertFalse(t *testing.T, condition bool, message string) {
	if condition {
		t.Errorf("%s: expected false", message)
	}
}

// AssertError checks if an error occurred
func AssertError(t *testing.T, err error, message string) {
	if err == nil {
		t.Errorf("%s: expected error, got nil", message)
	}
}

// AssertNoError checks if no error occurred
func AssertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Errorf("%s: expected no error, got %v", message, err)
	}
}

// AssertContains checks if a string contains a substring
func AssertContains(t *testing.T, haystack, needle string, message string) {
	if !contains(haystack, needle) {
		t.Errorf("%s: expected %q to contain %q", message, haystack, needle)
	}
}

// AssertNotContains checks if a string does not contain a substring
func AssertNotContains(t *testing.T, haystack, needle string, message string) {
	if contains(haystack, needle) {
		t.Errorf("%s: expected %q to not contain %q", message, haystack, needle)
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}

// ContextWithTimeout creates a context with timeout
func ContextWithTimeout(seconds time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), seconds*time.Second)
}

// WaitFor waits for a condition to be true with timeout
func WaitFor(t *testing.T, condition func() bool, maxWait time.Duration, message string) {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("%s: condition not met within %v", message, maxWait)
}
