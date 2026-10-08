//go:build integration

package integration

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"gateway/packages/common/types"
)

type failingLeaseClient struct{}

func (failingLeaseClient) RequestQuotaLease(context.Context, *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	return nil, errors.New("lease unavailable")
}

func (failingLeaseClient) Close() error { return nil }

func TestIntegration_DegradedRateServiceUsesSafetyTier(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "ksafe", "free")
	h.setSafetyFallback(true, 3)
	h.triggerConfigReload()
	h.setLeaseClient(failingLeaseClient{})

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/safe", "ksafe")
		if resp.StatusCode == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("safety tier fallback policy did not propagate in time, last status=%d", resp.StatusCode)
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i := 2; i <= 3; i++ {
		resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/safe", "ksafe")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d expected 200 via safety tier, got %d", i, resp.StatusCode)
		}
	}

	// Exhausting the safety tier is rate limiting, not unavailability: 429
	// with Retry-After so clients back off until the window resets.
	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/safe", "ksafe")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after safety tier exhausted, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatalf("expected Retry-After on safety-tier 429")
	}

	if hits := h.upstreamHits.Load(); hits != 3 {
		t.Fatalf("expected upstream hits to equal allowed safety requests (3), got %d", hits)
	}
}
