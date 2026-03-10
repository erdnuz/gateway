//go:build integration

package integration

import (
	"net/http"
	"testing"
)

func TestIntegration_RateLeaseRefillAndExhaustion(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-rate", "free")

	for i := 1; i <= 20; i++ {
		resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-rate")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d expected 200 got=%d", i, resp.StatusCode)
		}
	}

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-rate")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after quota exhaustion got=%d", resp.StatusCode)
	}

	if hits := h.upstreamHits.Load(); hits != 20 {
		t.Fatalf("expected upstream hits to equal allowed requests (20), got %d", hits)
	}
}
