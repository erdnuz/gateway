//go:build integration

package integration

import (
	"net/http"
	"testing"
)

func TestIntegration_MultiEdgeLeaseFailureIsolation(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	if h.edgeCount() < 2 {
		t.Fatalf("expected at least 2 edges, got %d", h.edgeCount())
	}

	const apiKey = "iso-client"
	h.setTier("v1", apiKey, "free")

	// Break lease on edge 0 only; edge 1 should remain healthy.
	h.setLeaseClientAt(0, failingLeaseClient{})

	resp0, _ := h.edgeRequestAt(0, http.MethodPost, "/v1/auth-api/failover-a", apiKey)
	if resp0.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("edge 0 expected 503 when lease is unavailable, got %d", resp0.StatusCode)
	}

	resp1, _ := h.edgeRequestAt(1, http.MethodPost, "/v1/auth-api/failover-b", apiKey)
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("edge 1 expected 200 while edge 0 is degraded, got %d", resp1.StatusCode)
	}
}
