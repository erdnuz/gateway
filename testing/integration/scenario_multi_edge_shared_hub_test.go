//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"testing"
)

func TestMultiEdgeSharedHubQuota(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	if h.edgeCount() < 2 {
		t.Fatalf("expected at least 2 edge instances, got %d", h.edgeCount())
	}

	const (
		apiKey = "multi-edge-client"
	)
	h.setTier("v1", apiKey, "free")

	const totalRequests = 26
	statusCodes := make([]int, 0, totalRequests)
	for i := 0; i < totalRequests; i++ {
		edgeIdx := i % h.edgeCount()
		path := fmt.Sprintf("/v1/auth-api/test-%d", i)
		resp, _ := h.edgeRequestAt(edgeIdx, http.MethodGet, path, apiKey)
		statusCodes = append(statusCodes, resp.StatusCode)
	}

	okCount := 0
	rateLimitedCount := 0
	for _, code := range statusCodes {
		if code == http.StatusOK {
			okCount++
		}
		if code == http.StatusTooManyRequests {
			rateLimitedCount++
		}
	}

	if okCount == 0 {
		t.Fatalf("expected at least one successful request, got status codes=%v", statusCodes)
	}
	if rateLimitedCount == 0 {
		t.Fatalf("expected shared quota exhaustion across edges, got status codes=%v", statusCodes)
	}
}
