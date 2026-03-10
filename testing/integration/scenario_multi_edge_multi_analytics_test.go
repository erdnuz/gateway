//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestIntegration_MultiEdgeMultiAnalyticsConsistency(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	if h.edgeCount() < 2 {
		t.Fatalf("expected at least 2 edges, got %d", h.edgeCount())
	}
	if h.analyticsCount() < 2 {
		t.Fatalf("expected at least 2 analytics instances, got %d", h.analyticsCount())
	}

	const apiKey = "multi-topology-client"
	h.setTier("v1", apiKey, "free")

	const totalRequests = 10
	for i := 0; i < totalRequests; i++ {
		edgeIdx := i % h.edgeCount()
		path := fmt.Sprintf("/v1/auth-api/multi-%d", i)
		resp, _ := h.edgeRequestAt(edgeIdx, http.MethodGet, path, apiKey)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d via edge %d expected 200 got %d", i, edgeIdx, resp.StatusCode)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		primaryEvents, err1 := h.analyticsEventsAt(0, 100)
		secondaryEvents, err2 := h.analyticsEventsAt(1, 100)
		if err1 == nil && err2 == nil {
			if len(primaryEvents) >= totalRequests && len(secondaryEvents) >= totalRequests {
				if len(primaryEvents) != len(secondaryEvents) {
					t.Fatalf("analytics replicas diverged: primary=%d secondary=%d", len(primaryEvents), len(secondaryEvents))
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("analytics did not converge in time; primaryErr=%v secondaryErr=%v", err1, err2)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
