//go:build integration

package integration

import (
	"net/http"
	"testing"
	"time"
)

func TestIntegration_ConfigReloadDisablesCacheAtRuntime(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-reload", "free")

	resp1, _ := h.edgeRequest(http.MethodGet, "/v1/auth-api/reload", "k-reload")
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first status=%d", resp1.StatusCode)
	}
	if got := resp1.Header.Get("X-Cache"); got != "MISS" {
		t.Fatalf("first request expected MISS got=%q", got)
	}

	resp2, _ := h.edgeRequest(http.MethodGet, "/v1/auth-api/reload", "k-reload")
	if got := resp2.Header.Get("X-Cache"); got != "HIT" {
		t.Fatalf("second request expected HIT got=%q", got)
	}
	if hits := h.upstreamHits.Load(); hits != 1 {
		t.Fatalf("expected 1 upstream hit before reload, got %d", hits)
	}

	h.setCacheEnabled(false)
	h.triggerConfigReload()

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp3, _ := h.edgeRequest(http.MethodGet, "/v1/auth-api/reload", "k-reload")
		if resp3.Header.Get("X-Cache") == "MISS" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("edge did not pick up cache-disabled config after reload")
		}
		time.Sleep(50 * time.Millisecond)
	}

	_, _ = h.edgeRequest(http.MethodGet, "/v1/auth-api/reload", "k-reload")
	if hits := h.upstreamHits.Load(); hits < 3 {
		t.Fatalf("expected upstream to be called after cache disabled; hits=%d", hits)
	}
}
