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

func TestIntegration_ConfigReloadInvalidatesResponseCacheAcrossEdges(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-reload-multi", "free")

	seed, _ := h.edgeRequestAt(0, http.MethodGet, "/v1/auth-api/reload-multi", "k-reload-multi")
	if seed.StatusCode != http.StatusOK {
		t.Fatalf("seed status=%d", seed.StatusCode)
	}
	if got := seed.Header.Get("X-Cache"); got != "MISS" {
		t.Fatalf("seed request expected MISS got=%q", got)
	}

	for i := 0; i < h.edgeCount(); i++ {
		resp, _ := h.edgeRequestAt(i, http.MethodGet, "/v1/auth-api/reload-multi", "k-reload-multi")
		if got := resp.Header.Get("X-Cache"); got != "HIT" {
			t.Fatalf("edge %d warm verification expected HIT got=%q", i, got)
		}
	}

	beforeReloadHits := h.upstreamHits.Load()
	h.setServiceTargetURL(h.upstreamSrv.URL + "/unused-target-change")
	h.triggerConfigReload()

	deadline := time.Now().Add(1200 * time.Millisecond)
	sawMiss := false
	for {
		for i := 0; i < h.edgeCount(); i++ {
			resp, _ := h.edgeRequestAt(i, http.MethodGet, "/v1/auth-api/reload-multi", "k-reload-multi")
			if resp.Header.Get("X-Cache") == "MISS" {
				sawMiss = true
				break
			}
		}
		if sawMiss {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("did not observe cache invalidation MISS after reload within bounded window")
		}
		time.Sleep(30 * time.Millisecond)
	}

	afterReloadHits := h.upstreamHits.Load()
	if delta := afterReloadHits - beforeReloadHits; delta < 1 {
		t.Fatalf("expected at least one new upstream hit from invalidated cache, got delta=%d", delta)
	}

	for i := 0; i < h.edgeCount(); i++ {
		resp, _ := h.edgeRequestAt(i, http.MethodGet, "/v1/auth-api/reload-multi", "k-reload-multi")
		if got := resp.Header.Get("X-Cache"); got != "HIT" {
			t.Fatalf("edge %d expected converged HIT after refill got=%q", i, got)
		}
	}
}

func (h *integrationHarness) setServiceTargetURL(url string) {
	h.t.Helper()
	cfg, err := readConfigFile(h.configPath)
	if err != nil {
		h.t.Fatalf("read config file: %v", err)
	}
	if len(cfg.Prefixes) == 0 || len(cfg.Prefixes[0].Services) == 0 {
		h.t.Fatal("config file structure is missing service")
	}
	cfg.Prefixes[0].Services[0].TargetURL = url
	if err := writeConfigFile(h.configPath, cfg); err != nil {
		h.t.Fatalf("write config file: %v", err)
	}
}
