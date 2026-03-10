//go:build integration

package integration

import (
	"encoding/json"
	"gateway/packages/common/types"
	"math"
	"net/http"
	"testing"
	"time"
)

func TestIntegration_CacheHitAndAnalyticsQuery(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k1", "free")

	resp1, body1 := h.edgeRequest(http.MethodGet, "/v1/auth-api/hello", "k1")
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first status=%d", resp1.StatusCode)
	}
	if got := resp1.Header.Get("X-Cache"); got != "MISS" {
		t.Fatalf("first request expected MISS got=%q", got)
	}

	resp2, body2 := h.edgeRequest(http.MethodGet, "/v1/auth-api/hello", "k1")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second status=%d", resp2.StatusCode)
	}
	if got := resp2.Header.Get("X-Cache"); got != "HIT" {
		t.Fatalf("second request expected HIT got=%q", got)
	}
	if body1 != body2 {
		t.Fatalf("cached body mismatch first=%s second=%s", body1, body2)
	}
	if hits := h.upstreamHits.Load(); hits != 1 {
		t.Fatalf("expected 1 upstream hit, got %d", hits)
	}

	deadline := time.Now().Add(2 * time.Second)
	var observed []types.AnalyticsEntry
	for {
		events, err := h.analyticsEvents(20)
		if err == nil && len(events) >= 2 {
			observed = events
			var hasHit bool
			for _, e := range events {
				if e.CacheHit {
					hasHit = true
					break
				}
			}
			if hasHit {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("analytics events did not include cache hit in time")
		}
		time.Sleep(50 * time.Millisecond)
	}

	var miss, hit *types.AnalyticsEntry
	for i := range observed {
		e := observed[i]
		if e.Method == http.MethodGet && e.Prefix == "v1" && e.Service == "auth-api" && e.Tier == "free" && e.ResponseCode == http.StatusOK {
			if e.CacheHit {
				eCopy := e
				hit = &eCopy
			} else {
				eCopy := e
				miss = &eCopy
			}
		}
	}
	if miss == nil || hit == nil {
		t.Fatalf("expected both cache miss and hit analytics entries; got miss=%v hit=%v total=%d", miss != nil, hit != nil, len(observed))
	}
	if miss.TotalLatency <= 0 || hit.TotalLatency <= 0 {
		t.Fatalf("expected positive total latencies; miss=%v hit=%v", miss.TotalLatency, hit.TotalLatency)
	}

	resp, err := http.Get(h.analyticsSrv.URL + "/analytics/summary?limit=20")
	if err != nil {
		t.Fatalf("summary query failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("summary status=%d", resp.StatusCode)
	}
	var summary struct {
		TotalEntries    int     `json:"count"`
		CacheHitRatePct float64 `json:"cache_hit_rate_pct"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.TotalEntries < 2 {
		t.Fatalf("expected >=2 analytics entries got %d", summary.TotalEntries)
	}
	if math.Abs(summary.CacheHitRatePct-50) > 50 {
		t.Fatalf("unexpected cache hit rate %.2f", summary.CacheHitRatePct)
	}
}
