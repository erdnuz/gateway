package analyticsapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"gateway/packages/common/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func entryAt(t time.Time, code uint16, service, tier, edge string, latency time.Duration) types.AnalyticsEntry {
	return types.AnalyticsEntry{
		Timestamp: t, ResponseCode: code, Service: service, Tier: tier, EdgeID: edge, Method: "GET",
		TotalLatency: latency, UpstreamLatency: latency - time.Millisecond,
	}
}

func TestOverview_AggregatesOutcomesTiersAndEdges(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 30, 20, 0, time.UTC)
	spec, err := parseOverviewQuery(url.Values{"range": {"15m"}}, now)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Bucket != 15*time.Second || !spec.End.Equal(time.Date(2026, 10, 9, 12, 30, 30, 0, time.UTC)) {
		t.Fatalf("spec %+v", spec)
	}
	in := spec.End.Add(-time.Minute)
	entries := []types.AnalyticsEntry{
		entryAt(in, 200, "orders", "pro", "edge-1", 10*time.Millisecond),
		entryAt(in, 200, "orders", "pro", "edge-2", 20*time.Millisecond),
		entryAt(in, 429, "orders", "free", "edge-1", 2*time.Millisecond),
		entryAt(in, 404, "users", "free", "edge-2", 3*time.Millisecond),
		entryAt(in, 502, "users", "free", "edge-2", 30*time.Millisecond),
		// previous window, and one outside both
		entryAt(spec.Start.Add(-time.Minute), 200, "orders", "pro", "edge-1", 5*time.Millisecond),
		entryAt(spec.PrevStart.Add(-time.Minute), 200, "orders", "pro", "edge-1", 5*time.Millisecond),
	}
	ov := buildOverview(spec, entries)

	k := ov.KPIs
	if k["requests"].Value != 5 || k["requests"].Previous != 1 {
		t.Fatalf("requests %+v", k["requests"])
	}
	if k["rate_limited_share"].Value != 0.2 || k["success_rate"].Value != 0.8 || k["edges_active"].Value != 2 {
		t.Fatalf("kpis %+v", k)
	}
	if k["p99_ms"].Value != 30 || k["gateway_overhead_p90_ms"].Value != 1 {
		t.Fatalf("latency kpis %+v %+v", k["p99_ms"], k["gateway_overhead_p90_ms"])
	}
	if len(ov.Series) != 60 {
		t.Fatalf("series has %d buckets, want 60", len(ov.Series))
	}
	var sum overviewPoint
	for _, p := range ov.Series {
		sum.OK += p.OK
		sum.RateLimited += p.RateLimited
		sum.ClientError += p.ClientError
		sum.UpstreamError += p.UpstreamError
	}
	if sum.OK != 2 || sum.RateLimited != 1 || sum.ClientError != 1 || sum.UpstreamError != 1 {
		t.Fatalf("outcomes %+v", sum)
	}
	if ov.ByTier[0].Name != "free" || ov.ByTier[0].Requests != 3 || fmt.Sprintf("%.3f", ov.ByTier[0].RateLimitedShare) != "0.333" {
		t.Fatalf("by tier %+v", ov.ByTier)
	}
	if len(ov.ByEdge) != 2 || ov.ByEdge[0].Name != "edge-2" || ov.ByEdge[0].Share != 0.6 {
		t.Fatalf("by edge %+v", ov.ByEdge)
	}
	if len(ov.Recent) != 5 || strings.Join(ov.Filters.Services, ",") != "orders,users" {
		t.Fatalf("recent %d, filters %+v", len(ov.Recent), ov.Filters)
	}
}

func TestOverview_FiltersKeepDropdownOptions(t *testing.T) {
	now := time.Now()
	spec, _ := parseOverviewQuery(url.Values{"range": {"1h"}, "tier": {"pro"}}, now)
	in := now.Add(-time.Minute)
	ov := buildOverview(spec, []types.AnalyticsEntry{
		entryAt(in, 200, "orders", "pro", "e1", time.Millisecond),
		entryAt(in, 200, "orders", "free", "e1", time.Millisecond),
	})
	if ov.KPIs["requests"].Value != 1 || strings.Join(ov.Filters.Tiers, ",") != "free,pro" {
		t.Fatalf("requests %v, tiers %v", ov.KPIs["requests"], ov.Filters.Tiers)
	}
}

func TestOverview_HTTP(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "secret")
	b, _ := json.Marshal(entryAt(time.Now().Add(-time.Minute), 200, "orders", "pro", "e1", time.Millisecond))
	mr.Lpush(types.DefaultAnalyticsKey, string(b))

	call := func(path, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec
	}
	if rec := call("/analytics/overview", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", rec.Code)
	}
	if rec := call("/analytics/overview?range=2y", "secret"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad range: %d", rec.Code)
	}
	rec := call("/analytics/overview?range=15m", "secret")
	var ov overviewResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &ov); err != nil || ov.KPIs["requests"].Value != 1 {
		t.Fatalf("overview %d %s", rec.Code, rec.Body)
	}
	if rec := call("/analytics/scaling", "secret"); !strings.Contains(rec.Body.String(), `"enabled":false`) {
		t.Fatalf("scaling without Prometheus: %s", rec.Body)
	}
}

func TestScaling_ReadsPrometheus(t *testing.T) {
	values := map[string]string{
		`sum(rate(gate_requests_total[1m]))`:           "1000",
		`sum(rate(gate_hub_lease_requests_total[1m]))`: "8",
		`sum(rate(gate_key_resolutions_total[1m]))`:    "2",
		`count(up{job="edge"} == 1)`:                   "3",
	}
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, ok := values[r.URL.Query().Get("query")]
		result := "[]"
		if ok {
			result = fmt.Sprintf(`[{"metric":{},"value":[1700000000,%q]}]`, v)
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":%s}}`, result)
	}))
	defer prom.Close()

	srv := NewServer(nil, types.DefaultAnalyticsKey, "")
	srv.SetPrometheus(prom.URL + "/")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/analytics/scaling", nil))
	var sc scalingResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &sc); err != nil {
		t.Fatal(err)
	}
	if !sc.Enabled || sc.Error != "" || sc.Values["edges_serving"] != 3 || sc.HubWorkPerRequest != 0.01 {
		t.Fatalf("scaling %+v", sc)
	}

	prom.Close() // cached for a few seconds, then reported as unreachable
	srv.prom.fetched = time.Time{}
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/analytics/scaling", nil))
	if !strings.Contains(rec.Body.String(), "unreachable") {
		t.Fatalf("down Prometheus: %s", rec.Body)
	}
}
