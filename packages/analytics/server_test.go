package analyticsapi

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gateway/packages/common/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestIngestAndQuery(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	entry := types.AnalyticsEntry{
		Prefix:           "v1",
		Service:          "auth-api",
		Method:           http.MethodGet,
		Tier:             "free",
		TotalLatency:     20 * time.Millisecond,
		UpstreamLatency:  10 * time.Millisecond,
		LimitUsedOfTotal: 0.5,
		ResponseCode:     200,
		Timestamp:        time.Now(),
	}
	b, _ := json.Marshal(entry)
	if err := rdb.RPush(context.Background(), types.DefaultAnalyticsKey, b).Err(); err != nil {
		t.Fatalf("failed to seed redis: %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/analytics/events?service=auth-api", nil)
	getReq.Header.Set("Authorization", "Bearer token")
	getRes := httptest.NewRecorder()
	srv.ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRes.Code)
	}

	sumReq := httptest.NewRequest(http.MethodGet, "/analytics/summary?group_by=service", nil)
	sumReq.Header.Set("Authorization", "Bearer token")
	if err := srv.refreshSummaryCache(context.Background()); err != nil {
		t.Fatalf("refresh summary cache: %v", err)
	}
	sumRes := httptest.NewRecorder()
	srv.ServeHTTP(sumRes, sumReq)
	if sumRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", sumRes.Code)
	}

	length, err := rdb.LLen(context.Background(), types.DefaultAnalyticsKey).Result()
	if err != nil || length != 1 {
		t.Fatalf("expected stored event, len=%d err=%v", length, err)
	}
}

func TestEventsPostNotAllowed(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	req := httptest.NewRequest(http.MethodPost, "/analytics/events", nil)
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	srv.ServeHTTP(res, req)

	if res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", res.Code)
	}
}

func TestAuthRequired(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	req := httptest.NewRequest(http.MethodGet, "/analytics/events", nil)
	res := httptest.NewRecorder()
	srv.ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", res.Code)
	}
}

func TestServesFrontendRoot(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	res := httptest.NewRecorder()
	srv.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}
	if !bytes.Contains(res.Body.Bytes(), []byte("Analytics Contract Dashboard")) {
		t.Fatalf("expected frontend html content")
	}
}

func TestComputeSummarySubMillisecondPrecision(t *testing.T) {
	entries := []types.AnalyticsEntry{
		{TotalLatency: 500 * time.Microsecond, UpstreamLatency: 250 * time.Microsecond, ResponseCode: 200, CacheHit: true, Tier: "free", RequestSize: 100, ResponseSize: 200},
		{TotalLatency: 1500 * time.Microsecond, UpstreamLatency: 750 * time.Microsecond, ResponseCode: 200, CacheHit: false, Tier: "pro", RequestSize: 300, ResponseSize: 500},
	}

	s := computeSummary(entries)

	if math.Abs(s.AvgTotalLatencyMs-1.0) > 0.0001 {
		t.Fatalf("expected avg_total_latency_ms=1.0, got %f", s.AvgTotalLatencyMs)
	}
	if math.Abs(s.AvgUpstreamLatencyMs-0.5) > 0.0001 {
		t.Fatalf("expected avg_upstream_latency_ms=0.5, got %f", s.AvgUpstreamLatencyMs)
	}
	if math.Abs(s.AvgRateLimiterMs-0.5) > 0.0001 {
		t.Fatalf("expected avg_rate_limiter_latency_ms=0.5, got %f", s.AvgRateLimiterMs)
	}
	if math.Abs(s.P95RateLimiterMs-0.725) > 0.0001 {
		t.Fatalf("expected p95_rate_limiter_latency_ms=0.725, got %f", s.P95RateLimiterMs)
	}
	if math.Abs(s.CacheHitRatePct-50.0) > 0.0001 {
		t.Fatalf("expected cache_hit_rate_pct=50, got %f", s.CacheHitRatePct)
	}
	if math.Abs(s.SuccessRatePct-100.0) > 0.0001 {
		t.Fatalf("expected success_rate_pct=100, got %f", s.SuccessRatePct)
	}
	if math.Abs(s.AvgRequestSizeBytes-200.0) > 0.0001 {
		t.Fatalf("expected avg_request_size_bytes=200, got %f", s.AvgRequestSizeBytes)
	}
	if math.Abs(s.AvgResponseSizeBytes-350.0) > 0.0001 {
		t.Fatalf("expected avg_response_size_bytes=350, got %f", s.AvgResponseSizeBytes)
	}
	if s.ActiveTiersCount != 2 {
		t.Fatalf("expected active_tiers_count=2, got %d", s.ActiveTiersCount)
	}
}

func TestComputeSummaryRateLimiterLatencyClampedAtZero(t *testing.T) {
	entries := []types.AnalyticsEntry{
		{TotalLatency: 800 * time.Microsecond, UpstreamLatency: 1200 * time.Microsecond, ResponseCode: 200},
		{TotalLatency: 1000 * time.Microsecond, UpstreamLatency: 200 * time.Microsecond, ResponseCode: 200},
	}

	s := computeSummary(entries)

	if s.P95RateLimiterMs < 0 {
		t.Fatalf("expected non-negative p95_rate_limiter_latency_ms, got %f", s.P95RateLimiterMs)
	}
}

func TestAdvancedFiltersOnEvents(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	entries := []types.AnalyticsEntry{
		{Prefix: "v1", Service: "auth-api", EdgeID: "edge-a", Method: http.MethodGet, Tier: "free", TotalLatency: 10 * time.Millisecond, CacheHit: true, ResponseCode: 200, UpstreamError: false, Timestamp: time.Now()},
		{Prefix: "v1", Service: "auth-api", EdgeID: "edge-b", Method: http.MethodGet, Tier: "free", TotalLatency: 90 * time.Millisecond, CacheHit: false, ResponseCode: 502, UpstreamError: true, Timestamp: time.Now()},
	}
	for _, e := range entries {
		b, _ := json.Marshal(e)
		if err := rdb.RPush(context.Background(), types.DefaultAnalyticsKey, b).Err(); err != nil {
			t.Fatalf("failed to seed redis: %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/analytics/events?response_code_min=500&cache_hit=false&upstream_error=true&total_latency_ms_min=50&edge_id=edge-b", nil)
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	srv.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload struct {
		Count  int                    `json:"count"`
		Events []types.AnalyticsEntry `json:"events"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if payload.Count != 1 {
		t.Fatalf("expected exactly one filtered event, got %d", payload.Count)
	}
	if payload.Events[0].ResponseCode != 502 {
		t.Fatalf("expected filtered event response_code=502, got %d", payload.Events[0].ResponseCode)
	}
	if payload.Events[0].EdgeID != "edge-b" {
		t.Fatalf("expected filtered event edge_id=edge-b, got %q", payload.Events[0].EdgeID)
	}
}

func TestAnalyticsPolicyControlsQueryBounds(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	policy := types.AnalyticsRuntimePolicy{
		DefaultEventsLimit:  2,
		MaxEventsLimit:      3,
		MaxEventsOffset:     100,
		DefaultSummaryLimit: 1,
		MaxSummaryLimit:     2,
	}
	srv := NewServerWithPolicy(rdb, types.DefaultAnalyticsKey, "token", policy)

	for i := 0; i < 5; i++ {
		e := types.AnalyticsEntry{Prefix: "v1", Service: "svc", Method: http.MethodGet, Tier: "free", ResponseCode: 200, Timestamp: time.Now()}
		b, _ := json.Marshal(e)
		if err := rdb.RPush(context.Background(), types.DefaultAnalyticsKey, b).Err(); err != nil {
			t.Fatalf("failed to seed redis: %v", err)
		}
	}

	defaultReq := httptest.NewRequest(http.MethodGet, "/analytics/events", nil)
	defaultReq.Header.Set("Authorization", "Bearer token")
	defaultRes := httptest.NewRecorder()
	srv.ServeHTTP(defaultRes, defaultReq)
	if defaultRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", defaultRes.Code)
	}
	var defaultPayload struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(defaultRes.Body.Bytes(), &defaultPayload); err != nil {
		t.Fatalf("decode default events payload: %v", err)
	}
	if defaultPayload.Count != 2 {
		t.Fatalf("expected default events limit=2 from policy, got %d", defaultPayload.Count)
	}

	maxReq := httptest.NewRequest(http.MethodGet, "/analytics/events?limit=999", nil)
	maxReq.Header.Set("Authorization", "Bearer token")
	maxRes := httptest.NewRecorder()
	srv.ServeHTTP(maxRes, maxReq)
	if maxRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", maxRes.Code)
	}
	var maxPayload struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(maxRes.Body.Bytes(), &maxPayload); err != nil {
		t.Fatalf("decode max events payload: %v", err)
	}
	if maxPayload.Count != 3 {
		t.Fatalf("expected max events limit=3 from policy, got %d", maxPayload.Count)
	}

	summaryReq := httptest.NewRequest(http.MethodGet, "/analytics/summary?limit=999", nil)
	summaryReq.Header.Set("Authorization", "Bearer token")
	if err := srv.refreshSummaryCache(context.Background()); err != nil {
		t.Fatalf("refresh summary cache: %v", err)
	}
	summaryRes := httptest.NewRecorder()
	srv.ServeHTTP(summaryRes, summaryReq)
	if summaryRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", summaryRes.Code)
	}
	var summaryPayload struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(summaryRes.Body.Bytes(), &summaryPayload); err != nil {
		t.Fatalf("decode summary payload: %v", err)
	}
	if summaryPayload.Count != 5 {
		t.Fatalf("expected cached summary count=5, got %d", summaryPayload.Count)
	}
}

func TestSummaryReturns202WhenCacheNotReady(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	req := httptest.NewRequest(http.MethodGet, "/analytics/summary", nil)
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	srv.ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202 before cache warmup, got %d", res.Code)
	}
}

func TestSummarySeriesZeroFillUsesNullMetricsForEmptyBuckets(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	e := types.AnalyticsEntry{
		Prefix:          "v1",
		Service:         "svc",
		Method:          http.MethodGet,
		Tier:            "free",
		ResponseCode:    200,
		TotalLatency:    20 * time.Millisecond,
		UpstreamLatency: 10 * time.Millisecond,
		Timestamp:       time.Now().UTC(),
	}
	b, _ := json.Marshal(e)
	if err := rdb.RPush(context.Background(), types.DefaultAnalyticsKey, b).Err(); err != nil {
		t.Fatalf("failed to seed redis: %v", err)
	}
	if err := srv.refreshSummaryCache(context.Background()); err != nil {
		t.Fatalf("refresh summary cache: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/analytics/summary?interval=1m", nil)
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	srv.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload struct {
		Count  int `json:"count"`
		Series []struct {
			Count           int      `json:"count"`
			AvgTotalLatency *float64 `json:"avg_total_latency_ms"`
			CacheHitRatePct *float64 `json:"cache_hit_rate_pct"`
			SuccessRatePct  *float64 `json:"success_rate_pct"`
		} `json:"series"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode summary payload: %v", err)
	}
	if payload.Count != 1 {
		t.Fatalf("expected count=1, got %d", payload.Count)
	}

	emptyBucketFound := false
	for _, b := range payload.Series {
		if b.Count == 0 {
			emptyBucketFound = true
			if b.AvgTotalLatency != nil || b.CacheHitRatePct != nil || b.SuccessRatePct != nil {
				t.Fatalf("expected null metrics for empty bucket")
			}
		}
	}
	if !emptyBucketFound {
		t.Fatalf("expected at least one zero-filled empty bucket")
	}
}

func TestSummarySupportsOneHourInterval(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	e := types.AnalyticsEntry{
		Prefix:          "v1",
		Service:         "svc",
		Method:          http.MethodGet,
		Tier:            "free",
		ResponseCode:    200,
		TotalLatency:    15 * time.Millisecond,
		UpstreamLatency: 8 * time.Millisecond,
		Timestamp:       time.Now().UTC(),
	}
	b, _ := json.Marshal(e)
	if err := rdb.RPush(context.Background(), types.DefaultAnalyticsKey, b).Err(); err != nil {
		t.Fatalf("failed to seed redis: %v", err)
	}
	if err := srv.refreshSummaryCache(context.Background()); err != nil {
		t.Fatalf("refresh summary cache: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/analytics/summary?interval=1h", nil)
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	srv.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload struct {
		Interval string `json:"interval"`
		Series   []struct {
			Count int `json:"count"`
		} `json:"series"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode summary payload: %v", err)
	}
	if payload.Interval != "1h" {
		t.Fatalf("expected interval=1h, got %q", payload.Interval)
	}
	if len(payload.Series) != 168 {
		t.Fatalf("expected 168 buckets for 7-day 1h window, got %d", len(payload.Series))
	}
}

func TestSummaryAppliesFiltersAndTimeBounds(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	now := time.Now().UTC().Truncate(time.Second)
	inside := types.AnalyticsEntry{
		Prefix:          "v1",
		Service:         "auth-api",
		EdgeID:          "edge-a",
		Method:          http.MethodGet,
		Tier:            "free",
		ResponseCode:    200,
		TotalLatency:    20 * time.Millisecond,
		UpstreamLatency: 9 * time.Millisecond,
		Timestamp:       now.Add(-2 * time.Minute),
	}
	outsideService := types.AnalyticsEntry{
		Prefix:          "v1",
		Service:         "billing-api",
		EdgeID:          "edge-a",
		Method:          http.MethodGet,
		Tier:            "free",
		ResponseCode:    200,
		TotalLatency:    25 * time.Millisecond,
		UpstreamLatency: 10 * time.Millisecond,
		Timestamp:       now.Add(-2 * time.Minute),
	}
	outsideEdge := types.AnalyticsEntry{
		Prefix:          "v1",
		Service:         "auth-api",
		EdgeID:          "edge-b",
		Method:          http.MethodGet,
		Tier:            "free",
		ResponseCode:    200,
		TotalLatency:    26 * time.Millisecond,
		UpstreamLatency: 11 * time.Millisecond,
		Timestamp:       now.Add(-2 * time.Minute),
	}
	outsideTime := types.AnalyticsEntry{
		Prefix:          "v1",
		Service:         "auth-api",
		EdgeID:          "edge-a",
		Method:          http.MethodGet,
		Tier:            "free",
		ResponseCode:    200,
		TotalLatency:    30 * time.Millisecond,
		UpstreamLatency: 12 * time.Millisecond,
		Timestamp:       now.Add(-90 * time.Minute),
	}

	for _, e := range []types.AnalyticsEntry{inside, outsideService, outsideEdge, outsideTime} {
		b, _ := json.Marshal(e)
		if err := rdb.RPush(context.Background(), types.DefaultAnalyticsKey, b).Err(); err != nil {
			t.Fatalf("failed to seed redis: %v", err)
		}
	}

	start := now.Add(-10 * time.Minute).Format(time.RFC3339)
	end := now.Add(1 * time.Minute).Format(time.RFC3339)
	req := httptest.NewRequest(http.MethodGet, "/analytics/summary?interval=1m&service=auth-api&edge_id=edge-a&start="+start+"&end="+end, nil)
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	srv.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode summary payload: %v", err)
	}
	if payload.Count != 1 {
		t.Fatalf("expected filtered summary count=1, got %d", payload.Count)
	}
}

func TestClearEndpointEnabledByDeploymentFlag(t *testing.T) {
	t.Setenv("ANALYTICS_ENABLE_TESTING_CLEAR", "true")
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	e := types.AnalyticsEntry{Prefix: "v1", Service: "auth-api", Method: http.MethodGet, Tier: "free", ResponseCode: 200, Timestamp: time.Now().UTC()}
	b, _ := json.Marshal(e)
	if err := rdb.RPush(context.Background(), types.DefaultAnalyticsKey, b).Err(); err != nil {
		t.Fatalf("failed to seed redis: %v", err)
	}

	if err := srv.refreshSummaryCache(context.Background()); err != nil {
		t.Fatalf("refresh summary cache: %v", err)
	}

	clearReq := httptest.NewRequest(http.MethodPost, "/analytics/clear", nil)
	clearReq.Header.Set("Authorization", "Bearer token")
	clearRes := httptest.NewRecorder()
	srv.ServeHTTP(clearRes, clearReq)
	if clearRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", clearRes.Code)
	}

	length, err := rdb.LLen(context.Background(), types.DefaultAnalyticsKey).Result()
	if err != nil {
		t.Fatalf("lllen failed: %v", err)
	}
	if length != 0 {
		t.Fatalf("expected cleared analytics list length=0, got %d", length)
	}

	summaryReq := httptest.NewRequest(http.MethodGet, "/analytics/summary?interval=1m", nil)
	summaryReq.Header.Set("Authorization", "Bearer token")
	summaryRes := httptest.NewRecorder()
	srv.ServeHTTP(summaryRes, summaryReq)
	if summaryRes.Code != http.StatusAccepted {
		t.Fatalf("expected 202 after clear while cache warms, got %d", summaryRes.Code)
	}
}

func TestBoundedPercentilesAccuracy(t *testing.T) {
	bp := newBoundedPercentiles(4096)
	for i := int64(1); i <= 1000; i++ {
		bp.add(i)
	}

	p50 := bp.percentile(50)
	p90 := bp.percentile(90)
	p95 := bp.percentile(95)

	if math.Abs(p50-500.5) > 1 {
		t.Fatalf("expected p50 near 500.5, got %f", p50)
	}
	if math.Abs(p90-900.1) > 1 {
		t.Fatalf("expected p90 near 900.1, got %f", p90)
	}
	if math.Abs(p95-950.05) > 1 {
		t.Fatalf("expected p95 near 950.05, got %f", p95)
	}
}

func TestStatsAccumulatorLongRunMemoryBounded(t *testing.T) {
	acc := &statsAccumulator{}
	for i := 0; i < 200000; i++ {
		acc.add(types.AnalyticsEntry{
			Prefix:          "v1",
			Service:         "auth-api",
			Method:          http.MethodGet,
			Tier:            "free",
			ResponseCode:    200,
			TotalLatency:    time.Duration((i%1000)+1) * time.Microsecond,
			UpstreamLatency: time.Duration((i%700)+1) * time.Microsecond,
			Timestamp:       time.Now().UTC(),
		})
	}

	if acc.Totals == nil || acc.RateLimiter == nil {
		t.Fatal("expected bounded percentile trackers to be initialized")
	}
	if len(acc.Totals.values) > defaultPercentileCap {
		t.Fatalf("totals sample exceeded cap: got=%d cap=%d", len(acc.Totals.values), defaultPercentileCap)
	}
	if len(acc.RateLimiter.values) > defaultPercentileCap {
		t.Fatalf("rate limiter sample exceeded cap: got=%d cap=%d", len(acc.RateLimiter.values), defaultPercentileCap)
	}
}

func TestIngestionMetricsEndpoint(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	srv.ingestStats.received.Add(3)
	srv.ingestStats.enqueued.Add(2)
	srv.ingestStats.dropped.Add(1)

	req := httptest.NewRequest(http.MethodGet, "/analytics/ingestion-metrics", nil)
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	srv.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}
	var payload ingestionMetrics
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode ingestion metrics: %v", err)
	}
	if payload.Received != 3 || payload.Enqueued != 2 || payload.Dropped != 1 {
		t.Fatalf("unexpected ingestion metrics payload: %+v", payload)
	}
}

func TestPersistAnalyticsEntryTracksRetriesAndFailures(t *testing.T) {
	mr, _ := miniredis.Run()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := NewServer(rdb, types.DefaultAnalyticsKey, "token")

	_ = rdb.Close()
	mr.Close()

	srv.persistAnalyticsEntry(context.Background(), types.AnalyticsEntry{Prefix: "v1", Service: "svc", Timestamp: time.Now().UTC()})
	metrics := srv.snapshotIngestionMetrics()
	if metrics.PersistFailures == 0 {
		t.Fatalf("expected persist failures to be tracked, got %+v", metrics)
	}
	if metrics.PersistRetries < ingestMaxRetries {
		t.Fatalf("expected persist retries >= %d, got %+v", ingestMaxRetries, metrics)
	}
}
