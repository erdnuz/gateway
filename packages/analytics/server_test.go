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
	if !bytes.Contains(res.Body.Bytes(), []byte("Owner Dashboard")) {
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
		{Prefix: "v1", Service: "auth-api", Method: http.MethodGet, Tier: "free", TotalLatency: 10 * time.Millisecond, CacheHit: true, ResponseCode: 200, UpstreamError: false, Timestamp: time.Now()},
		{Prefix: "v1", Service: "auth-api", Method: http.MethodGet, Tier: "free", TotalLatency: 90 * time.Millisecond, CacheHit: false, ResponseCode: 502, UpstreamError: true, Timestamp: time.Now()},
	}
	for _, e := range entries {
		b, _ := json.Marshal(e)
		if err := rdb.RPush(context.Background(), types.DefaultAnalyticsKey, b).Err(); err != nil {
			t.Fatalf("failed to seed redis: %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/analytics/events?response_code_min=500&cache_hit=false&upstream_error=true&total_latency_ms_min=50", nil)
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
	if summaryPayload.Count != 2 {
		t.Fatalf("expected max summary limit=2 from policy, got %d", summaryPayload.Count)
	}
}
