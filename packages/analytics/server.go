package analyticsapi

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

//go:embed static/index.html static/style.css static/app.js
var staticAssets embed.FS

type Server struct {
	rdb       *redis.Client
	key       string
	authToken string
}

func NewServer(rdb *redis.Client, key, authToken string) *Server {
	if key == "" {
		key = types.DefaultAnalyticsKey
	}
	return &Server{rdb: rdb, key: key, authToken: strings.TrimSpace(authToken)}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/", "/index.html":
		s.serveFrontend(w, r)
	case "/assets/style.css", "/assets/app.js":
		s.serveFrontend(w, r)
	case "/health":
		s.handleHealth(w, r)
	case "/analytics/events":
		if !s.authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.queryEvents(w, r)
	case "/analytics/summary":
		if !s.authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.handleSummary(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) serveFrontend(w http.ResponseWriter, r *http.Request) {
	assets, err := fs.Sub(staticAssets, "static")
	if err != nil {
		http.Error(w, "frontend unavailable", http.StatusInternalServerError)
		return
	}

	if r.URL.Path == "/" || r.URL.Path == "/index.html" {
		b, err := fs.ReadFile(assets, "index.html")
		if err != nil {
			http.Error(w, "frontend unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(b)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/assets/") {
		http.StripPrefix("/assets/", http.FileServer(http.FS(assets))).ServeHTTP(w, r)
		return
	}

	http.NotFound(w, r)
}

func (s *Server) authorize(r *http.Request) bool {
	if s.authToken == "" {
		return true
	}
	authz := r.Header.Get("Authorization")
	const bearer = "Bearer "
	if !strings.HasPrefix(authz, bearer) {
		return false
	}
	return strings.TrimPrefix(authz, bearer) == s.authToken
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) storeBatch(ctx context.Context, batch []types.AnalyticsEntry) error {
	pipe := s.rdb.Pipeline()
	for _, e := range batch {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		pipe.RPush(ctx, s.key, b)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// StartKafkaSubscriber consumes analytics events from Kafka and persists them in Redis.
func (s *Server) StartKafkaSubscriber(ctx context.Context, brokers []string, topic, groupID string) error {
	clean := make([]string, 0, len(brokers))
	for _, b := range brokers {
		v := strings.TrimSpace(b)
		if v != "" {
			clean = append(clean, v)
		}
	}
	if len(clean) == 0 {
		clean = []string{"localhost:9092"}
	}
	if strings.TrimSpace(topic) == "" {
		topic = "analytics-events"
	}
	if strings.TrimSpace(groupID) == "" {
		groupID = "analytics-subscribers"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  clean,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	go func() {
		defer func() { _ = reader.Close() }()
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}

			var e types.AnalyticsEntry
			if err := json.Unmarshal(msg.Value, &e); err != nil {
				continue
			}
			_ = s.storeBatch(ctx, []types.AnalyticsEntry{e})
		}
	}()

	return nil
}

func (s *Server) queryEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit := parseIntWithBounds(r.URL.Query().Get("limit"), 100, 1, 5000)
	offset := parseIntWithBounds(r.URL.Query().Get("offset"), 0, 0, 1_000_000)

	vals, err := s.readWindow(ctx, limit, offset)
	if err != nil {
		http.Error(w, "redis error", http.StatusInternalServerError)
		return
	}
	entries := filterEntries(parseEntries(vals), r)
	writeJSON(w, map[string]interface{}{"count": len(entries), "events": entries})
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()
	limit := parseIntWithBounds(r.URL.Query().Get("limit"), 1000, 1, 10000)
	offset := parseIntWithBounds(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	groupBy := r.URL.Query().Get("group_by")

	vals, err := s.readWindow(ctx, limit, offset)
	if err != nil {
		http.Error(w, "redis error", http.StatusInternalServerError)
		return
	}
	entries := filterEntries(parseEntries(vals), r)
	if groupBy == "" {
		writeJSON(w, computeSummary(entries))
		return
	}

	groups := map[string][]types.AnalyticsEntry{}
	for _, e := range entries {
		key := groupKey(e, groupBy)
		groups[key] = append(groups[key], e)
	}
	result := map[string]summary{}
	for k, items := range groups {
		result[k] = computeSummary(items)
	}
	writeJSON(w, result)
}

func groupKey(e types.AnalyticsEntry, groupBy string) string {
	switch groupBy {
	case "prefix":
		return e.Prefix
	case "service":
		return e.Service
	case "tier":
		return e.Tier
	case "method":
		return e.Method
	default:
		return "unknown"
	}
}

func filterEntries(entries []types.AnalyticsEntry, r *http.Request) []types.AnalyticsEntry {
	prefix := r.URL.Query().Get("prefix")
	service := r.URL.Query().Get("service")
	tier := r.URL.Query().Get("tier")
	method := r.URL.Query().Get("method")
	codeMin, hasCodeMin := parseOptionalInt64(r.URL.Query().Get("response_code_min"))
	codeMax, hasCodeMax := parseOptionalInt64(r.URL.Query().Get("response_code_max"))
	cacheHit, hasCacheHit := parseOptionalBool(r.URL.Query().Get("cache_hit"))
	upstreamErr, hasUpstreamErr := parseOptionalBool(r.URL.Query().Get("upstream_error"))
	totalMsMin, hasTotalMsMin := parseOptionalFloat64(r.URL.Query().Get("total_latency_ms_min"))
	totalMsMax, hasTotalMsMax := parseOptionalFloat64(r.URL.Query().Get("total_latency_ms_max"))
	out := make([]types.AnalyticsEntry, 0, len(entries))
	for _, e := range entries {
		if prefix != "" && e.Prefix != prefix {
			continue
		}
		if service != "" && e.Service != service {
			continue
		}
		if tier != "" && e.Tier != tier {
			continue
		}
		if method != "" && e.Method != method {
			continue
		}
		if hasCodeMin && int64(e.ResponseCode) < codeMin {
			continue
		}
		if hasCodeMax && int64(e.ResponseCode) > codeMax {
			continue
		}
		if hasCacheHit && e.CacheHit != cacheHit {
			continue
		}
		if hasUpstreamErr && e.UpstreamError != upstreamErr {
			continue
		}
		totalLatencyMs := microsecondsToMilliseconds(float64(e.TotalLatency.Microseconds()))
		if hasTotalMsMin && totalLatencyMs < totalMsMin {
			continue
		}
		if hasTotalMsMax && totalLatencyMs > totalMsMax {
			continue
		}
		out = append(out, e)
	}
	return out
}

func parseOptionalInt64(raw string) (int64, bool) {
	if strings.TrimSpace(raw) == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func parseOptionalFloat64(raw string) (float64, bool) {
	if strings.TrimSpace(raw) == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func parseOptionalBool(raw string) (bool, bool) {
	v := strings.TrimSpace(strings.ToLower(raw))
	if v == "" {
		return false, false
	}
	if v == "true" || v == "1" || v == "yes" {
		return true, true
	}
	if v == "false" || v == "0" || v == "no" {
		return false, true
	}
	return false, false
}

func parseEntries(vals []string) []types.AnalyticsEntry {
	entries := make([]types.AnalyticsEntry, 0, len(vals))
	for _, raw := range vals {
		var e types.AnalyticsEntry
		if err := json.Unmarshal([]byte(raw), &e); err == nil {
			entries = append(entries, e)
		}
	}
	return entries
}

func (s *Server) readWindow(ctx context.Context, limit, offset int64) ([]string, error) {
	if limit <= 0 {
		return []string{}, nil
	}
	length, err := s.rdb.LLen(ctx, s.key).Result()
	if err != nil {
		return nil, err
	}
	if length == 0 || offset >= length {
		return []string{}, nil
	}
	end := length - 1 - offset
	start := end - limit + 1
	if start < 0 {
		start = 0
	}
	return s.rdb.LRange(ctx, s.key, start, end).Result()
}

type summary struct {
	Count                int     `json:"count"`
	AvgTotalLatencyMs    float64 `json:"avg_total_latency_ms"`
	AvgUpstreamLatencyMs float64 `json:"avg_upstream_latency_ms"`
	AvgRateLimiterMs     float64 `json:"avg_rate_limiter_latency_ms"`
	P95RateLimiterMs     float64 `json:"p95_rate_limiter_latency_ms"`
	CacheHitRatePct      float64 `json:"cache_hit_rate_pct"`
	SuccessRatePct       float64 `json:"success_rate_pct"`
	AvgRequestSizeBytes  float64 `json:"avg_request_size_bytes"`
	AvgResponseSizeBytes float64 `json:"avg_response_size_bytes"`
	ActiveTiersCount     int     `json:"active_tiers_count"`
	RateLimitedCount     int     `json:"rate_limited_count"`
	UpstreamErrorCount   int     `json:"upstream_error_count"`
}

func computeSummary(entries []types.AnalyticsEntry) summary {
	s := summary{Count: len(entries)}
	if len(entries) == 0 {
		return s
	}
	totalsUs := make([]int64, 0, len(entries))
	upstreamsUs := make([]int64, 0, len(entries))
	rateLimiterUs := make([]int64, 0, len(entries))
	cacheHits := 0
	successes := 0
	var requestBytesTotal uint64
	var responseBytesTotal uint64
	activeTiers := map[string]struct{}{}
	for _, e := range entries {
		totalUs := e.TotalLatency.Microseconds()
		upstreamUs := e.UpstreamLatency.Microseconds()
		rateLimiterCostUs := totalUs - upstreamUs
		if rateLimiterCostUs < 0 {
			rateLimiterCostUs = 0
		}

		totalsUs = append(totalsUs, totalUs)
		upstreamsUs = append(upstreamsUs, upstreamUs)
		rateLimiterUs = append(rateLimiterUs, rateLimiterCostUs)
		if e.CacheHit {
			cacheHits++
		}
		if e.ResponseCode >= 200 && e.ResponseCode < 300 {
			successes++
		}
		requestBytesTotal += e.RequestSize
		responseBytesTotal += e.ResponseSize
		activeTiers[e.Tier] = struct{}{}
		if e.ResponseCode == http.StatusTooManyRequests {
			s.RateLimitedCount++
		}
		if e.UpstreamError {
			s.UpstreamErrorCount++
		}
	}
	s.AvgTotalLatencyMs = microsecondsToMilliseconds(meanMicroseconds(totalsUs))
	s.AvgUpstreamLatencyMs = microsecondsToMilliseconds(meanMicroseconds(upstreamsUs))
	s.AvgRateLimiterMs = microsecondsToMilliseconds(meanMicroseconds(rateLimiterUs))
	s.P95RateLimiterMs = microsecondsToMilliseconds(percentileMicroseconds(rateLimiterUs, 95))
	s.CacheHitRatePct = (float64(cacheHits) / float64(s.Count)) * 100
	s.SuccessRatePct = (float64(successes) / float64(s.Count)) * 100
	s.AvgRequestSizeBytes = float64(requestBytesTotal) / float64(s.Count)
	s.AvgResponseSizeBytes = float64(responseBytesTotal) / float64(s.Count)
	s.ActiveTiersCount = len(activeTiers)
	return s
}

func microsecondsToMilliseconds(us float64) float64 {
	return us / 1000.0
}

func meanMicroseconds(data []int64) float64 {
	if len(data) == 0 {
		return 0
	}
	var sum int64
	for _, v := range data {
		sum += v
	}
	return float64(sum) / float64(len(data))
}

func percentileMicroseconds(data []int64, p float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sorted := append([]int64(nil), data...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	rank := p / 100 * float64(len(sorted)-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		return float64(sorted[lo])
	}
	frac := rank - float64(lo)
	return float64(sorted[lo])*(1-frac) + float64(sorted[hi])*frac
}

func parseIntWithBounds(raw string, def, min, max int64) int64 {
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return def
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
