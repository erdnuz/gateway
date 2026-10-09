package analyticsapi

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	"gateway/packages/common/httpx"
	"gateway/packages/common/types"
)

// The dashboard's time ranges and the bucket size each one is drawn with.
var overviewRanges = map[string]struct{ span, bucket time.Duration }{
	"15m": {15 * time.Minute, 15 * time.Second},
	"1h":  {time.Hour, time.Minute},
	"6h":  {6 * time.Hour, 5 * time.Minute},
	"24h": {24 * time.Hour, 15 * time.Minute},
	"7d":  {7 * 24 * time.Hour, time.Hour},
}

// overviewEntryLimit bounds the events the in-process path aggregates; the
// ClickHouse path aggregates in SQL and has no such limit.
const overviewEntryLimit = 200_000

type overviewSpec struct {
	Range     string
	Span      time.Duration
	Bucket    time.Duration
	Start     time.Time // current window: [Start, End)
	End       time.Time
	PrevStart time.Time // previous window: [PrevStart, Start)
	Filters   analyticsFilterSpec
}

func parseOverviewQuery(values url.Values, now time.Time) (overviewSpec, error) {
	rng := strings.TrimSpace(values.Get("range"))
	if rng == "" {
		rng = "1h"
	}
	r, ok := overviewRanges[rng]
	if !ok {
		return overviewSpec{}, fmt.Errorf("unsupported range %q (use 15m, 1h, 6h, 24h or 7d)", rng)
	}
	end := now.UTC().Truncate(r.bucket).Add(r.bucket) // include the bucket in progress
	start := end.Add(-r.span)
	return overviewSpec{
		Range: rng, Span: r.span, Bucket: r.bucket,
		Start: start, End: end, PrevStart: start.Add(-r.span),
		Filters: analyticsFilterSpec{
			Services: sliceQueryValues(values, "service"),
			Tiers:    sliceQueryValues(values, "tier"),
			EdgeIDs:  sliceQueryValues(values, "edge_id"),
		},
	}, nil
}

type overviewKPI struct {
	Value    float64 `json:"value"`
	Previous float64 `json:"previous"`
}

type overviewPoint struct {
	T             time.Time `json:"t"`
	OK            int64     `json:"ok"`
	RateLimited   int64     `json:"rate_limited"`
	ClientError   int64     `json:"client_error"`
	UpstreamError int64     `json:"upstream_error"`
	P50Ms         float64   `json:"p50_ms"`
	P99Ms         float64   `json:"p99_ms"`
}

type overviewRow struct {
	Name             string  `json:"name"`
	Requests         int64   `json:"requests"`
	Share            float64 `json:"share"`
	RateLimitedShare float64 `json:"rate_limited_share"`
	ErrorShare       float64 `json:"error_share"`
	CacheHitRate     float64 `json:"cache_hit_rate"`
	P90Ms            float64 `json:"p90_ms"`
}

type overviewEvent struct {
	Time      time.Time `json:"time"`
	Method    string    `json:"method"`
	Service   string    `json:"service"`
	Tier      string    `json:"tier"`
	Edge      string    `json:"edge_id"`
	Status    int       `json:"status"`
	LatencyMs float64   `json:"latency_ms"`
	CacheHit  bool      `json:"cache_hit"`
}

type overviewFilters struct {
	Services []string `json:"services"`
	Tiers    []string `json:"tiers"`
	Edges    []string `json:"edges"`
}

// overviewResponse is exactly what the dashboard renders.
type overviewResponse struct {
	Range     string                 `json:"range"`
	BucketSec int64                  `json:"bucket_seconds"`
	Start     time.Time              `json:"start"`
	End       time.Time              `json:"end"`
	KPIs      map[string]overviewKPI `json:"kpis"`
	Series    []overviewPoint        `json:"series"`
	ByTier    []overviewRow          `json:"by_tier"`
	ByService []overviewRow          `json:"by_service"`
	ByEdge    []overviewRow          `json:"by_edge"`
	Recent    []overviewEvent        `json:"recent"`
	Filters   overviewFilters        `json:"filters"`
}

// windowStats summarises one window; both stores produce it.
type windowStats struct {
	Requests, RateLimited, UpstreamErrors, ClientErrors, CacheHits int64
	P50Ms, P99Ms, OverheadP90Ms                                    float64
	Edges                                                          int64
}

func (w windowStats) kpis(span time.Duration) map[string]float64 {
	n := float64(w.Requests)
	return map[string]float64{
		"requests":                float64(w.Requests),
		"rps":                     n / span.Seconds(),
		"success_rate":            ratio(n-float64(w.UpstreamErrors), n),
		"rate_limited_share":      ratio(float64(w.RateLimited), n),
		"p50_ms":                  w.P50Ms,
		"p99_ms":                  w.P99Ms,
		"gateway_overhead_p90_ms": w.OverheadP90Ms,
		"cache_hit_rate":          ratio(float64(w.CacheHits), n),
		"edges_active":            float64(w.Edges),
	}
}

func newOverview(spec overviewSpec, cur, prev windowStats) overviewResponse {
	resp := overviewResponse{
		Range: spec.Range, BucketSec: int64(spec.Bucket.Seconds()), Start: spec.Start, End: spec.End,
		KPIs:   map[string]overviewKPI{},
		Series: []overviewPoint{}, ByTier: []overviewRow{}, ByService: []overviewRow{}, ByEdge: []overviewRow{},
		Recent: []overviewEvent{}, Filters: overviewFilters{Services: []string{}, Tiers: []string{}, Edges: []string{}},
	}
	pk := prev.kpis(spec.Span)
	for k, v := range cur.kpis(spec.Span) {
		resp.KPIs[k] = overviewKPI{Value: finite(v), Previous: finite(pk[k])}
	}
	return resp
}

// fillSeries returns one point per bucket of the window, zeros where idle.
func fillSeries(spec overviewSpec, byBucket map[int64]overviewPoint) []overviewPoint {
	out := make([]overviewPoint, 0, int(spec.Span/spec.Bucket))
	for t := spec.Start; t.Before(spec.End); t = t.Add(spec.Bucket) {
		p := byBucket[t.Unix()]
		p.T = t
		out = append(out, p)
	}
	return out
}

// outcome classifies a request the way the dashboard colours it.
type outcome int

const (
	outcomeOK outcome = iota
	outcomeRateLimited
	outcomeClientError
	outcomeUpstreamError
)

func outcomeOf(e types.AnalyticsEntry) outcome {
	switch {
	case e.ResponseCode == http.StatusTooManyRequests:
		return outcomeRateLimited
	case e.UpstreamError || e.ResponseCode >= 500:
		return outcomeUpstreamError
	case e.ResponseCode >= 400:
		return outcomeClientError
	}
	return outcomeOK
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.Error(w, r, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	spec, err := parseOverviewQuery(r.URL.Query(), time.Now())
	if err != nil {
		httpx.Error(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if ch, ok := s.store.(*clickHouseAnalyticsStore); ok {
		resp, err := ch.queryOverview(r.Context(), spec)
		if err != nil {
			httpx.Logger(r.Context()).Error("overview query failed", "err", err)
			httpx.Error(w, r, http.StatusInternalServerError, "analytics storage error")
			return
		}
		writeJSON(w, resp)
		return
	}
	entries, err := s.overviewEntries(r.Context(), spec)
	if err != nil {
		httpx.Error(w, r, http.StatusInternalServerError, "analytics storage error")
		return
	}
	writeJSON(w, buildOverview(spec, entries))
}

func (s *Server) overviewEntries(ctx context.Context, spec overviewSpec) ([]types.AnalyticsEntry, error) {
	if s.store != nil {
		return s.store.LoadRecentEntries(ctx, spec.PrevStart, spec.End, overviewEntryLimit)
	}
	return s.readEntries(ctx, overviewEntryLimit, 0)
}

// buildOverview aggregates events in process (the Redis and in-memory
// stores, which hold development-sized data).
func buildOverview(spec overviewSpec, entries []types.AnalyticsEntry) overviewResponse {
	var cur, prev []types.AnalyticsEntry
	opts := map[string]map[string]bool{"service": {}, "tier": {}, "edge": {}}
	for _, e := range entries {
		ts := e.Timestamp.UTC()
		inCur := !ts.Before(spec.Start) && ts.Before(spec.End)
		if inCur {
			opts["service"][e.Service] = true
			opts["tier"][e.Tier] = true
			opts["edge"][e.EdgeID] = true
		}
		if !matchesOverviewFilters(e, spec.Filters) {
			continue
		}
		switch {
		case inCur:
			cur = append(cur, e)
		case !ts.Before(spec.PrevStart) && ts.Before(spec.Start):
			prev = append(prev, e)
		}
	}

	resp := newOverview(spec, statsOf(cur), statsOf(prev))

	buckets := map[int64][]types.AnalyticsEntry{}
	for _, e := range cur {
		k := e.Timestamp.UTC().Truncate(spec.Bucket).Unix()
		buckets[k] = append(buckets[k], e)
	}
	points := map[int64]overviewPoint{}
	for k, es := range buckets {
		var p overviewPoint
		lat := make([]float64, 0, len(es))
		for _, e := range es {
			switch outcomeOf(e) {
			case outcomeRateLimited:
				p.RateLimited++
			case outcomeUpstreamError:
				p.UpstreamError++
			case outcomeClientError:
				p.ClientError++
			default:
				p.OK++
			}
			lat = append(lat, ms(e.TotalLatency))
		}
		sort.Float64s(lat)
		p.P50Ms, p.P99Ms = quantile(lat, 0.5), quantile(lat, 0.99)
		points[k] = p
	}
	resp.Series = fillSeries(spec, points)

	resp.ByTier = rowsBy(cur, func(e types.AnalyticsEntry) string { return e.Tier })
	resp.ByService = rowsBy(cur, func(e types.AnalyticsEntry) string { return e.Service })
	resp.ByEdge = rowsBy(cur, func(e types.AnalyticsEntry) string { return e.EdgeID })

	recent := slices.Clone(cur)
	sort.Slice(recent, func(i, j int) bool { return recent[i].Timestamp.After(recent[j].Timestamp) })
	for _, e := range recent[:min(len(recent), 15)] {
		resp.Recent = append(resp.Recent, eventOf(e))
	}

	resp.Filters = overviewFilters{Services: sortedKeys(opts["service"]), Tiers: sortedKeys(opts["tier"]), Edges: sortedKeys(opts["edge"])}
	return resp
}

func statsOf(entries []types.AnalyticsEntry) windowStats {
	var w windowStats
	lat := make([]float64, 0, len(entries))
	var overhead []float64
	edges := map[string]bool{}
	for _, e := range entries {
		w.Requests++
		switch outcomeOf(e) {
		case outcomeRateLimited:
			w.RateLimited++
		case outcomeUpstreamError:
			w.UpstreamErrors++
		case outcomeClientError:
			w.ClientErrors++
		}
		if e.CacheHit {
			w.CacheHits++
		}
		if e.EdgeID != "" {
			edges[e.EdgeID] = true
		}
		lat = append(lat, ms(e.TotalLatency))
		if proxied(e) {
			overhead = append(overhead, ms(e.TotalLatency-e.UpstreamLatency))
		}
	}
	sort.Float64s(lat)
	sort.Float64s(overhead)
	w.P50Ms, w.P99Ms, w.OverheadP90Ms = quantile(lat, 0.5), quantile(lat, 0.99), quantile(overhead, 0.9)
	w.Edges = int64(len(edges))
	return w
}

// proxied reports whether a request reached the upstream, so its total minus
// upstream latency is the time the gateway itself added.
func proxied(e types.AnalyticsEntry) bool {
	return !e.CacheHit && e.ResponseCode != http.StatusTooManyRequests && e.UpstreamLatency > 0
}

func rowsBy(entries []types.AnalyticsEntry, key func(types.AnalyticsEntry) string) []overviewRow {
	type acc struct {
		row overviewRow
		lat []float64
	}
	groups := map[string]*acc{}
	for _, e := range entries {
		k := key(e)
		if k == "" {
			k = "(none)"
		}
		a := groups[k]
		if a == nil {
			a = &acc{row: overviewRow{Name: k}}
			groups[k] = a
		}
		a.row.Requests++
		switch outcomeOf(e) {
		case outcomeRateLimited:
			a.row.RateLimitedShare++
		case outcomeUpstreamError:
			a.row.ErrorShare++
		}
		if e.CacheHit {
			a.row.CacheHitRate++
		}
		a.lat = append(a.lat, ms(e.TotalLatency))
	}
	rows := make([]overviewRow, 0, len(groups))
	for _, a := range groups {
		sort.Float64s(a.lat)
		r := a.row
		n := float64(r.Requests)
		r.RateLimitedShare, r.ErrorShare, r.CacheHitRate = r.RateLimitedShare/n, r.ErrorShare/n, r.CacheHitRate/n
		r.P90Ms = quantile(a.lat, 0.9)
		rows = append(rows, r)
	}
	return finishRows(rows, int64(len(entries)))
}

// finishRows orders rows by traffic and fills in each one's share of it.
func finishRows(rows []overviewRow, total int64) []overviewRow {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Requests != rows[j].Requests {
			return rows[i].Requests > rows[j].Requests
		}
		return rows[i].Name < rows[j].Name
	})
	for i := range rows {
		rows[i].Share = ratio(float64(rows[i].Requests), float64(total))
	}
	return rows
}

func eventOf(e types.AnalyticsEntry) overviewEvent {
	return overviewEvent{
		Time: e.Timestamp.UTC(), Method: e.Method, Service: e.Service, Tier: e.Tier, Edge: e.EdgeID,
		Status: int(e.ResponseCode), LatencyMs: ms(e.TotalLatency), CacheHit: e.CacheHit,
	}
}

func matchesOverviewFilters(e types.AnalyticsEntry, f analyticsFilterSpec) bool {
	in := func(v string, set []string) bool { return len(set) == 0 || slices.Contains(set, v) }
	return in(e.Service, f.Services) && in(e.Tier, f.Tiers) && in(e.EdgeID, f.EdgeIDs)
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func ratio(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func finite(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		if k != "" {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
