package analyticsapi

import (
	"context"
	"fmt"
	"time"
)

// SQL fragments shared by the overview queries. Aggregation stays in
// ClickHouse, so the dashboard costs the same whatever the event volume.
const (
	sqlRateLimited = "countIf(response_code = 429)"
	sqlUpstreamErr = "countIf(response_code != 429 AND (upstream_error = 1 OR response_code >= 500))"
	sqlClientErr   = "countIf(response_code >= 400 AND response_code < 500 AND response_code != 429 AND upstream_error = 0)"
	sqlProxied     = "cache_hit = 0 AND response_code != 429 AND upstream_latency_ns > 0"
)

func sqlQuantileMs(q float64) string {
	return fmt.Sprintf("if(count() = 0, 0.0, quantileTDigest(%g)(toFloat64(total_latency_ns)) / 1e6)", q)
}

// queryOverview builds the dashboard response with a handful of aggregate
// queries.
func (s *clickHouseAnalyticsStore) queryOverview(ctx context.Context, spec overviewSpec) (overviewResponse, error) {
	cur, err := s.overviewWindow(ctx, spec.Start, spec.End, spec.Filters)
	if err != nil {
		return overviewResponse{}, err
	}
	prev, err := s.overviewWindow(ctx, spec.PrevStart, spec.Start, spec.Filters)
	if err != nil {
		return overviewResponse{}, err
	}
	resp := newOverview(spec, cur, prev)

	if resp.Series, err = s.overviewSeries(ctx, spec); err != nil {
		return overviewResponse{}, err
	}
	for dim, dst := range map[string]*[]overviewRow{"tier": &resp.ByTier, "service": &resp.ByService, "edge_id": &resp.ByEdge} {
		if *dst, err = s.overviewRows(ctx, spec, dim, cur.Requests); err != nil {
			return overviewResponse{}, err
		}
	}
	if resp.Recent, err = s.overviewRecent(ctx, spec); err != nil {
		return overviewResponse{}, err
	}
	// Dropdown options ignore the active filters, so choosing one does not
	// hide the others.
	unfiltered := analyticsFilterSpec{}
	for dim, dst := range map[string]*[]string{"service": &resp.Filters.Services, "tier": &resp.Filters.Tiers, "edge_id": &resp.Filters.Edges} {
		counts, err := s.queryWindowCountsByDimension(ctx, spec.Start, spec.End, unfiltered, dim)
		if err != nil {
			return overviewResponse{}, err
		}
		set := map[string]bool{}
		for name := range counts {
			set[name] = true
		}
		*dst = sortedKeys(set)
	}
	return resp, nil
}

func (s *clickHouseAnalyticsStore) overviewWindow(ctx context.Context, start, end time.Time, f analyticsFilterSpec) (windowStats, error) {
	var args []any
	where := buildClickHouseWhere(start, end, f, &args)
	q := fmt.Sprintf(`
		SELECT count(), %s, %s, %s, countIf(cache_hit = 1),
			%s, %s,
			if(countIf(%s) = 0, 0.0, quantileTDigestIf(0.9)(toFloat64(total_latency_ns - upstream_latency_ns), %s) / 1e6),
			uniqExact(edge_id)
		FROM %s WHERE %s`,
		sqlRateLimited, sqlUpstreamErr, sqlClientErr, sqlQuantileMs(0.5), sqlQuantileMs(0.99),
		sqlProxied, sqlProxied, s.table, where)
	var w windowStats
	var requests, limited, upstream, client, hits, edges uint64
	err := s.db.QueryRowContext(ctx, q, args...).Scan(&requests, &limited, &upstream, &client, &hits, &w.P50Ms, &w.P99Ms, &w.OverheadP90Ms, &edges)
	w.Requests, w.RateLimited, w.UpstreamErrors, w.ClientErrors, w.CacheHits, w.Edges =
		int64(requests), int64(limited), int64(upstream), int64(client), int64(hits), int64(edges)
	w.P50Ms, w.P99Ms, w.OverheadP90Ms = finite(w.P50Ms), finite(w.P99Ms), finite(w.OverheadP90Ms)
	return w, err
}

func (s *clickHouseAnalyticsStore) overviewSeries(ctx context.Context, spec overviewSpec) ([]overviewPoint, error) {
	args := []any{int64(spec.Bucket.Seconds())}
	where := buildClickHouseWhere(spec.Start, spec.End, spec.Filters, &args)
	q := fmt.Sprintf(`
		SELECT toStartOfInterval(timestamp, toIntervalSecond(?)) AS bucket,
			count() - %s - %s - %s, %s, %s, %s, %s, %s
		FROM %s WHERE %s
		GROUP BY bucket ORDER BY bucket`,
		sqlRateLimited, sqlUpstreamErr, sqlClientErr, sqlRateLimited, sqlClientErr, sqlUpstreamErr,
		sqlQuantileMs(0.5), sqlQuantileMs(0.99), s.table, where)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := map[int64]overviewPoint{}
	for rows.Next() {
		var t time.Time
		var ok, limited, client, upstream uint64
		var p overviewPoint
		if err := rows.Scan(&t, &ok, &limited, &client, &upstream, &p.P50Ms, &p.P99Ms); err != nil {
			return nil, err
		}
		p.OK, p.RateLimited, p.ClientError, p.UpstreamError = int64(ok), int64(limited), int64(client), int64(upstream)
		p.P50Ms, p.P99Ms = finite(p.P50Ms), finite(p.P99Ms)
		points[t.UTC().Unix()] = p
	}
	return fillSeries(spec, points), rows.Err()
}

func (s *clickHouseAnalyticsStore) overviewRows(ctx context.Context, spec overviewSpec, dim string, total int64) ([]overviewRow, error) {
	var args []any
	where := buildClickHouseWhere(spec.Start, spec.End, spec.Filters, &args)
	q := fmt.Sprintf(`
		SELECT if(%[1]s = '', '(none)', %[1]s) AS name, count(), %[2]s, %[3]s, countIf(cache_hit = 1), %[4]s
		FROM %[5]s WHERE %[6]s
		GROUP BY name ORDER BY count() DESC LIMIT 50`,
		dim, sqlRateLimited, sqlUpstreamErr, sqlQuantileMs(0.9), s.table, where)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []overviewRow{}
	for rows.Next() {
		var r overviewRow
		var n, limited, upstream, hits uint64
		if err := rows.Scan(&r.Name, &n, &limited, &upstream, &hits, &r.P90Ms); err != nil {
			return nil, err
		}
		r.Requests = int64(n)
		r.RateLimitedShare = ratio(float64(limited), float64(n))
		r.ErrorShare = ratio(float64(upstream), float64(n))
		r.CacheHitRate = ratio(float64(hits), float64(n))
		r.P90Ms = finite(r.P90Ms)
		out = append(out, r)
	}
	return finishRows(out, total), rows.Err()
}

func (s *clickHouseAnalyticsStore) overviewRecent(ctx context.Context, spec overviewSpec) ([]overviewEvent, error) {
	var args []any
	where := buildClickHouseWhere(spec.Start, spec.End, spec.Filters, &args)
	q := fmt.Sprintf(`
		SELECT timestamp, method, service, tier, edge_id, response_code, total_latency_ns, cache_hit
		FROM %s WHERE %s ORDER BY timestamp DESC LIMIT 15`, s.table, where)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []overviewEvent{}
	for rows.Next() {
		var e overviewEvent
		var status uint16
		var latencyNs int64
		var hit uint8
		if err := rows.Scan(&e.Time, &e.Method, &e.Service, &e.Tier, &e.Edge, &status, &latencyNs, &hit); err != nil {
			return nil, err
		}
		e.Status, e.LatencyMs, e.CacheHit = int(status), float64(latencyNs)/1e6, hit == 1
		e.Time = e.Time.UTC()
		out = append(out, e)
	}
	return out, rows.Err()
}
