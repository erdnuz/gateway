package analyticsapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"gateway/packages/common/types"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/redis/go-redis/v9"
)

const (
	defaultClickHouseTable    = "analytics_events"
	defaultBootstrapLoadLimit = int64(100000)
)

type analyticsStore interface {
	Ping(ctx context.Context) error
	InsertEntries(ctx context.Context, batch []types.AnalyticsEntry) error
	ListEntries(ctx context.Context, limit, offset int64) ([]types.AnalyticsEntry, error)
	LoadRecentEntries(ctx context.Context, start, end time.Time, limit int64) ([]types.AnalyticsEntry, error)
	Clear(ctx context.Context) error
	Close() error
}

type redisAnalyticsStore struct {
	rdb *redis.Client
	key string
}

func newRedisAnalyticsStore(rdb *redis.Client, key string) analyticsStore {
	if rdb == nil {
		return nil
	}
	return &redisAnalyticsStore{rdb: rdb, key: key}
}

func NewClickHouseAnalyticsStore(dsn, table string) (analyticsStore, error) {
	return newClickHouseAnalyticsStore(dsn, table)
}

func (s *redisAnalyticsStore) Ping(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("analytics redis store is not configured")
	}
	return s.rdb.Ping(ctx).Err()
}

func (s *redisAnalyticsStore) InsertEntries(ctx context.Context, batch []types.AnalyticsEntry) error {
	pipe := s.rdb.Pipeline()
	for _, entry := range batch {
		payload, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		pipe.RPush(ctx, s.key, payload)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *redisAnalyticsStore) ListEntries(ctx context.Context, limit, offset int64) ([]types.AnalyticsEntry, error) {
	if limit <= 0 {
		return []types.AnalyticsEntry{}, nil
	}
	length, err := s.rdb.LLen(ctx, s.key).Result()
	if err != nil {
		return nil, err
	}
	if offset >= length {
		return []types.AnalyticsEntry{}, nil
	}
	end := length - 1 - offset
	start := end - limit + 1
	if start < 0 {
		start = 0
	}
	vals, err := s.rdb.LRange(ctx, s.key, start, end).Result()
	if err != nil {
		return nil, err
	}
	return parseEntries(vals), nil
}

func (s *redisAnalyticsStore) LoadRecentEntries(ctx context.Context, start, end time.Time, limit int64) ([]types.AnalyticsEntry, error) {
	if limit <= 0 {
		limit = defaultBootstrapLoadLimit
	}
	entries, err := s.ListEntries(ctx, limit, 0)
	if err != nil {
		return nil, err
	}
	out := make([]types.AnalyticsEntry, 0, len(entries))
	for _, entry := range entries {
		ts := entry.Timestamp.UTC()
		if !start.IsZero() && ts.Before(start.UTC()) {
			continue
		}
		if !end.IsZero() && ts.After(end.UTC()) {
			continue
		}
		out = append(out, entry)
	}
	return out, nil
}

func (s *redisAnalyticsStore) Clear(ctx context.Context) error {
	return s.rdb.Del(ctx, s.key).Err()
}

func (s *redisAnalyticsStore) Close() error { return nil }

type clickHouseAnalyticsStore struct {
	db    *sql.DB
	table string
}

func newClickHouseAnalyticsStore(dsn, table string) (*clickHouseAnalyticsStore, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("clickhouse dsn is required")
	}
	if strings.TrimSpace(table) == "" {
		table = defaultClickHouseTable
	}
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	store := &clickHouseAnalyticsStore{db: db, table: table}
	if err := store.ensureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *clickHouseAnalyticsStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *clickHouseAnalyticsStore) InsertEntries(ctx context.Context, batch []types.AnalyticsEntry) error {
	if len(batch) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (timestamp, prefix, service, edge_id, method, tier, total_latency_ns, upstream_latency_ns, cache_hit, limit_used, limit_used_of_total, request_size_bytes, response_size_bytes, response_code, upstream_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", s.table))
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, entry := range batch {
		_, err = stmt.ExecContext(
			ctx,
			entry.Timestamp.UTC(),
			entry.Prefix,
			entry.Service,
			entry.EdgeID,
			entry.Method,
			entry.Tier,
			entry.TotalLatency.Nanoseconds(),
			entry.UpstreamLatency.Nanoseconds(),
			boolToUInt8(entry.CacheHit),
			entry.LimitUsed,
			entry.LimitUsedOfTotal,
			entry.RequestSize,
			entry.ResponseSize,
			entry.ResponseCode,
			boolToUInt8(entry.UpstreamError),
		)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *clickHouseAnalyticsStore) ListEntries(ctx context.Context, limit, offset int64) ([]types.AnalyticsEntry, error) {
	if limit <= 0 {
		return []types.AnalyticsEntry{}, nil
	}
	query := fmt.Sprintf("SELECT timestamp, prefix, service, edge_id, method, tier, total_latency_ns, upstream_latency_ns, cache_hit, limit_used, limit_used_of_total, request_size_bytes, response_size_bytes, response_code, upstream_error FROM %s ORDER BY timestamp DESC LIMIT ? OFFSET ?", s.table)
	rows, err := s.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAnalyticsEntries(rows)
}

func (s *clickHouseAnalyticsStore) LoadRecentEntries(ctx context.Context, start, end time.Time, limit int64) ([]types.AnalyticsEntry, error) {
	if limit <= 0 {
		limit = defaultBootstrapLoadLimit
	}
	query := fmt.Sprintf("SELECT timestamp, prefix, service, edge_id, method, tier, total_latency_ns, upstream_latency_ns, cache_hit, limit_used, limit_used_of_total, request_size_bytes, response_size_bytes, response_code, upstream_error FROM %s WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC LIMIT ?", s.table)
	rows, err := s.db.QueryContext(ctx, query, start.UTC(), end.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAnalyticsEntries(rows)
}

func (s *clickHouseAnalyticsStore) Clear(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", s.table))
	return err
}

func (s *clickHouseAnalyticsStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *clickHouseAnalyticsStore) ensureSchema(ctx context.Context) error {
	ddl := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			timestamp DateTime64(9, 'UTC'),
			prefix String,
			service String,
			edge_id String,
			method String,
			tier String,
			total_latency_ns Int64,
			upstream_latency_ns Int64,
			cache_hit UInt8,
			limit_used UInt64,
			limit_used_of_total Float64,
			request_size_bytes UInt64,
			response_size_bytes UInt64,
			response_code UInt16,
			upstream_error UInt8
		) ENGINE = MergeTree()
		ORDER BY (timestamp, edge_id, prefix, service)
	`, s.table)
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS edge_id String AFTER service", s.table))
	return err
}

type contractWindowAggregate struct {
	Requests           float64
	LatencyTotalP50    float64
	LatencyTotalP90    float64
	LatencyTotalP95    float64
	LatencyUpstreamP50 float64
	LatencyUpstreamP90 float64
	LatencyUpstreamP95 float64
	VolumeRequestAvg   float64
	VolumeResponseAvg  float64
	RatesCacheHit      float64
	RatesUpstreamErr   float64
	RatesRateLimited   float64
}

type contractBucketAggregate struct {
	SampleCount        int64
	Bucket             time.Time
	LatencyTotalP50    float64
	LatencyTotalP90    float64
	LatencyTotalP95    float64
	LatencyUpstreamP50 float64
	LatencyUpstreamP90 float64
	LatencyUpstreamP95 float64
	VolumeRequestAvg   float64
	VolumeResponseAvg  float64
	RatesCacheHit      float64
	RatesUpstreamErr   float64
	RatesRateLimited   float64
}

func (s *clickHouseAnalyticsStore) QueryContractSummary(ctx context.Context, spec analyticsSummaryQuerySpec) (contractSummaryResponse, error) {
	resp := newContractSummaryResponse()

	current, err := s.queryWindowAggregate(ctx, spec.WindowStart, spec.WindowEnd, spec.Filters)
	if err != nil {
		return resp, err
	}
	previous, err := s.queryWindowAggregate(ctx, spec.PreviousStart, spec.PreviousEnd, spec.Filters)
	if err != nil {
		return resp, err
	}

	buckets, err := s.queryBucketAggregates(ctx, spec)
	if err != nil {
		return resp, err
	}
	bucketTimes := enumerateBuckets(spec.WindowStart, spec.WindowEnd, spec.BucketSize)
	latestBucket, previousBucket, hasBucketPair := lastTwoSampledBuckets(buckets, bucketTimes)

	prefixCurrent, err := s.queryWindowCountsByDimension(ctx, spec.WindowStart, spec.WindowEnd, spec.Filters, "prefix")
	if err != nil {
		return resp, err
	}
	prefixPrevious, err := s.queryWindowCountsByDimension(ctx, spec.PreviousStart, spec.PreviousEnd, spec.Filters, "prefix")
	if err != nil {
		return resp, err
	}
	serviceCurrent, err := s.queryWindowCountsByDimension(ctx, spec.WindowStart, spec.WindowEnd, spec.Filters, "service")
	if err != nil {
		return resp, err
	}
	servicePrevious, err := s.queryWindowCountsByDimension(ctx, spec.PreviousStart, spec.PreviousEnd, spec.Filters, "service")
	if err != nil {
		return resp, err
	}
	edgeCurrent, err := s.queryWindowCountsByDimension(ctx, spec.WindowStart, spec.WindowEnd, spec.Filters, "edge_id")
	if err != nil {
		return resp, err
	}
	edgePrevious, err := s.queryWindowCountsByDimension(ctx, spec.PreviousStart, spec.PreviousEnd, spec.Filters, "edge_id")
	if err != nil {
		return resp, err
	}
	tierCurrent, err := s.queryWindowCountsByDimension(ctx, spec.WindowStart, spec.WindowEnd, spec.Filters, "tier")
	if err != nil {
		return resp, err
	}
	tierPrevious, err := s.queryWindowCountsByDimension(ctx, spec.PreviousStart, spec.PreviousEnd, spec.Filters, "tier")
	if err != nil {
		return resp, err
	}

	prefixSeries, err := s.queryBucketCountsByDimension(ctx, spec, "prefix")
	if err != nil {
		return resp, err
	}
	serviceSeries, err := s.queryBucketCountsByDimension(ctx, spec, "service")
	if err != nil {
		return resp, err
	}
	edgeSeries, err := s.queryBucketCountsByDimension(ctx, spec, "edge_id")
	if err != nil {
		return resp, err
	}
	tierSeries, err := s.queryBucketCountsByDimension(ctx, spec, "tier")
	if err != nil {
		return resp, err
	}

	resp.Summary[summaryKeyRequests] = contractMetric{LastValue: current.Requests, Trend: trendDeltaWithRecentFallback(current.Requests, previous.Requests, hasBucketPair, float64(latestBucket.SampleCount), float64(previousBucket.SampleCount))}
	resp.Summary[summaryKeyLatencyTotalP90] = contractMetric{LastValue: current.LatencyTotalP90, Trend: trendDeltaWithRecentFallback(current.LatencyTotalP90, previous.LatencyTotalP90, hasBucketPair, latestBucket.LatencyTotalP90, previousBucket.LatencyTotalP90)}
	resp.Summary[summaryKeyLatencyAddedP90] = contractMetric{LastValue: current.LatencyTotalP90 - current.LatencyUpstreamP90, Trend: trendDeltaWithRecentFallback(current.LatencyTotalP90-current.LatencyUpstreamP90, previous.LatencyTotalP90-previous.LatencyUpstreamP90, hasBucketPair, latestBucket.LatencyTotalP90-latestBucket.LatencyUpstreamP90, previousBucket.LatencyTotalP90-previousBucket.LatencyUpstreamP90)}
	resp.Summary[summaryKeyVolumeRequestAvg] = contractMetric{LastValue: current.VolumeRequestAvg, Trend: trendDeltaWithRecentFallback(current.VolumeRequestAvg, previous.VolumeRequestAvg, hasBucketPair, latestBucket.VolumeRequestAvg, previousBucket.VolumeRequestAvg)}
	resp.Summary[summaryKeyVolumeResponseAvg] = contractMetric{LastValue: current.VolumeResponseAvg, Trend: trendDeltaWithRecentFallback(current.VolumeResponseAvg, previous.VolumeResponseAvg, hasBucketPair, latestBucket.VolumeResponseAvg, previousBucket.VolumeResponseAvg)}
	resp.Summary[summaryKeyRatesCacheHit] = contractMetric{LastValue: current.RatesCacheHit, Trend: trendDeltaWithRecentFallback(current.RatesCacheHit, previous.RatesCacheHit, hasBucketPair, latestBucket.RatesCacheHit, previousBucket.RatesCacheHit)}
	resp.Summary[summaryKeyRatesUpstreamErr] = contractMetric{LastValue: current.RatesUpstreamErr, Trend: trendDeltaWithRecentFallback(current.RatesUpstreamErr, previous.RatesUpstreamErr, hasBucketPair, latestBucket.RatesUpstreamErr, previousBucket.RatesUpstreamErr)}
	resp.Summary[summaryKeyRatesRateLimited] = contractMetric{LastValue: current.RatesRateLimited, Trend: trendDeltaWithRecentFallback(current.RatesRateLimited, previous.RatesRateLimited, hasBucketPair, latestBucket.RatesRateLimited, previousBucket.RatesRateLimited)}

	for key, currentCount := range prefixCurrent {
		prev := prefixPrevious[key]
		resp.Summary["prefixes_"+key] = contractMetric{LastValue: currentCount, Trend: trendDelta(currentCount, prev)}
	}
	for key, currentCount := range serviceCurrent {
		prev := servicePrevious[key]
		resp.Summary["services_"+key] = contractMetric{LastValue: currentCount, Trend: trendDelta(currentCount, prev)}
	}
	for key, currentCount := range edgeCurrent {
		prev := edgePrevious[key]
		resp.Summary["edges_"+key] = contractMetric{LastValue: currentCount, Trend: trendDelta(currentCount, prev)}
	}
	for key, currentCount := range tierCurrent {
		prev := tierPrevious[key]
		resp.Summary["tiers_"+key] = contractMetric{LastValue: currentCount, Trend: trendDelta(currentCount, prev)}
	}

	for _, bucketTime := range bucketTimes {
		bucket, ok := buckets[bucketTime.Unix()]
		if !ok {
			resp.Series.Latency = append(resp.Series.Latency, contractLatencyPoint{Time: bucketTime.UTC().Format(time.RFC3339), SampleCount: 0})
			resp.Series.Volume = append(resp.Series.Volume, contractVolumePoint{Time: bucketTime.UTC().Format(time.RFC3339), SampleCount: 0})
			resp.Series.Rates = append(resp.Series.Rates, contractRatesPoint{Time: bucketTime.UTC().Format(time.RFC3339), SampleCount: 0})
		} else {
			resp.Series.Latency = append(resp.Series.Latency, contractLatencyPoint{
				SampleCount:        bucket.SampleCount,
				Time:               bucketTime.UTC().Format(time.RFC3339),
				LatencyTotalP90:    bucket.LatencyTotalP90,
				LatencyUpstreamP90: bucket.LatencyUpstreamP90,
				LatencyAddedP90:    bucket.LatencyTotalP90 - bucket.LatencyUpstreamP90,
			})
			resp.Series.Volume = append(resp.Series.Volume, contractVolumePoint{SampleCount: bucket.SampleCount, Time: bucketTime.UTC().Format(time.RFC3339), RequestAvg: bucket.VolumeRequestAvg, ResponseAvg: bucket.VolumeResponseAvg})
			resp.Series.Rates = append(resp.Series.Rates, contractRatesPoint{SampleCount: bucket.SampleCount, Time: bucketTime.UTC().Format(time.RFC3339), CacheHit: bucket.RatesCacheHit, UpstreamErr: bucket.RatesUpstreamErr, RateLimited: bucket.RatesRateLimited})
		}

		prefixRow := map[string]interface{}{"time": bucketTime.UTC().Format(time.RFC3339)}
		if values, ok := prefixSeries[bucketTime.Unix()]; ok {
			for name, count := range values {
				prefixRow[name] = count
			}
		}
		resp.Series.Prefixes = append(resp.Series.Prefixes, prefixRow)

		serviceRow := map[string]interface{}{"time": bucketTime.UTC().Format(time.RFC3339)}
		if values, ok := serviceSeries[bucketTime.Unix()]; ok {
			for name, count := range values {
				serviceRow[name] = count
			}
		}
		resp.Series.Services = append(resp.Series.Services, serviceRow)

		edgeRow := map[string]interface{}{"time": bucketTime.UTC().Format(time.RFC3339)}
		if values, ok := edgeSeries[bucketTime.Unix()]; ok {
			for name, count := range values {
				edgeRow[name] = count
			}
		}
		resp.Series.Edges = append(resp.Series.Edges, edgeRow)

		tierRow := map[string]interface{}{"time": bucketTime.UTC().Format(time.RFC3339)}
		if values, ok := tierSeries[bucketTime.Unix()]; ok {
			for name, count := range values {
				tierRow[name] = count
			}
		}
		resp.Series.Tiers = append(resp.Series.Tiers, tierRow)
	}

	return resp, nil
}

func (s *clickHouseAnalyticsStore) queryWindowAggregate(ctx context.Context, start, end time.Time, filters analyticsFilterSpec) (contractWindowAggregate, error) {
	args := make([]interface{}, 0, 16)
	whereClause := buildClickHouseWhere(start, end, filters, &args)
	query := fmt.Sprintf(`
		SELECT
			toFloat64(count()) AS requests,
			if(count() = 0, 0.0, quantileTDigest(0.50)(toFloat64(total_latency_ns)) / 1000000.0) AS latency_total_p50,
			if(count() = 0, 0.0, quantileTDigest(0.90)(toFloat64(total_latency_ns)) / 1000000.0) AS latency_total_p90,
			if(count() = 0, 0.0, quantileTDigest(0.95)(toFloat64(total_latency_ns)) / 1000000.0) AS latency_total_p95,
			if(countIf(cache_hit = 0 AND response_code != 429) = 0, 0.0, quantileTDigestIf(0.50)(toFloat64(upstream_latency_ns), cache_hit = 0 AND response_code != 429) / 1000000.0) AS latency_upstream_p50,
			if(countIf(cache_hit = 0 AND response_code != 429) = 0, 0.0, quantileTDigestIf(0.90)(toFloat64(upstream_latency_ns), cache_hit = 0 AND response_code != 429) / 1000000.0) AS latency_upstream_p90,
			if(countIf(cache_hit = 0 AND response_code != 429) = 0, 0.0, quantileTDigestIf(0.95)(toFloat64(upstream_latency_ns), cache_hit = 0 AND response_code != 429) / 1000000.0) AS latency_upstream_p95,
			if(count() = 0, 0.0, avg(toFloat64(request_size_bytes))) AS volume_request_avg,
			if(count() = 0, 0.0, avg(toFloat64(response_size_bytes))) AS volume_response_avg,
			if(count() = 0, 0.0, avg(toFloat64(cache_hit))) AS rates_cache_hit,
			if(count() = 0, 0.0, avg(toFloat64(upstream_error))) AS rates_upstream_err,
			if(count() = 0, 0.0, avg(if(response_code = 429, 1.0, 0.0))) AS rates_rate_limited
		FROM %s
		WHERE %s
	`, s.table, whereClause)
	var out contractWindowAggregate
	err := s.db.QueryRowContext(ctx, query, args...).Scan(
		&out.Requests,
		&out.LatencyTotalP50,
		&out.LatencyTotalP90,
		&out.LatencyTotalP95,
		&out.LatencyUpstreamP50,
		&out.LatencyUpstreamP90,
		&out.LatencyUpstreamP95,
		&out.VolumeRequestAvg,
		&out.VolumeResponseAvg,
		&out.RatesCacheHit,
		&out.RatesUpstreamErr,
		&out.RatesRateLimited,
	)
	return out, err
}

func (s *clickHouseAnalyticsStore) queryBucketAggregates(ctx context.Context, spec analyticsSummaryQuerySpec) (map[int64]contractBucketAggregate, error) {
	args := make([]interface{}, 0, 16)
	whereClause := buildClickHouseWhere(spec.WindowStart, spec.WindowEnd, spec.Filters, &args)
	query := fmt.Sprintf(`
		SELECT
			toStartOfInterval(timestamp, toIntervalSecond(?)) AS bucket_time,
			count() AS sample_count,
			if(count() = 0, 0.0, quantileTDigest(0.50)(toFloat64(total_latency_ns)) / 1000000.0) AS latency_total_p50,
			if(count() = 0, 0.0, quantileTDigest(0.90)(toFloat64(total_latency_ns)) / 1000000.0) AS latency_total_p90,
			if(count() = 0, 0.0, quantileTDigest(0.95)(toFloat64(total_latency_ns)) / 1000000.0) AS latency_total_p95,
			if(countIf(cache_hit = 0 AND response_code != 429) = 0, 0.0, quantileTDigestIf(0.50)(toFloat64(upstream_latency_ns), cache_hit = 0 AND response_code != 429) / 1000000.0) AS latency_upstream_p50,
			if(countIf(cache_hit = 0 AND response_code != 429) = 0, 0.0, quantileTDigestIf(0.90)(toFloat64(upstream_latency_ns), cache_hit = 0 AND response_code != 429) / 1000000.0) AS latency_upstream_p90,
			if(countIf(cache_hit = 0 AND response_code != 429) = 0, 0.0, quantileTDigestIf(0.95)(toFloat64(upstream_latency_ns), cache_hit = 0 AND response_code != 429) / 1000000.0) AS latency_upstream_p95,
			if(count() = 0, 0.0, avg(toFloat64(request_size_bytes))) AS volume_request_avg,
			if(count() = 0, 0.0, avg(toFloat64(response_size_bytes))) AS volume_response_avg,
			if(count() = 0, 0.0, avg(toFloat64(cache_hit))) AS rates_cache_hit,
			if(count() = 0, 0.0, avg(toFloat64(upstream_error))) AS rates_upstream_err,
			if(count() = 0, 0.0, avg(if(response_code = 429, 1.0, 0.0))) AS rates_rate_limited
		FROM %s
		WHERE %s
		GROUP BY bucket_time
		ORDER BY bucket_time ASC
	`, s.table, whereClause)
	queryArgs := make([]interface{}, 0, len(args)+1)
	queryArgs = append(queryArgs, int64(spec.BucketSize.Seconds()))
	queryArgs = append(queryArgs, args...)
	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]contractBucketAggregate{}
	for rows.Next() {
		var row contractBucketAggregate
		if err := rows.Scan(
			&row.Bucket,
			&row.SampleCount,
			&row.LatencyTotalP50,
			&row.LatencyTotalP90,
			&row.LatencyTotalP95,
			&row.LatencyUpstreamP50,
			&row.LatencyUpstreamP90,
			&row.LatencyUpstreamP95,
			&row.VolumeRequestAvg,
			&row.VolumeResponseAvg,
			&row.RatesCacheHit,
			&row.RatesUpstreamErr,
			&row.RatesRateLimited,
		); err != nil {
			return nil, err
		}
		out[row.Bucket.UTC().Unix()] = row
	}
	return out, rows.Err()
}

func (s *clickHouseAnalyticsStore) queryWindowCountsByDimension(ctx context.Context, start, end time.Time, filters analyticsFilterSpec, dimension string) (map[string]float64, error) {
	args := make([]interface{}, 0, 16)
	whereClause := buildClickHouseWhere(start, end, filters, &args)
	query := fmt.Sprintf(`
		SELECT %s, toFloat64(count()) AS c
		FROM %s
		WHERE %s
		GROUP BY %s
		ORDER BY c DESC
	`, dimension, s.table, whereClause, dimension)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var name string
		var c float64
		if err := rows.Scan(&name, &c); err != nil {
			return nil, err
		}
		if strings.TrimSpace(name) == "" {
			continue
		}
		out[name] = c
	}
	return out, rows.Err()
}

func (s *clickHouseAnalyticsStore) queryBucketCountsByDimension(ctx context.Context, spec analyticsSummaryQuerySpec, dimension string) (map[int64]map[string]float64, error) {
	args := make([]interface{}, 0, 16)
	whereClause := buildClickHouseWhere(spec.WindowStart, spec.WindowEnd, spec.Filters, &args)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(timestamp, toIntervalSecond(?)) AS bucket_time, %s, toFloat64(count()) AS c
		FROM %s
		WHERE %s
		GROUP BY bucket_time, %s
		ORDER BY bucket_time ASC, c DESC
	`, dimension, s.table, whereClause, dimension)
	queryArgs := make([]interface{}, 0, len(args)+1)
	queryArgs = append(queryArgs, int64(spec.BucketSize.Seconds()))
	queryArgs = append(queryArgs, args...)
	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]map[string]float64{}
	for rows.Next() {
		var bucket time.Time
		var name string
		var c float64
		if err := rows.Scan(&bucket, &name, &c); err != nil {
			return nil, err
		}
		bucketKey := bucket.UTC().Unix()
		if out[bucketKey] == nil {
			out[bucketKey] = map[string]float64{}
		}
		if strings.TrimSpace(name) != "" {
			out[bucketKey][name] = c
		}
	}
	return out, rows.Err()
}

func buildClickHouseWhere(start, end time.Time, filters analyticsFilterSpec, args *[]interface{}) string {
	clauses := []string{"timestamp >= ?", "timestamp < ?"}
	*args = append(*args, start.UTC(), end.UTC())

	appendInClause(&clauses, args, "prefix", filters.Prefixes)
	appendInClause(&clauses, args, "service", filters.Services)
	appendInClause(&clauses, args, "edge_id", filters.EdgeIDs)
	appendInClause(&clauses, args, "tier", filters.Tiers)
	appendInClause(&clauses, args, "method", filters.Methods)

	if filters.ResponseCodeMin != nil {
		clauses = append(clauses, "response_code >= ?")
		*args = append(*args, *filters.ResponseCodeMin)
	}
	if filters.ResponseCodeMax != nil {
		clauses = append(clauses, "response_code <= ?")
		*args = append(*args, *filters.ResponseCodeMax)
	}
	if filters.CacheHit != nil {
		clauses = append(clauses, "cache_hit = ?")
		*args = append(*args, boolToUInt8(*filters.CacheHit))
	}
	if filters.UpstreamError != nil {
		clauses = append(clauses, "upstream_error = ?")
		*args = append(*args, boolToUInt8(*filters.UpstreamError))
	}
	if filters.LatencyMinMs != nil {
		clauses = append(clauses, "toFloat64(total_latency_ns) >= ?")
		*args = append(*args, *filters.LatencyMinMs*1_000_000)
	}
	if filters.LatencyMaxMs != nil {
		clauses = append(clauses, "toFloat64(total_latency_ns) <= ?")
		*args = append(*args, *filters.LatencyMaxMs*1_000_000)
	}

	return strings.Join(clauses, " AND ")
}

func appendInClause(clauses *[]string, args *[]interface{}, column string, values []string) {
	if len(values) == 0 {
		return
	}
	clean := make([]string, 0, len(values))
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v != "" {
			clean = append(clean, v)
		}
	}
	if len(clean) == 0 {
		return
	}
	placeholders := make([]string, len(clean))
	for i, value := range clean {
		placeholders[i] = "?"
		*args = append(*args, value)
	}
	*clauses = append(*clauses, fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ",")))
}

func enumerateBuckets(start, end time.Time, interval time.Duration) []time.Time {
	if interval <= 0 || !end.After(start) {
		return []time.Time{}
	}
	out := make([]time.Time, 0, int(end.Sub(start)/interval)+1)
	cursor := start.UTC().Truncate(interval)
	if cursor.Before(start.UTC()) {
		cursor = cursor.Add(interval)
	}
	for cursor.Before(end.UTC()) {
		out = append(out, cursor)
		cursor = cursor.Add(interval)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

func trendDelta(current, previous float64) float64 {
	if previous == 0 {
		return 0
	}
	return (current - previous) / previous
}

func trendDeltaWithRecentFallback(currentWindow, previousWindow float64, hasBucketPair bool, latestBucketValue, previousBucketValue float64) float64 {
	trend := trendDelta(currentWindow, previousWindow)
	if trend != 0 || previousWindow != 0 {
		return trend
	}
	if !hasBucketPair {
		return trend
	}
	return trendDelta(latestBucketValue, previousBucketValue)
}

func lastTwoSampledBuckets(buckets map[int64]contractBucketAggregate, ordered []time.Time) (contractBucketAggregate, contractBucketAggregate, bool) {
	var latest contractBucketAggregate
	var previous contractBucketAggregate
	count := 0
	for i := len(ordered) - 1; i >= 0; i-- {
		bucket, ok := buckets[ordered[i].Unix()]
		if !ok || bucket.SampleCount <= 0 {
			continue
		}
		if count == 0 {
			latest = bucket
			count++
			continue
		}
		previous = bucket
		return latest, previous, true
	}
	return contractBucketAggregate{}, contractBucketAggregate{}, false
}

func scanAnalyticsEntries(rows *sql.Rows) ([]types.AnalyticsEntry, error) {
	entries := make([]types.AnalyticsEntry, 0)
	for rows.Next() {
		var entry types.AnalyticsEntry
		var totalLatencyNs int64
		var upstreamLatencyNs int64
		var cacheHit uint8
		var upstreamError uint8
		if err := rows.Scan(
			&entry.Timestamp,
			&entry.Prefix,
			&entry.Service,
			&entry.EdgeID,
			&entry.Method,
			&entry.Tier,
			&totalLatencyNs,
			&upstreamLatencyNs,
			&cacheHit,
			&entry.LimitUsed,
			&entry.LimitUsedOfTotal,
			&entry.RequestSize,
			&entry.ResponseSize,
			&entry.ResponseCode,
			&upstreamError,
		); err != nil {
			return nil, err
		}
		entry.TotalLatency = time.Duration(totalLatencyNs)
		entry.UpstreamLatency = time.Duration(upstreamLatencyNs)
		entry.CacheHit = cacheHit == 1
		entry.UpstreamError = upstreamError == 1
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func boolToUInt8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}
