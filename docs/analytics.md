# Analytics

Analytics ingests one event per sampled request from the edges (over NATS),
stores them in ClickHouse (or Redis for development), and serves a query API
and a live dashboard. Run it with `gate analytics`;
[internal/app/analyticsapp](../internal/app/analyticsapp/analyticsapp.go)
wires it. Consumers join a NATS queue group, so ingestion scales by adding
replicas.

## The dashboard

Open the analytics address: http://localhost:8091 with `make up`, or the
`analytics` URL that `gate dev` prints. The page refreshes every 5 seconds
and keeps the range and filters in the URL, so a view can be shared as a
link.

| Section | Shows |
| --- | --- |
| Filter bar | time range (15m, 1h, 6h, 24h, 7d) and service, tier and edge filters; every section below follows them |
| Key metrics | requests (and req/s), success rate (not 5xx), p99 latency (p50 alongside), gateway overhead p90 (time added beyond the upstream), share rate limited (429), cache hit rate. Each shows the change from the previous window of the same length, and a trend line where one applies |
| Scaling | requests/s across the edges next to the hub's lease and key-lookup calls, which are about 1% of requests: leasing keeps the control plane off the request path. It needs `PROMETHEUS_URL` and is hidden without it |
| Requests by outcome | stacked columns per interval: OK, rate limited, client error, upstream error. The legend toggles series, and there is a table view |
| Latency | p50 and p99 per interval |
| Rate limiting by tier | each tier's requests, with the part answered 429 |
| Load across edges | each edge's share of requests |
| Services | requests, errors, 429s, p90 and cache hits per service, sortable |
| Recent requests | the last 15 requests. Callers appear by tier only: API keys never reach analytics |

When `ANALYTICS_API_TOKEN` is set, the page asks for it once and keeps it in
the browser's local storage.

## API

Every `/analytics/*` endpoint except `features` needs
`Authorization: Bearer $ANALYTICS_API_TOKEN` when the token is set. Errors are
RFC 9457 problem responses.

| Endpoint | Purpose |
| --- | --- |
| `GET /analytics/overview?range=1h[&service=&tier=&edge_id=]` | everything the dashboard renders: KPIs with previous-window values, the per-interval series, rows by tier, service and edge, recent events, and filter options |
| `GET /analytics/scaling` | control-plane numbers read from Prometheus (cached for 5 seconds); `{"enabled": false}` without `PROMETHEUS_URL` |
| `GET /analytics/summary` | the general summary contract: metrics with trends, and series for latency, volume, rates and dimensions. Accepts the filters below |
| `GET /analytics/events` | raw events, paginated |
| `GET /analytics/features`, `/analytics/ingestion-metrics` | feature flags and ingest counters |
| `POST /analytics/clear` | delete all events (only with `ANALYTICS_ENABLE_TESTING_CLEAR=true`) |

`/analytics/summary` filters: `prefix`, `service`, `edge_id`, `tier`,
`method`, `response_code_min|max`, `cache_hit`, `upstream_error`,
`total_latency_ms_min|max`, `start` and `end`.

## Storage

With ClickHouse, the overview is computed in SQL: a few aggregate queries
per refresh, with quantiles in ClickHouse, so dashboard cost does not grow
with event volume. The table is partitioned by day, ordered by `(prefix,
service, timestamp)`, and expires rows after `ANALYTICS_RETENTION_DAYS` (30).
The Redis store is for development: the overview aggregates up to 200,000
recent events in process.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8091` | API and dashboard |
| `HUB_ADDR`, `HUB_AUTH_TOKEN` | required | runtime policy and readiness |
| `CLICKHOUSE_DSN` | none | e.g. `clickhouse://user:pass@host:9000/db` |
| `REDIS_ADDR` | none | Redis store when `CLICKHOUSE_DSN` is unset |
| `NATS_URL` | required | event stream |
| `PROMETHEUS_URL` | none | enables the dashboard's scaling section (`http://prometheus:9090` in `make up`) |
| `ANALYTICS_API_TOKEN` | none | protects the API |
| `ANALYTICS_RETENTION_DAYS` | `30` | ClickHouse TTL |
