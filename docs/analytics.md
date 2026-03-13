# Analytics Service

Analytics provides event ingestion, query APIs, and the embedded dashboard.

## Main Entry

- cmd/analytics/main.go

## Responsibilities

- Consume analytics events from NATS.
- Persist events in ClickHouse.
- Serve summary/event APIs and static dashboard assets.
- Compute trends using current and previous windows.

## Contract Shape

Summary endpoint follows strict contract:

- summary: metric map with last_value and trend
- series: latency, volume, rates, prefixes, services, edges

Each series row includes an ISO 8601 time field.

Latency summary metrics include only p90 variants.

## Filtering

Supported filters include:

- prefix, service, edge_id, tier, method
- response_code_min, response_code_max
- cache_hit, upstream_error
- total_latency_ms_min, total_latency_ms_max
- start, end

## Dashboard

Frontend assets under packages/analytics/static provide:

- summary cards with trend indicators
- latency, volume, rates charts
- dynamic traffic-by-prefix, traffic-by-service, and traffic-by-edge charts
- filter controls including edge_id

## Operational Endpoints

- /health
- /readyz
- /analytics/events
- /analytics/summary
- /analytics/features
- /analytics/ingestion-metrics
- /analytics/clear (deployment-gated)

## Key Analytics Packages

- packages/analytics/server.go
- packages/analytics/store.go
- packages/analytics/contract.go
- packages/analytics/static/index.html
- packages/analytics/static/app.js
