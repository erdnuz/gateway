# Gate: Distributed API Governance Layer

`gate` is a Go API governance platform with three runtime services:

- `edge`: data-plane gateway (auth, tier lookup, local rate limiting, cache, proxy, analytics capture)
- `hub`: control-plane authority (config, tier assignments, authoritative rate counters)
- `analytics`: read API + embedded UI for analytics events persisted in Redis

## Current Architecture

### Runtime Binaries

- `cmd/edge/main.go`
- `cmd/hub/main.go`
- `cmd/analytics/main.go`

### Messaging and Storage

- NATS subjects:
  - `tier.updates` (hub -> edge tier cache updates)
  - `analytics.events` (edge -> analytics event stream)
- Redis:
  - edge local counters and tier/cache entries
  - hub authoritative counters and persistent user tier assignments (`hub:tier:*`)
  - analytics event list (default key `rate-analytics`)

## Request Paths

### Edge pipeline

Request shape: `/{prefix}/{service}/...`

Middleware order in `packages/edge/server.go`:

1. `analyticsMiddleware`
2. `corsMiddleware`
3. `authMiddleware`
4. `cacheMiddleware`
5. `rateLimitMiddleware`
6. `proxyHandler`

Key behavior:

- Requires `X-API-Key`
- Tier lookup first checks Redis cache, then hub
- Hub outage behavior follows policy: `fail-closed`, `default-tier`, or `stale-or-default`
- Upstream retries/fallback behavior is controlled by `failure.upstream.*`
- Cache applies to GET only, includes API key in cache key generation

### Hub API

- `GET /health` (public)
- `GET /config` (auth)
- `GET|POST /rate/{prefix}/{api_key}` (auth)
- `GET|POST|PUT|DELETE /tiers/{prefix}/{api_key}` (auth)

All routes except `/health` require:

- `Authorization: Bearer <HUB_AUTH_TOKEN>`

### Analytics API

- `GET /health`
- `GET /analytics/events`
- `GET /analytics/summary`
- `GET /` and `/assets/*` serve embedded frontend

If `ANALYTICS_API_TOKEN` is set, analytics endpoints require:

- `Authorization: Bearer <ANALYTICS_API_TOKEN>`

## Configuration

Two config layers are active:

1. Service runtime environment variables (`cmd/*/main.go`)
2. Gateway policy JSON (`cmd/config.json`, served by hub)

### Edge env vars (selected)

- `PORT` (default `:8080`)
- `REDIS_ADDR` (default `localhost:6379`)
- `HUB_ADDR` (default `http://localhost:8081`)
- `HUB_AUTH_TOKEN`
- `HUB_HTTP_TIMEOUT_SECONDS` (default `5`)
- `EDGE_BOOTSTRAP_CONFIG_FILE` (optional startup fallback)
- `EDGE_CONFIG_REFRESH_SECONDS` (default `0`, disabled)
- `EDGE_MAX_DELTA` (default `100`)
- `RATE_QUEUE_CAPACITY`, `RATE_QUEUE_WORKERS`, `RATE_QUEUE_SUBMIT_TIMEOUT`
- `RATE_QUEUE_RETRY_MAX`, `RATE_QUEUE_RETRY_BACKOFF`
- `RATE_HARD_THRESHOLD_PERCENT` (default `90`)
- `ANALYTICS_ENABLED` (default `true`)
- `ANALYTICS_BUFFER_SIZE` (default `1000`)
- `NATS_URL`
- `NATS_TIER_UPDATES_SUBJECT`
- `NATS_EDGE_QUEUE`
- `NATS_ANALYTICS_SUBJECT`

### Hub env vars (selected)

- `PORT` (default `8080`)
- `REDIS_ADDR` (default `localhost:6379`)
- `HUB_AUTH_TOKEN` (required)
- `CONFIG_FILE_PATH` (default `/cmd/config.json`)
- `HUB_ENFORCE_REDIS_NOEVICTION` (default `true`)
- `MAX_DELTA` (default `10000`)
- `HUB_QUEUE_WORKERS`, `HUB_QUEUE_SUBMIT_TIMEOUT`
- `HUB_QUEUE_RETRY_MAX`, `HUB_QUEUE_RETRY_BACKOFF`
- `NATS_URL`
- `NATS_TIER_UPDATES_SUBJECT`

### Analytics env vars

- `PORT` (default `:8091`)
- `REDIS_ADDR` (default `localhost:6379`)
- `ANALYTICS_REDIS_KEY` (default `rate-analytics`)
- `ANALYTICS_API_TOKEN` (optional but recommended)
- `NATS_URL`
- `NATS_ANALYTICS_SUBJECT`
- `NATS_ANALYTICS_QUEUE`

### Policy config (`cmd/config.json`)

Per service policy supports:

- `tiers`
- `transform`
- `cors`
- `analytics`
- `cache`
- `failure`

`failure` supports both legacy fields and explicit policies:

- Legacy: `fail_open`, `fallback_tier`
- Hub policy: `hub.tier_lookup_strategy`, `hub.default_tier`, `hub.stale_tier_max_age`, `hub.allow_on_rate_service_error`
- Upstream policy: `upstream.mode`, `upstream.max_retries`, `upstream.retry_backoff`, `upstream.retry_on_statuses`, `upstream.retry_non_idempotent_methods`, `upstream.attempt_timeout`, `upstream.fallback_status_code`, `upstream.fallback_body`, `upstream.fallback_headers`

## Local Run

### Full stack (with analytics service)

From repo root:

```bash
docker compose -f deployments/docker-compose.yaml up --build
```

Required environment variables for compose:

- `HUB_AUTH_TOKEN`
- `ANALYTICS_API_TOKEN`

### Lite stack (no analytics deployment)

`lite` mode keeps edge + hub + NATS + Redis and explicitly disables edge analytics capture. The analytics API/UI service is not deployed.

```bash
docker compose -f deployments/docker-compose.lite.yaml up --build
```

Required environment variables for lite compose:

- `HUB_AUTH_TOKEN`

## Hub Config Validation

Hub now validates the full gateway config before loading it into the in-memory source of truth.

Validation includes:

- required structure checks (prefixes, services, tiers)
- duplicate detection for prefixes, services, and tiers
- `target_url` must be valid `http` or `https`
- policy checks for tier lookup strategy and upstream mode
- retry status code range checks (`100-599`)
- analytics sampling range checks (`0..1`)

Invalid configs are rejected at hub startup with a descriptive error.

## Tests

```bash
go test ./...
```

### Integration Tests

The advanced end-to-end suite runs under an `integration` build tag and boots a distributed topology with real infra:

- one Hub instance (control plane + lease gRPC)
- multiple Edge instances (simulating separate machines)
- multiple Analytics API instances (simulating horizontal replicas)
- containerized Redis/NATS/Mongo via testcontainers
- upstream HTTP origin + mTLS lease gRPC

```bash
go test -tags=integration ./testing/integration/... -count=1
```

Or via Makefile targets:

```bash
make integration-test
make integration-test-verbose
```

## Setup CLI

Use the interactive setup wizard to generate service-specific `.env` files with validation and connectivity checks.

```bash
go run ./cmd/setup
```

The wizard asks which service you are configuring (`analytics`, `hub`, or `edge`) and supports:

- simple mode with strong defaults (recommended)
- advanced mode for extra knobs
- optional dependency bootstrap via Docker Compose before configuration checks

For Hub setup, the wizard now prompts for route/service details and generates a validated gateway config JSON automatically (no manual config file required).

Then it:

- validates required values (ports, URLs, files, config JSON)
- checks connectivity to Redis/NATS and Hub health where relevant
- writes an output env file (default `.env.<service>`)

## Repository Layout

```text
cmd/
  edge/        edge runtime
  hub/         hub runtime
  analytics/   analytics runtime
packages/
  edge/        edge middleware, proxy, cache, rate sync
  hub/         hub API, config, tiers, authoritative rates
  analytics/   analytics query API + embedded UI
  common/      shared env parser, types, worker queue
testing/       test helpers and simulation scripts
deployments/   Dockerfiles and compose
```