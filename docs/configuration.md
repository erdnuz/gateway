# Configuration

Configuration is split across environment variables (process wiring) and the gateway config file (routing, tiers, limits, policies).

## Layers

- Environment variables: service runtime wiring and infra connections.
- Gateway config JSON: routing, tiers, failure policies, caching, analytics settings.

Every component runs from the `gate` binary and is configured by environment
variables (`gate dev` needs none). Defaults are shown in parentheses.

## Hub (`gate hub`)

| Variable | Default | Purpose |
| --- | --- | --- |
| `CONFIG_FILE_PATH` | required | gateway config file |
| `HUB_AUTH_TOKEN` | required | bearer token for the hub HTTP API |
| `PORT` | `8080` | HTTP API |
| `HUB_GRPC_PORT` | `9090` | quota-lease gRPC service (mTLS) |
| `HUB_TLS_CERT_FILE`, `HUB_TLS_KEY_FILE`, `HUB_TLS_CA_FILE` | required | gRPC server cert and client CA |
| `HUB_GRPC_CLIENT_AUTH_MODE` | `require-and-verify` | `require-any` / `none` for development only |
| `REDIS_ADDR` | `localhost:6379` | ledger and tier store |
| `HUB_ENFORCE_REDIS_NOEVICTION` | `true` | refuse readiness unless Redis uses `noeviction` |
| `NATS_URL` (`NATS_TIER_URL`) | `nats://127.0.0.1:4222` | tier updates and config reload events |
| `NATS_TIER_UPDATES_SUBJECT` | `runtime.hub.tier_updates_subject` | |
| `HUB_CONFIG_RELOAD_SUBJECT` | `gate.config.reload` | NATS subject for reload events |
| `HUB_CONFIG_RELOAD_CHANNEL` | `gate:config:reload` | legacy Redis pub/sub channel (still published) |
| `HUB_EDGE_ALLOWED_ORIGINS` | none | CORS origins allowed to call the hub API |

## Edge (`gate edge`)

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8080` | gateway listener |
| `EDGE_ID` | `unknown-edge` | reported in analytics |
| `HUB_ADDR` | `http://localhost:8081` | hub HTTP API (config and tier lookups) |
| `HUB_AUTH_TOKEN` | none | hub bearer token |
| `HUB_GRPC_ADDR` | `localhost:9090` | lease service |
| `HUB_GRPC_SERVER_NAME` | required | must match a SAN in the hub certificate |
| `EDGE_TLS_CERT_FILE`, `EDGE_TLS_KEY_FILE`, `EDGE_TLS_CA_FILE` | required | client cert and hub CA |
| `REDIS_ADDR` | `localhost:6379` | tier and response caches |
| `EDGE_LEASE_STORE` | `memory` | `redis` only when several edge replicas share one Redis |
| `EDGE_LEASE_SIZE` | `runtime.edge.rate_lease_size` | default lease quantum; per-prefix `leasing.lease_quantum` wins |
| `EDGE_LEASE_LOW_WATER_PERCENT` | `runtime.edge.rate_low_water_pct` | renew when this share of a lease is left |
| `NATS_URL` (`NATS_TIER_URL`, `NATS_ANALYTICS_URL`) | `nats://127.0.0.1:4222` | |
| `NATS_TIER_UPDATES_SUBJECT`, `NATS_ANALYTICS_SUBJECT` | from runtime policy | |
| `GATE_ANALYTICS_ENABLED` | `true` | publish request events |
| `EDGE_CONFIG_RELOAD_SUBJECT` | `gate.config.reload` | |
| `EDGE_CONFIG_RELOAD_CHANNEL` | `gate:config:reload` | legacy; only works if hub and edge share Redis |
| `EDGE_CONFIG_REFRESH_SECONDS` | `0` (off) | periodic config poll |
| `EDGE_BOOTSTRAP_CONFIG_FILE` | none | config to use if the hub is unreachable at startup |
| `EDGE_ADMIN_TOKEN` | none | bearer token for `/analytics/metrics`; unset disables it |

## Analytics (`gate analytics`)

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8091` | API and dashboard |
| `HUB_ADDR`, `HUB_AUTH_TOKEN` | required | runtime policy and readiness |
| `CLICKHOUSE_DSN` | none | e.g. `clickhouse://user:pass@host:9000/db` |
| `ANALYTICS_CLICKHOUSE_TABLE` | `analytics_events` | |
| `REDIS_ADDR` | none | Redis-backed store when `CLICKHOUSE_DSN` is unset (development) |
| `NATS_URL` (`NATS_ANALYTICS_URL`) | required | |
| `NATS_ANALYTICS_SUBJECT`, `NATS_ANALYTICS_QUEUE` | from runtime policy | |
| `ANALYTICS_API_TOKEN` | none | protects the query API |

## Gateway Policy JSON

Examples: [examples/minimal.json](../examples/minimal.json), [examples/full.json](../examples/full.json); the compose stack uses [deploy/gateway.json](../deploy/gateway.json). Check a file with `gate config validate <file>`.

Key per-service sections:

- tiers
- analytics
- cache
- cors
- failure
- transform

Failure policy supports hub and upstream sub-policies, including fallback and retry behavior.

## Runtime Config Reload

Hub supports runtime reload in two forms:

- File reload: `POST /config-reload` with empty body loads from `CONFIG_FILE_PATH`.
- Payload reload: `POST /config-reload` with `GatewayConfig` JSON body.

Dry-run endpoint (no apply):

- `POST /config-reload/dry-run`
- Supports both file and payload modes using the same request body behavior.

Authentication:

- Reload endpoints require `Authorization: Bearer <HUB_AUTH_TOKEN>`.

Dry-run response includes:

- `mode` (`file` or `payload`)
- `valid`
- `analysis.changed_sections`
- `analysis.invalidations.response_cache`
- `analysis.warnings`
- `next_epoch`

Apply behavior:

- Hub validates the next config.
- Hub applies atomically.
- Hub publishes a structured reload event on NATS subject `HUB_CONFIG_RELOAD_SUBJECT` (default `gate.config.reload`) and, for single-Redis setups, on Redis channel `HUB_CONFIG_RELOAD_CHANNEL`.
- Edge subscribes on NATS (and Redis) and refreshes config on receipt. Events are deduplicated by `(boot_id, epoch)`, so a hub restart (epoch back to 1) is still applied.

Current invalidation scope:

- Response cache invalidation only (scoped by prefix/service in event payload).

Current lease/rate behavior:

- Existing lease/rate state is not force-cleared during reload.
- New enforcement behavior is picked up on subsequent lease renewals and request evaluation.

## Recommended Practices

- Keep secrets in environment files or secret managers, not committed JSON.
- Keep config validation enabled and fail fast on invalid policy.
- Use distinct EDGE_ID values for each edge instance.
- Use `POST /config-reload/dry-run` before apply in production workflows.
