# Configuration

Configuration is split across environment variables and gateway policy JSON.

## Layers

- Environment variables: service runtime wiring and infra connections.
- Gateway config JSON: routing, tiers, failure policies, caching, analytics settings.

## Edge (selected env vars)

- PORT
- REDIS_ADDR
- HUB_ADDR
- HUB_AUTH_TOKEN
- HUB_HTTP_TIMEOUT_SECONDS
- EDGE_CONFIG_RELOAD_CHANNEL
- EDGE_BOOTSTRAP_CONFIG_FILE
- EDGE_CONFIG_REFRESH_SECONDS
- EDGE_ID
- EDGE_MAX_DELTA
- EDGE_LEASE_SIZE
- EDGE_LEASE_LOW_WATER_PERCENT
- RATE_HARD_THRESHOLD_PERCENT
- NATS_URL
- NATS_TIER_URL
- NATS_ANALYTICS_URL
- NATS_TIER_UPDATES_SUBJECT
- NATS_ANALYTICS_SUBJECT
- EDGE_TLS_CERT_FILE
- EDGE_TLS_KEY_FILE
- EDGE_TLS_CA_FILE
- HUB_GRPC_ADDR
- HUB_GRPC_SERVER_NAME

## Hub (selected env vars)

- PORT
- REDIS_ADDR
- HUB_AUTH_TOKEN
- CONFIG_FILE_PATH
- HUB_CONFIG_RELOAD_CHANNEL
- NATS_URL
- NATS_TIER_URL
- NATS_TIER_UPDATES_SUBJECT
- HUB_TLS_CERT_FILE
- HUB_TLS_KEY_FILE
- HUB_TLS_CA_FILE
- HUB_GRPC_ADDR

## Analytics (selected env vars)

- PORT
- ANALYTICS_API_TOKEN
- CLICKHOUSE_DSN
- ANALYTICS_CLICKHOUSE_TABLE
- NATS_URL
- NATS_ANALYTICS_URL
- NATS_ANALYTICS_SUBJECT
- NATS_ANALYTICS_QUEUE
- ANALYTICS_ENABLE_TESTING_CLEAR

## Gateway Policy JSON

Main file in this repo: cmd/config.json

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
- Hub publishes a structured reload event to Redis Pub/Sub channel `HUB_CONFIG_RELOAD_CHANNEL` (default `gate:config:reload`).
- Edge subscribes via `EDGE_CONFIG_RELOAD_CHANNEL` and refreshes config on receipt.

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
