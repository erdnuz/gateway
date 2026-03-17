# Hub Service

Hub is the control-plane authority.

## Main Entry

- cmd/hub/main.go

## Responsibilities

- Load, validate, and serve gateway config.
- Manage tier assignments per prefix and API key.
- Provide authoritative quota/lease coordination.
- Publish tier update events to edge instances.

## APIs

Common endpoints include:

- `GET /healthz`
- `GET /readyz`
- `GET /config`
- `POST|PUT /config-reload`
- `POST|PUT /config-reload/dry-run`
- `GET|POST /rate/{prefix}/{api_key}`
- `GET|POST|PUT|DELETE /tiers/{prefix}/{api_key}`

Most endpoints require Authorization: Bearer <HUB_AUTH_TOKEN>.

`/healthz` and `/readyz` do not require auth.

## Runtime Config Updates

Hub supports runtime config updates in two modes:

- File mode: send an empty body to `POST /config-reload` (or `/config-reload/dry-run`) and Hub loads from `CONFIG_FILE_PATH`.
- Payload mode: send a JSON `GatewayConfig` body to `POST /config-reload` (or `/config-reload/dry-run`).

Dry-run validates and analyzes the update without applying it.

Example dry-run response fields:

- `mode`: `file` or `payload`
- `valid`: validation result
- `analysis.changed_sections`
- `analysis.invalidations.response_cache`
- `analysis.warnings`
- `next_epoch`

Apply endpoint behavior:

- Validates config before apply.
- Applies atomically in Hub.
- Publishes a structured reload event to the config reload channel.
- Returns `204 No Content` on success.

## Config Reload Event

Hub publishes a `ConfigReloadEvent` on Redis channel `HUB_CONFIG_RELOAD_CHANNEL` (default: `gate:config:reload`).

Event includes:

- `version`
- `epoch` (monotonic)
- `reloaded_at_utc`
- `invalidations.response_cache` scopes

Edge subscribers use this event to refresh config and execute scoped response-cache invalidation.

## Config Validation

Hub validates policy at startup, including:

- prefix/service/tier structure
- duplicate detection
- target URL validity
- policy and retry range checks

## Messaging and State

- Stores authoritative state in Redis.
- Publishes tier update invalidation events via NATS.
- Publishes config reload events via Redis Pub/Sub.
- Coordinates with edge lease clients over mTLS gRPC where configured.

## Key Hub Packages

- packages/hub/server.go
- packages/hub/config.go
- packages/hub/tiers.go
- packages/hub/rates.go
- packages/hub/quota_lease_server.go
