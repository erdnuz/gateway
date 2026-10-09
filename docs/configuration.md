# Configuration

Configuration is split across environment variables (process wiring) and the gateway config file (routing, tiers, limits, policies).

## Layers

- Environment variables: service runtime wiring and infra connections.
- Gateway config file (YAML or JSON): routing, tiers, failure policies, caching, analytics settings.

Every component runs from the `gate` binary and is configured by environment
variables (`gate dev` needs none). Defaults are shown in parentheses.

All components also read `METRICS_ADDR` (`:9100`, Prometheus `/metrics`;
empty disables it), `LOG_FORMAT` (`json`; `text` for `gate dev`), `LOG_LEVEL`
(`info`) and the standard `OTEL_EXPORTER_OTLP_*` variables. Tracing is on
only when an OTLP endpoint is set. See [observability](observability.md).

## Hub (`gate hub`)

| Variable | Default | Purpose |
| --- | --- | --- |
| `CONFIG_FILE_PATH` | required | gateway config file |
| `HUB_AUTH_TOKEN` | required | admin bearer token for the HTTP API |
| `HUB_READ_TOKEN` | none | optional bearer token that may only read (GET) |
| `PORT` | `8080` | admin HTTP API |
| `HUB_GRPC_PORT` | `9090` | edge-facing gRPC: quota leases and the control-plane stream |
| `HUB_TLS_CERT_FILE`, `HUB_TLS_KEY_FILE`, `HUB_TLS_CA_FILE` | required unless gRPC listens on loopback | gRPC server cert and client CA (mTLS) |
| `HUB_GRPC_CLIENT_AUTH_MODE` | `require-and-verify` | `require-any` / `none` for development only |
| `REDIS_ADDR` | `localhost:6379` | quota ledger, tier store, and the channel hubs share changes on |
| `HUB_ENFORCE_REDIS_NOEVICTION` | `true` | refuse readiness unless Redis uses `noeviction` |
| `HUB_EDGE_ALLOWED_ORIGINS` | none | CORS origins allowed to call the hub API |

Redis is the hub's only dependency. Several hubs can share one Redis behind
a gRPC load balancer: each change is published on a Redis channel that every
hub relays to the edges connected to it.

## Edge (`gate edge`)

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8080` | gateway listener |
| `EDGE_ID` | `unknown-edge` | reported in analytics and hub logs |
| `HUB_GRPC_ADDR` | `localhost:9090` | the hub: leases, key lookups, config and tier changes |
| `HUB_GRPC_SERVER_NAME` | required with TLS | must match a SAN in the hub certificate |
| `EDGE_TLS_CERT_FILE`, `EDGE_TLS_KEY_FILE`, `EDGE_TLS_CA_FILE` | required unless the hub is on loopback | client cert and hub CA |
| `EDGE_CACHE_BYTES` | `67108864` (64 MiB) | in-memory response cache budget |
| `REDIS_ADDR` | none | keep cached responses in this Redis instead of memory |
| `EDGE_LEASE_SIZE` | `runtime.edge.rate_lease_size` | default lease quantum; per-service `limits.lease_size` wins |
| `EDGE_LEASE_LOW_WATER_PERCENT` | `runtime.edge.rate_low_water_pct` | renew when this share of a lease is left |
| `GATE_ANALYTICS_ENABLED` | `true` | publish request events |
| `NATS_URL` | `nats://127.0.0.1:4222` | analytics events only |
| `NATS_ANALYTICS_SUBJECT` | `runtime.analytics.nats_subject` | |
| `EDGE_BOOTSTRAP_CONFIG_FILE` | none | serve this config until the hub sends one |
| `EDGE_ADMIN_TOKEN` | none | bearer token for `/analytics/metrics`; unset disables it |

## Analytics (`gate analytics`)

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8091` | API and dashboard |
| `HUB_ADDR`, `HUB_AUTH_TOKEN` | required | runtime policy and readiness |
| `CLICKHOUSE_DSN` | none | e.g. `clickhouse://user:pass@host:9000/db` |
| `ANALYTICS_CLICKHOUSE_TABLE` | `analytics_events` | |
| `ANALYTICS_RETENTION_DAYS` | `30` | ClickHouse TTL |
| `PROMETHEUS_URL` | none | enables the dashboard's scaling section |
| `REDIS_ADDR` | none | Redis-backed store when `CLICKHOUSE_DSN` is unset (development) |
| `NATS_URL` (`NATS_ANALYTICS_URL`) | required | |
| `NATS_ANALYTICS_SUBJECT`, `NATS_ANALYTICS_QUEUE` | from runtime policy | |
| `ANALYTICS_API_TOKEN` | none | protects the query API |

## Gateway config file

The gateway config is a YAML (or JSON) file, format version 2. The hub loads
it from `CONFIG_FILE_PATH` and serves the compiled result to edges.

```yaml
version: 2
services:
  - name: orders
    match: { path_prefix: /v1/orders }
    upstream: http://orders:8080
    limits:
      window: 1m
      tiers:
        free: { requests: 60 }
        pro:  { requests: 6000, cost: { POST: 2 } }
      when_hub_unavailable: { allow: 10 }
    cache: { ttl: 15s }
```

- **Examples:** [basic.yaml](../examples/basic.yaml), [multi-service.yaml](../examples/multi-service.yaml), [advanced.yaml](../examples/advanced.yaml) (replicated upstreams, JWT and anonymous callers, concurrency limits), and [full.yaml](../examples/full.yaml), which shows every option with its default.
- **Durations** are strings with units: `500ms`, `30s`, `5m`, `1h`.
- **Defaults:** every field under `runtime:` is optional, as is everything in a service beyond `name`, `match`, `upstream` and `limits`.
- **Editor support:** [schema/gate.schema.json](../schema/gate.schema.json) is a JSON Schema generated from the Go types. Files that start with `# yaml-language-server: $schema=...` get completion and inline docs in VS Code (YAML extension) and JetBrains IDEs.
- **Current routing constraint:** `match.path_prefix` is `/<group>/<name>`. Services in the same group share one quota window and lease size, and an API key's tier is assigned per group.

### Commands

```bash
gate config validate gateway.yaml   # every problem, with line numbers
gate config migrate old.json        # version 1 JSON -> version 2 YAML on stdout
gate config schema                  # print the JSON Schema
```

`validate` reports all problems in one pass:

```text
gate: gateway.yaml: 3 problem(s)
    line 5: services[0].upstrem: unknown field "upstrem" (did you mean "upstream"?)
    line 7: duration 60 has no unit; write e.g. "60s" or "60ms"
    line 8: services[0].limits.default_tier: "gold" is not one of the tiers (free)
```

### Migrating from version 1

Version 1 files (JSON with `prefixes` and nanosecond durations) are rejected
with a pointer to `gate config migrate`. Migration keeps only values that
differ from defaults. Mapping notes:

| Version 1 | Version 2 |
| --- | --- |
| `prefixes[].prefix` + `services[].service_id` | `match.path_prefix: /<prefix>/<service_id>`, `name` |
| `quota_period`, `leasing.lease_quantum` | `limits.window`, `limits.lease_size` |
| `tiers[]` with `quota`, `*_cost` | `limits.tiers.<id>.requests`, `cost` (only non-1 costs) |
| `safety_tier` + `failure.hub.allow_on_rate_service_error` | `limits.when_hub_unavailable.allow` |
| `failure.hub.default_tier` | `limits.default_tier` |
| `transform` | `strip_path_prefix`, `headers` |
| `failure.upstream` | `retries`, `on_upstream_error` |
| cache key `$method:$path` | `{method}:{path}` |

## Runtime Config Reload

Hub supports runtime reload in two forms:

- File reload: `POST /config-reload` with empty body loads from `CONFIG_FILE_PATH`.
- Payload reload: `POST /config-reload` with a version 2 config document (YAML or JSON) as the body.

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

Apply behavior:

- Hub validates the next config and applies it atomically.
- Hub publishes the change on its Redis control channel; every hub adopts it
  and pushes a config snapshot down each connected edge's watch stream.
- Edges swap the config in atomically and drop the response-cache scopes the
  change invalidated. An edge that reconnects always starts from a fresh
  snapshot, so changes made while it was disconnected are never lost.

Current invalidation scope:

- Response cache invalidation only (scoped by prefix/service in event payload).

Current lease/rate behavior:

- Existing lease/rate state is not force-cleared during reload.
- New enforcement behavior is picked up on subsequent lease renewals and request evaluation.

## Recommended Practices

- Keep secrets in environment variables or a secret manager, not in the config file.
- Keep config validation enabled and fail fast on invalid policy.
- Use distinct EDGE_ID values for each edge instance.
- Use `POST /config-reload/dry-run` before apply in production workflows.
