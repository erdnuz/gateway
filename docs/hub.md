# Hub

The hub is the control plane. Run it with `gate hub`;
[internal/app/hubapp](../internal/app/hubapp/hubapp.go) wires it. It keeps
no state of its own (everything lives in Redis), so hubs can run as replicas
behind a gRPC load balancer. See [architecture](architecture.md#one-control-plane-channel).

## Edge-facing gRPC (`HUB_GRPC_PORT`, mTLS)

Defined in [api/proto/quota_lease.proto](../api/proto/quota_lease.proto):

| Service | Call | Purpose |
| --- | --- | --- |
| `QuotaLeaseService` | `RequestQuotaLease` | grant tokens from a key's quota for the current window, or answer WAITING |
| `ControlPlane` | `Watch` | stream a config snapshot, then config and tier changes |
| `ControlPlane` | `ResolveKey` | a key's tier, for an edge's tier-cache miss |

A change made through any hub is published on the Redis channel
`gate:control`, and every hub relays it to its own watchers
([controlplane.go](../packages/hub/controlplane.go)). Config changes from
another hub are validated and adopted, so all hubs serve the same config.

## Admin HTTP API (`PORT`)

Every endpoint except `/healthz` and `/readyz` needs
`Authorization: Bearer $HUB_AUTH_TOKEN`. Errors are RFC 9457 problem
responses.

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz`, `GET /readyz` | liveness, and readiness (Redis reachable, `noeviction`) |
| `GET /status` | edges connected to this hub |
| `GET /config` | the compiled config (analytics reads it) |
| `POST /config-reload` | reload from `CONFIG_FILE_PATH` (empty body) or apply a v2 document (body) |
| `POST /config-reload/dry-run` | validate and analyse a reload without applying it |
| `GET\|POST\|DELETE /tiers/{group}/{api_key}` | read, assign (`{"tier_id": "pro"}`) or revoke a key's tier |

A reload or tier change takes effect on every edge within one Redis
publish and one stream send. The load test's `propagation` scenario
measures it.

Dry-run responses report `mode` (`file` or `payload`), `valid`,
`analysis.changed_sections`, `analysis.invalidations.response_cache` (the
response-cache scopes edges will drop) and `analysis.warnings`.

## State

| Key | Holds |
| --- | --- |
| `lease-used:{group}:{service}:{key}:{window}` | tokens granted in a window; one Lua script grants atomically, and the key expires after two windows |
| `hub:tier:{group}:{key}` | a key's tier |
| channel `gate:control` | config and tier changes, shared between hubs |

Redis must use `maxmemory-policy noeviction`: an evicted ledger key would
re-grant a window's quota.
