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

Every endpoint except `/healthz` and `/readyz` needs a bearer token:
`HUB_AUTH_TOKEN` may do anything, and the optional `HUB_READ_TOKEN` may only
read (GET). Errors are RFC 9457 problem responses. The full contract is
[api/openapi/hub.yaml](../api/openapi/hub.yaml), and `gate ctl` wraps it:
`keys create|list|set-tier|rotate|revoke`, `status`, `audit` and
`config diff|apply`.

Keys are stored by SHA-256 hash only: `gk_<id>_<secret>` is shown once when
it is issued or rotated, and edges hash every key on arrival, so the hub, its
Redis and the edge-hub protocol never see a usable key.

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz`, `GET /readyz` | liveness, and readiness (Redis reachable, `noeviction`) |
| `GET /status` | edges connected to this hub |
| `GET /config` | the compiled config (analytics reads it) |
| `POST /config-reload` | reload from `CONFIG_FILE_PATH` (empty body) or apply a v2 document (body) |
| `POST /config-reload/dry-run` | validate and analyse a reload without applying it |
| `POST /v2/keys`, `GET /v2/keys[/{id}]` | issue a key (the secret is returned once) and list keys |
| `PATCH /v2/keys/{id}`, `POST /v2/keys/{id}/rotate`, `DELETE /v2/keys/{id}` | change tier or labels, replace the secret, revoke |
| `GET /v2/audit` | recent administrative changes (Redis stream `hub:audit`, shared by hubs) |
| `GET\|POST\|DELETE /tiers/{group}/{api_key}` | assign (`{"tier_id": "pro"}`), read or revoke the tier of an operator-chosen key |

A reload or tier change takes effect on every edge within one Redis
publish and one stream send. The load test's `propagation` scenario
measures it.

Dry-run responses report `mode` (`file` or `payload`), `valid`,
`analysis.changed_sections`, `analysis.invalidations.response_cache` (the
response-cache scopes edges will drop) and `analysis.warnings`.

## State

| Key | Holds |
| --- | --- |
| `lease-used:{group}:{service}:{sha256(key)}:{window}` | tokens granted in a window; one Lua script grants atomically, and the key expires after two windows |
| `hub:tier:{group}:{sha256(key)}` | a key's tier |
| `hub:key:{id}`, set `hub:keys` | issued keys' metadata (owner, name, tier, hash) |
| stream `hub:audit` | administrative changes, capped at about 10,000 entries |
| channel `gate:control` | config and tier changes, shared between hubs |

Redis must use `maxmemory-policy noeviction`: an evicted ledger key would
re-grant a window's quota.
