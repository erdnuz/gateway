# Multi-host deployment

Each component runs from the same image (`gate:<version>`, built from
[deploy/Dockerfile](../deploy/Dockerfile)) and is configured by environment
variables only. [deploy/compose.yaml](../deploy/compose.yaml) is the
reference wiring; split its services across hosts as below.

## Hub host

Runs `gate hub` with its Redis. Run several hubs against the same Redis for
availability: edges reach any of them through a gRPC load balancer (or DNS
with several A records). Host NATS here too if you run analytics.

| Variable | Example | Notes |
| --- | --- | --- |
| `CONFIG_FILE_PATH` | `/etc/gate/gateway.yaml` | required |
| `HUB_AUTH_TOKEN` | strong random value | required; admin API, shared with analytics |
| `REDIS_ADDR` | `redis:6379` | must use `maxmemory-policy noeviction` |
| `HUB_TLS_CERT_FILE`, `HUB_TLS_KEY_FILE`, `HUB_TLS_CA_FILE` | `/certs/hub/tls.crt` … | gRPC for edges, mTLS |
| `PORT`, `HUB_GRPC_PORT` | `8080`, `9090` | edges need only the gRPC port |

Generate certificates with the hub's public DNS name as a SAN:

```bash
deploy/gen-dev-certs.sh ./certs hub.internal.example.com
```

Copy `certs/ca/ca.crt` and `certs/edge/*` to every edge host. Use a real CA
for anything beyond a lab.

## Edge hosts (one or more)

Run `gate edge`. Edges need nothing but the hub's gRPC port (and NATS for
analytics); they keep caches in memory, so add as many as traffic needs.

| Variable | Example |
| --- | --- |
| `HUB_GRPC_ADDR` | `hub.internal.example.com:9090` |
| `HUB_GRPC_SERVER_NAME` | `hub.internal.example.com` (must match a hub cert SAN) |
| `EDGE_TLS_CERT_FILE`, `EDGE_TLS_KEY_FILE`, `EDGE_TLS_CA_FILE` | `/certs/edge/tls.crt` … |
| `NATS_URL` | `nats://hub.internal.example.com:4222` (analytics only) |
| `EDGE_ID` | unique per edge (`edge-eu-1`) |
| `REDIS_ADDR` | optional: a Redis for responses larger than `EDGE_CACHE_BYTES` |

Put edges behind your load balancer and use `/readyz` for its health checks.

## Analytics host

Run `gate analytics` next to ClickHouse.

| Variable | Example |
| --- | --- |
| `HUB_ADDR`, `HUB_AUTH_TOKEN` | `http://hub.internal.example.com:8080`, the hub token |
| `CLICKHOUSE_DSN` | `clickhouse://gate:secret@localhost:9000/gate` |
| `NATS_URL` | `nats://hub.internal.example.com:4222` |
| `ANALYTICS_API_TOKEN` | protects the query API |

## Order

Start the hub first and wait for `/readyz`. Then start edges and analytics in
any order: both wait for the hub on their own.
