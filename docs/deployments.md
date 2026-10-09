# Deployments

Every component ships in one image built from [deploy/Dockerfile](../deploy/Dockerfile):
`gate hub`, `gate edge`, `gate analytics`, `gate echo`. The same file's
`tools` target builds the load generator.

## Local stack

[deploy/compose.yaml](../deploy/compose.yaml) runs the full topology on one machine:

| Service | Role | Profile |
| --- | --- | --- |
| `certs` | generates a throwaway CA and hub/edge certificates on first start | default |
| `redis` | hub ledger, tier store and hub-to-hub change channel | default |
| `hub` | control plane; HTTP API on :8081, gRPC (mTLS) for edges inside the network | default |
| `edge` | gateway on :8080; caches in memory | default |
| `echo` | demo upstream | default |
| `nats`, `clickhouse`, `analytics` | event bus, event store, API and dashboard on :8091 | `analytics` |
| `traffic` | seeds demo keys and sends steady traffic | `demo` |

```bash
make up      # default + analytics
make demo    # default + analytics + demo traffic
make logs    # follow logs (make logs ARGS=edge for one service)
make down    # stop; make clean also deletes volumes
```

Startup order is enforced by health checks: certs → Redis → hub `/readyz`
→ edge. The edge holds proxy traffic (503) until the hub has streamed it a
config. After that a hub outage is handled per request by the failure
policy, and an analytics outage never blocks traffic.

To run the stack with your own gateway config, set
`GATE_CONFIG=/path/to/gateway.yaml` before `make up`. Change the hub token
with `GATE_HUB_TOKEN`.

## Certificates

Private keys are never committed. `deploy/gen-dev-certs.sh [out_dir] [hub_sans]`
writes a CA, a hub certificate (`serverAuth`, with the given DNS SANs) and an
edge certificate (`clientAuth`). On Windows/Git Bash it runs itself in an
Alpine container. The compose stack runs it automatically; `make certs`
writes to `deploy/certs/`.

## Load testing

`make loadtest` brings up a production-shaped stack (hub and its Redis, two
edges, a minimal upstream) from
[testing/loadtest/compose.yaml](../testing/loadtest/compose.yaml) and runs the
rate-limiter scenarios. See [testing/loadtest/README.md](../testing/loadtest/README.md).

## Health and readiness

| Component | Liveness | Readiness |
| --- | --- | --- |
| Edge | `/healthz` | `/readyz`: config received from the hub (and a Redis response store reachable, if used) |
| Hub | `/healthz` | `/readyz`: Redis reachable, with `noeviction` |
| Analytics | `/healthz` | `/readyz`: hub, store and NATS reachable |

Running components on separate machines: [deploy-multi-host.md](deploy-multi-host.md).
