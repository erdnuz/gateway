# Deployments

Every component ships in one image built from [deploy/Dockerfile](../deploy/Dockerfile):
`gate hub`, `gate edge`, `gate analytics`, `gate echo`. The same file's
`tools` target builds the load generator.

## Local stack

[deploy/compose.yaml](../deploy/compose.yaml) runs the full topology on one machine:

| Service | Role | Profile |
| --- | --- | --- |
| `certs` | generates a throwaway CA and hub/edge certificates on first start | default |
| `redis`, `nats` | hub ledger and tier store; control-plane and analytics bus | default |
| `hub` | control plane; HTTP API on :8081, lease gRPC (mTLS) inside the network | default |
| `edge-redis`, `edge` | gateway on :8080 with its own cache | default |
| `echo` | demo upstream | default |
| `clickhouse`, `analytics` | event store, API and dashboard on :8091 | `analytics` |
| `traffic` | seeds demo keys and sends steady traffic | `demo` |

```bash
make up      # default + analytics
make demo    # default + analytics + demo traffic
make logs    # follow logs (make logs ARGS=edge for one service)
make down    # stop; make clean also deletes volumes
```

Startup order is enforced by health checks: certs → Redis/NATS → hub `/readyz`
→ edge. The edge holds proxy traffic (503) until the hub is ready once. After
that, edge `/readyz` tracks only edge-local dependencies: a hub outage is
handled per request by the failure policy, and an analytics outage never
blocks traffic.

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

`make loadtest` brings up a production-shaped stack (hub, two edges with
private Redis, NATS, a minimal upstream) from
[testing/loadtest/compose.yaml](../testing/loadtest/compose.yaml) and runs the
rate-limiter scenarios. See [testing/loadtest/README.md](../testing/loadtest/README.md).

## Health and readiness

| Component | Liveness | Readiness |
| --- | --- | --- |
| Edge | `/healthz` | `/readyz`: hub seen ready once, edge Redis reachable |
| Hub | `/healthz` | `/readyz`: Redis (with `noeviction`) and NATS reachable |
| Analytics | `/healthz` | `/readyz`: hub, store and NATS reachable |

Running components on separate machines: [deploy-multi-host.md](deploy-multi-host.md).
