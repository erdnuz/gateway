# Gate

[![CI](https://github.com/erdnuz/gateway/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/erdnuz/gateway/actions/workflows/ci.yml)
[![Load test](https://github.com/erdnuz/gateway/actions/workflows/loadtest.yml/badge.svg)](https://github.com/erdnuz/gateway/actions/workflows/loadtest.yml)
[![Go version](https://img.shields.io/github/go-mod/go-version/erdnuz/gateway)](go.mod)

Gate is an API gateway with **distributed rate limiting by quota leasing**.
Edges enforce per-key quotas locally from token leases granted by a central
hub, so limits hold globally across many edges without a network round trip
per request.

- **Edge**: the gateway. Authenticates API keys, enforces quotas, caches, and proxies to upstreams.
- **Hub**: the control plane. Owns config, key tiers and the quota ledger, and grants leases over mTLS gRPC.
- **Analytics**: ingests request events from the edges and serves a query API and dashboard.

Edges share no state on the request path, so capacity grows linearly with
edges, and the hub's load grows with leases, not requests. See
[docs/architecture.md](docs/architecture.md) for where state lives, how each
tier scales, and what happens when a component fails.

## Quick start

### 1. Everything in one process: no Docker, no infrastructure

```bash
go run ./cmd/gate dev
```

This starts hub, edge, analytics and a demo echo upstream with embedded Redis
and NATS, issues one key per tier, and prints how to use them:

```text
gate dev is running — embedded Redis and NATS, all state in memory
  gateway     http://127.0.0.1:8080
  hub API     http://127.0.0.1:8081   (Authorization: Bearer dev-…)
  analytics   http://127.0.0.1:8091   (dashboard)
  upstream    http://127.0.0.1:9000   (built-in echo)

API keys:
  freekey      tier free
  prokey       tier pro

Try it:
  curl -i -H "X-API-Key: freekey" http://127.0.0.1:8080/v1/echo/hello
```

The `free` tier allows 20 requests a minute. Request 21 returns `429` with
`RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` and `Retry-After`.

No Go toolchain? `make dev` runs the same thing in a container.

### 2. The real topology in Docker

```bash
make demo    # or: bash scripts/dev.sh demo   (where make is unavailable)
```

This runs hub, edge, analytics, Redis, NATS, ClickHouse and an echo upstream
from [deploy/compose.yaml](deploy/compose.yaml). Edges lease quota from the
hub over mTLS gRPC, with certificates generated on first start. A traffic
generator sends steady load across tiers, including one key that keeps
exceeding its quota.

| URL | What |
| --- | --- |
| <http://localhost:8080/v1/echo/hello> | the gateway (`X-API-Key: demopro1`) |
| <http://localhost:8081> | hub API (`Authorization: Bearer gate-local-token`) |
| <http://localhost:8091> | analytics dashboard |

`make up` starts the same stack without demo traffic. `make down` stops it,
and `make clean` also removes its volumes.

## The `gate` binary

One binary runs every component:

```text
gate dev                 hub + edge + analytics in one process, no external services
gate hub                 control plane          (configured by environment)
gate edge                gateway data plane     (configured by environment)
gate analytics           analytics + dashboard  (configured by environment)
gate echo                demo upstream that echoes each request as JSON
gate config validate F   check a config file and list every problem with its line
gate config migrate F    convert a version 1 JSON config to version 2 YAML
gate config schema       print the JSON Schema used for editor completion
```

## Configuration

A gateway config is a short YAML file. Durations are written like `1m`, and
everything not shown here has a working default:

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
        pro:  { requests: 6000 }
```

See [examples/](examples) and [docs/configuration.md](docs/configuration.md).

## How rate limiting works

Each edge asks the hub for a *lease*, a batch of tokens for one key, service
and quota window. It then spends those tokens locally, in memory. It renews
in the background when the balance runs low, and drops leftover tokens when
the window rolls over. The hub's ledger is the single source of truth, so the
global limit holds however many edges serve a key. If the hub is unreachable,
edges keep serving under a local per-window *safety* quota instead of failing
closed.

The [load test](testing/loadtest/README.md) checks these properties against a
production-shaped stack: exact admission per window, no carry-over between
windows, isolation between services, propagation to every edge, and
behaviour during a hub outage. See the
[before/after results](testing/loadtest/results/COMPARISON.md).

## Development

```bash
make test               # go vet + unit tests with the race detector
make test-integration   # testcontainers suite (Redis, NATS)
make loadtest           # end-to-end rate-limiter load test
make help               # everything else
```

Targets use a local Go toolchain when present and a `golang:1.26` container
otherwise.

## Repository layout

| Path | Contents |
| --- | --- |
| `cmd/gate` | the single binary |
| `internal/app` | per-component wiring (`hubapp`, `edgeapp`, `analyticsapp`) and the in-process dev stack |
| `packages/limiter` | the `Limiter` interface: `Lease` (global quota from hub leases) and `Local` (per-edge safety limit) |
| `packages/edge`, `packages/hub`, `packages/analytics` | component logic |
| `packages/config` | config file format: parsing, validation, migration, JSON Schema |
| `packages/common` | shared runtime types, RFC 9457 problems and request IDs (`httpx`), env helpers, readiness |
| `api/proto` | hub ↔ edge lease protocol |
| `deploy` | Dockerfile, compose stack, dev certificate generator |
| `examples`, `schema` | gateway config examples and their JSON Schema |
| `testing` | integration scenarios and the load-test harness |

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Deployments](docs/deployments.md) and [multi-host deployment](docs/deploy-multi-host.md)
- [Edge](docs/edge.md), [Hub](docs/hub.md), [Analytics](docs/analytics.md)
