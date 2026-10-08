# Deployments

This repository provides container assets per service under deployments/.

## Deployment Files

- deployments/edge.Dockerfile
- deployments/hub.Dockerfile
- deployments/analytics.Dockerfile
- deployments/docker-compose.edge.yaml
- deployments/docker-compose.hub.yaml
- deployments/docker-compose.analytics.yaml

## Runtime Dependencies

- Redis (edge and hub runtime state)
- NATS (tier updates and analytics event transport)
- ClickHouse (analytics storage and query)

## Certificates and Secrets

Private keys and env files are not committed (see `.gitignore`). Generate
development mTLS material before the first deploy:

```bash
deployments/gen-dev-certs.sh                     # writes deployments/certs/{ca,hub,edge}
deployments/gen-dev-certs.sh out/ hub.example.com,hub-server   # custom SANs
```

The hub certificate carries `serverAuth`, the edge certificate `clientAuth`, so
the hub's default `HUB_GRPC_CLIENT_AUTH_MODE=require-and-verify` works without
downgrading to `none`. Copy `deployments/.env.*.example` to `deployments/.env.*`
and set a strong `HUB_AUTH_TOKEN`.

## Typical Deployment Pattern

1. Start Hub-stage infrastructure (Hub Redis and tier-update NATS).
2. Start Hub and wait for /readyz.
3. Start Analytics-stage infrastructure (ClickHouse and analytics NATS) and wait for Analytics /readyz.
4. Start Edge-stage infrastructure (Edge Redis), then start Edge nodes and wait for /readyz.
5. Verify /healthz liveness endpoints and dashboard/API access.

## Three-Machine Rollout Order

Use one compose file per machine and keep strict order to avoid deadlock:

1. Hub machine:
2. `docker compose -f deployments/docker-compose.hub.yaml --env-file deployments/.env.hub up -d --build`
3. Wait for `http://<hub-host>:8080/readyz` to return `200`.
4. Analytics machine:
5. `docker compose -f deployments/docker-compose.analytics.yaml --env-file deployments/.env.analytics up -d --build`
6. Wait for `http://<analytics-host>:8091/readyz` to return `200`.
7. Edge machine:
8. `docker compose -f deployments/docker-compose.edge.yaml --env-file deployments/.env.edge up -d --build`
9. Wait for `http://<edge-host>:8082/readyz` to return `200`.

Edge blocks client proxy traffic until the Hub ready endpoint is healthy once at startup, so requests may return `503` with pending dependency details during convergence. After startup, Edge `/readyz` tracks only edge-local dependencies (its Redis): a Hub outage is handled per request by the failure policy (safety tier, stale tiers), and an Analytics outage never blocks traffic.

## Enforced Startup Order

The deployment scripts must enforce this order:

1. Hub -> /readyz is healthy
2. Analytics -> /readyz is healthy (recommended; Edge does not wait for it)
3. Edge -> /readyz is healthy

Tier-update NATS must be available before Hub boot. In the compose deployment, that broker is part of the Hub stage so Hub can satisfy its startup dependency checks without relying on the Edge stack being started early.

Local orchestration script: deployments/redeploy_fresh_all_docker.sh deploys and gates each stack sequentially using `/readyz`.

## Load Testing

`testing/loadtest/run.sh <label>` brings up a production-shaped stack (hub, two
edges with private Redis, NATS, upstream) from
`deployments/docker-compose.loadtest.yaml`, runs the rate-limiter correctness
and performance scenarios, and writes `testing/loadtest/results/<label>/report.md`.

## Health and Readiness Endpoints

- Edge: /healthz, /readyz
- Hub: /healthz, /readyz
- Analytics: /healthz, /readyz

## Multi-Edge Notes

- Set a unique EDGE_ID per edge instance.
- Ensure all edges publish to the same analytics subject for combined analytics views.
- Use edge_id filter on analytics summary queries when debugging a single edge.

## Deployment Validation Checklist

- Hub auth token is configured and consistent for edge/hub calls.
- Edge lease mTLS files and server name are valid.
- Analytics can reach ClickHouse and NATS.
- /analytics/summary returns contract-compliant summary and series payloads.
