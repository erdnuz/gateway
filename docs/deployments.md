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

## Typical Deployment Pattern

1. Start infrastructure (Redis, NATS, ClickHouse).
2. Start hub and validate readiness.
3. Start edge instances with hub and mTLS lease settings.
4. Start analytics service with ClickHouse and NATS settings.
5. Verify health endpoints and dashboard/API access.

## Health and Readiness Endpoints

- Edge: /healthz, /readyz
- Hub: /health, /readyz
- Analytics: /health, /readyz

## Multi-Edge Notes

- Set a unique EDGE_ID per edge instance.
- Ensure all edges publish to the same analytics subject for combined analytics views.
- Use edge_id filter on analytics summary queries when debugging a single edge.

## Deployment Validation Checklist

- Hub auth token is configured and consistent for edge/hub calls.
- Edge lease mTLS files and server name are valid.
- Analytics can reach ClickHouse and NATS.
- /analytics/summary returns contract-compliant summary and series payloads.
