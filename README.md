# Gate

Gate is a distributed API governance platform built from three services:

- Edge: request path gateway and enforcement plane
- Hub: control-plane authority for config, tiers, and quotas
- Analytics: event ingestion, query API, and dashboard

## Documentation

- Architecture: [docs/architecture.md](docs/architecture.md)
- Deployments: [docs/deployments.md](docs/deployments.md)
- Configuration: [docs/configuration.md](docs/configuration.md)
- Edge Service: [docs/edge.md](docs/edge.md)
- Hub Service: [docs/hub.md](docs/hub.md)
- Analytics Service: [docs/analytics.md](docs/analytics.md)

## Repository Layout

- cmd/: service entrypoints
- packages/: service implementations and shared modules
- deployments/: Dockerfiles and compose assets
- testing/: unit, race, integration helpers and scenarios

## Test Command

```bash
go test ./...
```
