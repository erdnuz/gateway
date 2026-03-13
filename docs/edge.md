# Edge Service

Edge is the request path gateway.

## Main Entry

- cmd/edge/main.go

## Responsibilities

- Authenticate requests using X-API-Key.
- Resolve prefix/service routing.
- Resolve user tier and enforce quotas.
- Apply cache policy for GET requests.
- Proxy upstream requests.
- Capture and emit analytics events.

## Middleware Pipeline

Implemented in packages/edge/server.go:

1. analyticsMiddleware
2. corsMiddleware
3. authMiddleware
4. cacheMiddleware
5. rateLimitMiddleware
6. proxyHandler

## Rate and Lease Behavior

- Uses local counters and queued lease renewals.
- Renews quotas through hub gRPC lease service.
- Supports low-watermark lease renewal strategy.

## Analytics Emission

- Publishes analytics entries to NATS.
- Includes edge_id on every entry (from EDGE_ID).
- Captures latency, response code, cache hit, sizes, and upstream error signals.

## Operational Endpoints

- /healthz
- /readyz
- /analytics/metrics (when analytics sink exposes stats)

## Key Edge Packages

- packages/edge/server.go
- packages/edge/rates.go
- packages/edge/lease_manager.go
- packages/edge/cache.go
- packages/edge/analytics.go
