# Edge Service

Edge is the request path gateway.

## Main Entry

- internal/app/edgeapp (wiring; run with `gate edge`)

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
- /analytics/metrics (requires `Authorization: Bearer $EDGE_ADMIN_TOKEN`; 404 when unset)

## Rate Limiting

- Quotas are per `(prefix, service, api key)` per epoch-aligned window of the prefix `quota_period`; usage resets when the window rolls and leftover leased tokens are dropped.
- Responses carry `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset`; a `429` also carries `Retry-After`.
- API keys must match `runtime.hub.api_key_pattern`; malformed or unknown keys get `401`, keys the hub refuses for a service get `403`.
- When the hub is unreachable and `failure.hub.allow_on_rate_service_error` is set, the service's `safety_tier` quota is enforced locally per window.

## Key Edge Packages

- packages/edge/server.go
- packages/edge/rates.go
- packages/edge/lease_manager.go
- packages/edge/cache.go
- packages/edge/analytics.go
