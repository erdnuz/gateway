# Edge

The edge is the data plane: every client request goes through it. Run it
with `gate edge`; [internal/app/edgeapp](../internal/app/edgeapp/edgeapp.go)
wires it. Edges share no state with each other, so you scale them by adding
replicas behind a load balancer (see [architecture](architecture.md)).

## Request chain

[`edge.NewServer`](../packages/edge/server.go) builds one middleware chain
over a typed per-request state:

| Step | File | Behavior |
| --- | --- | --- |
| request ID | [`httpx`](../packages/common/httpx/httpx.go) | keeps a sane inbound `X-Request-Id` or mints one; echoes it and forwards it upstream |
| analytics | [`analytics.go`](../packages/edge/analytics.go) | records status, bytes and latency; publishes to NATS from a bounded buffer |
| route | [`route.go`](../packages/edge/route.go) | `/{group}/{service}/...` to a configured service, otherwise `404 no_route` |
| CORS | [`route.go`](../packages/edge/route.go) | answers preflights before authentication |
| auth | [`auth.go`](../packages/edge/auth.go) | `X-API-Key` must match `runtime.hub.api_key_pattern`; maps the key to a tier ([in-memory cache](../packages/edge/tiers.go), then the hub) |
| cache | [`cache.go`](../packages/edge/cache.go) | GET hits are served before rate limiting, so they cost no quota |
| rate limit | [`ratelimit.go`](../packages/edge/ratelimit.go) | charges the key's quota through a [`limiter.Limiter`](../packages/limiter/limiter.go) |
| proxy | [`proxy.go`](../packages/edge/proxy.go) | `httputil.ReverseProxy`: streaming, trailers and protocol upgrades; retries and the failure policy |

## Callers

`auth.type` decides who a caller is. The identity is hashed on arrival, and
only the hash travels further.

| `auth.type` | Caller | Tier |
| --- | --- | --- |
| `api_key` (default) | `X-API-Key`, issued or assigned on the hub | the hub's record, cached in memory and pushed on change |
| `jwt` | `Authorization: Bearer`, verified against `auth.jwt.jwks_url` (issuer, audience, expiry; RS/PS/ES algorithms); identity is `iss` + `sub` | the `auth.jwt.tier_claim` claim, else `limits.default_tier` |
| `none` | the client IP (the connection's peer address) | `limits.default_tier`, or the only tier |

JWKS documents are cached per URL for 10 minutes and refetched early when a
token names an unknown key, at most every 30 seconds. Verification never
waits on the issuer per request. For `jwt` and `none` the edge tells the hub
which tier it resolved; for API keys the hub always uses its own record.

## Rate limiting

- Quotas apply per `(group, service, API key)` and per epoch-aligned window.
- [`limiter.Lease`](../packages/limiter/lease.go) spends tokens leased from
  the hub and renews them in the background (see [architecture](architecture.md#distributed-rate-limiting-by-leasing)).
- Responses carry `RateLimit-Limit`, `RateLimit-Remaining` and
  `RateLimit-Reset`. A `429 rate_limited` response also carries `Retry-After`.
- When the hub is unreachable, a service with `when_hub_unavailable` applies
  that limit locally on each edge ([`limiter.Local`](../packages/limiter/local.go)).
  A service without it returns `503 rate_limit_unavailable`.
- Malformed or unknown keys get `401`, and keys the hub refuses for a
  service get `403 key_not_permitted`.
- `limits.max_concurrent` caps a caller's requests in flight on each edge
  (`429 concurrency_limited`). It protects upstreams from one caller's burst
  without any coordination between edges.

## Proxying

- `upstreams` lists replicas of a service. `load_balancing` is `round_robin`
  or `least_requests` (fewest in-flight requests from this edge).
- The circuit breaker (`circuit_breaker`, on by default with several
  replicas) ejects a replica after consecutive connection errors or 5xx
  responses and tries it again after the cooldown. A retry goes to a
  different replica when one is healthy. When every replica is ejected the
  edge answers `503 upstream_circuit_open` at once instead of queueing on a
  dead upstream. Each edge judges replicas by its own traffic, so there is no
  health-check fan-out to grow with the number of edges.
- `match.methods` and `match.host` restrict a service (`405` with `Allow`, or
  `404`); `timeout` bounds the whole upstream exchange (`504`), and
  `max_request_body` the request body (`413`).

- The upstream sees `X-Forwarded-For` (appended), `X-Forwarded-Host`,
  `X-Forwarded-Proto` and `X-Request-Id`. The edge strips `X-API-Key` and any
  inbound `X-Gate-*` headers, then applies the service's header rules.
- Retries cover idempotent methods (and others with `non_idempotent: true`),
  on connection errors and on the configured statuses. A retryable request
  body is buffered up to the body limit (10 MiB by default).
- Once retries are exhausted, `on_upstream_error` either returns a fallback
  response (`fail-open`, marked `X-Upstream-Error: true`) or a problem:
  `502 upstream_unavailable`, or `504 upstream_timeout` when a deadline expired.

## Errors

Every error the edge generates is an RFC 9457 `application/problem+json`
body:

```json
{"title":"Too Many Requests","status":429,"detail":"quota exhausted for this window; see Retry-After","code":"rate_limited","request_id":"6f1c…"}
```

| Status | `code` |
| --- | --- |
| 401 | `missing_api_key`, `invalid_api_key`, `missing_token`, `invalid_token` |
| 403 | `tier_not_allowed`, `key_not_permitted` |
| 404 | `no_route` |
| 405 | `method_not_allowed` |
| 413 | `body_too_large` |
| 429 | `rate_limited`, `concurrency_limited` |
| 502 | `upstream_unavailable`, `bad_upstream` |
| 503 | `starting`, `tier_lookup_failed`, `rate_limit_unavailable`, `upstream_circuit_open` |
| 504 | `upstream_timeout` |

## Operational endpoints

- `/healthz`: the process is up.
- `/readyz`: the edge has received a config from the hub (or was started with
  a bootstrap config), and its Redis response store answers if it uses one.
  Later hub outages deliberately do not fail it.
- `/analytics/metrics`: analytics buffer counters. It needs
  `Authorization: Bearer $EDGE_ADMIN_TOKEN` and returns 404 when the token is unset.
