# 10. The limiter as a service: gate limiter

**Status:** accepted

## Context

Gate's limiter was only reachable as Gate's own HTTP proxy. Teams already
running Envoy (directly, or through Istio, Contour, Emissary or Gloo), and
workloads that are not HTTP at all (queue consumers, batch jobs), could not
use it.

## Decision

A separate stateless process, `gate limiter`, reuses the hub client, config
following, `limiter.Lease` and the batcher, and serves:

- Envoy's `envoy.service.ratelimit.v3.RateLimitService` (go-control-plane
  types), with descriptors configured in envoyproxy/ratelimit's shape;
- a JSON API: `POST /v1/check` (optionally waiting up to `max_wait_ms` for
  quota) and `POST /v1/settle`.

All descriptors in a request are all-of with local rollback. Reservations are
HMAC-signed tokens carrying everything needed to settle; settling sends a
deduplicated return (positive or negative) straight to the hub, so any
replica can settle any reservation.

## Consequences

- Envoy users get exact global limits across every Envoy and every limiter
  replica, with sub-millisecond decisions from leased tokens.
- The limiter adds a process to deploy, but it scales like an edge.
- Settling costs one hub call; acceptable for the JSON API's callers.
