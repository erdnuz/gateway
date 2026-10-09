# 2. One gRPC stream for the control plane

**Status:** accepted (replaces HTTP polling plus NATS and Redis pub/sub)

## Context

Edges used to reach the hub four ways: HTTP for config and tier lookups,
gRPC for leases, NATS for tier updates and reloads, and Redis pub/sub for
reloads in single-Redis setups. Each path had its own failure modes,
credentials and retry logic, and a missed message left an edge stale until
a TTL expired.

## Decision

Each edge holds one mTLS gRPC connection to a hub for leases, `ResolveKey`,
and a server stream (`Watch`) of config snapshots and tier changes. Every
stream starts with a full snapshot and a "revalidate cached tiers" flag. A
hub that cannot keep up with an edge disconnects it, and the edge reconnects
and resynchronises the same way.

Hubs relay changes to each other through a Redis channel, since they already
share Redis for the ledger.

## Consequences

- No resume tokens or replay logs are needed: reconnecting is the recovery
  path, and it is exercised constantly.
- Hubs are stateless and can be replicated behind a gRPC load balancer.
- Edges need neither NATS nor their own Redis. NATS carries only analytics.
- A change reaches edges in one Redis publish plus one stream send (single
  milliseconds in the load test's `propagation` scenario).
