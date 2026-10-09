# 3. Edges share nothing on the request path

**Status:** accepted

## Context

Earlier versions could keep lease tokens, key tiers and cached responses in
a Redis that several edges shared. Sharing state couples edges: a slow
Redis slows every edge, and capacity no longer grows with replicas.

## Decision

All request-path state lives in edge memory:

- leased tokens: atomics
- key tiers: a sharded TTL cache, filled by `ResolveKey` and updated by pushes
- config: an atomically swapped snapshot
- responses: a byte-budgeted cache

The only cross-edge coordination is through the hub, and only for leases
and key misses. A Redis response store remains available as an opt-in for
caches larger than an edge's memory.

## Consequences

- Edges scale out linearly behind any load balancer, with no sticky sessions.
- Cold edges warm up with one `ResolveKey` per active key and one lease per
  active key and service.
- Per-edge limits (`max_concurrent`, `when_hub_unavailable`) are documented
  as per edge, because coordinating them would reintroduce sharing.
