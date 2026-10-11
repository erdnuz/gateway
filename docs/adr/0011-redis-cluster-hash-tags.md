# 11. Shard the ledger with Redis Cluster hash tags; balance across hubs

**Status:** accepted

## Context

Hubs are stateless and replicate freely, but every grant ran on one Redis.
Client-side consistent hashing over independent Redis instances would shard
it, at the cost of Gate owning resharding and failover.

## Decision

Use Redis Cluster. Every ledger key carries a hash tag (the subject, or the
org for nested budgets), so a grant is one script on one slot; key records
share one tag so their create and delete stay transactional (migrated from
the old layout on hub start). `REDIS_ADDRS` with several seed nodes selects
the cluster client; readiness and the no-eviction check cover every master.
Edges dial `dns:///hub:9090` with `round_robin`, spreading lease RPCs over
all hub replicas.

## Consequences

- Ledger capacity grows with shards; hub capacity with replicas.
- One hot quota (a service-wide limit) still lands on one slot; adaptive
  grants keep its call rate proportional to edges.
- Grant scripts touch keys they derive (window counters) rather than
  declare; they share the declared key's slot, which Redis Cluster serves.
