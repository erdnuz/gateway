# 1. Leases instead of a central counter

**Status:** accepted

## Context

A per-key limit must hold across every edge. Counting each request in Redis
is exact, but it puts a network round trip on every request and makes one
Redis the ceiling on gateway throughput.

## Decision

The hub grants blocks of tokens ("leases") from a ledger in Redis, and edges
spend them in process memory, renewing in the background at a low-water
mark. Windows are epoch-aligned, so no coordination is needed to agree on
them.

## Consequences

- A decision costs about 100–250 ns and no I/O. Hub work grows with
  `requests / lease size`, not with requests or edges.
- The limit is never exceeded. Near a window's end, up to
  `(edges − 1) × lease size` tokens can be stranded on quiet edges, so lease
  size trades accuracy against hub load.
- Edges need a degraded mode for hub outages (`when_hub_unavailable`).

See [the design note](../design/distributed-rate-limiting.md).
