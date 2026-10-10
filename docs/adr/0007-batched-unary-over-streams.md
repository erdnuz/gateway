# 7. Batched unary lease calls, not a lease stream

**Status:** accepted

## Context

An edge serving many cold keys sends one lease RPC per key, so hub RPCs grew
with active keys. A long-lived bidirectional stream per edge would collapse
them, but it pins each edge to one hub: lease traffic could no longer spread
across hub replicas by ordinary gRPC load balancing.

## Decision

`RequestQuotaLeases` takes a batch and answers each item (with its own gRPC
status). On the edge, `limiter.Batcher` implements the existing
`LeaseClient`, so `Lease` is unchanged. It batches naturally rather than on a
timer: a request goes out immediately when fewer than two batches are in
flight, and otherwise joins the next one. The hub runs a batch as one Redis
pipeline (split per shard by the cluster client).

## Consequences

- An idle edge pays no added latency; a busy one sends a few large batches
  (`manykeys` scenario: mean batch size is reported).
- Lease RPCs balance across hubs (`dns:///` target with `round_robin`); only
  the watch stream is sticky.
- Edges fall back to unary calls when a hub answers `Unimplemented`.
