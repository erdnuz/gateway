# 6. Adaptive grants, endgame splitting and lease returns

**Status:** accepted

## Context

With a fixed lease size, quota stranded on quiet edges at the end of a
window is up to `(edges − 1) × lease size`, and an edge that shuts down
mid-window takes its whole lease with it. A small lease cuts stranding but
multiplies hub calls for busy keys; a large one does the opposite.

## Decision

- **Adaptive grants.** Each edge reports the cost per second it spends on a
  key (an EWMA between fetches). The hub sizes the grant to
  `rate × horizon` (default 2 s), within `[min, max]`. Busy keys get large
  grants and few hub calls; cold keys get small ones.
- **Endgame splitting.** The grant script keeps a sorted set of edges active
  on the quota (ZADD by time, ZREMRANGEBYSCORE older than 2 × horizon). When
  less than `active edges × target` is left, a grant is
  `ceil(remaining / active edges)`.
- **Returns.** On graceful shutdown (and when a key goes idle) an edge swaps
  its balance to zero and returns it. The hub credits only the window the
  tokens came from, once per dedupe ID.

## Consequences

- Stranded quota is about one token per active edge instead of a lease per
  edge: saturated windows in the simulation use 98–99% of the quota, and the
  load test's 80/20 `endgame` scenario reaches `Q − 4` or better.
- A draining edge strands nothing (`drain` scenario).
- Grants depend on demand, so hub calls per request vary with traffic shape;
  the `throughput` scenario tracks hub Redis commands per request.
- The active-edge set is one more key per quota window, on the quota's own
  slot.
