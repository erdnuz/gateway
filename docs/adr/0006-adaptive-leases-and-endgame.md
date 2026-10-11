# 6. Adaptive grants, endgame splitting and lease returns

**Status:** accepted

## Context

With a fixed lease size, quota stranded on quiet edges at the end of a
window is up to `(edges − 1) × lease size`, and an edge that shuts down
mid-window takes its whole lease with it. A small lease cuts stranding but
multiplies hub calls for busy keys; a large one does the opposite.

## Decision

- **Adaptive grants.** Each edge reports the cost per second it spends on a
  key (an EWMA between fetches). The hub grants the larger of the lease size
  and `rate × horizon` (default 2 s), within `[min, max]`: busy keys get
  large grants and few hub calls. (A first version also shrank grants below
  the lease size for slow keys; the load test showed that only multiplied
  hub calls, since the endgame split already prevents stranding.)
- **Endgame splitting.** Once less than 64 grants' worth of a quota is left,
  each grant registers its edge in a sorted set (ZADD by time) and counts the
  edges seen in the last 2 horizons (ZCOUNT). When less than
  `active edges × target` is left, a grant is
  `ceil(remaining / active edges)`. Far from the endgame grants skip this
  bookkeeping; every edge that keeps asking registers long before the split
  applies.
- **Returns.** On graceful shutdown (and when a key goes idle) an edge swaps
  its balance to zero and returns it. The hub credits only the window the
  tokens came from, once per dedupe ID.

## Consequences

- Stranded quota is about one token per active edge instead of a lease per
  edge: saturated windows in the simulation use 99% of the quota on average,
  and the load test's 80/20 `endgame` scenario admits exactly 1000 of 1000
  in every window.
- A draining edge strands nothing (`drain` scenario).
- Grants depend on demand, so hub calls per request vary with traffic shape;
  the `throughput` scenario tracks hub Redis commands per request (0.15,
  against v2's 0.11: reading the ledger's clock is the difference).
- The active-edge set is one more key per quota window, on the quota's own
  slot.
