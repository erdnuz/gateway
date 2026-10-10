# 5. The ledger's clock defines windows; one ledger, two implementations

**Status:** accepted

## Context

Edges and hubs each computed window IDs from their own clocks. A grant made
just after a boundary on the hub's clock was spent in the previous window on
an edge whose clock ran ahead, so a window could admit more than its quota
(the load test recorded 1001 of 1000 and tolerated up to Q+4). Every later
feature (sliding windows, debt, regions) would widen that gap.

Grant logic also lived inline in the hub's gRPC handler, so it could only be
tested against Redis.

## Decision

- The ledger computes windows from its own clock (`redis.call('TIME')` in
  the grant script) and answers with the window ID and how long the tokens
  live, relative to the moment of the grant. The edge measures that lifetime
  from when it *sent* the request, so its view of expiry is never later than
  the ledger's, and it keeps one balance per hub window.
- `packages/ledger` defines `Ledger`. `Redis` runs Lua; `Mem` runs the same
  algorithms in Go on an injectable clock. A conformance test drives both
  through thousands of random operations and requires identical answers.
- Keys carry Redis Cluster hash tags: everything one grant touches (key,
  window counters, parent budget, active-edge set, dedupe keys) is on one
  slot, so the same scripts run on a single Redis or a cluster.

## Consequences

- No hub window ever admits more than its quota, whatever the clock skew
  between edges and hubs (simulated at ±500 ms; the load test now checks
  `<= Q` exactly, using the `Gate-Window` response header).
- The simulation and most unit tests run without Redis, yet exercise the
  production arithmetic.
- Two copies of each algorithm must stay in step; the conformance test is the
  guard, and it has to grow with every new ledger operation.
- Existing ledger keys were abandoned on upgrade: counters started from zero
  once.
