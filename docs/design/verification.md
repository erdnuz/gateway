# How Gate's limiter is verified

A distributed rate limiter fails in rare interleavings: a reply lost at a
window boundary, a retried return, an edge whose clock runs ahead. Example
tests rarely hit them. Gate checks its guarantees five ways, from fastest to
most realistic.

## 1. Ledger conformance

`packages/ledger/conformance_test.go` drives the Redis scripts (on
miniredis, with its clock pinned) and the in-memory ledger through thousands
of random grants, returns, lends and credits on the same clock, and requires
identical answers. This is what lets everything below use the in-memory
ledger and still test the production arithmetic.

## 2. Deterministic simulation (`make sim`)

`testing/sim` runs the production code end to end — edges (`limiter.Lease`
with the lease batcher), the hub's lease server, the ledger — inside a
`testing/synctest` bubble, where time is virtual: an hour of traffic takes
seconds, and sleeps, timeouts and lease expiries happen exactly when they
should. Between edges and hub sits a fault injector.

Each scenario is generated from a seed:

| Dimension | Range |
| --- | --- |
| topology | 1–6 edges, 1–8 keys, a hot key taking 0–90% of traffic |
| limits | fixed window, sliding window or token bucket; optional service-wide limit |
| leasing | fixed lease sizes or adaptive grants with 250 ms – 2 s horizons |
| demand | 0.5–4× the quota, optionally moving between edges halfway |
| time | hub clock up to ±500 ms off the edges' |
| lifecycle | an edge draining mid-run; post-hoc costs of 0–3× the estimate |
| faults | RPC latency, drops, replies lost after the hub acted, duplicated requests, hub outages |

Invariants checked on every run:

1. For every quota and hub window: admitted cost ≤ Q (plus `edges ×
   max_debt` with post-hoc costs). Admissions are attributed to the window
   of the token they spent.
2. Token buckets: in any interval τ, admitted ≤ `burst + rate × (τ +
   lease lifetime)`.
3. Liveness: in saturated, fault-free scenarios, at least 85% of the hot
   key's quota is used each window (the mean is about 98%).

The multi-region simulation (`TestRegions`) adds 2–3 regions with their own
ledgers and rebalancing nodes, demand that moves between regions, and an
inter-region network that drops, duplicates, loses replies and partitions.
The invariant: across all regions, no key exceeds its global quota in any
window.

A failure prints its scenario and a replay command:

```text
go test ./testing/sim -run Simulation -sim.seed=42
```

Goroutine interleaving inside a bubble is not fully deterministic, so a seed
reproduces the scenario, not always the exact schedule; the invariants hold
for every schedule, so any that breaks one fails.

**What it found.** While being built, the simulation caught two real bugs:
returning post-hoc debt drove a Prometheus counter negative (a panic in the
hub), and admissions that blocked on a lease fetch across a window boundary
were attributed to the wrong window, which led to holding each window's
tokens in one atomically swapped structure.

## 3. Model checking (`make tla`)

The two protocols whose correctness rests on message ordering have TLA+
specifications in [docs/spec](../spec), checked exhaustively by TLC within
small bounds:

| Spec | Models | Invariants | States |
| --- | --- | --- | --- |
| [LeaseProtocol](../spec/LeaseProtocol.tla) | grants for the ledger's current window, lost and late grant replies, spending only while the token's window is the ledger's, returns that swap the balance out first, retried returns, window rolls | no window admits more than Q; admissions never exceed net grants | 540,412 distinct |
| [RegionTransfer](../spec/RegionTransfer.tla) | debit-first lending, retries answered with the recorded amount, lost and duplicated replies, replies arriving after a roll | allocations never sum to more than Q; regions together never admit more than Q | 67,320 distinct |

The specs have teeth: removing the borrower's once-per-transfer-ID check
makes TLC report `AllocationsBounded` violated within six steps.

## 4. Integration tests (`make test-integration`)

Real Redis, NATS and gRPC with mTLS via testcontainers: clock-skew
resilience, hub fairness, failure isolation, config propagation, startup
safety.

## 5. Load tests (`make loadtest`)

A production-shaped Docker stack (hub, Redis, two edges, two decision-service
replicas) driven by `testing/loadtest/cmd/loadgen`. Window checks use the
`Gate-Window` header each edge returns, so requests are attributed to the hub
window that charged them and the bound is exact (`<= Q`), not
`<= Q + tolerance`. See [the load-test README](../../testing/loadtest/README.md).
