# Gate rate-limiter e2e: v3

- commit: 8dfc752
- date: 2026-10-10T17:44:58Z
- topology: hub(2 cpu) + hub Redis + 2 edges(2 cpu each, in-memory caches) + 2 gate limiter replicas(1 cpu each) + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## windows — PASS

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Requests are attributed to the hub window that charged them (Gate-Window). Expect admitted per window in [Q-200, Q], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179165431 | 1989 | **1000** | [512 488] | map[200:1000 429:989] |
| 179165432 | 2001 | **1000** | [499 501] | map[200:1000 429:1001] |
| 179165433 | 2000 | **1000** | [503 497] | map[200:1000 429:1000] |
| 179165434 | 2000 | **1000** | [497 503] | map[200:1000 429:1000] |
| 179165435 | 10 | **10** | [5 5] | map[200:10] |

- PASS: window 179165431 admitted 1000 (want 800..1000)
- PASS: window 179165432 admitted 1000 (want 800..1000)
- PASS: window 179165433 admitted 1000 (want 800..1000)
- PASS: window 179165434 admitted 1000 (want 800..1000)

## carryover — PASS

Window N: 10 rps for 2s (each edge leases up to 100, uses ~10). Window N+1: 300 rps. Expect admitted in N+1 <= Q.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179165437 | 2850 | **1000** | [499 501] | map[200:1000 429:1850] |

- PASS: window N+1 admitted 1000 (limit 1000)
- PASS: window N+1 admitted 1000 is not starved (>= 800)

## isolation — PASS

Exhaust key on `svc` (1100 requests), then 50 requests to `svc2` in the same window.

- svc: map[200:1000 429:100]
- svc2: map[200:50]

- PASS: svc2 admitted 50/50 after svc exhausted

## bypass — PASS

1000 requests with `X-API-Key: victim0/x<i>` (40 suffixes) plus 10 never-issued keys.

- statuses: map[401:1010]

- PASS: 0 malformed/unknown-key requests admitted (want 0)

## propagation — PASS

Delete a tier on the hub, then add a service via `/config-reload`; poll each edge. `-1` = never converged.

| edge | tier delete visible (ms) | config reload visible (ms) |
|---|---|---|
| 1 | 1 | 57 |
| 2 | 4 | 57 |

- PASS: edge 1 sees tier delete within 1s (1 ms)
- PASS: edge 1 sees config reload within 2s (57 ms)
- PASS: edge 2 sees tier delete within 1s (4 ms)
- PASS: edge 2 sees config reload within 2s (57 ms)

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 34836 | **3483** | 3.29 | 9.08 | **21.70** | 38.87 | 65.7 | map[200:34836] |
| 64 | 35530 | **3548** | 13.86 | 36.92 | **71.75** | 108.28 | 151.3 | map[200:35530] |
| 256 | 49463 | **4881** | 41.24 | 108.02 | **202.88** | 270.47 | 360.6 | map[200:49463] |


### control-plane cost (throughput scenario)

- hub Redis commands: 151283 for 119829 requests = **1.26 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 170%
  - gate-lt-edge-2-1: 161%
  - gate-lt-hub-redis-1: 43%
  - gate-lt-hub-server-1: 71%
  - gate-lt-limiter-1-1: 15%
  - gate-lt-limiter-2-1: 14%
  - gate-lt-loadgen-run-345693ff2729: 159%
  - gate-lt-upstream-1: 129%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 51968 | **5147** | 39.70 | 105.18 | **193.25** | 251.45 | 365.4 | map[200:51968] |


## endgame — PASS

200 rps (2x quota) on one key, 80% to edge 1 and 20% to edge 2. Expect every window in [Q-4, Q]: stranded quota is about one token per active edge, not a lease per edge.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179165448 | 1990 | **1000** | [769 231] | map[200:1000 429:990] |
| 179165449 | 1998 | **1000** | [761 239] | map[200:1000 429:998] |
| 179165450 | 2002 | **1000** | [788 212] | map[200:1000 429:1002] |
| 179165451 | 10 | **10** | [8 2] | map[200:10] |

- PASS: window 179165448 admitted 1000 (want 996..1000)
- PASS: window 179165449 admitted 1000 (want 996..1000)
- PASS: window 179165450 admitted 1000 (want 996..1000)

## algorithms — PASS

**sliding_window** (Q=1000 per 10s), 200 rps:

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179165452 | 1989 | **1000** | [512 488] | map[200:1000 429:989] |
| 179165453 | 2001 | **998** | [541 457] | map[200:998 429:1003] |
| 179165454 | 1999 | **998** | [440 558] | map[200:998 429:1001] |
| 179165455 | 11 | **5** | [3 2] | map[200:5 429:6] |

- most admitted in any 10s span: **1004** (a fixed window can admit 2Q across a reset)

**token_bucket** (burst 100, 50/s), 200 rps for 20s:

- admitted **1099** (bound 900..1202)
- most in any second: 148

- PASS: sliding: window 179165452 admitted 1000 <= 1000
- PASS: sliding: window 179165453 admitted 998 <= 1000
- PASS: sliding: window 179165454 admitted 998 <= 1000
- PASS: sliding: any 10s span admitted 1004 <= 1.3Q
- PASS: bucket admitted 1099 in 20s (want 900..1202)

## multilimit — PASS

Ten keys (tier quota 1000 each) from one client address, 100 rps total, with `also: per-ip 300 per window`. Expect the per-IP limit to bind.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179165458 | 994 | **300** | [153 147] | map[200:300 429:694] |
| 179165459 | 1001 | **300** | [154 146] | map[200:300 429:701] |
| 179165460 | 5 | **5** | [2 3] | map[200:5] |

- PASS: window 179165458 admitted 300 (per-IP limit 300)
- PASS: window 179165459 admitted 300 (per-IP limit 300)

## llm — PASS

Quota 5000 tokens per window; each request reserves 50, the upstream reports 0-100, the edge settles the difference (max_debt 200). Expect tokens charged per window <= 5400.

| window | requests admitted | tokens used |
|---|---|---|
| 179165461 | 96 | **4998** |
| 179165462 | 99 | **4946** |
| 179165463 | 94 | **5040** |
| 179165464 | 2 | **89** |

- PASS: window 179165461 used 4998 tokens <= 5400
- PASS: window 179165461 used 4998 tokens: the quota is spent on actual, not estimated, cost (>= 3500)
- PASS: window 179165462 used 4946 tokens <= 5400
- PASS: window 179165462 used 4946 tokens: the quota is spent on actual, not estimated, cost (>= 3500)
- PASS: window 179165463 used 5040 tokens <= 5400
- PASS: window 179165463 used 5040 tokens: the quota is spent on actual, not estimated, cost (>= 3500)

## manykeys — PASS

5000 cold keys on each of 2 edges, 256 concurrent requests, in 6.792s.

- lease requests: 10000 in **363 RPCs** (mean batch 27.5)
- admitted: 10000 of 10000

- PASS: every request admitted (10000)
- PASS: concurrent leases were batched (mean batch 27.5)

## rls — PASS

Two gate limiter replicas, 100 checks/s alternating between them, one descriptor limited to 500 per window.

| window | admitted |
|---|---|
| 179165466 | **500** |
| 179165467 | **500** |
| 179165468 | **500** |
| 179165469 | **5** |

- PASS: window 179165467 admitted 500 (quota 500)
- PASS: window 179165468 admitted 500 (quota 500)

## drain — PASS

Both edges lease 400 of 1000 at the start of a 30s window; edge 1 is stopped (SIGTERM); edge 2 is flooded for the rest of the window.

- admitted in the window: **1000** (without lease return at most 601)
- statuses: map[200:998 429:9077]

- PASS: window admitted 1000 >= 950: the drained edge's lease was returned
- PASS: window admitted 1000 <= quota 1000

## outage — PASS

Hub stopped. 100 rps on a `pro` key for 3 windows. Expect service to continue (degraded) with admitted <= S x edges = 100 per window (first window may also drain leftover lease tokens).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179165477 | 995 | **100** | [50 50] | map[200:100 429:895] |
| 179165478 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179165479 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179165480 | 5 | **5** | [2 3] | map[200:5] |

- PASS: window 179165477 still serves traffic during hub outage (admitted 100)
- PASS: window 179165478 still serves traffic during hub outage (admitted 100)
- PASS: window 179165478 admitted 100 <= safety cap 100
- PASS: window 179165479 still serves traffic during hub outage (admitted 100)
- PASS: window 179165479 admitted 100 <= safety cap 100

## outage-recovery — PASS

Hub restarted; 200 rps for one window on the same key. Expect normal quota (~Q=1000).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179165482 | 1900 | **1000** | [499 501] | map[200:1000 429:900] |

- PASS: recovered window admitted 1000 (want 800..1000)

## Summary

- passed: windows carryover isolation bypass propagation throughput hotkey endgame algorithms multilimit llm manykeys rls drain outage outage-check
- failed: none
