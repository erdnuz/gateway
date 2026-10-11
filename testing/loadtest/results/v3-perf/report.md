# Gate rate-limiter e2e: v3-perf

- commit: 8dfc752 (dirty)
- date: 2026-10-10T18:04:11Z
- topology: hub(2 cpu) + hub Redis + 2 edges(2 cpu each, in-memory caches) + 2 gate limiter replicas(1 cpu each) + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## windows — PASS

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Requests are attributed to the hub window that charged them (Gate-Window). Expect admitted per window in [Q-200, Q], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179165547 | 1986 | **1000** | [489 511] | map[200:1000 429:986] |
| 179165548 | 2001 | **1000** | [492 508] | map[200:1000 429:1001] |
| 179165549 | 2000 | **1000** | [491 509] | map[200:1000 429:1000] |
| 179165550 | 2000 | **1000** | [508 492] | map[200:1000 429:1000] |
| 179165551 | 13 | **13** | [6 7] | map[200:13] |

- PASS: window 179165547 admitted 1000 (want 800..1000)
- PASS: window 179165548 admitted 1000 (want 800..1000)
- PASS: window 179165549 admitted 1000 (want 800..1000)
- PASS: window 179165550 admitted 1000 (want 800..1000)

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 21027 | **2102** | 4.47 | 16.52 | **49.45** | 73.73 | 105.4 | map[200:21027] |
| 64 | 26909 | **2687** | 17.28 | 49.56 | **110.06** | 202.95 | 301.3 | map[200:26909] |
| 256 | 34038 | **3388** | 57.91 | 162.29 | **312.89** | 440.25 | 515.1 | map[200:34038] |


### control-plane cost (throughput scenario)

- hub Redis commands: 39344 for 81974 requests = **0.48 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 206%
  - gate-lt-edge-2-1: 186%
  - gate-lt-hub-redis-1: 49%
  - gate-lt-hub-server-1: 95%
  - gate-lt-limiter-1-1: 43%
  - gate-lt-limiter-2-1: 26%
  - gate-lt-loadgen-run-79aa71537e7a: 162%
  - gate-lt-upstream-1: 135%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 38156 | **3800** | 49.72 | 151.71 | **301.61** | 433.54 | 514.3 | map[200:38156] |


## endgame — PASS

200 rps (2x quota) on one key, 80% to edge 1 and 20% to edge 2. Expect every window in [Q-4, Q]: stranded quota is about one token per active edge, not a lease per edge.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179165559 | 1990 | **1000** | [777 223] | map[200:1000 429:990] |
| 179165560 | 2001 | **1000** | [782 218] | map[200:1000 429:1001] |
| 179165561 | 2000 | **1000** | [781 219] | map[200:1000 429:1000] |
| 179165562 | 9 | **9** | [8 1] | map[200:9] |

- PASS: window 179165559 admitted 1000 (want 996..1000)
- PASS: window 179165560 admitted 1000 (want 996..1000)
- PASS: window 179165561 admitted 1000 (want 996..1000)

## manykeys — PASS

5000 cold keys on each of 2 edges, 256 concurrent requests, in 2.983s.

- lease requests: 10000 in **353 RPCs** (mean batch 28.3)
- admitted: 10000 of 10000

- PASS: every request admitted (10000)
- PASS: concurrent leases were batched (mean batch 28.3)

## drain — PASS

Both edges lease 400 of 1000 at the start of a 30s window; edge 1 is stopped (SIGTERM); edge 2 is flooded for the rest of the window.

- admitted in the window: **1000** (without lease return at most 601)
- statuses: map[200:998 429:9680]

- PASS: window admitted 1000 >= 950: the drained edge's lease was returned
- PASS: window admitted 1000 <= quota 1000

## Summary

- passed: windows throughput hotkey endgame manykeys drain
- failed: none
