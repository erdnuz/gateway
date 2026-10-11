# Gate rate-limiter e2e: v3-final

- commit: b3d6ba8
- date: 2026-10-11T05:26:33Z
- topology: hub(2 cpu) + hub Redis + 2 edges(2 cpu each, in-memory caches) + 2 gate limiter replicas(1 cpu each) + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## windows — PASS

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Requests are attributed to the hub window that charged them (Gate-Window). Expect admitted per window in [Q-200, Q], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179169643 | 1989 | **1000** | [512 488] | map[200:1000 429:989] |
| 179169644 | 2001 | **1000** | [491 509] | map[200:1000 429:1001] |
| 179169645 | 2000 | **1000** | [505 495] | map[200:1000 429:1000] |
| 179169646 | 2000 | **1000** | [509 491] | map[200:1000 429:1000] |
| 179169647 | 10 | **10** | [5 5] | map[200:10] |

- PASS: window 179169643 admitted 1000 (want 800..1000)
- PASS: window 179169644 admitted 1000 (want 800..1000)
- PASS: window 179169645 admitted 1000 (want 800..1000)
- PASS: window 179169646 admitted 1000 (want 800..1000)

## carryover — PASS

Window N: 10 rps for 2s (each edge leases up to 100, uses ~10). Window N+1: 300 rps. Expect admitted in N+1 <= Q.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179169649 | 2850 | **1000** | [513 487] | map[200:1000 429:1850] |

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
| 1 | 2 | 11 |
| 2 | 2 | 63 |

- PASS: edge 1 sees tier delete within 1s (2 ms)
- PASS: edge 1 sees config reload within 2s (11 ms)
- PASS: edge 2 sees tier delete within 1s (2 ms)
- PASS: edge 2 sees config reload within 2s (63 ms)

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 49757 | **4975** | 2.38 | 6.07 | **15.39** | 30.50 | 48.4 | map[200:49757] |
| 64 | 50828 | **5069** | 9.12 | 26.04 | **60.04** | 110.28 | 141.7 | map[200:50828] |
| 256 | 64060 | **6386** | 31.99 | 84.13 | **158.84** | 256.90 | 331.6 | map[200:64060] |


### control-plane cost (throughput scenario)

- hub Redis commands: 25392 for 164645 requests = **0.15 per request**
- peak CPU per container (docker stats samples):

  - buildx_buildkit_default: 0%
  - gate-lt-edge-1-1: 179%
  - gate-lt-edge-2-1: 164%
  - gate-lt-hub-redis-1: 24%
  - gate-lt-hub-server-1: 42%
  - gate-lt-limiter-1-1: 14%
  - gate-lt-limiter-2-1: 15%
  - gate-lt-loadgen-run-785ad5bf3a6f: 154%
  - gate-lt-upstream-1: 146%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 66435 | **6608** | 31.90 | 81.86 | **136.36** | 185.85 | 274.6 | map[200:66435] |


## endgame — PASS

200 rps (2x quota) on one key, 80% to edge 1 and 20% to edge 2. Expect every window in [Q-4, Q]: stranded quota is about one token per active edge, not a lease per edge.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179169660 | 1989 | **1000** | [777 223] | map[200:1000 429:989] |
| 179169661 | 2001 | **1000** | [782 218] | map[200:1000 429:1001] |
| 179169662 | 2001 | **1000** | [782 218] | map[200:1000 429:1001] |
| 179169663 | 9 | **9** | [8 1] | map[200:9] |

- PASS: window 179169660 admitted 1000 (want 996..1000)
- PASS: window 179169661 admitted 1000 (want 996..1000)
- PASS: window 179169662 admitted 1000 (want 996..1000)

## algorithms — PASS

**sliding_window** (Q=1000 per 10s), 200 rps:

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179169664 | 1988 | **1000** | [512 488] | map[200:1000 429:988] |
| 179169665 | 2002 | **998** | [345 653] | map[200:998 429:1004] |
| 179169666 | 2000 | **999** | [476 523] | map[200:999 429:1001] |
| 179169667 | 10 | **5** | [4 1] | map[200:5 429:5] |

- most admitted in any 10s span: **1005** (a fixed window can admit 2Q across a reset)

**token_bucket** (burst 100, 50/s), 200 rps for 20s:

- admitted **1099** (bound 900..1202)
- most in any second: 148

- PASS: sliding: window 179169664 admitted 1000 <= 1000
- PASS: sliding: window 179169665 admitted 998 <= 1000
- PASS: sliding: window 179169666 admitted 999 <= 1000
- PASS: sliding: any 10s span admitted 1005 <= 1.3Q
- PASS: bucket admitted 1099 in 20s (want 900..1202)

## multilimit — PASS

Ten keys (tier quota 1000 each) from one client address, 100 rps total, with `also: per-ip 300 per window`. Expect the per-IP limit to bind.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179169670 | 994 | **300** | [153 147] | map[200:300 429:694] |
| 179169671 | 1001 | **300** | [153 147] | map[200:300 429:701] |
| 179169672 | 5 | **5** | [2 3] | map[200:5] |

- PASS: window 179169670 admitted 300 (per-IP limit 300)
- PASS: window 179169671 admitted 300 (per-IP limit 300)

## llm — PASS

Quota 5000 tokens per window; each request reserves 50, the upstream reports 0-100, the edge settles the difference (max_debt 200). Expect tokens charged per window <= 5400.

| window | requests admitted | tokens used |
|---|---|---|
| 179169673 | 108 | **4938** |
| 179169674 | 101 | **4951** |
| 179169675 | 98 | **4996** |
| 179169676 | 2 | **21** |

- PASS: window 179169673 used 4938 tokens <= 5400
- PASS: window 179169673 used 4938 tokens: the quota is spent on actual, not estimated, cost (>= 3500)
- PASS: window 179169674 used 4951 tokens <= 5400
- PASS: window 179169674 used 4951 tokens: the quota is spent on actual, not estimated, cost (>= 3500)
- PASS: window 179169675 used 4996 tokens <= 5400
- PASS: window 179169675 used 4996 tokens: the quota is spent on actual, not estimated, cost (>= 3500)

## manykeys — PASS

5000 cold keys on each of 2 edges, 256 concurrent requests, in 2.299s.

- lease requests: 10000 in **397 RPCs** (mean batch 25.2)
- admitted: 10000 of 10000

- PASS: every request admitted (10000)
- PASS: concurrent leases were batched (mean batch 25.2)

## rls — PASS

Two gate limiter replicas, 100 checks/s alternating between them, one descriptor limited to 500 per window.

| window | admitted |
|---|---|
| 179169677 | **500** |
| 179169678 | **500** |
| 179169679 | **500** |
| 179169680 | **5** |

- PASS: window 179169678 admitted 500 (quota 500)
- PASS: window 179169679 admitted 500 (quota 500)

## drain — PASS

Both edges lease 400 of 1000 at the start of a 30s window; edge 1 is stopped (SIGTERM); edge 2 is flooded for the rest of the window.

- admitted in the window: **1000** (without lease return at most 601)
- statuses: map[200:998 429:8904]

- PASS: window admitted 1000 >= 950: the drained edge's lease was returned
- PASS: window admitted 1000 <= quota 1000

## outage — PASS

Hub stopped. 100 rps on a `pro` key for 3 windows. Expect service to continue (degraded) with admitted <= S x edges = 100 per window (first window may also drain leftover lease tokens).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179169686 | 995 | **100** | [50 50] | map[200:100 429:895] |
| 179169687 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179169688 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179169689 | 5 | **5** | [2 3] | map[200:5] |

- PASS: window 179169686 still serves traffic during hub outage (admitted 100)
- PASS: window 179169687 still serves traffic during hub outage (admitted 100)
- PASS: window 179169687 admitted 100 <= safety cap 100
- PASS: window 179169688 still serves traffic during hub outage (admitted 100)
- PASS: window 179169688 admitted 100 <= safety cap 100

## outage-recovery — PASS

Hub restarted; 200 rps for one window on the same key. Expect normal quota (~Q=1000).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179169691 | 1900 | **1000** | [492 508] | map[200:1000 429:900] |

- PASS: recovered window admitted 1000 (want 800..1000)

## Summary

- passed: windows carryover isolation bypass propagation throughput hotkey endgame algorithms multilimit llm manykeys rls drain outage outage-check
- failed: none
