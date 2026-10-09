# Gate rate-limiter e2e: final

- commit: 53cdceb
- date: 2026-10-09T13:48:33Z
- topology: hub(2 cpu) + hub Redis + 2 edges(2 cpu each, in-memory caches) + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## windows — PASS

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Expect admitted per window in [Q-200, Q+4], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179155373 | 1990 | **1001** | [500 501] | map[200:1001 429:989] |
| 179155374 | 2000 | **999** | [500 499] | map[200:999 429:1001] |
| 179155375 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179155376 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179155377 | 10 | **10** | [5 5] | map[200:10] |

- PASS: window 179155373 admitted 1001 (want 800..1004)
- PASS: window 179155374 admitted 999 (want 800..1004)
- PASS: window 179155375 admitted 1000 (want 800..1004)
- PASS: window 179155376 admitted 1000 (want 800..1004)

## carryover — PASS

Window N: 10 rps for 2s (each edge leases 100, uses ~10). Window N+1: 300 rps. Expect admitted in N+1 <= Q+4.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179155379 | 2850 | **1000** | [500 500] | map[200:1000 429:1850] |

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
| 1 | 1 | 3 |
| 2 | 1 | 56 |

- PASS: edge 1 sees tier delete within 1s (1 ms)
- PASS: edge 1 sees config reload within 2s (3 ms)
- PASS: edge 2 sees tier delete within 1s (1 ms)
- PASS: edge 2 sees config reload within 2s (56 ms)

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 43298 | **4328** | 2.69 | 6.93 | **18.15** | 47.83 | 82.4 | map[200:43298] |
| 64 | 46811 | **4680** | 9.60 | 28.81 | **69.07** | 116.53 | 202.3 | map[200:46811] |
| 256 | 52093 | **5193** | 37.81 | 101.60 | **237.67** | 349.86 | 421.7 | map[200:52093] |


### control-plane cost (throughput scenario)

- hub Redis commands: 20226 for 142202 requests = **0.14 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 161%
  - gate-lt-edge-2-1: 166%
  - gate-lt-hub-redis-1: 8%
  - gate-lt-hub-server-1: 21%
  - gate-lt-loadgen-run-7cea59d25d66: 156%
  - gate-lt-upstream-1: 156%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 39306 | **3888** | 52.66 | 130.65 | **253.59** | 326.90 | 417.4 | map[200:39306] |


## outage — PASS

Hub stopped. 100 rps on a `pro` key for 3 windows. Expect service to continue (degraded) with admitted <= S x edges = 100 per window (first window may also drain leftover lease tokens).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179155391 | 995 | **100** | [50 50] | map[200:100 429:895] |
| 179155392 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179155393 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179155394 | 5 | **5** | [2 3] | map[200:5] |

- PASS: window 179155391 still serves traffic during hub outage (admitted 100)
- PASS: window 179155392 still serves traffic during hub outage (admitted 100)
- PASS: window 179155392 admitted 100 <= safety cap 100
- PASS: window 179155393 still serves traffic during hub outage (admitted 100)
- PASS: window 179155393 admitted 100 <= safety cap 100

## outage-recovery — PASS

Hub restarted; 200 rps for one window on the same key. Expect normal quota (~Q=1000).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179155396 | 1900 | **1000** | [500 500] | map[200:1000 429:900] |

- PASS: recovered window admitted 1000 (want 800..1004)

## Summary

- passed: windows carryover isolation bypass propagation throughput hotkey outage outage-check
- failed: none
