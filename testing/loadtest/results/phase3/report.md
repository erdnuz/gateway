# Gate rate-limiter e2e: phase3

- commit: 56b006a (dirty)
- date: 2026-10-09T01:22:58Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241410048 bytes mem, Docker Desktop

## windows — PASS

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Expect admitted per window in [Q-200, Q+4], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179150899 | 1989 | **1000** | [500 500] | map[200:1000 429:989] |
| 179150900 | 2001 | **1001** | [500 501] | map[200:1001 429:1000] |
| 179150901 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179150902 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179150903 | 10 | **10** | [5 5] | map[200:10] |

- PASS: window 179150899 admitted 1000 (want 800..1004)
- PASS: window 179150900 admitted 1001 (want 800..1004)
- PASS: window 179150901 admitted 1000 (want 800..1004)
- PASS: window 179150902 admitted 1000 (want 800..1004)

## carryover — PASS

Window N: 10 rps for 2s (each edge leases 100, uses ~10). Window N+1: 300 rps. Expect admitted in N+1 <= Q+4.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179150905 | 2850 | **1000** | [500 500] | map[200:1000 429:1850] |

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
| 1 | 2 | 60 |
| 2 | 59 | 61 |

- PASS: edge 1 sees tier delete within 1s (2 ms)
- PASS: edge 1 sees config reload within 2s (60 ms)
- PASS: edge 2 sees tier delete within 1s (59 ms)
- PASS: edge 2 sees config reload within 2s (61 ms)

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 50053 | **5004** | 2.48 | 6.02 | **12.27** | 24.21 | 49.3 | map[200:50053] |
| 64 | 51786 | **5174** | 10.64 | 22.81 | **42.71** | 61.52 | 81.7 | map[200:51786] |
| 256 | 59016 | **5884** | 39.71 | 76.22 | **136.52** | 188.16 | 309.5 | map[200:59016] |


### edge Redis cost (throughput scenario)

- edge Redis commands: 162999 for 160855 requests = **1.01 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 172%
  - gate-lt-edge-1-redis-1: 22%
  - gate-lt-edge-2-1: 171%
  - gate-lt-edge-2-redis-1: 22%
  - gate-lt-hub-redis-1: 5%
  - gate-lt-hub-server-1: 12%
  - gate-lt-loadgen-run-fdc505fe38f7: 131%
  - gate-lt-nats-1: 2%
  - gate-lt-upstream-1: 118%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 79555 | **7925** | 29.24 | 58.43 | **90.37** | 144.60 | 174.6 | map[200:79555] |


## outage — PASS

Hub stopped. 100 rps on a `pro` key for 3 windows. Expect service to continue (degraded) with admitted <= S x edges = 100 per window (first window may also drain leftover lease tokens).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179150915 | 995 | **100** | [50 50] | map[200:100 429:895] |
| 179150916 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179150917 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179150918 | 5 | **5** | [2 3] | map[200:5] |

- PASS: window 179150915 still serves traffic during hub outage (admitted 100)
- PASS: window 179150916 still serves traffic during hub outage (admitted 100)
- PASS: window 179150916 admitted 100 <= safety cap 100
- PASS: window 179150917 still serves traffic during hub outage (admitted 100)
- PASS: window 179150917 admitted 100 <= safety cap 100

## outage-recovery — PASS

Hub restarted; 200 rps for one window on the same key. Expect normal quota (~Q=1000).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179150920 | 1900 | **1000** | [500 500] | map[200:1000 429:900] |

- PASS: recovered window admitted 1000 (want 800..1004)

## Summary

- passed: windows carryover isolation bypass propagation throughput hotkey outage outage-check
- failed: none
