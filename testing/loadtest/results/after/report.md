# Gate rate-limiter e2e: after

- commit: 253ef58 (dirty)
- date: 2026-10-08T03:42:22Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## windows — PASS

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Expect admitted per window in [Q-200, Q+4], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179143095 | 1991 | **1000** | [500 500] | map[200:1000 429:991] |
| 179143096 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179143097 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179143098 | 2002 | **1000** | [500 500] | map[200:1000 429:1002] |
| 179143099 | 7 | **7** | [3 4] | map[200:7] |

- PASS: window 179143095 admitted 1000 (want 800..1004)
- PASS: window 179143096 admitted 1000 (want 800..1004)
- PASS: window 179143097 admitted 1000 (want 800..1004)
- PASS: window 179143098 admitted 1000 (want 800..1004)

## carryover — PASS

Window N: 10 rps for 2s (each edge leases 100, uses ~10). Window N+1: 300 rps. Expect admitted in N+1 <= Q+4.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179143101 | 2850 | **1000** | [500 500] | map[200:1000 429:1850] |

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
| 1 | 2 | 53 |
| 2 | 2 | 53 |

- PASS: edge 1 sees tier delete within 1s (2 ms)
- PASS: edge 1 sees config reload within 2s (53 ms)
- PASS: edge 2 sees tier delete within 1s (2 ms)
- PASS: edge 2 sees config reload within 2s (53 ms)

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 41194 | **4119** | 3.16 | 7.26 | **12.63** | 18.86 | 37.6 | map[200:41194] |
| 64 | 49836 | **4978** | 11.46 | 23.05 | **39.24** | 61.95 | 100.1 | map[200:49836] |
| 256 | 56886 | **5665** | 42.92 | 76.32 | **120.54** | 157.15 | 213.0 | map[200:56886] |


### edge Redis cost (throughput scenario)

- edge Redis commands: 150082 for 147916 requests = **1.01 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 170%
  - gate-lt-edge-1-redis-1: 21%
  - gate-lt-edge-2-1: 166%
  - gate-lt-edge-2-redis-1: 22%
  - gate-lt-hub-redis-1: 23%
  - gate-lt-hub-server-1: 27%
  - gate-lt-loadgen-run-9066a8624725: 120%
  - gate-lt-nats-1: 1%
  - gate-lt-upstream-1: 111%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 32858 | **3249** | 66.12 | 152.24 | **253.63** | 392.35 | 460.4 | map[200:32858] |


## outage — PASS

Hub stopped. 100 rps on a `pro` key for 3 windows. Expect service to continue (degraded) with admitted <= S x edges = 100 per window (first window may also drain leftover lease tokens).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179143112 | 994 | **100** | [50 50] | map[200:100 429:894] |
| 179143113 | 1001 | **100** | [50 50] | map[200:100 429:901] |
| 179143114 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179143115 | 5 | **5** | [2 3] | map[200:5] |

- PASS: window 179143112 still serves traffic during hub outage (admitted 100)
- PASS: window 179143113 still serves traffic during hub outage (admitted 100)
- PASS: window 179143113 admitted 100 <= safety cap 100
- PASS: window 179143114 still serves traffic during hub outage (admitted 100)
- PASS: window 179143114 admitted 100 <= safety cap 100

## outage-recovery — PASS

Hub restarted; 200 rps for one window on the same key. Expect normal quota (~Q=1000).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179143117 | 1900 | **1000** | [500 500] | map[200:1000 429:900] |

- PASS: recovered window admitted 1000 (want 800..1004)

## Summary

- passed: windows carryover isolation bypass propagation throughput hotkey outage outage-check
- failed: none
