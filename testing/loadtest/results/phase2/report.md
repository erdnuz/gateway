# Gate rate-limiter e2e: phase2

- commit: 39d2429
- date: 2026-10-08T10:42:24Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241418240 bytes mem, Docker Desktop

## windows — PASS

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Expect admitted per window in [Q-200, Q+4], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179145616 | 1990 | **1000** | [500 500] | map[200:1000 429:990] |
| 179145617 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179145618 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179145619 | 2002 | **1000** | [500 500] | map[200:1000 429:1002] |
| 179145620 | 8 | **8** | [4 4] | map[200:8] |

- PASS: window 179145616 admitted 1000 (want 800..1004)
- PASS: window 179145617 admitted 1000 (want 800..1004)
- PASS: window 179145618 admitted 1000 (want 800..1004)
- PASS: window 179145619 admitted 1000 (want 800..1004)

## carryover — PASS

Window N: 10 rps for 2s (each edge leases 100, uses ~10). Window N+1: 300 rps. Expect admitted in N+1 <= Q+4.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179145622 | 2850 | **1002** | [501 501] | map[200:1002 429:1848] |

- PASS: window N+1 admitted 1002 (limit 1000)
- PASS: window N+1 admitted 1002 is not starved (>= 800)

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
| 1 | 2 | 57 |
| 2 | 6 | 8 |

- PASS: edge 1 sees tier delete within 1s (2 ms)
- PASS: edge 1 sees config reload within 2s (57 ms)
- PASS: edge 2 sees tier delete within 1s (6 ms)
- PASS: edge 2 sees config reload within 2s (8 ms)

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 10130 | **1011** | 12.60 | 33.20 | **61.87** | 93.54 | 110.0 | map[200:10130] |
| 64 | 14197 | **1417** | 38.27 | 87.57 | **147.56** | 222.17 | 335.7 | map[200:14197] |
| 256 | 31088 | **3087** | 67.56 | 154.96 | **382.48** | 685.49 | 867.5 | map[200:31088] |


### edge Redis cost (throughput scenario)

- edge Redis commands: 59423 for 55415 requests = **1.07 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 192%
  - gate-lt-edge-1-redis-1: 24%
  - gate-lt-edge-2-1: 182%
  - gate-lt-edge-2-redis-1: 20%
  - gate-lt-hub-redis-1: 25%
  - gate-lt-hub-server-1: 33%
  - gate-lt-loadgen-run-92b4b7fb4713: 130%
  - gate-lt-nats-1: 1%
  - gate-lt-upstream-1: 100%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 19523 | **1940** | 117.27 | 237.39 | **416.04** | 534.02 | 603.7 | map[200:19523] |


## outage — PASS

Hub stopped. 100 rps on a `pro` key for 3 windows. Expect service to continue (degraded) with admitted <= S x edges = 100 per window (first window may also drain leftover lease tokens).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179149161 | 992 | **100** | [50 50] | map[200:100 429:892] |
| 179149162 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179149163 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179149164 | 8 | **8** | [4 4] | map[200:8] |

- PASS: window 179149161 still serves traffic during hub outage (admitted 100)
- PASS: window 179149162 still serves traffic during hub outage (admitted 100)
- PASS: window 179149162 admitted 100 <= safety cap 100
- PASS: window 179149163 still serves traffic during hub outage (admitted 100)
- PASS: window 179149163 admitted 100 <= safety cap 100

## outage-recovery — PASS

Hub restarted; 200 rps for one window on the same key. Expect normal quota (~Q=1000).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179149166 | 1900 | **1000** | [500 500] | map[200:1000 429:900] |

- PASS: recovered window admitted 1000 (want 800..1004)

## Summary

- passed: windows carryover isolation bypass propagation throughput hotkey outage outage-check
- failed: none
