# Gate rate-limiter e2e: baseline

- commit: 253ef58
- date: 2026-10-08T02:31:46Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## windows — FAIL

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Expect admitted per window in [Q-200, Q+4], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179142672 | 1990 | **960** | [480 480] | map[200:960 429:1030] |
| 179142673 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179142674 | 2001 | **40** | [20 20] | map[200:40 429:1961] |
| 179142675 | 2000 | **0** | [0 0] | map[429:2000] |
| 179142676 | 9 | **0** | [0 0] | map[429:9] |

- PASS: window 179142672 admitted 960 (want 800..1004)
- PASS: window 179142673 admitted 1000 (want 800..1004)
- FAIL: window 179142674 admitted 40 (want 800..1004)
- FAIL: window 179142675 admitted 0 (want 800..1004)

## carryover — FAIL

Window N: 10 rps for 2s (each edge leases 100, uses ~10). Window N+1: 300 rps. Expect admitted in N+1 <= Q+4.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179142678 | 2850 | **1140** | [570 570] | map[200:1140 429:1710] |

- FAIL: window N+1 admitted 1140 (limit 1000)
- PASS: window N+1 admitted 1140 is not starved (>= 800)

## isolation — FAIL

Exhaust key on `svc` (1100 requests), then 50 requests to `svc2` in the same window.

- svc: map[200:960 429:140]
- svc2: map[429:50]

- FAIL: svc2 admitted 0/50 after svc exhausted

## bypass — FAIL

1000 requests with `X-API-Key: victim0/x<i>` (40 suffixes) plus 10 never-issued keys.

- statuses: map[200:1000 403:10]

- FAIL: 1000 malformed/unknown-key requests admitted (want 0)

## propagation — FAIL

Delete a tier on the hub, then add a service via `/config-reload`; poll each edge. `-1` = never converged.

| edge | tier delete visible (ms) | config reload visible (ms) |
|---|---|---|
| 1 | -1 | -1 |
| 2 | 6 | -1 |

- FAIL: edge 1 sees tier delete within 1s (-1 ms)
- FAIL: edge 1 sees config reload within 2s (-1 ms)
- PASS: edge 2 sees tier delete within 1s (6 ms)
- FAIL: edge 2 sees config reload within 2s (-1 ms)

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 16327 | **1632** | 7.81 | 18.12 | **38.33** | 62.49 | 107.3 | map[200:16327] |
| 64 | 24090 | **2405** | 23.77 | 47.23 | **77.63** | 129.73 | 169.7 | map[200:24090] |
| 256 | 10221 | **989** | 167.71 | 567.66 | **818.01** | 957.64 | 1096.8 | map[200:10220 500:1] |


### edge Redis cost (throughput scenario)

- edge Redis commands: 282003 for 50638 requests = **5.57 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 218%
  - gate-lt-edge-1-redis-1: 33%
  - gate-lt-edge-2-1: 205%
  - gate-lt-edge-2-redis-1: 32%
  - gate-lt-hub-redis-1: 28%
  - gate-lt-hub-server-1: 82%
  - gate-lt-loadgen-run-80eb25fa1cd4: 89%
  - gate-lt-nats-1: 1%
  - gate-lt-upstream-1: 107%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 13768 | **1322** | 153.00 | 370.59 | **657.03** | 820.31 | 1069.3 | map[200:12634 429:337 503:797] |


## outage — FAIL

Hub stopped. 100 rps on a `pro` key for 3 windows. Expect service to continue (degraded) with admitted <= S x edges = 100 per window (first window may also drain leftover lease tokens).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179142690 | 995 | **0** | [0 0] | map[503:995] |
| 179142691 | 1000 | **0** | [0 0] | map[503:1000] |
| 179142692 | 1001 | **0** | [0 0] | map[503:1001] |
| 179142693 | 4 | **0** | [0 0] | map[503:4] |

- FAIL: window 179142690 still serves traffic during hub outage (admitted 0)
- FAIL: window 179142691 still serves traffic during hub outage (admitted 0)
- PASS: window 179142691 admitted 0 <= safety cap 100
- FAIL: window 179142692 still serves traffic during hub outage (admitted 0)
- PASS: window 179142692 admitted 0 <= safety cap 100

## outage-recovery — PASS

Hub restarted; 200 rps for one window on the same key. Expect normal quota (~Q=1000).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179142695 | 1900 | **960** | [480 480] | map[200:960 429:940] |

- PASS: recovered window admitted 960 (want 800..1004)

## Summary

- passed: throughput hotkey outage-check
- failed: windows carryover isolation bypass propagation outage
