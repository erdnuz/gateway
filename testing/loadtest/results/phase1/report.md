# Gate rate-limiter e2e: phase1

- commit: e08e181 (dirty)
- date: 2026-10-08T10:21:18Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241418240 bytes mem, Docker Desktop

## windows — PASS

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Expect admitted per window in [Q-200, Q+4], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179145489 | 1990 | **1000** | [500 500] | map[200:1000 429:990] |
| 179145490 | 2001 | **1001** | [501 500] | map[200:1001 429:1000] |
| 179145491 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179145492 | 2000 | **1000** | [500 500] | map[200:1000 429:1000] |
| 179145493 | 9 | **9** | [4 5] | map[200:9] |

- PASS: window 179145489 admitted 1000 (want 800..1004)
- PASS: window 179145490 admitted 1001 (want 800..1004)
- PASS: window 179145491 admitted 1000 (want 800..1004)
- PASS: window 179145492 admitted 1000 (want 800..1004)

## carryover — PASS

Window N: 10 rps for 2s (each edge leases 100, uses ~10). Window N+1: 300 rps. Expect admitted in N+1 <= Q+4.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179145495 | 2850 | **1002** | [501 501] | map[200:1002 429:1848] |

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
| 1 | 3 | 55 |
| 2 | 58 | 55 |

- PASS: edge 1 sees tier delete within 1s (3 ms)
- PASS: edge 1 sees config reload within 2s (55 ms)
- PASS: edge 2 sees tier delete within 1s (58 ms)
- PASS: edge 2 sees config reload within 2s (55 ms)

## outage — PASS

Hub stopped. 100 rps on a `pro` key for 3 windows. Expect service to continue (degraded) with admitted <= S x edges = 100 per window (first window may also drain leftover lease tokens).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179145499 | 996 | **101** | [50 51] | map[200:101 429:895] |
| 179145500 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179145501 | 1000 | **100** | [50 50] | map[200:100 429:900] |
| 179145502 | 4 | **4** | [2 2] | map[200:4] |

- PASS: window 179145499 still serves traffic during hub outage (admitted 101)
- PASS: window 179145500 still serves traffic during hub outage (admitted 100)
- PASS: window 179145500 admitted 100 <= safety cap 100
- PASS: window 179145501 still serves traffic during hub outage (admitted 100)
- PASS: window 179145501 admitted 100 <= safety cap 100

## outage-recovery — PASS

Hub restarted; 200 rps for one window on the same key. Expect normal quota (~Q=1000).

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179145504 | 1900 | **1000** | [500 500] | map[200:1000 429:900] |

- PASS: recovered window admitted 1000 (want 800..1004)

## Summary

- passed: windows carryover isolation bypass propagation outage outage-check
- failed: none
