# Gate rate-limiter e2e: phase1

- commit: b3514e4 (dirty)
- date: 2026-10-08T04:38:00Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241418240 bytes mem, Docker Desktop

## windows — PASS

Offered 200 rps (2x quota) on one key, round-robin over 2 edges. Expect admitted per window in [Q-200, Q+4], Q=1000.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179143429 | 1992 | **1001** | [500 501] | map[200:1001 429:991] |
| 179143430 | 1999 | **999** | [500 499] | map[200:999 429:1000] |
| 179143431 | 2001 | **1001** | [500 501] | map[200:1001 429:1000] |
| 179143432 | 2000 | **1001** | [501 500] | map[200:1001 429:999] |
| 179143433 | 8 | **8** | [4 4] | map[200:8] |

- PASS: window 179143429 admitted 1001 (want 800..1004)
- PASS: window 179143430 admitted 999 (want 800..1004)
- PASS: window 179143431 admitted 1001 (want 800..1004)
- PASS: window 179143432 admitted 1001 (want 800..1004)

## carryover — PASS

Window N: 10 rps for 2s (each edge leases 100, uses ~10). Window N+1: 300 rps. Expect admitted in N+1 <= Q+4.

| window | sent | admitted | per edge | statuses |
|---|---|---|---|---|
| 179143435 | 2850 | **1002** | [501 501] | map[200:1002 429:1848] |

- PASS: window N+1 admitted 1002 (limit 1000)
- PASS: window N+1 admitted 1002 is not starved (>= 800)

## isolation — PASS

Exhaust key on `svc` (1100 requests), then 50 requests to `svc2` in the same window.

- svc: map[200:1000 429:100]
- svc2: map[200:50]

- PASS: svc2 admitted 50/50 after svc exhausted
