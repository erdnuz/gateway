# Rate limiter: baseline vs. fixed

Runs of [`run.sh`](../run.sh) on one Docker Desktop host (8 CPU, 8 GB).
`baseline` = commit 253ef58 unmodified; `after` = the audit fixes.
Raw reports: [baseline](baseline/report.md), [after](after/report.md),
A/B perf pairs `perf-base-{1,2}` / `perf-after-{1,2}` (run interleaved:
base, after, base, after — single runs on this host vary up to 2×).

## Correctness (Q = 1000 per 10 s window, 2 edges, safety tier S = 50)

| scenario | baseline | after |
| --- | --- | --- |
| windows: admitted per window, 4 windows at 2× load | 960, 1000, **40, 0** (key locked out for good) | **1000, 1000, 1000, 1000** |
| carryover: window after a trickle | **1140** (> Q) | **1000** |
| isolation: other service after `svc` exhausted | **0 / 50** admitted | 50 / 50 |
| bypass: `X-API-Key: victim0/x<i>` | **1000 / 1000 admitted** | 0 (all 401) |
| propagation: tier delete / config reload reach both edges | one edge / **neither** | 2 ms / 53 ms on both |
| hub outage: traffic served per window | **0** (every request 503) | 100 = S × edges |
| outage recovery | 960 | 1000 |

## Performance (1000 keys, quota never binding; hot key = 1 key)

| | baseline (2 runs) | after (2 A/B runs + final) |
| --- | --- | --- |
| c16 rps | 1,433 / 1,171 | 1,804 / 2,213 / 4,119 |
| c64 rps | 1,199 / 1,678 | 2,656 / 3,543 / 4,978 |
| c256 rps | 1,218 / 1,173 | 1,792 / 4,216 / 5,665 |
| c256 p99 | 635 / 629 ms | 589 / 166 / 121 ms |
| hot key c256 rps | 1,237 / 1,100 | 4,287 / 4,973 / 3,249 |
| hot key errors | 920 / 766 (spurious 429 + 503) | 0 |
| edge Redis commands / request | 5.8 | 1.0 |

`perf-after-1`'s c256 row predates the readiness fix (a Redis PING slower
than 150 ms marked the edge unready and 503'd all traffic for 2 s).

## What changed

- Per-window accounting aligned with the hub (`unix / period`); leftover lease tokens dropped at roll-over.
- Counters keyed by prefix + service + key.
- Safety tier is a real per-window limit.
- API keys validated at the edge against `api_key_pattern`; hub rejections (unknown tier/key) are 403, not a safety-tier fallback.
- Lease tokens in process memory by default (`EDGE_LEASE_STORE=redis` to share between replicas); one decision ≈ 0.3–0.7 µs.
- Lease misses retry while the hub still grants; held tokens stay spendable while the hub is exhausted.
- Config reload and tier updates fan out over NATS to every edge.
- Edge serves traffic through hub/analytics outages; `/readyz` tracks only edge-local health.
