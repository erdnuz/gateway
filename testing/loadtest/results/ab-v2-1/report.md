# Gate rate-limiter e2e: ab-v2-1

- commit: 29a265f
- date: 2026-10-10T18:22:36Z
- topology: hub(2 cpu) + hub Redis + 2 edges(2 cpu each, in-memory caches) + upstream; loadgen 4 cpu
- host: 8 cpu, 8241418240 bytes mem, Docker Desktop

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 55142 | **5513** | 2.31 | 5.37 | **11.33** | 18.03 | 40.3 | map[200:55142] |
| 64 | 60669 | **6056** | 8.26 | 21.40 | **40.38** | 62.94 | 107.7 | map[200:60669] |
| 256 | 64902 | **6460** | 33.27 | 81.58 | **148.70** | 210.54 | 279.0 | map[200:64902] |


### control-plane cost (throughput scenario)

- hub Redis commands: 20243 for 180713 requests = **0.11 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 158%
  - gate-lt-edge-2-1: 158%
  - gate-lt-hub-redis-1: 20%
  - gate-lt-hub-server-1: 44%
  - gate-lt-loadgen-run-1b3489c657dc: 152%
  - gate-lt-upstream-1: 149%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 74248 | **7402** | 29.20 | 68.44 | **106.01** | 136.69 | 194.9 | map[200:74248] |


## Summary

- passed: throughput hotkey
- failed: none
