# Gate rate-limiter e2e: ab-v2-2

- commit: 29a265f
- date: 2026-10-10T18:25:49Z
- topology: hub(2 cpu) + hub Redis + 2 edges(2 cpu each, in-memory caches) + upstream; loadgen 4 cpu
- host: 8 cpu, 8241418240 bytes mem, Docker Desktop

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 23314 | **2330** | 3.83 | 16.01 | **39.80** | 64.68 | 104.9 | map[200:23314] |
| 64 | 68915 | **6888** | 7.31 | 17.98 | **38.17** | 71.76 | 124.0 | map[200:68915] |
| 256 | 74754 | **7458** | 27.39 | 72.17 | **133.68** | 181.70 | 291.7 | map[200:74754] |


### control-plane cost (throughput scenario)

- hub Redis commands: 20282 for 166983 requests = **0.12 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 151%
  - gate-lt-edge-2-1: 160%
  - gate-lt-hub-redis-1: 36%
  - gate-lt-hub-server-1: 135%
  - gate-lt-loadgen-run-eec62a170de9: 158%
  - gate-lt-upstream-1: 136%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 66483 | **6618** | 31.17 | 77.63 | **147.25** | 247.29 | 337.2 | map[200:66483] |


## Summary

- passed: throughput hotkey
- failed: none
