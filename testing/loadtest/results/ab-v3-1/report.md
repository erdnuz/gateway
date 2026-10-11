# Gate rate-limiter e2e: ab-v3-1

- commit: 8dfc752 (dirty)
- date: 2026-10-10T18:24:15Z
- topology: hub(2 cpu) + hub Redis + 2 edges(2 cpu each, in-memory caches) + 2 gate limiter replicas(1 cpu each) + upstream; loadgen 4 cpu
- host: 8 cpu, 8241418240 bytes mem, Docker Desktop

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 51811 | **5180** | 2.41 | 5.81 | **12.23** | 20.91 | 50.2 | map[200:51811] |
| 64 | 66869 | **6683** | 7.84 | 18.79 | **34.63** | 59.16 | 86.7 | map[200:66869] |
| 256 | 70479 | **7005** | 29.70 | 76.89 | **131.81** | 180.03 | 211.8 | map[200:70479] |


### control-plane cost (throughput scenario)

- hub Redis commands: 29280 for 189159 requests = **0.15 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 163%
  - gate-lt-edge-2-1: 163%
  - gate-lt-hub-redis-1: 23%
  - gate-lt-hub-server-1: 47%
  - gate-lt-limiter-1-1: 9%
  - gate-lt-limiter-2-1: 11%
  - gate-lt-loadgen-run-fa7a4075748d: 152%
  - gate-lt-upstream-1: 150%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 82803 | **8251** | 24.92 | 64.65 | **109.01** | 143.65 | 175.1 | map[200:82803] |


## Summary

- passed: throughput hotkey
- failed: none
