# Gate rate-limiter e2e: perf-after-2

- commit: 253ef58 (dirty)
- date: 2026-10-08T03:25:24Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 22156 | **2213** | 5.67 | 14.02 | **26.20** | 46.11 | 76.9 | map[200:22156] |
| 64 | 35464 | **3543** | 15.15 | 33.91 | **67.43** | 112.76 | 147.1 | map[200:35464] |
| 256 | 42354 | **4216** | 56.42 | 110.83 | **166.42** | 229.21 | 327.7 | map[200:42354] |


### edge Redis cost (throughput scenario)

- edge Redis commands: 102149 for 99974 requests = **1.02 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 166%
  - gate-lt-edge-1-redis-1: 22%
  - gate-lt-edge-2-1: 182%
  - gate-lt-edge-2-redis-1: 21%
  - gate-lt-hub-redis-1: 33%
  - gate-lt-hub-server-1: 90%
  - gate-lt-loadgen-run-44f1efa656f4: 122%
  - gate-lt-nats-1: 1%
  - gate-lt-upstream-1: 101%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 50030 | **4973** | 48.76 | 90.06 | **140.48** | 186.19 | 225.2 | map[200:50030] |


## Summary

- passed: throughput hotkey
- failed: none
