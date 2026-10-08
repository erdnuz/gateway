# Gate rate-limiter e2e: perf-after-1

- commit: 253ef58 (dirty)
- date: 2026-10-08T03:17:04Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 18068 | **1804** | 5.83 | 18.03 | **51.93** | 94.35 | 105.3 | map[200:18068] |
| 64 | 26658 | **2656** | 20.30 | 47.02 | **81.66** | 113.60 | 173.9 | map[200:26658] |
| 256 | 18126 | **1792** | 120.97 | 253.30 | **588.87** | 778.91 | 858.7 | map[200:18126] |


### edge Redis cost (throughput scenario)

- edge Redis commands: 64791 for 62852 requests = **1.03 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 169%
  - gate-lt-edge-1-redis-1: 20%
  - gate-lt-edge-2-1: 195%
  - gate-lt-edge-2-redis-1: 25%
  - gate-lt-hub-redis-1: 43%
  - gate-lt-hub-server-1: 120%
  - gate-lt-loadgen-run-ab4a312c6048: 120%
  - gate-lt-nats-1: 3%
  - gate-lt-upstream-1: 104%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 43323 | **4287** | 52.70 | 105.21 | **188.17** | 312.38 | 560.5 | map[200:43323] |


## Summary

- passed: throughput hotkey
- failed: none
