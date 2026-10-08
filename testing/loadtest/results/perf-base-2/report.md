# Gate rate-limiter e2e: perf-base-2

- commit: 253ef58
- date: 2026-10-08T03:21:32Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 11718 | **1171** | 11.14 | 26.91 | **48.35** | 65.60 | 84.4 | map[200:11718] |
| 64 | 16824 | **1678** | 33.91 | 68.88 | **111.79** | 149.49 | 182.1 | map[200:16824] |
| 256 | 12531 | **1173** | 179.99 | 410.74 | **629.43** | 758.20 | 892.1 | map[200:12531] |


### edge Redis cost (throughput scenario)

- edge Redis commands: 236039 for 41073 requests = **5.75 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 187%
  - gate-lt-edge-1-redis-1: 34%
  - gate-lt-edge-2-1: 184%
  - gate-lt-edge-2-redis-1: 34%
  - gate-lt-hub-redis-1: 16%
  - gate-lt-hub-server-1: 63%
  - gate-lt-loadgen-run-ba3065446ead: 76%
  - gate-lt-nats-1: 1%
  - gate-lt-upstream-1: 103%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 11204 | **1100** | 186.64 | 485.49 | **753.73** | 1018.95 | 1123.1 | map[200:10438 429:477 503:289] |


## Summary

- passed: throughput hotkey
- failed: none
