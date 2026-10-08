# Gate rate-limiter e2e: perf-base-1

- commit: 253ef58
- date: 2026-10-08T03:13:01Z
- topology: hub(2 cpu) + 2 edges(2 cpu each, private Redis) + NATS + upstream; loadgen 4 cpu
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 14345 | **1433** | 9.10 | 20.54 | **44.17** | 58.30 | 112.0 | map[200:14345] |
| 64 | 12044 | **1199** | 46.02 | 99.58 | **161.16** | 216.40 | 243.4 | map[200:12044] |
| 256 | 12542 | **1218** | 185.51 | 380.81 | **635.15** | 839.03 | 1169.2 | map[200:12542] |


### edge Redis cost (throughput scenario)

- edge Redis commands: 225419 for 38931 requests = **5.79 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 174%
  - gate-lt-edge-1-redis-1: 36%
  - gate-lt-edge-2-1: 165%
  - gate-lt-edge-2-redis-1: 34%
  - gate-lt-hub-redis-1: 26%
  - gate-lt-hub-server-1: 72%
  - gate-lt-loadgen-run-ffdcebc543b1: 89%
  - gate-lt-nats-1: 4%
  - gate-lt-upstream-1: 101%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 12743 | **1237** | 194.65 | 390.03 | **560.64** | 740.44 | 864.7 | map[200:11823 429:722 503:198] |


## Summary

- passed: throughput hotkey
- failed: none
