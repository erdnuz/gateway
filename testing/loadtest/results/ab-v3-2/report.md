# Gate rate-limiter e2e: ab-v3-2

- commit: 8dfc752 (dirty)
- date: 2026-10-10T18:27:25Z
- topology: hub(2 cpu) + hub Redis + 2 edges(2 cpu each, in-memory caches) + 2 gate limiter replicas(1 cpu each) + upstream; loadgen 4 cpu
- host: 8 cpu, 8241418240 bytes mem, Docker Desktop

## throughput

1000 keys (bulk tier, quota never binding), closed loop, 10s per level, 2 edges.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 16 | 24502 | **2450** | 4.09 | 14.42 | **32.27** | 57.47 | 67.6 | map[200:24502] |
| 64 | 46906 | **4688** | 10.20 | 28.38 | **58.36** | 104.92 | 146.8 | map[200:46906] |
| 256 | 57653 | **5731** | 35.79 | 92.86 | **167.06** | 239.31 | 309.1 | map[200:57653] |


### control-plane cost (throughput scenario)

- hub Redis commands: 29311 for 129061 requests = **0.23 per request**
- peak CPU per container (docker stats samples):

  - gate-lt-edge-1-1: 169%
  - gate-lt-edge-2-1: 170%
  - gate-lt-hub-redis-1: 41%
  - gate-lt-hub-server-1: 72%
  - gate-lt-limiter-1-1: 16%
  - gate-lt-limiter-2-1: 16%
  - gate-lt-loadgen-run-385a722dcd1b: 179%
  - gate-lt-upstream-1: 145%

## hotkey

Single key `hot0` (bulk tier), 256 concurrent workers, 10s.

| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |
|---|---|---|---|---|---|---|---|---|
| 256 | 33158 | **3295** | 61.69 | 160.64 | **319.80** | 475.24 | 587.6 | map[200:33158] |


## Summary

- passed: throughput hotkey
- failed: none
