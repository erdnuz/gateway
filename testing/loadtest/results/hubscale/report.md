# Hub scaling: hubscale

- commit: fed2d39 (dirty)
- date: 2026-10-10T22:11:39Z
- host: 8 cpu, 8241414144 bytes mem
- each hub and Redis shard capped at 1 CPU; loadgen 2 CPU, 64 callers, 64 leases per RPC, 100k keys

| topology | hubs | grants/s | RPCs/s | p50 ms | p99 ms | errors |
|---|---|---|---|---|---|---|
| 1 hub(s) + 1 shard(s) | 1 | **56723** | 886 | 70.20 | **93.76** | 0 |
| 2 hub(s) + 2 shard(s) | 2 | **66640** | 1041 | 58.03 | **127.63** | 0 |
| 3 hub(s) + 3 shard(s) | 3 | **82659** | 1292 | 48.94 | **85.88** | 0 |
