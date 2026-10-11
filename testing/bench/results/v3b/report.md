# Rate-limiter benchmark: v3b

- commit: d956448
- date: 2026-10-11T05:12:42Z
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop
- every data-plane replica, rate-limit service, hub and Redis is capped at 1 CPU; the load generator at 2
- every key's limit: 500 per second (1 s windows)
- accuracy: one key offered 2x its quota for 10 s (most / mean admitted per full second)
- cold: 1200 rps open loop over 1000 keys for 20 s (about 1 request per key per second: leasing's worst case)
- busy: 1200 rps open loop over 20 keys for 20 s (60 requests per key per second)

| system | replicas | phase | admitted per s (max / mean) | rps | p50 ms | p99 ms | p99.9 ms | errors | Redis cmds / req |
|---|---|---|---|---|---|---|---|---|---|
| gate-x1 | 1 | accuracy | **500** / 500 | 1000 | 1.42 | **288.09** | 386.51 | 0 | 0.046 |
| gate-x1 | 1 | cold | - | 1200 | 5000.12 | **5042.48** | 5163.35 | 17333 | 5.041 |
| gate-x1 | 1 | busy | - | 1200 | 3301.77 | **5004.39** | 5020.50 | 14333 | 0.159 |
| gate-x2 | 2 | accuracy | **500** / 500 | 1000 | 1.28 | **65.27** | 170.91 | 0 | 0.106 |
| gate-x2 | 2 | cold | - | 1200 | 4.07 | **93.98** | 177.09 | 0 | 6.792 |
| gate-x2 | 2 | busy | - | 1200 | 1.32 | **37.12** | 205.56 | 0 | 0.155 |
| gate-x4 | 4 | accuracy | **500** / 500 | 1000 | 1.22 | **10.43** | 43.86 | 0 | 0.196 |
| gate-x4 | 4 | cold | - | 1200 | 4.08 | **106.21** | 193.22 | 0 | 6.804 |
| gate-x4 | 4 | busy | - | 1200 | 1.32 | **17.64** | 69.64 | 0 | 0.153 |
| envoy-gate-x1 | 1 | accuracy | **504** / 497 | 1000 | 4.16 | **282.77** | 319.74 | 0 | 0.044 |
| envoy-gate-x1 | 1 | cold | - | 1200 | 31.81 | **962.12** | 1042.47 | 0 | 6.708 |
| envoy-gate-x1 | 1 | busy | - | 1200 | 1397.08 | **3691.40** | 4030.59 | 758 | 0.157 |
| envoy-gate-x2 | 2 | accuracy | **508** / 498 | 1000 | 2.37 | **45.14** | 93.07 | 0 | 0.043 |
| envoy-gate-x2 | 2 | cold | - | 1200 | 15.52 | **177.81** | 209.72 | 0 | 6.762 |
| envoy-gate-x2 | 2 | busy | - | 1200 | 2.54 | **51.96** | 81.87 | 0 | 0.148 |
| envoy-gate-x4 | 4 | accuracy | **517** / 500 | 1000 | 2.98 | **78.72** | 149.14 | 0 | 0.047 |
| envoy-gate-x4 | 4 | cold | - | 1200 | 13.72 | **296.71** | 527.81 | 0 | 6.760 |
| envoy-gate-x4 | 4 | busy | - | 1200 | 2.52 | **34.80** | 67.26 | 0 | 0.152 |
