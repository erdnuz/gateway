# Rate-limiter benchmark: v3

- commit: f7ffc52 (dirty)
- date: 2026-10-10T22:41:39Z
- host: 8 cpu, 8241414144 bytes mem, Docker Desktop
- every data-plane replica, rate-limit service, hub and Redis is capped at 1 CPU; the load generator at 2
- every key's limit: 500 per second (1 s windows)
- accuracy: one key offered 2x its quota for 10 s (most / mean admitted per full second)
- cold: 1200 rps open loop over 1000 keys for 20 s (about 1 request per key per second: leasing's worst case)
- busy: 1200 rps open loop over 20 keys for 20 s (60 requests per key per second)

| system | replicas | phase | admitted per s (max / mean) | rps | p50 ms | p99 ms | p99.9 ms | errors | Redis cmds / req |
|---|---|---|---|---|---|---|---|---|---|
| gate-x1 | 1 | accuracy | **500** / 500 | 1000 | 0.85 | **2.44** | 16.00 | 0 | 0.056 |
| gate-x1 | 1 | cold | - | 1200 | 11.18 | **3916.75** | 4858.63 | 11 | 9.309 |
| gate-x1 | 1 | busy | - | 1200 | 1.07 | **34.49** | 61.28 | 0 | 0.197 |
| gate-x2 | 2 | accuracy | **500** / 500 | 1000 | 0.86 | **1.95** | 3.93 | 0 | 0.125 |
| gate-x2 | 2 | cold | - | 1200 | 2.53 | **60.75** | 101.92 | 0 | 8.402 |
| gate-x2 | 2 | busy | - | 1200 | 0.82 | **2.93** | 15.76 | 0 | 0.181 |
| gate-x4 | 4 | accuracy | **500** / 500 | 1000 | 0.89 | **5.27** | 24.20 | 0 | 0.264 |
| gate-x4 | 4 | cold | - | 1200 | 9.59 | **193.98** | 294.77 | 0 | 8.545 |
| gate-x4 | 4 | busy | - | 1200 | 1.46 | **34.07** | 59.67 | 0 | 0.194 |
| counter-x1 | 1 | accuracy | **500** / 500 | 1000 | 1.24 | **49.92** | 103.78 | 0 | 2.494 |
| counter-x1 | 1 | cold | - | 1200 | 1.37 | **60.16** | 137.60 | 0 | 2.850 |
| counter-x1 | 1 | busy | - | 1200 | 2500.14 | **5008.09** | 5023.78 | 5503 | 1.726 |
| counter-x2 | 2 | accuracy | **500** / 500 | 1000 | 4.47 | **233.73** | 515.17 | 0 | 2.542 |
| counter-x2 | 2 | cold | - | 1200 | 2.17 | **1352.91** | 2004.17 | 0 | 2.869 |
| counter-x2 | 2 | busy | - | 1200 | 1.85 | **52.18** | 101.89 | 0 | 2.022 |
| counter-x4 | 4 | accuracy | **500** / 500 | 1000 | 2.01 | **144.17** | 212.97 | 0 | 2.504 |
| counter-x4 | 4 | cold | - | 1200 | 1.93 | **15.59** | 42.12 | 0 | 2.846 |
| counter-x4 | 4 | busy | - | 1200 | 2.49 | **52.01** | 90.54 | 0 | 2.022 |
| envoy-ref-x1 | 1 | accuracy | **502** / 482 | 1000 | 3.97 | **653.64** | 697.78 | 0 | 1.037 |
| envoy-ref-x1 | 1 | cold | - | 1200 | 7.21 | **393.59** | 461.05 | 0 | 2.006 |
| envoy-ref-x1 | 1 | busy | - | 1200 | 9.71 | **230.59** | 261.04 | 0 | 2.006 |
| envoy-ref-x2 | 2 | accuracy | **502** / 500 | 1000 | 2.72 | **32.32** | 76.62 | 0 | 1.051 |
| envoy-ref-x2 | 2 | cold | - | 1200 | 13.50 | **184.53** | 259.39 | 0 | 2.006 |
| envoy-ref-x2 | 2 | busy | - | 1200 | 6.58 | **110.15** | 151.33 | 0 | 2.006 |
| envoy-ref-x4 | 4 | accuracy | **501** / 500 | 1000 | 2.93 | **36.54** | 50.88 | 0 | 1.056 |
| envoy-ref-x4 | 4 | cold | - | 1200 | 12.46 | **384.17** | 638.72 | 0 | 2.006 |
| envoy-ref-x4 | 4 | busy | - | 1200 | 7.19 | **155.09** | 201.57 | 0 | 2.006 |
| envoy-gate-x1 | 1 | accuracy | **502** / 500 | 1000 | 3.95 | **348.07** | 416.85 | 0 | 0.050 |
| envoy-gate-x1 | 1 | cold | - | 1200 | 88.48 | **2580.52** | 2710.89 | 0 | 7.319 |
| envoy-gate-x1 | 1 | busy | - | 1200 | 5000.16 | **5178.05** | 5326.40 | 15437 | 0.161 |
| envoy-gate-x2 | 2 | accuracy | **502** / 500 | 1000 | 2.83 | **62.84** | 86.77 | 0 | 0.051 |
| envoy-gate-x2 | 2 | cold | - | 1200 | 7.98 | **242.14** | 374.52 | 0 | 7.599 |
| envoy-gate-x2 | 2 | busy | - | 1200 | 2.96 | **200.96** | 236.88 | 0 | 0.169 |
| envoy-gate-x4 | 4 | accuracy | **501** / 500 | 1000 | 2.15 | **5.60** | 13.44 | 0 | 0.049 |
| envoy-gate-x4 | 4 | cold | - | 1200 | 5.18 | **91.04** | 155.30 | 0 | 7.571 |
| envoy-gate-x4 | 4 | busy | - | 1200 | 2.35 | **22.08** | 39.57 | 0 | 0.170 |
