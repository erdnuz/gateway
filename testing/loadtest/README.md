# Rate-limiter load test

End-to-end correctness and performance checks for edge rate limiting, run
against a production-shaped stack: hub + hub Redis, NATS, two edges that each
own a private Redis, and a minimal upstream
([compose.yaml](compose.yaml)).

```bash
testing/loadtest/run.sh <label>          # e.g. baseline, after
KEEP=1 testing/loadtest/run.sh dev       # leave the stack running afterwards
LT_EDGE_LEASE_STORE=redis testing/loadtest/run.sh redis-store
```

Requires Docker only; dev certificates are generated on first run
(`deploy/gen-dev-certs.sh`). Output lands in `results/<label>/`:
`report.md` (human summary) and one JSON file per scenario.

Config: [gateway-config.loadtest.yaml](gateway-config.loadtest.yaml) — prefix
`lt`, 10 s quota window, `pro` tier Q=1000, `bulk` tier effectively unlimited,
safety tier S=50, lease quantum 100.

| scenario | checks |
| --- | --- |
| `windows` | 2× quota on one key across both edges for 4 windows; admitted per window ≈ Q |
| `carryover` | tokens leased but unused in window N must not inflate window N+1 |
| `isolation` | exhausting a key on one service leaves its quota on another service intact |
| `bypass` | `X-API-Key: <key>/<suffix>` and never-issued keys are never admitted |
| `propagation` | tier deletion and config reload reach every edge (each has its own Redis) |
| `throughput` | 1000 keys, closed loop at concurrency 16/64/256; rps and latency percentiles |
| `hotkey` | one key at concurrency 256; no spurious denials or errors |
| `outage` | hub stopped: edges keep serving under the safety tier, ≤ S per edge per window |
| `outage-recovery` | hub restarted: normal quota resumes |

The loadgen exits non-zero when a scenario's invariant fails; `run.sh` lists
passes and failures at the end of the report.
