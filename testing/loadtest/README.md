# Rate-limiter load test

End-to-end correctness and performance checks for edge rate limiting, run
against a production-shaped stack: hub + hub Redis, two edges that each
keep their caches in memory, and a minimal upstream
([compose.yaml](compose.yaml)).

```bash
testing/loadtest/run.sh <label>          # e.g. baseline, after
KEEP=1 testing/loadtest/run.sh dev       # leave the stack running afterwards
ONLY="throughput hotkey" testing/loadtest/run.sh perf   # a subset of scenarios
```

Requires Docker only; dev certificates are generated on first run
(`deploy/gen-dev-certs.sh`). Output lands in `results/<label>/`:
`report.md` (human summary) and one JSON file per scenario.

Config: [gateway-config.loadtest.yaml](gateway-config.loadtest.yaml) — prefix
`lt`, 10 s quota window, `pro` tier Q=1000, `bulk` tier effectively unlimited,
safety tier S=50, first lease 100 (then adaptive). Each edge returns the hub
window that charged a request in `Gate-Window`, so window checks are exact:
`<= Q`, not `<= Q + tolerance`.

| scenario | checks |
| --- | --- |
| `windows` | 2× quota on one key across both edges for 4 windows; admitted per window ≈ Q |
| `carryover` | tokens leased but unused in window N must not inflate window N+1 |
| `isolation` | exhausting a key on one service leaves its quota on another service intact |
| `bypass` | `X-API-Key: <key>/<suffix>` and never-issued keys are never admitted |
| `propagation` | tier deletion and config reload reach every edge over its watch stream |
| `throughput` | 1000 keys, closed loop at concurrency 16/64/256; rps and latency percentiles |
| `hotkey` | one key at concurrency 256; no spurious denials or errors |
| `outage` | hub stopped: edges keep serving under the safety tier, ≤ S per edge per window |
| `outage-recovery` | hub restarted: normal quota resumes |
| `endgame` | 2× quota, 80% to one edge and 20% to the other: every window still reaches `Q − 4` (the hub splits the last of the quota between active edges) |
| `algorithms` | a sliding window admits ≤ Q per window and ≤ 1.3Q in any window-length span; a token bucket admits its burst plus its rate |
| `multilimit` | ten keys from one client address with an `also: per-ip` limit of 300: the per-IP limit binds |
| `llm` | post-hoc cost: tokens charged per window stay within Q + edges × max_debt, and the quota is spent on actual, not estimated, cost |
| `manykeys` | 5000 cold keys on each edge at once: every request admitted, lease requests batched |
| `rls` | two `gate limiter` replicas answer one descriptor from one global quota: ≤ Q per window |
| `drain` | both edges lease 400 of 1000; edge 1 is stopped gracefully mid-window; edge 2 still uses nearly the whole quota because edge 1 returned its lease |

The loadgen exits non-zero when a scenario's invariant fails; `run.sh` lists
passes and failures at the end of the report.
