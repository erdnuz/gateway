# Benchmarks

Three sets of measurements, all on one 8-CPU, 8 GB Docker Desktop host, so
treat absolute numbers as indicative and comparisons within a table as the
point. Raw reports are linked from each section.

## 1. Gate against other rate limiters

[`testing/bench`](../testing/bench) runs the same upstream, load generator
and limit (500 requests per second per API key, 1 s windows) through four
systems, scaling the data plane to 1, 2 and 4 replicas of 1 CPU each:

| System | What it is |
| --- | --- |
| **gate** | Gate edges spending quota leased from the hub |
| **counter** | Gate edges with `EDGE_LIMITER=redis-counter`: one atomic Redis increment per request, the textbook central counter |
| **envoy-ref** | Envoy + [envoyproxy/ratelimit](https://github.com/envoyproxy/ratelimit) (pipelining and its local over-limit cache on) + Redis |
| **envoy-gate** | Envoy + `gate limiter` |

Three phases, each with its own count of Redis commands:

- **accuracy**: one key offered twice its quota for 10 s;
- **busy**: 1200 rps over 20 keys (60 requests per key per second);
- **cold**: 1200 rps over 1000 keys, about one request per key per second,
  so nearly every request is a key's first in its window: the worst case for
  leasing.

With 4 data-plane replicas (full results: [counter and envoy-ref](../testing/bench/results/v3/report.md);
[gate and envoy-gate](../testing/bench/results/v3b/report.md), rerun after
the hub's tier cache landed):

| | accuracy: most admitted in a second (quota 500) | busy: Redis cmds / req | busy: p99 | cold: Redis cmds / req | cold: p99 |
| --- | --- | --- | --- | --- | --- |
| gate | **500** | **0.15** | 18 ms | 6.80 | 106 ms |
| counter | 500 | 2.02 | 52 ms | 2.85 | 16 ms |
| envoy-ref | 501 (529 in another run)¹ | 2.01 | 155 ms | 2.01 | 384 ms |
| envoy-gate | 501 (517 in another run)¹ | **0.15** | 35 ms | 6.76 | 297 ms |

¹ Envoy does not pass Gate's `Gate-Window` header through, so the Envoy
rows count admissions per second of the load generator's clock, which can
put a request near a boundary in the neighbouring second. The gate and
counter rows count per ledger window. For envoy-gate the mean is 500 per
second.

What it shows:

- **Accuracy.** Gate admits exactly the quota in every one-second window, at
  every scale; its edges attribute each request to the hub window that
  charged it. envoyproxy/ratelimit, which caches over-limit keys locally per
  instance, admitted up to 529 of 500 in one run (see the note above on how
  the Envoy rows are counted).
- **Backend work when keys are busy.** Leasing does about a tenth of the
  Redis work of a per-request counter (0.15 commands per request against
  2.0), and the gap grows with traffic per key.
- **The worst case.** When every request is a key's first in its window,
  every request needs a grant (6.8 Redis commands per request against
  the counter's 2.85), and a grant (an atomic script that also
  keeps the ledger's clock and the endgame split's bookkeeping) costs more
  Redis work than one increment. Leasing pays off once a key sees more than
  a few requests per edge per window; longer windows (a minute rather than
  a second) move almost any traffic into that regime.
- **Latency** on one host is dominated by CPU contention and varies between
  runs; the 1-replica rows in the raw results include systems pushed past
  one CPU's capacity in some phases.

## 2. Hub scaling

[`testing/loadtest/hubscale.sh`](../testing/loadtest/hubscale.sh) drives
batched lease RPCs (64 leases each, 100k keys) straight at N hub replicas
over an N-shard Redis Cluster, every hub and shard capped at 1 CPU
([report](../testing/loadtest/results/hubscale/report.md)):

| Hubs + shards | Grants per second | p99 per RPC |
| --- | --- | --- |
| 1 + 1 | 56,723 | 94 ms |
| 2 + 2 | 66,640 | 128 ms |
| 3 + 3 | 82,659 | 86 ms |

Throughput grows with hubs and shards; it grows sublinearly here because the
3 + 3 topology and the load generator use all 8 of the host's CPUs. With
smaller batches (16 leases), splitting each batch across shards left Redis
spending its CPU on round trips and scaling flattened: batching matters as
much for the ledger as for the hub.

## 3. v3 against v2

Interleaved runs of the load test's throughput scenarios (1000 keys, closed
loop) on the same stack, v2 (`master` before this work) against v3
([v2](../testing/loadtest/results/ab-v2-1/report.md),
[v3](../testing/loadtest/results/ab-v3-1/report.md)):

| | rps at 16 / 64 / 256 concurrent | hot key rps | hub Redis cmds / req |
| --- | --- | --- | --- |
| v2, round 1 | 5,513 / 6,056 / 6,460 | 7,402 | 0.11 |
| v3, round 1 | 5,180 / 6,683 / 7,005 | 8,251 | 0.15 |

Throughput is at parity within this host's run-to-run noise (a second round
swung both versions by up to 2×). v3 does about 40% more Redis work per
grant, mostly reading the ledger's clock, which is what makes windows exact.
