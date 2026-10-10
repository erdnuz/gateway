# Rate-limit algorithms over leases

Every algorithm Gate offers is enforced through the same machinery: the
hub's ledger decides how many tokens it can grant, and edges spend granted
tokens locally. What differs is how the ledger counts. All three run as one
Lua script per grant (`packages/ledger/redis.go`) with an identical Go twin
(`packages/ledger/mem.go`), kept equal by a conformance test.

| Algorithm | Ledger state per quota | A grant of `n` | Token lifetime | Guarantee |
| --- | --- | --- | --- | --- |
| `fixed_window` | a counter per window | `used += n` while `used < Q` | until the window ends | at most Q per window |
| `sliding_window` | counters for this window and the last | as fixed, against `prev × (1 − f) + cur` | until the window ends | at most Q per window, and no 2Q burst across a reset |
| `token_bucket` (GCRA) | the theoretical arrival time `tat` | `tat = max(tat, now) + n × T` while within `burst × T` | the lease horizon (default 2s) | at most `burst + rate × (τ + horizon)` in any interval τ |

`T = 1 / rate`; `f` is the elapsed fraction of the current window.

```yaml
limits:
  window: 1m
  tiers:
    free:  { requests: 60 }                                   # fixed window
    pro:   { requests: 6000, algorithm: sliding_window }
    burst: { algorithm: token_bucket, rate: 20/s, burst: 200 }
```

## Fixed window

The window is `ledger time / window`, so it is the same for every edge and
hub. Tokens expire with their window: an edge never carries them into the
next one. Simple and exact, but a caller can spend Q at the end of one window
and Q again at the start of the next.

## Sliding window

The ledger estimates usage over the last window length as
`ceil(prev × (1 − f)) + cur`. A grant is limited to what that estimate leaves
of Q, and charged to the current window's counter. As the window ages, the
previous window's weight decays and capacity returns gradually instead of all
at once at the reset. The estimate assumes the previous window's requests were
spread evenly; it is the approximation Cloudflare and others use, with two
counters instead of a log of timestamps. The load test's `algorithms` scenario
measures the largest count in any window-length span.

## Token bucket (GCRA)

The ledger stores one number per quota, `tat`, the time at which the bucket
would be full again. A grant of `n` is allowed while `tat + n × T` stays
within `now + burst × T`; partial grants take what fits. When nothing fits,
the retry hint is the time until one token's worth of capacity returns (not
until the bucket is full), and edges deny locally until then.

Granted tokens are debited when granted but may be spent up to one lease
lifetime later. That is the price of leasing: the bound gains `rate ×
lifetime`, which adaptive grants keep small (an edge is granted about what it
spends in the horizon). Unspent tokens returned on drain move `tat` back.

## Choosing

- `fixed_window` for quotas customers read as "N per minute/hour/day", and
  for global (multi-region) quotas, which support it only.
- `sliding_window` when the burst at a reset matters (expensive endpoints).
- `token_bucket` to protect capacity: smooth rate with headroom, e.g. a
  service-wide `also:` limit in front of a fragile upstream.
