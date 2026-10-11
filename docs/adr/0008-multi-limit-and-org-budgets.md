# 8. Multiple limits as all-of checks; org budgets as nested grants

**Status:** accepted

## Context

Real APIs need more than one limit per request: a caller's tier, a budget
shared by all of a customer's keys, a cap per client address, and a ceiling
that protects the upstream. Envoy's rate-limit service evaluates several
descriptors per request too.

## Decision

- **Independent limits** (`also:` per key, client IP or service) each have
  their own ledger quota. `limiter.Reserve` charges them in turn; if one
  denies, the charges already made are refunded locally. No cross-slot
  script and no extra RPC.
- **Org budgets** (`org:`) are nested in the same grant: one script on the
  org's hash tag grants `min(target, key remaining, org remaining)`. API-key
  orgs come from the key's owner on the hub; JWT orgs from a claim.
- **Shadow mode** evaluates a limit and reports would-be denials (metric and
  `Gate-Shadow-Denied`) without denying; a shadow limit without room admits
  without charging, so it cannot starve real traffic.

## Consequences

- A denial on one limit may leave tokens leased for another: under-use,
  never overshoot.
- A service-wide limit is one hot quota; adaptive grants and batching keep
  its hub load proportional to edges.
- The org budget is split across keys and edges; adaptive grants keep that
  split small.
