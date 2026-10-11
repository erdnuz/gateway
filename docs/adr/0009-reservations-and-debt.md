# 9. Reserve an estimate, settle locally, carry bounded debt

**Status:** accepted

## Context

Some costs are known only after the response: tokens used by an LLM call,
rows scanned, bytes produced. Charging a fixed cost either over-limits
(estimate too high) or lets callers exceed their budget (too low).

## Decision

The edge reserves an estimate (configured, or the client's own header), then
settles the actual cost from a response header, or from a trailer once a
streamed body ends. The difference moves the edge's local balance: a refund
adds tokens, an overrun takes the balance negative (debt), which the next
grant pays off first. Debt carries across a window roll and is capped by
`max_debt`; a refund after the roll is dropped. A failed upstream is always
refunded.

## Consequences

- No RPC per settlement: the hub sees leases, not requests.
- Per window, charged cost stays within `Q + edges × max_debt` (checked by
  the simulation and the `llm` load-test scenario).
- On drain, remaining debt is returned to the ledger as negative tokens.
- The decision service settles differently (straight at the hub) because its
  reservations may be settled by another replica; see ADR 10.
