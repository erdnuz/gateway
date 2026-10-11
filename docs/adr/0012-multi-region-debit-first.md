# 12. Multi-region quotas: static floors, home regions, debit-first transfers

**Status:** accepted

## Context

A global quota across regions with one ledger would put a cross-region round
trip on every lease, and a partition would stop every region but one.
Splitting by measured demand, computed independently in each region, lets
regions disagree during a partition and together overshoot.

## Decision

- Each region runs its own hubs and ledger and enforces its **allocation**:
  a floor share of Q, plus, in the key's home region (rendezvous hash), the
  remainder. Every region computes the same split, so allocations sum to Q
  without coordination.
- A region whose allocation runs out records the quota; its node borrows
  from peers every 250 ms. The lender keeps what it is on pace to use for
  the rest of the window, **debits its own allocation before replying**,
  and answers retries of the same transfer ID with the first answer. The
  borrower credits once per transfer ID, and only within the same window.

## Consequences

- No cross-region call on the lease path; a partition leaves each region
  with its allocation (zero overshoot) and only stops rebalancing.
- Lost replies, duplicates and late transfers cost unused quota, never
  overshoot: 200 simulated scenarios with faults and partitions never admit
  more than Q across regions.
- Utilization while demand moves depends on window length against the
  rebalance interval: about 91% on average in fault-free simulations with
  2–5 s windows, lower for very short windows with zero floors.
- Global quotas are fixed-window only, and assume API keys exist in every
  region.
