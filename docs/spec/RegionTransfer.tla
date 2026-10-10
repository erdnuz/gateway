---------------------------- MODULE RegionTransfer ----------------------------
(* Global quotas across regions (packages/region, packages/ledger/region.go).

   Each region enforces its allocation of a global quota Q. A region that runs
   short borrows: the lender debits its own allocation (keeping what is in
   use), records the amount under the transfer ID and replies; the borrower
   credits the amount once per transfer ID, and only in the window it was
   lent for. The network may lose, duplicate and delay replies, and retry a
   request (same transfer ID, answered with the recorded amount). Windows
   roll, resetting allocations to their bases.

   Safety: in every window, the regions together never admit more than Q. *)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Regions,   \* e.g. {"eu", "us"}
          Base,      \* each region's base allocation; Sum(Base) = Q
          Q,
          MaxIds,    \* transfer IDs to explore
          MaxWindow  \* windows to explore

VARIABLES w,         \* the current window
          alloc,     \* region -> allocation in window w
          used,      \* region -> admitted in window w
          lent,      \* transfer ID -> [from, to, amt, w], recorded by the lender
          credited,  \* transfer IDs the borrower has applied
          net        \* replies in flight: a set (a reply may be delivered many times)

vars == <<w, alloc, used, lent, credited, net>>

RECURSIVE SumOver(_, _)
SumOver(f, S) == IF S = {} THEN 0 ELSE LET r == CHOOSE x \in S : TRUE IN f[r] + SumOver(f, S \ {r})

Ids == 1..MaxIds

Init == /\ w = 0
        /\ alloc = Base
        /\ used = [r \in Regions |-> 0]
        /\ lent = [i \in {} |-> 0]
        /\ credited = {}
        /\ net = {}

\* A region admits one request within its allocation.
Admit(r) == /\ used[r] < alloc[r]
            /\ used' = [used EXCEPT ![r] = @ + 1]
            /\ UNCHANGED <<w, alloc, lent, credited, net>>

\* Lender l answers a new borrow request from b: debit first, then reply.
Lend(l, b, id, amt) ==
    /\ l # b /\ id \notin DOMAIN lent
    /\ amt \in 1..(alloc[l] - used[l])
    /\ alloc' = [alloc EXCEPT ![l] = @ - amt]
    /\ lent' = lent @@ (id :> [from |-> l, to |-> b, amt |-> amt, w |-> w])
    /\ net' = net \cup {[id |-> id, to |-> b, amt |-> amt, w |-> w]}
    /\ UNCHANGED <<w, used, credited>>

\* A retried request with a known ID gets the recorded answer, no new debit.
Retry(id) == /\ id \in DOMAIN lent
             /\ net' = net \cup {[id |-> id, to |-> lent[id].to, amt |-> lent[id].amt, w |-> lent[id].w]}
             /\ UNCHANGED <<w, alloc, used, lent, credited>>

\* The borrower applies a reply: once per ID, and only in its window. The
\* reply stays in the network (it may be duplicated).
Credit(m) == /\ m \in net
             /\ IF m.id \notin credited /\ m.w = w
                  THEN /\ alloc' = [alloc EXCEPT ![m.to] = @ + m.amt]
                       /\ credited' = credited \cup {m.id}
                  ELSE UNCHANGED <<alloc, credited>>
             /\ UNCHANGED <<w, used, lent, net>>

Lose(m) == /\ m \in net
           /\ net' = net \ {m}
           /\ UNCHANGED <<w, alloc, used, lent, credited>>

\* The window rolls: allocations start again from the bases. Replies for the
\* old window may still arrive (and must be ignored).
Roll == /\ w < MaxWindow
        /\ w' = w + 1
        /\ alloc' = Base
        /\ used' = [r \in Regions |-> 0]
        /\ UNCHANGED <<lent, credited, net>>

Next == \/ \E r \in Regions : Admit(r)
        \/ \E l, b \in Regions, id \in Ids, amt \in 1..Q : Lend(l, b, id, amt)
        \/ \E id \in Ids : Retry(id)
        \/ \E m \in net : Credit(m) \/ Lose(m)
        \/ Roll

Spec == Init /\ [][Next]_vars

\* The regions' allocations never sum to more than Q ...
AllocationsBounded == SumOver(alloc, Regions) <= Q
\* ... so neither do their admissions, in any window.
Safety == SumOver(used, Regions) <= Q
\* And no region admits more than its own allocation.
WithinAllocation == \A r \in Regions : used[r] <= alloc[r]
=============================================================================
