---------------------------- MODULE LeaseProtocol ----------------------------
(* Quota leasing (packages/limiter/lease.go, packages/ledger).

   The ledger grants edges blocks of tokens for the current window; edges
   spend them locally. A grant names its window, and its tokens die when the
   ledger's window ends: an edge holds one balance per window and never
   spends a token of a window that has ended (the edge measures token
   lifetime from when it sent the request, so its expiry is never later than
   the ledger's). Grant replies may be lost or arrive late. An edge may return
   unspent tokens, swapping its balance to zero first; the ledger credits a
   return once per dedupe ID, and only to the window it is for.

   Safety: no window ever admits more than its quota Q. *)
EXTENDS Naturals, FiniteSets

CONSTANTS Edges, Q, MaxWindow, MaxIds

VARIABLES w,        \* the ledger's current window
          granted,  \* window -> tokens granted (net of returns)
          held,     \* edge -> [w, n]: the edge's balance and its window
          admitted, \* window -> requests admitted
          inflight, \* grant replies in flight
          returns,  \* return messages in flight
          seen      \* return dedupe IDs the ledger has applied

vars == <<w, granted, held, admitted, inflight, returns, seen>>

Windows == 0..MaxWindow

Init == /\ w = 0
        /\ granted = [x \in Windows |-> 0]
        /\ held = [e \in Edges |-> [w |-> 0, n |-> 0]]
        /\ admitted = [x \in Windows |-> 0]
        /\ inflight = {}
        /\ returns = {}
        /\ seen = {}

\* The ledger grants up to what is left of the current window.
Grant(e, n) == /\ n \in 1..(Q - granted[w])
               /\ granted' = [granted EXCEPT ![w] = @ + n]
               /\ inflight' = inflight \cup {[to |-> e, w |-> w, n |-> n]}
               /\ UNCHANGED <<w, held, admitted, returns, seen>>

\* An edge receives a grant: a newer window replaces its balance, the same
\* window adds to it, an older window (a late reply) is dropped.
Receive(m) == /\ m \in inflight
              /\ inflight' = inflight \ {m}
              /\ held' = [held EXCEPT ![m.to] =
                           IF m.w > @.w THEN [w |-> m.w, n |-> m.n]
                           ELSE IF m.w = @.w THEN [w |-> @.w, n |-> @.n + m.n]
                           ELSE @]
              /\ UNCHANGED <<w, granted, admitted, returns, seen>>

LoseGrant(m) == /\ m \in inflight
                /\ inflight' = inflight \ {m}
                /\ UNCHANGED <<w, granted, held, admitted, returns, seen>>

\* An edge spends a token, only while its window is the ledger's.
Spend(e) == /\ held[e].n > 0 /\ held[e].w = w
            /\ held' = [held EXCEPT ![e].n = @ - 1]
            /\ admitted' = [admitted EXCEPT ![w] = @ + 1]
            /\ UNCHANGED <<w, granted, inflight, returns, seen>>

\* An edge returns its balance: swap to zero, then send (retries reuse id).
Return(e, id) == /\ held[e].n > 0 /\ id \notin {r.id : r \in returns} /\ id \notin seen
                 /\ returns' = returns \cup {[id |-> id, w |-> held[e].w, n |-> held[e].n]}
                 /\ held' = [held EXCEPT ![e].n = 0]
                 /\ UNCHANGED <<w, granted, admitted, inflight, seen>>

\* The ledger applies a return once, to the current window only. The message
\* stays in flight (it may be retried or duplicated).
Credit(r) == /\ r \in returns
             /\ IF r.id \notin seen /\ r.w = w
                  THEN granted' = [granted EXCEPT ![w] = @ - r.n]
                  ELSE UNCHANGED granted
             /\ seen' = seen \cup {r.id}
             /\ UNCHANGED <<w, held, admitted, inflight, returns>>

Roll == /\ w < MaxWindow
        /\ w' = w + 1
        /\ UNCHANGED <<granted, held, admitted, inflight, returns, seen>>

Next == \/ \E e \in Edges, n \in 1..Q : Grant(e, n)
        \/ \E m \in inflight : Receive(m) \/ LoseGrant(m)
        \/ \E e \in Edges : Spend(e)
        \/ \E e \in Edges, id \in 1..MaxIds : Return(e, id)
        \/ \E r \in returns : Credit(r)
        \/ Roll

Spec == Init /\ [][Next]_vars

Safety == \A x \in Windows : admitted[x] <= Q
\* Every admission spent a token the ledger granted, net of what came back.
NoPhantomTokens == \A x \in Windows : admitted[x] <= granted[x]
=============================================================================
