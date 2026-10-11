--------------------------- MODULE MCRegionTransfer ---------------------------
\* Model values for RegionTransfer: Q = 3 split 2 (eu) / 1 (us).
EXTENDS RegionTransfer

MCBase == [r \in Regions |-> IF r = "eu" THEN 2 ELSE 1]
=============================================================================
