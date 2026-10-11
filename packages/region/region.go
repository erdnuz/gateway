// Package region splits global quotas between regions.
//
// Each region runs its own hubs and ledger, and serves its own traffic
// without a cross-region call on the lease path. A global quota Q (per key
// and window) is split into per-region allocations that sum to Q by
// construction, with no coordination:
//
//   - every region has a floor share (limits.global.floors), and
//   - the rest belongs to the key's home region, picked by rendezvous
//     hashing, so every region computes the same split independently.
//
// A region that runs out borrows unused allocation from its peers (Node).
// Transfers are debit-first: the lender lowers its allocation before it
// answers, and the borrower credits once per transfer ID, only within the
// same window. A lost reply, a duplicate, a partition or a late transfer
// leaves quota unused; none can make the regions admit more than Q.
package region

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sort"
)

// Base is region's allocation of quota q for subject: its floor, plus the
// remainder when it is the subject's home region. Bases over all regions in
// floors sum to exactly q.
func Base(q int64, floors map[string]float64, region, subject string) int64 {
	if _, ok := floors[region]; !ok {
		return 0
	}
	var sum int64
	for _, name := range Names(floors) {
		sum += floor(q, floors[name])
	}
	base := floor(q, floors[region])
	if Home(floors, subject) == region {
		base += q - sum
	}
	return base
}

func floor(q int64, share float64) int64 {
	return int64(math.Floor(float64(q) * math.Max(0, math.Min(share, 1))))
}

// Home is the region owning subject's unallocated remainder: the highest
// rendezvous hash of (region, subject). Every region computes the same
// answer, and adding a region moves only the subjects it now wins.
func Home(floors map[string]float64, subject string) string {
	best, bestScore := "", uint64(0)
	for _, name := range Names(floors) {
		h := sha256.Sum256([]byte(name + "\x00" + subject))
		if s := binary.BigEndian.Uint64(h[:8]); best == "" || s > bestScore {
			best, bestScore = name, s
		}
	}
	return best
}

// Names lists the regions in floors in a stable order.
func Names(floors map[string]float64) []string {
	out := make([]string, 0, len(floors))
	for n := range floors {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
