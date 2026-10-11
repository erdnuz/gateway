package sim

import (
	"flag"
	"testing"
	"testing/synctest"
)

var (
	seeds = flag.Int("sim.seeds", 60, "number of generated scenarios to run")
	only  = flag.Uint64("sim.seed", 0, "run only this scenario (replay a failure)")
)

// TestSimulation runs generated scenarios — traffic patterns, algorithms,
// clock skew, drains, post-hoc costs, and RPC faults — and checks that no
// quota is ever exceeded. Replay a failure with -sim.seed=N.
func TestSimulation(t *testing.T) {
	list := make([]uint64, 0, *seeds)
	if *only != 0 {
		list = append(list, *only)
	} else {
		for i := 1; i <= *seeds; i++ {
			list = append(list, uint64(i))
		}
	}
	var saturated, utilSum float64
	for _, seed := range list {
		s := Generate(seed)
		var res *Result
		synctest.Test(t, func(t *testing.T) { res = Run(s) })
		if bad := res.Check(); len(bad) > 0 {
			t.Errorf("scenario %s\n  admitted %d, denied %d, unavailable %d, RPCs %d\n  violations: %v\n  replay: go test ./testing/sim -run Simulation -sim.seed=%d",
				s, len(res.Admits), res.Denied, res.Unavail, res.RPCs, bad, seed)
		}
		if s.Saturated() {
			u := res.Utilization()
			if u < 0 {
				continue
			}
			saturated++
			utilSum += u
			// With demand at twice the quota and nothing failing, the hot
			// key's quota is used up: quota is not stranded on edges.
			if u < 0.85 {
				t.Errorf("scenario %s: utilization %.2f < 0.85 under saturating demand\n  replay: -sim.seed=%d", s, u, seed)
			}
		}
	}
	if saturated > 0 {
		t.Logf("%d scenarios, %d saturated with mean utilization %.3f", len(list), int(saturated), utilSum/saturated)
	}
}

var regionSeeds = flag.Int("sim.region-seeds", 30, "number of generated multi-region scenarios")

// TestRegions runs generated multi-region scenarios: demand moving between
// regions, over an inter-region network that drops, duplicates, loses
// replies and partitions. Across all regions, no key may exceed its global
// quota in any window.
func TestRegions(t *testing.T) {
	var clean, utilSum float64
	for seed := uint64(1); seed <= uint64(*regionSeeds); seed++ {
		if *only != 0 && seed != *only {
			continue
		}
		s := GenerateRegions(seed)
		var res *RegionResult
		synctest.Test(t, func(t *testing.T) { res = RunRegions(s) })
		if bad := res.Check(); len(bad) > 0 {
			t.Errorf("scenario %s\n  admitted %d, lent %d\n  violations: %v\n  replay: go test ./testing/sim -run Regions -sim.seed=%d",
				s, len(res.Admits), res.Lent, bad, seed)
		}
		if !s.faulty() {
			u := res.Utilization()
			clean++
			utilSum += u
			// Borrowing takes a few rebalance rounds (250ms each), so very
			// short windows with no floors use somewhat less.
			if u < 0.7 {
				t.Errorf("scenario %s: utilization %.2f < 0.7 while demand moved between regions (lent %d)", s, u, res.Lent)
			}
		}
	}
	if clean > 0 {
		t.Logf("%d fault-free scenarios, mean utilization %.3f", int(clean), utilSum/clean)
	}
}
