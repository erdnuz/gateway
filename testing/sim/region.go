package sim

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/hub"
	"gateway/packages/ledger"
	"gateway/packages/limiter"
	"gateway/packages/region"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegionScenario is a generated multi-region simulation: one global quota
// per key, split between regions that borrow from each other while demand
// moves between them, over a faulty inter-region network.
type RegionScenario struct {
	Seed     uint64
	Regions  []string
	Floors   map[string]float64
	Quota    int64
	Window   time.Duration
	Keys     int
	Demand   map[string]float64 // offered requests/s per region, first half
	Later    map[string]float64 // offered requests/s per region, second half
	Duration time.Duration
	// Inter-region faults.
	DropRate, LostReplyRate, DupRate float64
	PartitionFrom, PartitionTo       time.Duration
}

func (s RegionScenario) String() string {
	return fmt.Sprintf("seed=%d regions=%v floors=%v quota=%d window=%s keys=%d demand=%v later=%v drop=%.2f lost=%.2f dup=%.2f partition=[%s,%s)",
		s.Seed, s.Regions, s.Floors, s.Quota, s.Window, s.Keys, s.Demand, s.Later, s.DropRate, s.LostReplyRate, s.DupRate, s.PartitionFrom, s.PartitionTo)
}

func (s RegionScenario) faulty() bool {
	return s.DropRate > 0 || s.LostReplyRate > 0 || s.DupRate > 0 || s.PartitionTo > 0
}

// GenerateRegions draws a multi-region scenario from a seed.
func GenerateRegions(seed uint64) RegionScenario {
	r := rand.New(rand.NewPCG(seed, 0x51ed270b))
	names := []string{"eu", "us", "ap"}[:2+r.IntN(2)]
	s := RegionScenario{
		Seed: seed, Regions: names, Floors: map[string]float64{},
		Quota:    []int64{100, 300, 1000}[r.IntN(3)],
		Window:   time.Duration(2+r.IntN(4)) * time.Second,
		Keys:     1 + r.IntN(3),
		Demand:   map[string]float64{},
		Later:    map[string]float64{},
		Duration: time.Duration(20+r.IntN(20)) * time.Second,
	}
	floor := []float64{0, 0.1, 0.2}[r.IntN(3)]
	perSec := float64(s.Quota) / s.Window.Seconds() * float64(s.Keys)
	for _, n := range names {
		s.Floors[n] = floor
		s.Demand[n] = perSec * 2.5 * r.Float64()
		s.Later[n] = perSec * 2.5 * r.Float64()
	}
	// Demand moves: the busiest region early is quiet later.
	s.Demand[names[0]], s.Later[names[0]] = perSec*2.5, perSec*0.1
	s.Later[names[1]] = perSec * 2.5
	if r.IntN(2) == 0 {
		s.DropRate, s.LostReplyRate, s.DupRate = 0.2*r.Float64(), 0.2*r.Float64(), 0.2*r.Float64()
		if r.IntN(2) == 0 {
			s.PartitionFrom = s.Duration / 3
			s.PartitionTo = s.PartitionFrom + time.Duration(2+r.IntN(8))*time.Second
		}
	}
	return s
}

// RegionResult is what a multi-region run observed.
type RegionResult struct {
	Scenario   RegionScenario
	Admits     []admit // edge = region index
	Lent       int64
	start, end time.Time
}

// RunRegions simulates s inside a synctest bubble.
func RunRegions(s RegionScenario) *RegionResult {
	res := &RegionResult{Scenario: s}
	svc := types.ServiceConfig{
		ServiceID: service, Auth: types.AuthConfig{Type: "none"},
		Tiers:  []types.TierConfig{{TierID: tier, Quota: uint32(s.Quota), GetCost: 1, PostCost: 1, PutCost: 1, DeleteCost: 1, OtherCost: 1}},
		Global: &types.GlobalLimit{Floors: s.Floors},
	}
	cfg := &types.GatewayConfig{Prefixes: []types.PrefixConfig{{Prefix: scope, QuotaPeriod: s.Window, Services: []types.ServiceConfig{svc}}}}

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	res.start = start
	servers := map[string]*hub.QuotaLeaseServer{}
	nodes := map[string]*region.Node{}
	for _, name := range s.Regions {
		mem := ledger.NewMem()
		srv := hub.NewQuotaLeaseServer(mem, hub.NewStaticConfigManager(cfg), hub.NewInMemoryTierStore())
		node := &region.Node{Name: name, Ledger: mem, Peers: map[string]region.Peer{}}
		srv.SetRegion(node)
		servers[name], nodes[name] = srv, node
	}
	var mu sync.Mutex
	for i, name := range s.Regions {
		for j, peer := range s.Regions {
			if peer != name {
				nodes[name].Peers[peer] = &faultyPeer{srv: &hub.RegionServer{Leases: servers[peer]}, s: s, start: start,
					rng: rand.New(rand.NewPCG(s.Seed, uint64(100*i+j))), mu: &mu, res: res}
			}
		}
		go nodes[name].Run(ctx)
	}

	var wg sync.WaitGroup
	for i, name := range s.Regions {
		net := &faultyNet{srv: servers[name], start: start, rng: rand.New(rand.NewPCG(s.Seed, uint64(i)+1)), res: &Result{}, mu: &mu}
		lease := limiter.NewLease(limiter.NewBatcher(net), limiter.LeaseOptions{LeaseSize: 20, EdgeID: "edge-" + name})
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(s.Seed, uint64(i)+500))
			for {
				rate := s.Demand[name]
				if time.Since(start) > s.Duration/2 {
					rate = s.Later[name]
				}
				time.Sleep(time.Duration(rng.ExpFloat64() / max(rate, 0.01) * float64(time.Second)))
				if time.Since(start) >= s.Duration {
					return
				}
				subject := fmt.Sprintf("key%03d", rng.IntN(s.Keys))
				d, err := lease.Allow(ctx, limiter.Request{Scope: scope, Service: service, Key: subject, Tier: tier, Limit: s.Quota, Cost: 1, Window: s.Window})
				if err == nil && d.Allowed {
					mu.Lock()
					res.Admits = append(res.Admits, admit{at: time.Now(), win: d.Window, edge: i, subject: subject, cost: 1})
					mu.Unlock()
				}
			}
		}(i, name)
	}
	wg.Wait()
	res.end = time.Now()
	cancel()
	time.Sleep(10 * time.Second) // let in-flight renewals and transfers finish
	return res
}

// faultyPeer is the network between two regions' hubs.
type faultyPeer struct {
	srv   *hub.RegionServer
	s     RegionScenario
	start time.Time
	rng   *rand.Rand
	mu    *sync.Mutex
	res   *RegionResult
}

func (p *faultyPeer) roll(x float64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rng.Float64() < x
}

func (p *faultyPeer) Borrow(ctx context.Context, in *types.BorrowRequest) (*types.BorrowResponse, error) {
	time.Sleep(20 * time.Millisecond) // a cross-region round trip
	if el := time.Since(p.start); el >= p.s.PartitionFrom && el < p.s.PartitionTo {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	if p.roll(p.s.DropRate) {
		return nil, status.Error(codes.Unavailable, "dropped")
	}
	out, err := p.srv.Borrow(ctx, in)
	if p.roll(p.s.DupRate) {
		_, _ = p.srv.Borrow(ctx, in) // a retransmission: the transfer ID makes it a no-op
	}
	if err == nil {
		p.mu.Lock()
		p.res.Lent += out.Given
		p.mu.Unlock()
	}
	if p.roll(p.s.LostReplyRate) {
		return nil, status.Error(codes.DeadlineExceeded, "reply lost")
	}
	return out, err
}

// Check returns every invariant the run broke: across all regions, no key
// ever admits more than the global quota in one window.
func (r *RegionResult) Check() []string {
	per := map[string]int64{}
	for _, a := range r.Admits {
		per[fmt.Sprintf("%s/%d", a.subject, a.win)] += a.cost
	}
	var bad []string
	for k, n := range per {
		if n > r.Scenario.Quota {
			bad = append(bad, fmt.Sprintf("%s: %d admitted across regions > global quota %d", k, n, r.Scenario.Quota))
		}
	}
	return bad
}

// Utilization is the share of each key's global quota admitted in the
// windows fully inside the run.
func (r *RegionResult) Utilization() float64 {
	s := r.Scenario
	wms := s.Window.Milliseconds()
	first, last := r.start.UnixMilli()/wms+1, r.end.UnixMilli()/wms-1
	if last < first {
		return -1
	}
	var n int64
	for _, a := range r.Admits {
		if a.win >= first && a.win <= last {
			n += a.cost
		}
	}
	return float64(n) / float64(s.Quota*int64(s.Keys)*(last-first+1))
}
