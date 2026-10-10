// Package sim runs Gate's rate limiter on a virtual clock and checks that
// its guarantees hold under any traffic and any failure.
//
// A simulation is the production code end to end: N edges (limiter.Lease,
// with the lease batcher) talk to the real hub lease server, which grants
// from the ledger's in-memory twin (kept identical to the Redis scripts by
// the ledger's conformance test). Between them sits a fault injector that
// delays, drops, duplicates and loses RPCs and takes the hub down. Traffic
// is generated from a seed; time is virtual (testing/synctest), so an hour of
// traffic runs in milliseconds and a failing scenario is replayed from its
// seed.
//
// Goroutine interleaving inside a bubble is not fully deterministic, so a
// seed reproduces a scenario, not always the exact schedule. The invariants
// are checked on every run, so any schedule that breaks one fails.
package sim

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/hub"
	"gateway/packages/ledger"
	"gateway/packages/limiter"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Scenario is one generated simulation.
type Scenario struct {
	Seed      uint64
	Edges     int
	Keys      int
	Algo      ledger.Algo
	Quota     int64   // per window, or the bucket's burst
	Rate      float64 // token bucket: per second
	Window    time.Duration
	LeaseSize int64
	Fixed     bool          // fixed lease size instead of adaptive grants
	Horizon   time.Duration // adaptive horizon
	HubSkew   time.Duration // the hub clock's offset from the edges'
	Demand    []float64     // offered requests per second, per edge
	HotShare  float64       // share of requests for the hot key
	Shift     bool          // demand moves between edges halfway through
	DrainAt   time.Duration // edge 0 drains and leaves (0: never)
	PostHoc   bool          // actual costs settled after admission
	MaxDebt   int64
	Guard     int64 // a service-wide fixed-window limit (0: none)
	Duration  time.Duration
	Faults    Faults
}

// Faults is what the network and the hub do wrong.
type Faults struct {
	Latency       time.Duration // up to this much delay each way
	DropRate      float64       // the request never reaches the hub
	LostReplyRate float64       // the hub acts, the reply is lost
	DupRate       float64       // the hub acts twice
	OutageFrom    time.Duration // the hub is unreachable in [from, to)
	OutageTo      time.Duration
}

func (f Faults) any() bool { return f != Faults{} }

func (s Scenario) String() string {
	return fmt.Sprintf("seed=%d edges=%d keys=%d algo=%s quota=%d rate=%.1f window=%s lease=%d fixed=%v horizon=%s skew=%s demand=%v hot=%.2f shift=%v drain=%s posthoc=%v debt=%d guard=%d faults=%+v",
		s.Seed, s.Edges, s.Keys, s.Algo.Or(), s.Quota, s.Rate, s.Window, s.LeaseSize, s.Fixed, s.Horizon, s.HubSkew,
		roundAll(s.Demand), s.HotShare, s.Shift, s.DrainAt, s.PostHoc, s.MaxDebt, s.Guard, s.Faults)
}

func roundAll(xs []float64) []int {
	out := make([]int, len(xs))
	for i, x := range xs {
		out[i] = int(x)
	}
	return out
}

// Generate draws a scenario from a seed. Roughly half the scenarios inject
// faults; the rest are clean, so liveness can be checked too.
func Generate(seed uint64) Scenario {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	pick := func(xs ...int64) int64 { return xs[r.IntN(len(xs))] }
	s := Scenario{
		Seed:      seed,
		Edges:     1 + r.IntN(6),
		Keys:      1 + r.IntN(8),
		Window:    time.Duration(pick(1, 2, 5)) * time.Second,
		LeaseSize: pick(1, 5, 20, 100),
		Fixed:     r.IntN(3) == 0,
		Horizon:   time.Duration(pick(250, 1000, 2000)) * time.Millisecond,
		HotShare:  []float64{0, 0.5, 0.9}[r.IntN(3)],
		Shift:     r.IntN(3) == 0,
		Duration:  time.Duration(10+r.IntN(20)) * time.Second,
	}
	switch r.IntN(3) {
	case 0:
		s.Algo, s.Quota = ledger.FixedWindow, pick(20, 100, 500)
	case 1:
		s.Algo, s.Quota = ledger.SlidingWindow, pick(20, 100, 500)
	default:
		s.Algo, s.Quota, s.Rate = ledger.TokenBucket, pick(5, 50, 200), float64(pick(2, 20, 100))
	}
	if r.IntN(3) == 0 {
		s.HubSkew = time.Duration(r.IntN(1001)-500) * time.Millisecond
	}
	// Offer between half and four times the quota in total.
	perSecond := float64(s.Quota) / s.Window.Seconds()
	if s.Algo == ledger.TokenBucket {
		perSecond = s.Rate
	}
	total := perSecond * (0.5 + 3.5*r.Float64()) * float64(s.Keys)
	for i := 0; i < s.Edges; i++ {
		s.Demand = append(s.Demand, total*r.Float64()+1)
	}
	if r.IntN(4) == 0 {
		s.DrainAt = s.Duration / time.Duration(2+r.IntN(3))
	}
	if r.IntN(4) == 0 {
		s.PostHoc, s.MaxDebt = true, pick(5, 20)
	}
	if r.IntN(4) == 0 {
		s.Guard = int64(float64(s.Quota) * (0.5 + r.Float64()))
	}
	if r.IntN(2) == 0 {
		s.Faults = Faults{
			Latency:       time.Duration(r.IntN(50)) * time.Millisecond,
			DropRate:      0.1 * r.Float64(),
			LostReplyRate: 0.1 * r.Float64(),
			DupRate:       0.1 * r.Float64(),
		}
		if r.IntN(2) == 0 {
			s.Faults.OutageFrom = s.Duration / 3
			s.Faults.OutageTo = s.Faults.OutageFrom + time.Duration(1+r.IntN(5))*time.Second
		}
	}
	return s
}

// admit is one admitted request, as the invariants see it.
type admit struct {
	at      time.Time // hub clock, when the request arrived
	win     int64     // the hub window its token was charged to
	edge    int
	subject string
	limit   string
	cost    int64
}

// Result is what a run observed.
type Result struct {
	Scenario   Scenario
	Admits     []admit
	Denied     int
	Unavail    int
	RPCs       int
	Returned   int64
	ledger     *ledger.Mem
	start, end time.Time // hub clock
}

const service, scope, tier = "svc", "v1", "t"

// Run simulates s. It must be called inside a synctest bubble.
func Run(s Scenario) *Result {
	res := &Result{Scenario: s}
	cfg := s.config()
	mem := ledger.NewMem()
	mem.Now = func() time.Time { return time.Now().Add(s.HubSkew) }
	res.ledger = mem
	srv := hub.NewQuotaLeaseServer(mem, hub.NewStaticConfigManager(cfg), hub.NewInMemoryTierStore())

	start := time.Now()
	res.start = start.Add(s.HubSkew)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for e := 0; e < s.Edges; e++ {
		net := &faultyNet{srv: srv, f: s.Faults, start: start, rng: rand.New(rand.NewPCG(s.Seed, uint64(e)+1)), res: res, mu: &mu}
		lease := limiter.NewLease(limiter.NewBatcher(net), limiter.LeaseOptions{LeaseSize: s.LeaseSize, EdgeID: fmt.Sprintf("edge-%d", e)})
		wg.Add(1)
		go func(e int) {
			defer wg.Done()
			s.drive(e, lease, start, res, &mu)
		}(e)
	}
	wg.Wait()
	res.end = time.Now().Add(s.HubSkew)
	// Let in-flight background renewals finish (virtual time only advances
	// while the bubble's main goroutine is alive).
	time.Sleep(10 * time.Second)
	return res
}

func (s Scenario) config() *types.GatewayConfig {
	t := types.TierConfig{TierID: tier, Quota: uint32(s.Quota), GetCost: 1, PostCost: 1, PutCost: 1, DeleteCost: 1, OtherCost: 1, Algorithm: string(s.Algo), Rate: s.Rate}
	if s.Algo == ledger.FixedWindow {
		t.Algorithm = ""
	}
	svc := types.ServiceConfig{ServiceID: service, Auth: types.AuthConfig{Type: "none"}, Tiers: []types.TierConfig{t}}
	if s.Fixed {
		svc.Lease.Mode = "fixed"
	} else {
		svc.Lease.Horizon = s.Horizon
	}
	if s.Guard > 0 {
		svc.Limits = []types.LimitSpec{{ID: "guard", Per: types.PerService, Quota: s.Guard}}
	}
	return &types.GatewayConfig{Prefixes: []types.PrefixConfig{{Prefix: scope, QuotaPeriod: s.Window, Services: []types.ServiceConfig{svc}}}}
}

// drive offers one edge's traffic until the end of the run (or its drain).
func (s Scenario) drive(e int, lease *limiter.Lease, start time.Time, res *Result, mu *sync.Mutex) {
	rng := rand.New(rand.NewPCG(s.Seed, uint64(e)+1000))
	end := start.Add(s.Duration)
	if s.DrainAt > 0 && e == 0 {
		end = start.Add(s.DrainAt)
	}
	ctx := context.Background()
	for {
		rate := s.Demand[e]
		if s.Shift && time.Since(start) > s.Duration/2 {
			rate = s.Demand[(e+1)%len(s.Demand)]
		}
		time.Sleep(time.Duration(rng.ExpFloat64() / rate * float64(time.Second)))
		if !time.Now().Before(end) {
			break
		}
		subject := fmt.Sprintf("key%03d", rng.IntN(s.Keys))
		if rng.Float64() < s.HotShare {
			subject = "key000"
		}
		checks := []limiter.Request{{Scope: scope, Service: service, Key: subject, Tier: tier, Limit: s.Quota, Cost: 1, Window: s.Window, Algo: string(s.Algo), MaxDebt: s.MaxDebt}}
		if s.Guard > 0 {
			checks = append(checks, limiter.Request{Scope: scope, Service: service, LimitID: "guard", Key: types.ServiceSubject, Limit: s.Guard, Cost: 1, Window: s.Window, MaxDebt: s.MaxDebt})
		}
		hubNow := time.Now().Add(s.HubSkew)
		r, err := limiter.Reserve(ctx, lease, checks)
		mu.Lock()
		switch {
		case err != nil:
			res.Unavail++
		case !r.Allowed:
			res.Denied++
		default:
			cost := int64(1)
			if s.PostHoc {
				cost = rng.Int64N(4) // 0..3 against an estimate of 1
				r.Settle(cost)
			}
			res.Admits = append(res.Admits, admit{at: hubNow, win: r.Results[0].Window, edge: e, subject: subject, cost: cost})
			if s.Guard > 0 {
				res.Admits = append(res.Admits, admit{at: hubNow, win: r.Results[1].Window, edge: e, subject: types.ServiceSubject, limit: "guard", cost: cost})
			}
		}
		mu.Unlock()
	}
	if s.DrainAt > 0 && e == 0 {
		_ = lease.Drain(ctx)
	}
}

// faultyNet is the network between one edge and the hub.
type faultyNet struct {
	srv   *hub.QuotaLeaseServer
	f     Faults
	start time.Time
	mu    *sync.Mutex // guards rng and res
	rng   *rand.Rand
	res   *Result
}

func (n *faultyNet) roll(p float64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rng.Float64() < p
}

func (n *faultyNet) delay() {
	if n.f.Latency <= 0 {
		return
	}
	n.mu.Lock()
	d := time.Duration(n.rng.Int64N(int64(n.f.Latency)))
	n.mu.Unlock()
	time.Sleep(d)
}

// call runs one RPC through the faults.
func (n *faultyNet) call(do func() error) error {
	n.mu.Lock()
	n.res.RPCs++
	n.mu.Unlock()
	n.delay()
	if el := time.Since(n.start); el >= n.f.OutageFrom && el < n.f.OutageTo {
		return status.Error(codes.Unavailable, "hub down")
	}
	if n.roll(n.f.DropRate) {
		return status.Error(codes.Unavailable, "dropped")
	}
	err := do()
	if n.roll(n.f.DupRate) {
		_ = do() // a retransmission the hub also acts on
	}
	n.delay()
	if n.roll(n.f.LostReplyRate) {
		return status.Error(codes.DeadlineExceeded, "reply lost")
	}
	return err
}

func (n *faultyNet) RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	var out *types.QuotaLeaseResponse
	err := n.call(func() (err error) { out, err = n.srv.RequestQuotaLease(ctx, in); return })
	return out, err
}

func (n *faultyNet) RequestQuotaLeases(ctx context.Context, in *types.QuotaLeaseBatchRequest) (*types.QuotaLeaseBatchResponse, error) {
	var out *types.QuotaLeaseBatchResponse
	err := n.call(func() (err error) { out, err = n.srv.RequestQuotaLeases(ctx, in); return })
	return out, err
}

func (n *faultyNet) ReturnLeases(ctx context.Context, in *types.ReturnLeasesRequest) (*types.ReturnLeasesResponse, error) {
	var out *types.ReturnLeasesResponse
	err := n.call(func() (err error) { out, err = n.srv.ReturnLeases(ctx, in); return })
	if err == nil {
		n.mu.Lock()
		for _, r := range in.Returns {
			n.res.Returned += r.Tokens
		}
		n.mu.Unlock()
	}
	return out, err
}

// Check returns every invariant the run broke.
func (r *Result) Check() []string {
	s := r.Scenario
	var bad []string
	// Post-hoc costs may overdraw each edge's share by its debt bound.
	slack := int64(0)
	if s.PostHoc {
		slack = int64(s.Edges) * s.MaxDebt // each edge's balance overdraws by at most MaxDebt
	}
	bySubject := map[string][]admit{}
	for _, a := range r.Admits {
		bySubject[a.limit+"/"+a.subject] = append(bySubject[a.limit+"/"+a.subject], a)
	}
	for id, admits := range bySubject {
		quota, algo := s.Quota, s.Algo
		if strings.HasPrefix(id, "guard/") {
			quota, algo = s.Guard, ledger.FixedWindow
		}
		if algo == ledger.TokenBucket {
			bad = append(bad, r.checkBucket(id, admits, quota, slack)...)
			continue
		}
		// Fixed and sliding windows: never more than the quota in one
		// window of the hub's clock.
		perWindow := map[int64]int64{}
		for _, a := range admits {
			perWindow[a.win] += a.cost
		}
		for w, n := range perWindow {
			if n > quota+slack {
				bad = append(bad, fmt.Sprintf("%s: window %d admitted %d > quota %d (+%d debt)", id, w, n, quota, slack))
			}
		}
	}
	return bad
}

// checkBucket checks the token bucket's bound: in any interval of length τ,
// admitted ≤ burst + rate × (τ + lease lifetime). Tokens are debited from
// the bucket when granted and may be spent up to one lease lifetime later.
func (r *Result) checkBucket(id string, admits []admit, burst, slack int64) []string {
	s := r.Scenario
	ttl := time.Second
	if !s.Fixed {
		ttl = s.Horizon
	}
	sort.Slice(admits, func(i, j int) bool { return admits[i].at.Before(admits[j].at) })
	var bad []string
	for _, tau := range []time.Duration{time.Second, 5 * time.Second} {
		bound := float64(burst) + s.Rate*(tau+ttl).Seconds() + float64(slack)
		var sum int64
		j := 0
		for i := range admits {
			sum += admits[i].cost
			for admits[i].at.Sub(admits[j].at) > tau {
				sum -= admits[j].cost
				j++
			}
			if float64(sum) > math.Ceil(bound)+1 {
				bad = append(bad, fmt.Sprintf("%s: %d admitted within %s, bound %.0f", id, sum, tau, bound))
				break
			}
		}
	}
	return bad
}

// Utilization is the share of the hot key's quota admitted in the windows
// fully inside the run (fixed and sliding windows only; -1 otherwise).
func (r *Result) Utilization() float64 {
	s := r.Scenario
	if s.Algo == ledger.TokenBucket {
		return -1
	}
	wms := s.Window.Milliseconds()
	first, last := r.start.UnixMilli()/wms+1, r.end.UnixMilli()/wms-1
	if last < first {
		return -1
	}
	var n int64
	for _, a := range r.Admits {
		if w := a.at.UnixMilli() / wms; a.subject == "key000" && a.limit == "" && w >= first && w <= last {
			n += a.cost
		}
	}
	return float64(n) / float64(s.Quota*(last-first+1))
}

// Saturated reports whether the hot key's offered load exceeds its quota by
// a margin in every window, on every edge pattern (so utilization should be
// near 100%).
func (s Scenario) Saturated() bool {
	if s.Algo == ledger.TokenBucket || s.DrainAt > 0 || s.Faults.any() || s.Guard > 0 || s.PostHoc || s.Shift {
		return false
	}
	var total float64
	for _, d := range s.Demand {
		total += d
	}
	hot := total * (s.HotShare + (1-s.HotShare)/float64(s.Keys))
	return hot*s.Window.Seconds() > 2*float64(s.Quota)
}
