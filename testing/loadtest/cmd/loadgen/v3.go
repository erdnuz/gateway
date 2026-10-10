package main

// Scenarios for Gate v3: endgame splitting, lease return on drain, batching,
// the algorithms, multiple limits, post-hoc cost and the decision service.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// openLoopTo is openLoop with a choice of edge per request.
func openLoopTo(o options, path string, keyFn func(i int) string, edgeFn func(i int) int, rps int, d time.Duration) []sample {
	interval := time.Second / time.Duration(rps)
	total := int(d / interval)
	out := make([]sample, total)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < total; i++ {
		if w := time.Until(start.Add(time.Duration(i) * interval)); w > 0 {
			time.Sleep(w)
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := edgeFn(i) % len(o.edges)
			s := doFull(context.Background(), o.edges[e]+path, keyFn(i))
			s.edge = e
			out[i] = s
		}(i)
	}
	wg.Wait()
	return out
}

// fullWindows keeps the windows that received at least 90% of the offered
// requests (not the partial ones at either end).
func fullWindows(ws []windowStat, perWindow int) []windowStat {
	var out []windowStat
	for _, w := range ws {
		if w.Sent >= perWindow*9/10 {
			out = append(out, w)
		}
	}
	return out
}

// Endgame: two edges with 80/20 demand. The hub splits the last of the quota
// between the active edges, so the busy edge is not starved while the quiet
// one sits on tokens: every window still reaches the quota.
func scenarioEndgame(o options) *report {
	r := &report{Scenario: "endgame", Data: map[string]any{}}
	rps := int(2 * o.quota / int64(o.period/time.Second))
	o.alignToWindowStart()
	samples := openLoopTo(o, "/"+o.prefix+"/svc/ping", func(int) string { return "end0" },
		func(i int) int {
			if i%5 == 0 {
				return 1
			}
			return 0
		}, rps, 3*o.period)
	ws := perWindow(o, samples)
	r.Data["windows"] = ws
	slack := int64(len(o.edges)) * 2
	fmt.Fprintf(&r.markdown, "%d rps (2x quota) on one key, 80%% to edge 1 and 20%% to edge 2. Expect every window in [Q-%d, Q]: stranded quota is about one token per active edge, not a lease per edge.\n\n", rps, slack)
	windowTable(&r.markdown, ws)
	for _, w := range fullWindows(ws, rps*int(o.period/time.Second)) {
		a := int64(w.Admitted)
		r.check(a >= o.quota-slack && a <= o.quota, "window %d admitted %d (want %d..%d)", w.Window, a, o.quota-slack, o.quota)
	}
	return r
}

// The drain scenario uses group "ld": a 30s window and fixed leases of 400
// of a quota of 1000, so a lease left on a departing edge is obvious.
const drainPrefix, drainQuota, drainLease = "ld", 1000, 400

func drainWindowFile(o options) string { return filepath.Join(o.outDir, ".drain-window") }

// Drain, part 1: both edges take a 400-token lease at the start of a window.
// run.sh then stops edge 1 gracefully.
func scenarioDrainWarm(o options) *report {
	p := 30 * time.Second
	now := time.Now()
	next := now.Truncate(p).Add(p)
	if time.Until(next) < p-3*time.Second { // not near a window start: wait for one
		time.Sleep(time.Until(next) + 50*time.Millisecond)
	}
	var win int64
	for e := range o.edges {
		s := doFull(context.Background(), o.edges[e]+"/"+drainPrefix+"/fixed/ping", "drain0")
		log.Printf("drain-warm edge %d status %d window %d", e+1, s.status, s.win)
		if s.win > 0 {
			win = s.win
		}
	}
	_ = os.WriteFile(drainWindowFile(o), []byte(strconv.FormatInt(win, 10)), 0o644)
	return nil
}

// Drain, part 2: with edge 1 gone, flood edge 2 for the rest of the window.
// Edge 1 returned its unspent lease when it shut down, so edge 2 can still
// use nearly the whole quota; without returns it would stop near Q - 400.
func scenarioDrain(o options) *report {
	r := &report{Scenario: "drain", Data: map[string]any{}}
	raw, _ := os.ReadFile(drainWindowFile(o))
	win, _ := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	end := time.Unix((win+1)*30, 0)
	d := time.Until(end) - 300*time.Millisecond
	if win == 0 || d < 3*time.Second {
		r.check(false, "inconclusive: window %d has %s left after stopping edge 1", win, d)
		return r
	}
	survivor := len(o.edges) - 1
	samples := openLoopTo(o, "/"+drainPrefix+"/fixed/ping", func(int) string { return "drain0" }, func(int) int { return survivor }, 400, d)
	var inWindow int64
	for _, s := range samples {
		if admitted(s) && s.win == win {
			inWindow++
		}
	}
	total := inWindow + 2 // the two warm-up requests
	r.Data["window"] = win
	r.Data["admitted"] = total
	r.Data["statuses"] = statusCounts(samples)
	fmt.Fprintf(&r.markdown, "Both edges lease %d of %d at the start of a 30s window; edge 1 is stopped (SIGTERM); edge 2 is flooded for the rest of the window.\n\n- admitted in the window: **%d** (without lease return at most %d)\n- statuses: %v\n",
		drainLease, drainQuota, total, drainQuota-drainLease+1, statusCounts(samples))
	r.check(total >= drainQuota-drainLease/8, "window admitted %d >= %d: the drained edge's lease was returned", total, drainQuota-drainLease/8)
	r.check(total <= drainQuota, "window admitted %d <= quota %d", total, drainQuota)
	return r
}

// scrape sums Prometheus counters/histogram series on an edge's /metrics.
func scrape(edgeURL string) map[string]float64 {
	u := strings.Replace(edgeURL, ":8080", ":9100", 1) + "/metrics"
	out := map[string]float64{}
	resp, err := client.Get(u)
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		name, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err == nil {
			out[name] += f
		}
	}
	return out
}

// Many keys: 5000 cold keys at once. Concurrent lease requests share batched
// RPCs, so the hub sees far fewer calls than there are keys.
func scenarioManyKeys(o options) *report {
	r := &report{Scenario: "manykeys", Data: map[string]any{}}
	before := make([]map[string]float64, len(o.edges))
	for e := range o.edges {
		before[e] = scrape(o.edges[e])
	}
	var wg sync.WaitGroup
	var ok atomic.Int64
	sem := make(chan struct{}, 256)
	start := time.Now()
	for i := 0; i < 5000; i++ {
		for e := range o.edges {
			wg.Add(1)
			sem <- struct{}{}
			go func(i, e int) {
				defer wg.Done()
				defer func() { <-sem }()
				if code, _ := do(context.Background(), o.edges[e]+"/"+o.prefix+"/svc/ping", fmt.Sprintf("many%d", i)); code/100 == 2 {
					ok.Add(1)
				}
			}(i, e)
		}
	}
	wg.Wait()
	el := time.Since(start)
	var items, batches float64
	for e := range o.edges {
		after := scrape(o.edges[e])
		items += after["gate_edge_lease_batch_size_sum"] - before[e]["gate_edge_lease_batch_size_sum"]
		batches += after["gate_edge_lease_batch_size_count"] - before[e]["gate_edge_lease_batch_size_count"]
	}
	mean := 0.0
	if batches > 0 {
		mean = items / batches
	}
	r.Data["requests"] = 5000 * len(o.edges)
	r.Data["admitted"] = ok.Load()
	r.Data["lease_items"] = items
	r.Data["lease_rpcs"] = batches
	r.Data["mean_batch"] = mean
	fmt.Fprintf(&r.markdown, "5000 cold keys on each of %d edges, 256 concurrent requests, in %s.\n\n- lease requests: %.0f in **%.0f RPCs** (mean batch %.1f)\n- admitted: %d of %d\n",
		len(o.edges), el.Round(time.Millisecond), items, batches, mean, ok.Load(), 5000*len(o.edges))
	r.check(ok.Load() == int64(5000*len(o.edges)), "every request admitted (%d)", ok.Load())
	r.check(batches > 0 && mean >= 2, "concurrent leases were batched (mean batch %.1f)", mean)
	return r
}

// Algorithms: a sliding window has no 2x burst across a reset, and a token
// bucket admits its burst and then its rate.
func scenarioAlgorithms(o options) *report {
	r := &report{Scenario: "algorithms", Data: map[string]any{}}
	p := int(o.period / time.Second)

	// Sliding window: 2x quota for 3 windows. Per hub window <= Q, and in
	// any window-length interval (sampled each second) <= Q plus the
	// approximation's slack.
	rps := int(2 * o.quota / int64(p))
	o.alignToWindowStart()
	slide := openLoopTo(o, "/"+o.prefix+"/algo/ping", func(int) string { return "slide0" }, func(i int) int { return i }, rps, 3*o.period)
	ws := perWindow(o, slide)
	maxSpan := maxInSpan(slide, o.period)
	r.Data["sliding_windows"] = ws
	r.Data["sliding_max_per_window_span"] = maxSpan
	fmt.Fprintf(&r.markdown, "**sliding_window** (Q=%d per %ds), %d rps:\n\n", o.quota, p, rps)
	windowTable(&r.markdown, ws)
	fmt.Fprintf(&r.markdown, "\n- most admitted in any %ds span: **%d** (a fixed window can admit 2Q across a reset)\n", p, maxSpan)
	for _, w := range fullWindows(ws, rps*p) {
		r.check(int64(w.Admitted) <= o.quota, "sliding: window %d admitted %d <= %d", w.Window, w.Admitted, o.quota)
	}
	r.check(int64(maxSpan) <= o.quota*13/10, "sliding: any %ds span admitted %d <= 1.3Q", p, maxSpan)

	// Token bucket: burst 100, 50/s. Offer 200/s for 20s: about
	// burst + 50/s admitted.
	const burst, rate, secs = 100, 50.0, 20
	bucket := openLoopTo(o, "/"+o.prefix+"/algo/ping", func(int) string { return "bucket0" }, func(i int) int { return i }, 200, secs*time.Second)
	n := 0
	for _, s := range bucket {
		if admitted(s) {
			n++
		}
	}
	lo, hi := int(rate*secs*0.9), int(burst+rate*(secs+2))+2 // +2s: the grant lifetime
	maxSec := maxInSpan(bucket, time.Second)
	r.Data["bucket_admitted"] = n
	r.Data["bucket_max_per_second"] = maxSec
	fmt.Fprintf(&r.markdown, "\n**token_bucket** (burst %d, %.0f/s), 200 rps for %ds:\n\n- admitted **%d** (bound %d..%d)\n- most in any second: %d\n", burst, rate, secs, n, lo, hi, maxSec)
	r.check(n >= lo && n <= hi, "bucket admitted %d in %ds (want %d..%d)", n, secs, lo, hi)
	return r
}

// maxInSpan is the most requests admitted within any span of length d.
func maxInSpan(samples []sample, d time.Duration) int {
	var ts []time.Time
	for _, s := range samples {
		if admitted(s) {
			ts = append(ts, s.sentAt)
		}
	}
	best, j := 0, 0
	for i := range ts { // samples are sent in order
		for ts[i].Sub(ts[j]) >= d {
			j++
		}
		best = max(best, i-j+1)
	}
	return best
}

// Multi-limit: ten keys, each well within its tier, share one client
// address limited to 300 per window. The per-IP limit binds.
func scenarioMultiLimit(o options) *report {
	r := &report{Scenario: "multilimit", Data: map[string]any{}}
	const ipQuota = 300
	rps := 100 // 1000 per window from ten keys, 100 each
	o.alignToWindowStart()
	samples := openLoopTo(o, "/"+o.prefix+"/multi/ping", func(i int) string { return fmt.Sprintf("multi%d", i%10) }, func(i int) int { return i }, rps, 2*o.period)
	ws := perWindow(o, samples)
	r.Data["windows"] = ws
	fmt.Fprintf(&r.markdown, "Ten keys (tier quota %d each) from one client address, %d rps total, with `also: per-ip %d per window`. Expect the per-IP limit to bind.\n\n", o.quota, rps, ipQuota)
	windowTable(&r.markdown, ws)
	for _, w := range fullWindows(ws, rps*int(o.period/time.Second)) {
		r.check(w.Admitted <= ipQuota && w.Admitted >= ipQuota-2*int(len(o.edges)), "window %d admitted %d (per-IP limit %d)", w.Window, w.Admitted, ipQuota)
	}
	return r
}

// LLM: the upstream reports each request's actual cost (0-100 tokens); the
// edge reserved an estimate of 50 and settles the difference. Tokens charged
// per window stay within the quota plus each edge's debt bound.
func scenarioLLM(o options) *report {
	r := &report{Scenario: "llm", Data: map[string]any{}}
	const tokens, maxDebt = 5000, 200
	rps := 40 // ~ 40 x 50 x 10s = 20000 tokens offered per window
	o.alignToWindowStart()
	samples := openLoopTo(o, "/"+o.prefix+"/llm/usage", func(int) string { return "llm0" }, func(i int) int { return i }, rps, 3*o.period)
	used := map[int64]int64{}
	for _, s := range samples {
		if admitted(s) && s.usage >= 0 {
			used[s.win] += s.usage
		}
	}
	ws := perWindow(o, samples)
	r.Data["windows"] = ws
	r.Data["tokens_per_window"] = used
	bound := int64(tokens + maxDebt*len(o.edges))
	fmt.Fprintf(&r.markdown, "Quota %d tokens per window; each request reserves 50, the upstream reports 0-100, the edge settles the difference (max_debt %d). Expect tokens charged per window <= %d.\n\n| window | requests admitted | tokens used |\n|---|---|---|\n", tokens, maxDebt, bound)
	for _, w := range ws {
		fmt.Fprintf(&r.markdown, "| %d | %d | **%d** |\n", w.Window, w.Admitted, used[w.Window])
	}
	for _, w := range fullWindows(ws, rps*int(o.period/time.Second)) {
		u := used[w.Window]
		r.check(u <= bound, "window %d used %d tokens <= %d", w.Window, u, bound)
		r.check(u >= tokens*7/10, "window %d used %d tokens: the quota is spent on actual, not estimated, cost (>= %d)", w.Window, u, tokens*7/10)
	}
	return r
}

// RLS: two gate limiter replicas answer the same descriptor from one global
// quota (500 per window) at twice that rate.
func scenarioRLS(o options) *report {
	r := &report{Scenario: "rls", Data: map[string]any{}}
	limiters := []string{"http://limiter-1:8080", "http://limiter-2:8080"}
	const quota = 500
	rps := 2 * quota / int(o.period/time.Second)
	body := []byte(`{"domain":"lt","descriptors":[{"entries":[{"key":"user","value":"u1"}]}]}`)
	type res struct {
		win     int64
		allowed bool
	}
	o.alignToWindowStart()
	interval := time.Second / time.Duration(rps)
	total := int(3 * o.period / interval)
	out := make([]res, total)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < total; i++ {
		if w := time.Until(start.Add(time.Duration(i) * interval)); w > 0 {
			time.Sleep(w)
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := client.Post(limiters[i%2]+"/v1/check", "application/json", bytes.NewReader(body))
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var v struct {
				Allowed  bool `json:"allowed"`
				Statuses []struct {
					Window int64 `json:"window"`
				} `json:"statuses"`
			}
			if json.NewDecoder(resp.Body).Decode(&v) == nil && len(v.Statuses) > 0 {
				out[i] = res{win: v.Statuses[0].Window, allowed: v.Allowed}
			}
		}(i)
	}
	wg.Wait()
	per := map[int64]int{}
	for _, x := range out {
		if x.allowed {
			per[x.win]++
		}
	}
	r.Data["admitted_per_window"] = per
	fmt.Fprintf(&r.markdown, "Two gate limiter replicas, %d checks/s alternating between them, one descriptor limited to %d per window.\n\n| window | admitted |\n|---|---|\n", rps, quota)
	for _, w := range sortedWindows(per) {
		fmt.Fprintf(&r.markdown, "| %d | **%d** |\n", w, per[w])
	}
	full := sortedWindows(per)
	if len(full) > 2 {
		full = full[1 : len(full)-1] // drop the partial windows at either end
	}
	for _, w := range full {
		r.check(per[w] <= quota && per[w] >= quota-4, "window %d admitted %d (quota %d)", w, per[w], quota)
	}
	return r
}

func sortedWindows(m map[int64]int) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		if k > 0 {
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
