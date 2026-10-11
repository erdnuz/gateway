// Command loadgen drives end-to-end rate-limiter scenarios against a running
// Gate deployment (hub + N edges) and reports correctness and performance.
//
// Each scenario writes <out>/<scenario>.json and appends a markdown section to
// <out>/report.md. Scenarios that check an invariant record pass/fail.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gateway/packages/config"
)

type options struct {
	edges      []string
	hub        string
	hubToken   string
	prefix     string
	service    string // demo scenario only
	period     time.Duration
	quota      int64 // "pro" tier quota
	safety     int64 // safety tier quota
	lease      int64 // lease quantum
	configPath string
	outDir     string
	duration   time.Duration
}

type report struct {
	Scenario string         `json:"scenario"`
	Pass     *bool          `json:"pass,omitempty"`
	Checks   []string       `json:"checks,omitempty"`
	Data     map[string]any `json:"data"`
	markdown strings.Builder
}

func (r *report) check(ok bool, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	mark := "PASS"
	if !ok {
		mark = "FAIL"
	}
	r.Checks = append(r.Checks, mark+": "+msg)
	if r.Pass == nil {
		r.Pass = &ok
	} else if !ok {
		f := false
		r.Pass = &f
	}
}

func main() {
	var edges, scenario string
	o := options{}
	flag.StringVar(&edges, "edges", "http://edge-1:8080,http://edge-2:8080", "comma-separated edge base URLs")
	flag.StringVar(&o.hub, "hub", "http://hub-server:8080", "hub base URL")
	flag.StringVar(&o.hubToken, "hub-token", os.Getenv("HUB_AUTH_TOKEN"), "hub bearer token")
	flag.StringVar(&o.prefix, "prefix", "lt", "gateway prefix")
	flag.StringVar(&o.service, "service", "svc", "service for the demo scenario")
	flag.DurationVar(&o.period, "period", 10*time.Second, "quota period configured for the prefix")
	flag.Int64Var(&o.quota, "quota", 1000, "pro tier quota per period")
	flag.Int64Var(&o.safety, "safety", 50, "safety tier quota per period")
	flag.Int64Var(&o.lease, "lease", 100, "lease quantum")
	flag.StringVar(&o.configPath, "config", "/lt/gateway-config.loadtest.yaml", "gateway config used by the hub")
	flag.StringVar(&o.outDir, "out", "/results/run", "output directory")
	flag.DurationVar(&o.duration, "duration", 10*time.Second, "duration per throughput level")
	flag.StringVar(&scenario, "scenario", "", "scenario to run")
	flag.Parse()
	o.edges = strings.Split(edges, ",")

	if err := os.MkdirAll(o.outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	run := map[string]func(options) *report{
		"setup":        scenarioSetup,
		"windows":      scenarioWindows,
		"carryover":    scenarioCarryover,
		"isolation":    scenarioIsolation,
		"bypass":       scenarioBypass,
		"propagation":  scenarioPropagation,
		"throughput":   scenarioThroughput,
		"hotkey":       scenarioHotKey,
		"outage-warm":  scenarioOutageWarm,
		"outage":       scenarioOutage,
		"outage-check": scenarioOutageCheck,
		"demo":         scenarioDemo,
		// v3: smarter leasing, algorithms, multi-limit, post-hoc cost, the
		// decision service (v3.go).
		"endgame":    scenarioEndgame,
		"drain-warm": scenarioDrainWarm,
		"drain":      scenarioDrain,
		"manykeys":   scenarioManyKeys,
		"algorithms": scenarioAlgorithms,
		"multilimit": scenarioMultiLimit,
		"llm":        scenarioLLM,
		"rls":        scenarioRLS,
		// The comparative benchmark (testing/bench, bench.go).
		"bench-setup": scenarioBenchSetup,
		"bench":       scenarioBench,
		"hubscale":    scenarioHubScale,
	}
	fn, ok := run[scenario]
	if !ok {
		log.Fatalf("unknown scenario %q", scenario)
	}
	rep := fn(o)
	if rep == nil {
		return
	}
	writeReport(o, rep)
	if rep.Pass != nil && !*rep.Pass {
		os.Exit(2)
	}
}

// ---------------------------------------------------------------------------
// HTTP plumbing

var client = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 2048,
		IdleConnTimeout:     60 * time.Second,
	},
}

type sample struct {
	edge   int
	status int // 0 = transport error
	sentAt time.Time
	lat    time.Duration
	win    int64 // the hub window charged (Gate-Window), 0 when unknown
	usage  int64 // cost the upstream reported (X-Usage-Total-Tokens), -1 when none
}

func (o options) url(edge int, service string) string {
	return fmt.Sprintf("%s/%s/%s/ping", o.edges[edge%len(o.edges)], o.prefix, service)
}

func do(ctx context.Context, url, apiKey string) (int, time.Duration) {
	r := doFull(ctx, url, apiKey)
	return r.status, r.lat
}

// doFull is do with the response's Gate-Window and usage headers.
func doFull(ctx context.Context, url, apiKey string) sample {
	start := time.Now()
	s := sample{sentAt: start, usage: -1}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return s
	}
	req.Header.Set("X-API-Key", apiKey)
	resp, err := client.Do(req)
	if err != nil {
		s.lat = time.Since(start)
		return s
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	s.status, s.lat = resp.StatusCode, time.Since(start)
	s.win, _ = strconv.ParseInt(resp.Header.Get("Gate-Window"), 10, 64)
	if u, err := strconv.ParseInt(resp.Header.Get("X-Usage-Total-Tokens"), 10, 64); err == nil {
		s.usage = u
	}
	return s
}

func (o options) hubCall(method, path string, body any) (int, error) {
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case []byte: // already encoded (e.g. a YAML config document)
		rdr = bytes.NewReader(b)
	default:
		enc, err := json.Marshal(b)
		if err != nil {
			return 0, err
		}
		rdr = bytes.NewReader(enc)
	}
	req, err := http.NewRequest(method, o.hub+path, rdr)
	if err != nil {
		return 0, err
	}
	if o.hubToken != "" {
		req.Header.Set("Authorization", "Bearer "+o.hubToken)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

func (o options) setTier(apiKey, tier string) error { return o.setTierIn(o.prefix, apiKey, tier) }

func (o options) setTierIn(prefix, apiKey, tier string) error {
	code, err := o.hubCall(http.MethodPost, fmt.Sprintf("/tiers/%s/%s", prefix, apiKey), map[string]string{"tier_id": tier})
	if err != nil {
		return err
	}
	if code/100 != 2 {
		return fmt.Errorf("set tier %s=%s: status %d", apiKey, tier, code)
	}
	return nil
}

// openLoop sends requests at a fixed total rate, round-robin across edges,
// for the given duration. Each request runs in its own goroutine so slow
// responses do not throttle the offered load.
func openLoop(o options, service string, keyFn func(i int) string, rps int, d time.Duration) []sample {
	interval := time.Second / time.Duration(rps)
	total := int(d / interval)
	out := make([]sample, total)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < total; i++ {
		next := start.Add(time.Duration(i) * interval)
		if w := time.Until(next); w > 0 {
			time.Sleep(w)
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := doFull(context.Background(), o.url(i, service), keyFn(i))
			s.edge = i % len(o.edges)
			out[i] = s
		}(i)
	}
	wg.Wait()
	return out
}

// closedLoop runs `conc` workers back-to-back for duration d.
func closedLoop(o options, service string, keyFn func(worker, iter int) string, conc int, d time.Duration) ([]sample, time.Duration) {
	deadline := time.Now().Add(d)
	per := make([][]sample, conc)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := make([]sample, 0, 4096)
			for it := 0; time.Now().Before(deadline); it++ {
				sent := time.Now()
				code, lat := do(context.Background(), o.url(w+it, service), keyFn(w, it))
				local = append(local, sample{edge: (w + it) % len(o.edges), status: code, sentAt: sent, lat: lat})
			}
			per[w] = local
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	var all []sample
	for _, s := range per {
		all = append(all, s...)
	}
	return all, elapsed
}

func (o options) windowID(t time.Time) int64 {
	return t.Unix() / int64(o.period/time.Second)
}

// alignToWindowStart sleeps until just after the next window boundary and
// returns that window's id.
func (o options) alignToWindowStart() int64 {
	now := time.Now()
	p := int64(o.period / time.Second)
	next := (now.Unix()/p + 1) * p
	time.Sleep(time.Until(time.Unix(next, 0).Add(50 * time.Millisecond)))
	return next / p
}

// ---------------------------------------------------------------------------
// Stats

type latencyStats struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
}

func latencies(samples []sample, filter func(sample) bool) latencyStats {
	var ls []time.Duration
	for _, s := range samples {
		if filter == nil || filter(s) {
			ls = append(ls, s.lat)
		}
	}
	if len(ls) == 0 {
		return latencyStats{}
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i] < ls[j] })
	q := func(p float64) float64 {
		idx := int(p * float64(len(ls)-1))
		return float64(ls[idx].Microseconds()) / 1000
	}
	return latencyStats{Count: len(ls), P50: q(0.50), P90: q(0.90), P99: q(0.99), P999: q(0.999), Max: q(1)}
}

func statusCounts(samples []sample) map[string]int {
	m := map[string]int{}
	for _, s := range samples {
		m[fmt.Sprint(s.status)]++
	}
	return m
}

func admitted(s sample) bool { return s.status >= 200 && s.status < 300 }

type windowStat struct {
	Window   int64          `json:"window"`
	Sent     int            `json:"sent"`
	Admitted int            `json:"admitted"`
	PerEdge  []int          `json:"admitted_per_edge"`
	Statuses map[string]int `json:"statuses"`
}

func perWindow(o options, samples []sample) []windowStat {
	byID := map[int64]*windowStat{}
	for _, s := range samples {
		// Attribute each request to the hub window its quota came from when
		// the edge says (exact), else to the window it was sent in.
		id := s.win
		if id == 0 {
			id = o.windowID(s.sentAt)
		}
		ws, ok := byID[id]
		if !ok {
			ws = &windowStat{Window: id, PerEdge: make([]int, len(o.edges)), Statuses: map[string]int{}}
			byID[id] = ws
		}
		ws.Sent++
		ws.Statuses[fmt.Sprint(s.status)]++
		if admitted(s) {
			ws.Admitted++
			ws.PerEdge[s.edge]++
		}
	}
	out := make([]windowStat, 0, len(byID))
	for _, ws := range byID {
		out = append(out, *ws)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Window < out[j].Window })
	return out
}

func windowTable(b *strings.Builder, ws []windowStat) {
	b.WriteString("| window | sent | admitted | per edge | statuses |\n|---|---|---|---|---|\n")
	for _, w := range ws {
		fmt.Fprintf(b, "| %d | %d | **%d** | %v | %v |\n", w.Window, w.Sent, w.Admitted, w.PerEdge, w.Statuses)
	}
}

// ---------------------------------------------------------------------------
// Scenarios

func scenarioSetup(o options) *report {
	assign := map[string]string{
		"winkey0": "pro", "carry0": "pro", "iso0": "pro", "out0": "pro", "victim0": "pro",
		"prop0": "pro", "prop1": "pro", "hot0": "bulk",
		"end0": "pro", "slide0": "sliding", "bucket0": "bucket", "llm0": "pro",
	}
	for i := 0; i < 10; i++ {
		assign[fmt.Sprintf("multi%d", i)] = "pro"
	}
	for i := 0; i < 5000; i++ {
		assign[fmt.Sprintf("many%d", i)] = "bulk"
	}
	for i := 0; i < 1000; i++ {
		assign[fmt.Sprintf("bulk%d", i)] = "bulk"
	}
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for k, t := range assign {
		wg.Add(1)
		sem <- struct{}{}
		go func(k, t string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := o.setTier(k, t); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(k, t)
	}
	wg.Wait()
	if firstErr == nil {
		firstErr = o.setTierIn(drainPrefix, "drain0", "pro")
	}
	if firstErr != nil {
		log.Fatalf("setup failed: %v", firstErr)
	}
	log.Printf("setup: assigned %d tiers", len(assign))
	return nil
}

// S1: sustained 2x quota on one key across all edges for several windows.
func scenarioWindows(o options) *report {
	r := &report{Scenario: "windows", Data: map[string]any{}}
	windows := 4
	rps := int(2 * o.quota / int64(o.period/time.Second))
	o.alignToWindowStart()
	samples := openLoop(o, "svc", func(int) string { return "winkey0" }, rps, time.Duration(windows)*o.period)
	ws := perWindow(o, samples)
	r.Data["offered_rps"] = rps
	r.Data["windows"] = ws
	tol := o.lease * int64(len(o.edges))
	fmt.Fprintf(&r.markdown, "Offered %d rps (2x quota) on one key, round-robin over %d edges. Requests are attributed to the hub window that charged them (Gate-Window). Expect admitted per window in [Q-%d, Q], Q=%d.\n\n", rps, len(o.edges), tol, o.quota)
	windowTable(&r.markdown, ws)
	for _, w := range ws {
		if w.Sent < rps*int(o.period/time.Second)*9/10 {
			continue // partial window at the tail
		}
		a := int64(w.Admitted)
		r.check(a >= o.quota-tol && a <= o.quota,
			"window %d admitted %d (want %d..%d)", w.Window, a, o.quota-tol, o.quota)
	}
	return r
}

// S2: lease tokens left over from window N must not inflate window N+1.
func scenarioCarryover(o options) *report {
	r := &report{Scenario: "carryover", Data: map[string]any{}}
	o.alignToWindowStart()
	// Window N: a trickle so every edge takes a lease but uses little of it.
	warm := openLoop(o, "svc", func(int) string { return "carry0" }, 10, 2*time.Second)
	// Window N+1: flood.
	o.alignToWindowStart()
	rps := int(3 * o.quota / int64(o.period/time.Second))
	flood := openLoop(o, "svc", func(int) string { return "carry0" }, rps, o.period-500*time.Millisecond)
	ws := perWindow(o, flood)
	r.Data["warm_statuses"] = statusCounts(warm)
	r.Data["flood"] = ws
	fmt.Fprintf(&r.markdown, "Window N: 10 rps for 2s (each edge leases up to %d, uses ~10). Window N+1: %d rps. Expect admitted in N+1 <= Q.\n\n", o.lease, rps)
	windowTable(&r.markdown, ws)
	if len(ws) > 0 {
		a := int64(ws[0].Admitted)
		r.check(a <= o.quota, "window N+1 admitted %d (limit %d)", a, o.quota)
		r.check(a >= o.quota-o.lease*int64(len(o.edges)), "window N+1 admitted %d is not starved (>= %d)", a, o.quota-o.lease*int64(len(o.edges)))
	}
	return r
}

// S7: quota for one service must not be spent by another service.
func scenarioIsolation(o options) *report {
	r := &report{Scenario: "isolation", Data: map[string]any{}}
	o.alignToWindowStart()
	n := int(o.quota) + 100
	a := openLoop(o, "svc", func(int) string { return "iso0" }, 600, time.Duration(n)*time.Second/600)
	b := openLoop(o, "svc2", func(int) string { return "iso0" }, 100, 500*time.Millisecond)
	r.Data["svc_statuses"] = statusCounts(a)
	r.Data["svc2_statuses"] = statusCounts(b)
	okB := 0
	for _, s := range b {
		if admitted(s) {
			okB++
		}
	}
	fmt.Fprintf(&r.markdown, "Exhaust key on `svc` (%d requests), then 50 requests to `svc2` in the same window.\n\n- svc: %v\n- svc2: %v\n", n, statusCounts(a), statusCounts(b))
	r.check(okB == len(b), "svc2 admitted %d/%d after svc exhausted", okB, len(b))
	return r
}

// S6: API key with a path suffix must be rejected, not treated as a new key.
func scenarioBypass(o options) *report {
	r := &report{Scenario: "bypass", Data: map[string]any{}}
	var samples []sample
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("victim0/x%d", i)
		for j := 0; j < 25; j++ {
			code, lat := do(context.Background(), o.url(j, "svc"), key)
			samples = append(samples, sample{edge: j % len(o.edges), status: code, lat: lat})
		}
	}
	// Also a well-formed key that was never issued.
	for j := 0; j < 10; j++ {
		code, lat := do(context.Background(), o.url(j, "svc"), fmt.Sprintf("neverissued%d", j))
		samples = append(samples, sample{edge: j % len(o.edges), status: code, lat: lat})
	}
	st := statusCounts(samples)
	r.Data["statuses"] = st
	n := 0
	for _, s := range samples {
		if admitted(s) {
			n++
		}
	}
	fmt.Fprintf(&r.markdown, "1000 requests with `X-API-Key: victim0/x<i>` (40 suffixes) plus 10 never-issued keys.\n\n- statuses: %v\n", st)
	r.check(n == 0, "%d malformed/unknown-key requests admitted (want 0)", n)
	return r
}

// S8: tier deletion and config reload must reach every edge.
func scenarioPropagation(o options) *report {
	r := &report{Scenario: "propagation", Data: map[string]any{}}
	// Prime tier caches on every edge.
	for e := range o.edges {
		do(context.Background(), o.url(e, "svc"), "prop0")
	}
	if code, err := o.hubCall(http.MethodDelete, fmt.Sprintf("/tiers/%s/prop0", o.prefix), nil); err != nil || code/100 != 2 {
		log.Printf("delete tier failed: code=%d err=%v", code, err)
	}
	tierTimes := waitPerEdge(o, 3*time.Second, func(e int) bool {
		code, _ := do(context.Background(), o.url(e, "svc"), "prop0")
		return code == 401 || code == 403
	})

	// Config reload: add svc3 via payload; edges must learn about it.
	raw, err := os.ReadFile(o.configPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	cfg, err := config.Parse(raw, o.configPath)
	if err != nil {
		log.Fatalf("parse config: %v", err)
	}
	svc3 := cfg.Services[0]
	svc3.Name = "svc3"
	svc3.Match.PathPrefix = "/" + o.prefix + "/svc3"
	cfg.Services = append(cfg.Services, svc3)
	payload, err := config.Marshal(cfg)
	if err != nil {
		log.Fatalf("marshal config: %v", err)
	}
	code, err := o.hubCall(http.MethodPost, "/config-reload", payload)
	r.Data["reload_status"] = code
	if err != nil || code/100 != 2 {
		log.Printf("config reload failed: code=%d err=%v", code, err)
	}
	cfgTimes := waitPerEdge(o, 5*time.Second, func(e int) bool {
		c, _ := do(context.Background(), o.url(e, "svc3"), "prop1")
		return c != 404
	})
	// Restore the file config so later runs start clean.
	_, _ = o.hubCall(http.MethodPost, "/config-reload", nil)
	_ = o.setTier("prop0", "pro")

	r.Data["tier_delete_converge_ms"] = tierTimes
	r.Data["config_reload_converge_ms"] = cfgTimes
	fmt.Fprintf(&r.markdown, "Delete a tier on the hub, then add a service via `/config-reload`; poll each edge. `-1` = never converged.\n\n| edge | tier delete visible (ms) | config reload visible (ms) |\n|---|---|---|\n")
	for e := range o.edges {
		fmt.Fprintf(&r.markdown, "| %d | %d | %d |\n", e+1, tierTimes[e], cfgTimes[e])
	}
	for e := range o.edges {
		r.check(tierTimes[e] >= 0 && tierTimes[e] <= 1000, "edge %d sees tier delete within 1s (%d ms)", e+1, tierTimes[e])
		r.check(cfgTimes[e] >= 0 && cfgTimes[e] <= 2000, "edge %d sees config reload within 2s (%d ms)", e+1, cfgTimes[e])
	}
	return r
}

func waitPerEdge(o options, max time.Duration, cond func(edge int) bool) []int64 {
	out := make([]int64, len(o.edges))
	var wg sync.WaitGroup
	for e := range o.edges {
		wg.Add(1)
		go func(e int) {
			defer wg.Done()
			start := time.Now()
			out[e] = -1
			for time.Since(start) < max {
				if cond(e) {
					out[e] = time.Since(start).Milliseconds()
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}(e)
	}
	wg.Wait()
	return out
}

type levelStat struct {
	Concurrency int            `json:"concurrency"`
	Requests    int            `json:"requests"`
	RPS         float64        `json:"rps"`
	Latency     latencyStats   `json:"latency_all"`
	LatencyOK   latencyStats   `json:"latency_2xx"`
	Statuses    map[string]int `json:"statuses"`
}

func levelsTable(b *strings.Builder, ls []levelStat) {
	b.WriteString("| concurrency | requests | rps | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms | statuses |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, l := range ls {
		fmt.Fprintf(b, "| %d | %d | **%.0f** | %.2f | %.2f | **%.2f** | %.2f | %.1f | %v |\n",
			l.Concurrency, l.Requests, l.RPS, l.Latency.P50, l.Latency.P90, l.Latency.P99, l.Latency.P999, l.Latency.Max, l.Statuses)
	}
}

// S3: many keys, increasing concurrency, quota never binding.
func scenarioThroughput(o options) *report {
	r := &report{Scenario: "throughput", Data: map[string]any{}}
	// Warm: one request per key per edge so tier caches and leases are hot.
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	for i := 0; i < 1000; i++ {
		for e := range o.edges {
			wg.Add(1)
			sem <- struct{}{}
			go func(i, e int) {
				defer wg.Done()
				defer func() { <-sem }()
				do(context.Background(), o.url(e, "svc"), fmt.Sprintf("bulk%d", i))
			}(i, e)
		}
	}
	wg.Wait()
	var levels []levelStat
	total := 0
	for _, c := range []int{16, 64, 256} {
		s, el := closedLoop(o, "svc", func(w, it int) string { return fmt.Sprintf("bulk%d", (w*7919+it)%1000) }, c, o.duration)
		total += len(s)
		levels = append(levels, levelStat{
			Concurrency: c, Requests: len(s), RPS: float64(len(s)) / el.Seconds(),
			Latency: latencies(s, nil), LatencyOK: latencies(s, admitted), Statuses: statusCounts(s),
		})
	}
	r.Data["levels"] = levels
	r.Data["total_requests"] = total
	fmt.Fprintf(&r.markdown, "1000 keys (bulk tier, quota never binding), closed loop, %s per level, %d edges.\n\n", o.duration, len(o.edges))
	levelsTable(&r.markdown, levels)
	return r
}

// S4: one hot key at high concurrency (singleflight + per-key contention).
func scenarioHotKey(o options) *report {
	r := &report{Scenario: "hotkey", Data: map[string]any{}}
	do(context.Background(), o.url(0, "svc"), "hot0")
	do(context.Background(), o.url(1, "svc"), "hot0")
	s, el := closedLoop(o, "svc", func(int, int) string { return "hot0" }, 256, o.duration)
	l := levelStat{Concurrency: 256, Requests: len(s), RPS: float64(len(s)) / el.Seconds(),
		Latency: latencies(s, nil), LatencyOK: latencies(s, admitted), Statuses: statusCounts(s)}
	r.Data["level"] = l
	r.Data["total_requests"] = len(s)
	fmt.Fprintf(&r.markdown, "Single key `hot0` (bulk tier), 256 concurrent workers, %s.\n\n", o.duration)
	levelsTable(&r.markdown, []levelStat{l})
	return r
}

// S5a: prime tier cache + lease on every edge before the hub is stopped.
func scenarioOutageWarm(o options) *report {
	for e := range o.edges {
		code, _ := do(context.Background(), o.url(e, "svc"), "out0")
		log.Printf("outage-warm edge %d status %d", e+1, code)
	}
	return nil
}

// S5b: hub is down. Edges should keep serving under the safety tier
// (allow_on_rate_service_error=true) and cap each edge at S per window.
func scenarioOutage(o options) *report {
	r := &report{Scenario: "outage", Data: map[string]any{}}
	o.alignToWindowStart()
	rps := 100
	samples := openLoop(o, "svc", func(int) string { return "out0" }, rps, 3*o.period)
	ws := perWindow(o, samples)
	r.Data["windows"] = ws
	cap := o.safety * int64(len(o.edges))
	fmt.Fprintf(&r.markdown, "Hub stopped. %d rps on a `pro` key for 3 windows. Expect service to continue (degraded) with admitted <= S x edges = %d per window (first window may also drain leftover lease tokens).\n\n", rps, cap)
	windowTable(&r.markdown, ws)
	for i, w := range ws {
		if w.Sent < rps*int(o.period/time.Second)*9/10 {
			continue
		}
		r.check(w.Admitted > 0, "window %d still serves traffic during hub outage (admitted %d)", w.Window, w.Admitted)
		if i > 0 {
			r.check(int64(w.Admitted) <= cap, "window %d admitted %d <= safety cap %d", w.Window, w.Admitted, cap)
		}
	}
	return r
}

// S5c: after the hub returns, normal limiting resumes.
func scenarioOutageCheck(o options) *report {
	r := &report{Scenario: "outage-recovery", Data: map[string]any{}}
	time.Sleep(o.period)
	o.alignToWindowStart()
	samples := openLoop(o, "svc", func(int) string { return "out0" }, 200, o.period-500*time.Millisecond)
	ws := perWindow(o, samples)
	r.Data["windows"] = ws
	fmt.Fprintf(&r.markdown, "Hub restarted; 200 rps for one window on the same key. Expect normal quota (~Q=%d).\n\n", o.quota)
	windowTable(&r.markdown, ws)
	if len(ws) > 0 {
		a := int64(ws[0].Admitted)
		tol := o.lease * int64(len(o.edges))
		r.check(a >= o.quota-tol && a <= o.quota, "recovered window admitted %d (want %d..%d)", a, o.quota-tol, o.quota)
	}
	return r
}

// ---------------------------------------------------------------------------

func writeReport(o options, r *report) {
	b, _ := json.MarshalIndent(r, "", "  ")
	_ = os.WriteFile(filepath.Join(o.outDir, r.Scenario+".json"), b, 0o644)

	var md strings.Builder
	status := ""
	if r.Pass != nil {
		if *r.Pass {
			status = " — PASS"
		} else {
			status = " — FAIL"
		}
	}
	fmt.Fprintf(&md, "\n## %s%s\n\n%s\n", r.Scenario, status, r.markdown.String())
	for _, c := range r.Checks {
		fmt.Fprintf(&md, "- %s\n", c)
	}
	f, err := os.OpenFile(filepath.Join(o.outDir, "report.md"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err == nil {
		_, _ = f.WriteString(md.String())
		f.Close()
	}
	fmt.Print(md.String())
}

// scenarioDemo seeds demo keys and then sends steady, realistic traffic until
// killed: well-behaved pro and free keys, one free key that keeps exceeding
// its quota, and the occasional unknown key.
func scenarioDemo(o options) *report {
	keys := map[string]string{
		"demopro1": "pro", "demopro2": "pro",
		"demofree1": "free", "demofree2": "free", "demonoisy": "free",
	}
	for k, t := range keys {
		for attempt := 0; ; attempt++ {
			err := o.setTier(k, t)
			if err == nil {
				break
			}
			if attempt%10 == 0 {
				log.Printf("demo: waiting for hub to accept key %s: %v", k, err)
			}
			time.Sleep(time.Second)
		}
	}
	log.Printf("demo: seeded %d keys; sending traffic to %s/%s/%s", len(keys), o.edges[0], o.prefix, o.service)

	rates := map[string]time.Duration{ // interval between requests per key
		"demopro1": 100 * time.Millisecond, "demopro2": 250 * time.Millisecond,
		"demofree1": 4 * time.Second, "demofree2": 6 * time.Second,
		"demonoisy":   500 * time.Millisecond, // 120/min against a 20/min quota
		"unknownkey1": 15 * time.Second,
	}
	paths := []string{"orders", "users/42", "search?q=gate", "health", "items/7"}
	var mu sync.Mutex
	counts := map[string]map[int]int{}
	for key, every := range rates {
		go func(key string, every time.Duration) {
			t := time.NewTicker(every)
			defer t.Stop()
			for i := 0; ; i++ {
				<-t.C
				url := fmt.Sprintf("%s/%s/%s/%s", o.edges[i%len(o.edges)], o.prefix, o.service, paths[i%len(paths)])
				code, _ := do(context.Background(), url, key)
				mu.Lock()
				if counts[key] == nil {
					counts[key] = map[int]int{}
				}
				counts[key][code]++
				mu.Unlock()
			}
		}(key, every)
	}
	for range time.Tick(15 * time.Second) {
		mu.Lock()
		var b strings.Builder
		for _, k := range sortedKeys(counts) {
			fmt.Fprintf(&b, " %s=%v", k, counts[k])
		}
		counts = map[string]map[int]int{}
		mu.Unlock()
		log.Printf("demo: last 15s:%s", b.String())
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
