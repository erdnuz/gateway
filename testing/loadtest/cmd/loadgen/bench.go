package main

// The comparative benchmark (testing/bench): the same two phases against
// any HTTP data plane that limits each API key to 500 requests per second.

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	benchTarget = flag.String("bench-target", "edge:8080", "data plane service (host:port); every address it resolves to gets traffic")
	benchPath   = flag.String("bench-path", "/b/bench/ping", "request path")
	benchLabel  = flag.String("bench-label", "run", "row label")
	_           = flag.Int64("bench-calls-before", 0, "unused; kept for run.sh")
)

const (
	benchQuota    = 500  // per key per second
	benchKeys     = 1000 // keys for the latency phase
	benchRPS      = 2000 // latency phase, open loop
	benchAccuracy = 10 * time.Second
	benchLatency  = 20 * time.Second
)

func scenarioBenchSetup(o options) *report {
	sem := make(chan struct{}, 32)
	var wg sync.WaitGroup
	for i := 0; i <= benchKeys; i++ {
		key := fmt.Sprintf("bench%d", i)
		if i == benchKeys {
			key = "benchhot"
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := o.setTier(key, "pro"); err != nil {
				log.Printf("bench-setup: %v", err)
			}
		}()
	}
	wg.Wait()
	return nil
}

// benchTargets resolves the data plane to every replica's address.
func benchTargets() []string {
	host, port, _ := net.SplitHostPort(*benchTarget)
	ips, err := net.LookupHost(host)
	if err != nil || len(ips) == 0 {
		log.Fatalf("resolve %s: %v", host, err)
	}
	sort.Strings(ips)
	out := make([]string, len(ips))
	for i, ip := range ips {
		out[i] = "http://" + net.JoinHostPort(ip, port)
	}
	return out
}

func benchLoop(targets []string, keyFn func(i int) string, rps int, d time.Duration) []sample {
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
			s := doFull(context.Background(), targets[i%len(targets)]+*benchPath, keyFn(i))
			s.edge = i % len(targets)
			out[i] = s
		}(i)
	}
	wg.Wait()
	return out
}

func scenarioBench(o options) *report {
	targets := benchTargets()
	// Warm every replica's connections and leases.
	benchLoop(targets, func(i int) string { return fmt.Sprintf("bench%d", i%benchKeys) }, 500, 2*time.Second)

	// Accuracy: one key at twice its quota, per one-second window.
	acc := benchLoop(targets, func(int) string { return "benchhot" }, 2*benchQuota, benchAccuracy)
	per := map[int64]int{}
	for _, s := range acc {
		if admitted(s) {
			w := s.win
			if w == 0 {
				w = s.sentAt.Unix()
			}
			per[w]++
		}
	}
	var counts []int
	for _, w := range sortedWindows(per) {
		counts = append(counts, per[w])
	}
	if len(counts) > 2 {
		counts = counts[1 : len(counts)-1] // partial windows at either end
	}
	maxN, sum := 0, 0
	for _, n := range counts {
		maxN, sum = max(maxN, n), sum+n
	}
	mean := 0.0
	if len(counts) > 0 {
		mean = float64(sum) / float64(len(counts))
	}

	// Latency: quota never binding, many keys, a fixed open-loop rate.
	lat := benchLoop(targets, func(i int) string { return fmt.Sprintf("bench%d", (i*7919)%benchKeys) }, benchRPS, benchLatency)
	ls := latencies(lat, nil)
	errs := 0
	for _, s := range lat {
		if s.status == 0 || s.status >= 500 {
			errs++
		}
	}
	rps := float64(len(lat)) / benchLatency.Seconds()

	label := *benchLabel
	row := fmt.Sprintf("| %s | %d | **%d** / %.0f | %d | %.0f | %.2f | **%.2f** | %.2f | %d | REDIS_PER_REQ_%s |\n",
		label, len(targets), maxN, mean, benchQuota, rps, ls.P50, ls.P99, ls.P999, errs, label)
	appendFile(filepath.Join(o.outDir, "report.md"), row)
	fmt.Print(row)
	appendFile(filepath.Join(o.outDir, "results.csv"), fmt.Sprintf("%s,%d,%d,%.1f,%d,%.0f,%.3f,%.3f,%.3f,%d,REDIS_PER_REQ_%s\n",
		strings.SplitN(label, "-x", 2)[0], len(targets), maxN, mean, benchQuota, rps, ls.P50, ls.P99, ls.P999, errs, label))
	_ = os.WriteFile(filepath.Join(o.outDir, label+".requests"), []byte(fmt.Sprint(len(acc)+len(lat))), 0o644)
	return nil
}

func appendFile(path, s string) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err == nil {
		_, _ = f.WriteString(s)
		f.Close()
	}
}
