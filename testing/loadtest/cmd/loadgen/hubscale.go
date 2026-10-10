package main

// The hub-scaling test (testing/loadtest/hubscale.sh): drive batched lease
// RPCs straight at the hubs, so the control plane, not the edges, is the
// bottleneck, and measure grants per second as hubs and ledger shards grow.

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/edge"
)

var (
	hsTarget  = flag.String("hubscale-target", "hub-server:9090", "hub gRPC service; every address it resolves to gets traffic")
	hsLabel   = flag.String("hubscale-label", "run", "row label")
	hsWorkers = flag.Int("hubscale-workers", 64, "concurrent lease callers")
	hsBatch   = flag.Int("hubscale-batch", 16, "leases per RPC")
	hsSecs    = flag.Duration("hubscale-duration", 20*time.Second, "measurement time")
)

func scenarioHubScale(o options) *report {
	host, port, _ := net.SplitHostPort(*hsTarget)
	ips, err := net.LookupHost(host)
	if err != nil || len(ips) == 0 {
		log.Fatalf("resolve %s: %v", host, err)
	}
	sort.Strings(ips)
	tlsCfg, err := edge.ClientTLS("hub-server", "/certs/edge/tls.crt", "/certs/edge/tls.key", "/certs/ca/ca.crt")
	if err != nil {
		log.Fatal(err)
	}
	clients := make([]*edge.HubClient, len(ips))
	for i, ip := range ips {
		if clients[i], err = edge.DialHub(net.JoinHostPort(ip, port), tlsCfg); err != nil {
			log.Fatal(err)
		}
	}

	var grants, rpcs, errs atomic.Int64
	var mu sync.Mutex
	var lats []time.Duration
	deadline := time.Now().Add(*hsSecs)
	var wg sync.WaitGroup
	for w := 0; w < *hsWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := clients[w%len(clients)]
			var local []time.Duration
			for it := 0; time.Now().Before(deadline); it++ {
				in := &types.QuotaLeaseBatchRequest{}
				for b := 0; b < *hsBatch; b++ {
					key := fmt.Sprintf("hs%06d", (w*7919+it*131+b*17)%100000)
					in.Requests = append(in.Requests, &types.QuotaLeaseRequest{
						Prefix: "hs", ServiceId: "lease", ApiKey: key, TierId: "t", RequestedTokens: 10, EdgeId: fmt.Sprintf("w%d", w),
					})
				}
				start := time.Now()
				out, err := c.RequestQuotaLeases(context.Background(), in)
				local = append(local, time.Since(start))
				rpcs.Add(1)
				if err != nil {
					errs.Add(1)
					continue
				}
				for _, r := range out.GetResults() {
					if r.GetCode() == 0 && r.GetResponse().GetGrantedTokens() > 0 {
						grants.Add(1)
					}
				}
			}
			mu.Lock()
			lats = append(lats, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	q := func(p float64) float64 {
		if len(lats) == 0 {
			return 0
		}
		return float64(lats[int(p*float64(len(lats)-1))].Microseconds()) / 1000
	}
	secs := hsSecs.Seconds()
	row := fmt.Sprintf("| %s | %d | **%.0f** | %.0f | %.2f | **%.2f** | %d |\n",
		*hsLabel, len(ips), float64(grants.Load())/secs, float64(rpcs.Load())/secs, q(0.5), q(0.99), errs.Load())
	appendFile(filepath.Join(o.outDir, "report.md"), row)
	fmt.Print(row)
	return nil
}
