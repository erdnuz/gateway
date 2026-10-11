// Package telemetry holds Gate's Prometheus metrics and OpenTelemetry
// tracing setup. Every metric lives here so the full set is visible in one
// place; labels are bounded by config (service, tier), never by API key.
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var latencyBuckets = []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

// Edge (data plane).
var (
	Requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_requests_total",
		Help: "Requests handled by the edge, by service and status code.",
	}, []string{"service", "code"})

	RequestSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gate_request_duration_seconds",
		Help:    "Time from request arrival to response completion at the edge.",
		Buckets: latencyBuckets,
	}, []string{"service"})

	RateLimitDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_ratelimit_decisions_total",
		Help: "Rate-limit decisions: allowed, denied, safety_allowed, safety_denied, unavailable, rejected.",
	}, []string{"service", "tier", "result"})

	LeaseRPCSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gate_lease_rpc_seconds",
		Help:    "Lease RPCs from this edge to the hub, by result (granted, waiting, error).",
		Buckets: latencyBuckets,
	}, []string{"result"})

	KeyResolutions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_key_resolutions_total",
		Help: "Tier-cache misses resolved through the hub, by result (ok, error).",
	}, []string{"result"})

	UpstreamSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gate_upstream_duration_seconds",
		Help:    "Upstream round trips including retries, by service.",
		Buckets: latencyBuckets,
	}, []string{"service"})

	CacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_cache_lookups_total",
		Help: "Response-cache lookups, by service and result (hit, miss).",
	}, []string{"service", "result"})

	UpstreamEjections = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_upstream_ejections_total",
		Help: "Upstream replicas ejected by the circuit breaker, by service.",
	}, []string{"service"})

	ControlEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_control_events_total",
		Help: "Control-plane events applied by this edge (config, tier).",
	}, []string{"kind"})
)

// Hub (control plane).
var (
	LeaseGrants = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_hub_lease_requests_total",
		Help: "Lease requests served by the hub, by result (granted, waiting, rejected, error).",
	}, []string{"result"})

	TokensGranted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gate_hub_tokens_granted_total",
		Help: "Quota tokens granted to edges. Compare with gate_requests_total to see leasing amortise hub work.",
	})

	ShadowDenials = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_ratelimit_shadow_denials_total",
		Help: "Requests a shadow (dry-run) limit would have denied; they were admitted.",
	}, []string{"service", "limit"})

	LimitDenials = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_ratelimit_limit_denials_total",
		Help: "Requests denied by an additional limit (per client IP, per service, ...) rather than the caller's tier.",
	}, []string{"service", "limit"})

	CostSettled = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_ratelimit_cost_settlements_total",
		Help: "Post-hoc cost settlements: reported (the upstream's figure), estimated (none reported; the estimate stands) or refunded.",
	}, []string{"service", "how"})

	LeaseBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gate_edge_lease_batch_size",
		Help:    "Lease requests per batched RPC; batching keeps hub RPCs proportional to edges, not keys.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
	})

	RegionTransfers = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_hub_region_transfers_total",
		Help: "Attempts to borrow allocation from a peer region: credited, refused (nothing to spare), unreachable, late (window ended), lost.",
	}, []string{"peer", "result"})

	RegionTokensBorrowed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gate_hub_region_tokens_borrowed_total",
		Help: "Global-quota allocation this region borrowed from its peers.",
	})

	RegionTokensLent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gate_hub_region_tokens_lent_total",
		Help: "Global-quota allocation this region lent to its peers.",
	})

	TokensReturned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gate_hub_tokens_returned_total",
		Help: "Unspent tokens edges gave back on drain or idle eviction, so leaving edges strand no quota.",
	})

	DebtSettled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gate_hub_debt_settled_total",
		Help: "Post-hoc cost debt edges settled with the ledger when they drained.",
	})

	LeaseActiveEdges = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gate_hub_lease_active_edges",
		Help:    "Edges sharing a quota when a grant is made; above 1, the endgame split divides what is left between them.",
		Buckets: []float64{1, 2, 3, 4, 6, 8, 12, 16, 32},
	})

	ControlPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gate_hub_control_events_total",
		Help: "Config and tier changes delivered to this hub's watchers, by kind.",
	}, []string{"kind"})

	WatchersDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gate_hub_watchers_dropped_total",
		Help: "Edge streams closed because the edge fell too far behind.",
	})
)

// Analytics.
var EventsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gate_analytics_events_total",
	Help: "Request events received by analytics, by result (stored, invalid, error).",
}, []string{"result"})

var (
	watchersOnce  sync.Once
	watchersCount atomic.Pointer[func() int]
)

// RegisterWatchers exports the number of edges watching this hub. Calling
// it again (another hub in the same process, as in tests) replaces the
// source.
func RegisterWatchers(count func() int) {
	watchersCount.Store(&count)
	watchersOnce.Do(func() {
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "gate_hub_edges_connected",
			Help: "Edges with an open control-plane stream to this hub.",
		}, func() float64 { return float64((*watchersCount.Load())()) })
	})
}

// Since reports the seconds elapsed since start.
func Since(start time.Time) float64 { return time.Since(start).Seconds() }

// ServeMetrics serves /metrics on addr until ctx ends. An empty addr does
// nothing.
func ServeMetrics(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		slog.Info("metrics listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("metrics server stopped", "err", err)
		}
	}()
}
