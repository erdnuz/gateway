// Package edge is Gate's data plane. An edge routes each request to its
// service, authenticates the API key, serves cached responses, enforces rate
// limits and proxies to the upstream.
//
// Edges share nothing on the request path: config is pushed by the hub, key
// tiers and responses are cached in process, and quota comes from hub
// leases spent locally. Adding
// edges adds capacity linearly; the hub sees one lease RPC per block of
// requests, not one per request.
package edge

import (
	"context"
	"crypto/subtle"
	"net/http"
	"net/http/httputil"
	"sync/atomic"
	"time"

	"gateway/packages/common/httpx"
	"gateway/packages/common/types"
	"gateway/packages/limiter"
)

const (
	readyProbeInterval = 2 * time.Second
	readyProbeTimeout  = 500 * time.Millisecond
	// readyProbeFailures consecutive failed probes mark the edge unready; one
	// slow PING under load must not take it out of rotation.
	readyProbeFailures = 3

	defaultMaxRequestBodyBytes = 10 << 20
)

// Deps are the collaborators an edge server needs.
type Deps struct {
	Config *ConfigManager
	Tiers  *TierCache
	// Limiter enforces each key's global quota (limiter.Lease in production).
	Limiter limiter.Limiter
	// Analytics receives one entry per sampled request; nil disables it.
	Analytics AnalyticsSink
	// Responses caches upstream responses; nil disables caching. /readyz
	// probes it (for a Redis-backed store).
	Responses ResponseStore
}

// Options tune an edge server; zero values take the runtime defaults.
type Options struct {
	EdgeID string
	// UpstreamTimeout bounds the wait for an upstream's response headers.
	UpstreamTimeout         time.Duration
	UpstreamMaxIdleConns    int
	UpstreamIdleConnTimeout time.Duration
	MaxCacheableBytes       int64
	MaxRequestBodyBytes     int64
	// AdminToken guards internal endpoints (/analytics/metrics); empty
	// disables them.
	AdminToken string
}

// Server is an edge's HTTP handler.
type Server struct {
	Deps
	opts      Options
	safety    *limiter.Local
	transport *http.Transport
	proxy     *httputil.ReverseProxy
	keys      atomic.Pointer[keyMatcher]
	handler   http.Handler

	startupReady atomic.Bool // false holds all proxy traffic (503)
	localHealthy atomic.Bool // edge-local dependencies; reported by /readyz
	pending      atomic.Pointer[string]
}

// NewServer builds an edge server. Call StartBackgroundWorkers before serving.
func NewServer(d Deps, o Options) *Server {
	def := types.DefaultRuntimePolicy().Edge
	o.EdgeID = orDefault(o.EdgeID, "unknown-edge")
	if o.UpstreamTimeout <= 0 {
		o.UpstreamTimeout = def.UpstreamClientTimeout
	}
	if o.UpstreamMaxIdleConns <= 0 {
		o.UpstreamMaxIdleConns = def.UpstreamMaxIdleConns
	}
	if o.UpstreamIdleConnTimeout <= 0 {
		o.UpstreamIdleConnTimeout = def.UpstreamIdleConnTimeout
	}
	if o.MaxCacheableBytes <= 0 {
		o.MaxCacheableBytes = def.CacheMaxObjectBytes
	}
	if o.MaxRequestBodyBytes <= 0 {
		o.MaxRequestBodyBytes = defaultMaxRequestBodyBytes
	}
	s := &Server{Deps: d, opts: o, safety: limiter.NewLocal()}
	s.transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          o.UpstreamMaxIdleConns,
		MaxIdleConnsPerHost:   o.UpstreamMaxIdleConns,
		IdleConnTimeout:       o.UpstreamIdleConnTimeout,
		ResponseHeaderTimeout: o.UpstreamTimeout,
	}
	s.proxy = s.newProxy()
	s.startupReady.Store(true)
	s.localHealthy.Store(true)
	s.SetPendingDependency("")

	// Every proxied request runs the same chain; each step reads and fills
	// the request's state.
	gateway := chain(http.HandlerFunc(s.forward),
		s.withState,
		s.recordAnalytics,
		s.route,
		s.cors,
		s.authenticate,
		s.serveFromCache,
		s.rateLimit,
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", s.readyz)
	mux.HandleFunc("/analytics/metrics", s.analyticsMetrics)
	mux.Handle("/", s.holdUntilStarted(gateway))
	s.handler = httpx.WithRequestID(mux)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.handler.ServeHTTP(w, r) }

func chain(h http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// StartBackgroundWorkers starts limiter housekeeping.
func (s *Server) StartBackgroundWorkers(ctx context.Context) {
	if r, ok := s.Limiter.(interface{ Run(context.Context) }); ok {
		go r.Run(ctx)
	}
	go s.safety.Run(ctx)
}

// SetStartupReady opens (or holds) proxy traffic.
func (s *Server) SetStartupReady(ready bool) { s.startupReady.Store(ready) }

// SetPendingDependency names what /readyz is waiting for.
func (s *Server) SetPendingDependency(dep string) {
	dep = orDefault(dep, "unknown")
	s.pending.Store(&dep)
}

func (s *Server) holdUntilStarted(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.startupReady.Load() {
			w.Header().Set("Retry-After", "5")
			httpx.WriteProblem(w, r, http.StatusServiceUnavailable, "starting", "edge is waiting for "+*s.pending.Load())
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	if !s.startupReady.Load() || !s.localHealthy.Load() {
		httpx.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "pending", "missing_dependency": *s.pending.Load()})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// StartReadinessProbe ties /readyz to edge-local dependencies (a Redis
// response store). Hub or analytics outages must not take every edge out of
// rotation at once: the failure policies handle them per request.
func (s *Server) StartReadinessProbe(ctx context.Context) {
	if s.Responses == nil {
		return
	}
	go func() {
		tick := time.NewTicker(readyProbeInterval)
		defer tick.Stop()
		failures := 0
		for {
			probeCtx, cancel := context.WithTimeout(ctx, readyProbeTimeout)
			err := s.Responses.Ping(probeCtx)
			cancel()
			if err == nil {
				failures = 0
				s.localHealthy.Store(true)
			} else if failures++; failures >= readyProbeFailures {
				s.SetPendingDependency("response_cache")
				s.localHealthy.Store(false)
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
}

func (s *Server) analyticsMetrics(w http.ResponseWriter, r *http.Request) {
	token, ok := httpx.BearerToken(r)
	if s.opts.AdminToken == "" || !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.opts.AdminToken)) != 1 {
		http.NotFound(w, r)
		return
	}
	m, ok := s.Analytics.(interface{ Stats() AnalyticsManagerStats })
	if !ok {
		httpx.WriteProblem(w, r, http.StatusNotFound, "not_found", "analytics is disabled")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, m.Stats())
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
