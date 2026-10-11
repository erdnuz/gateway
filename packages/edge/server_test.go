package edge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gateway/packages/common/httpx"
	"gateway/packages/common/types"
	"gateway/packages/keys"
	"gateway/packages/limiter"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- fixture ---------------------------------------------------------------

type captureSink struct {
	mu      sync.Mutex
	entries []types.AnalyticsEntry
}

func (s *captureSink) Capture(e *types.AnalyticsEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, *e)
}

func (s *captureSink) Close() error { return nil }

func (s *captureSink) last() *types.AnalyticsEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return nil
	}
	return &s.entries[len(s.entries)-1]
}

// hubStub grants leases from a fixed quota per key (and per named limit,
// from limits), or fails with err.
type hubStub struct {
	mu     sync.Mutex
	quota  int64
	limits map[string]int64
	used   map[string]int64
	err    error
}

func (h *hubStub) RequestQuotaLease(_ context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	if h.err != nil {
		return nil, h.err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.used == nil {
		h.used = map[string]int64{}
	}
	quota, k := h.quota, in.LimitId+"/"+in.ApiKey
	if q, ok := h.limits[in.LimitId]; ok {
		quota = q
	}
	grant := min(in.RequestedTokens, quota-h.used[k])
	if grant <= 0 {
		return &types.QuotaLeaseResponse{WaitingForCapacity: true, RetryAfterMs: 1000}, nil
	}
	h.used[k] += grant
	return &types.QuotaLeaseResponse{GrantedTokens: grant, RemainingGlobal: quota - h.used[k], LeaseTtlSeconds: 3600}, nil
}

type fixture struct {
	t        testing.TB
	server   *Server
	keys     map[string]string // API key -> tier the stub hub resolves
	hub      *hubStub
	sink     *captureSink
	upstream *httptest.Server
}

func testService(upstream string) types.ServiceConfig {
	return types.ServiceConfig{
		ServiceID: "svc",
		TargetURL: upstream,
		Tiers: []types.TierConfig{
			{TierID: "free", Quota: 1000, GetCost: 1, PostCost: 2, PutCost: 2, DeleteCost: 3, OtherCost: 1},
		},
		Transform: types.TransformConfig{StripPrefix: true},
		Analytics: types.AnalyticsConfig{Enabled: true, SamplingRate: 1},
	}
}

// newFixture serves one service at /v1/svc. The key "testkey" has tier free;
// resolving any key missing from f.keys fails as if the hub were down.
func newFixture(t testing.TB, upstream http.HandlerFunc, configure func(*types.ServiceConfig), opts ...Options) *fixture {
	t.Helper()
	f := &fixture{t: t, keys: map[string]string{"testkey": "free"}, hub: &hubStub{quota: 1 << 40}, sink: &captureSink{}}
	if upstream == nil {
		upstream = func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }
	}
	f.upstream = httptest.NewServer(upstream)
	t.Cleanup(f.upstream.Close)

	svc := testService(f.upstream.URL)
	if configure != nil {
		configure(&svc)
	}
	cm := NewConfigManager(&types.GatewayConfig{Prefixes: []types.PrefixConfig{{Prefix: "v1", QuotaPeriod: time.Hour, Services: []types.ServiceConfig{svc}}}})
	tiers := NewTierCache(func(_ context.Context, _, hash string) (string, error) {
		for raw, tier := range f.keys { // the edge sends hashes, never raw keys
			if keys.Hash(raw) == hash {
				return tier, nil
			}
		}
		return "", errors.New("hub unreachable")
	})
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	f.server = NewServer(Deps{Config: cm, Tiers: tiers, Limiter: limiter.NewLease(f.hub, limiter.LeaseOptions{LeaseSize: 10}), Analytics: f.sink, Responses: NewMemoryResponses(1 << 20)}, o)
	return f
}

func (f *fixture) do(method, path string, header ...string) *httptest.ResponseRecorder {
	f.t.Helper()
	return f.send(httptest.NewRequest(method, path, nil), header...)
}

func (f *fixture) send(req *http.Request, header ...string) *httptest.ResponseRecorder {
	req.Header.Set("X-API-Key", "testkey")
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	rec := httptest.NewRecorder()
	f.server.ServeHTTP(rec, req)
	return rec
}

func problemOf(t *testing.T, rec *httptest.ResponseRecorder) httpx.Problem {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json (body %s)", ct, rec.Body)
	}
	var p httpx.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func expectStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, want, rec.Body)
	}
}

// --- routing and auth ------------------------------------------------------

func TestSplitRoute(t *testing.T) {
	for path, want := range map[string][2]string{
		"/v1/svc/users":  {"v1", "svc"},
		"/v1/svc":        {"v1", "svc"},
		"/v1/svc/":       {"v1", "svc"},
		"/v1":            {"", ""},
		"/":              {"", ""},
		"//svc/x":        {"", ""},
		"/v1//x":         {"", ""},
		"/api/v1/users/": {"api", "v1"},
	} {
		if s, svc := splitRoute(path); s != want[0] || svc != want[1] {
			t.Errorf("splitRoute(%q) = %q, %q; want %q", path, s, svc, want)
		}
	}
}

func TestUpstreamPath(t *testing.T) {
	for _, c := range []struct {
		path  string
		strip bool
		want  string
	}{
		{"/v1/svc/users/1", false, "/v1/svc/users/1"},
		{"/v1/svc/users/1", true, "/users/1"},
		{"/v1/svc", true, "/"},
		{"/v1/svc/", true, "/"},
	} {
		if got := upstreamPath(c.path, c.strip); got != c.want {
			t.Errorf("upstreamPath(%q, %v) = %q, want %q", c.path, c.strip, got, c.want)
		}
	}
}

func TestMethodCost(t *testing.T) {
	tier := &types.TierConfig{GetCost: 1, PostCost: 2, PutCost: 3, DeleteCost: 4, OtherCost: 5}
	for m, want := range map[string]int64{"GET": 1, "POST": 2, "PUT": 3, "DELETE": 4, "PATCH": 5} {
		if got := methodCost(m, tier); got != want {
			t.Errorf("%s: cost %d, want %d", m, got, want)
		}
	}
}

func TestErrorsAreProblemDetails(t *testing.T) {
	f := newFixture(t, nil, nil)
	for _, c := range []struct {
		name   string
		path   string
		key    string
		status int
		code   string
	}{
		{"no route", "/v1", "testkey", 404, "no_route"},
		{"unknown service", "/v1/nope/x", "testkey", 404, "no_route"},
		{"missing key", "/v1/svc/x", "", 401, "missing_api_key"},
		{"malformed key", "/v1/svc/x", "bad key!", 401, "invalid_api_key"},
	} {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, c.path, nil)
			req.Header.Set("X-Request-Id", "req-123")
			rec := httptest.NewRecorder()
			if c.key != "" {
				req.Header.Set("X-API-Key", c.key)
			}
			f.server.ServeHTTP(rec, req)
			expectStatus(t, rec, c.status)
			p := problemOf(t, rec)
			if p.Code != c.code || p.Status != c.status || p.RequestID != "req-123" {
				t.Fatalf("problem = %+v", p)
			}
		})
	}
}

func TestUnknownKeyIsUnauthorized(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.keys["revoked"] = ""
	rec := f.send(httptest.NewRequest(http.MethodGet, "/v1/svc/x", nil))
	expectStatus(t, rec, http.StatusOK)
	req := httptest.NewRequest(http.MethodGet, "/v1/svc/x", nil)
	rec = httptest.NewRecorder()
	req.Header.Set("X-API-Key", "revoked")
	f.server.ServeHTTP(rec, req)
	expectStatus(t, rec, http.StatusUnauthorized)
}

func TestTierLookupFallsBackToDefaultTier(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Failure.Hub.TierLookupStrategy = "default-tier"
		s.Failure.Hub.DefaultTier = "free"
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/svc/x", nil)
	req.Header.Set("X-API-Key", "uncached") // hub unreachable
	rec := httptest.NewRecorder()
	f.server.ServeHTTP(rec, req)
	expectStatus(t, rec, http.StatusOK)
}

func TestCORSPreflightNeedsNoKey(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.CORS = &types.CORSConfig{AllowedOrigins: []string{"https://app.example"}, AllowedMethods: []string{"GET", "POST"}, AllowedHeaders: []string{"Content-Type"}, MaxAge: time.Hour}
	})
	req := httptest.NewRequest(http.MethodOptions, "/v1/svc/x", nil)
	req.Header.Set("Origin", "https://app.example")
	rec := httptest.NewRecorder()
	f.server.ServeHTTP(rec, req)
	expectStatus(t, rec, http.StatusNoContent)
	h := rec.Header()
	if h.Get("Access-Control-Allow-Origin") != "https://app.example" || h.Get("Access-Control-Allow-Headers") != "Content-Type" || h.Get("Access-Control-Max-Age") != "3600" {
		t.Fatalf("preflight headers: %v", h)
	}
}

// --- rate limiting ---------------------------------------------------------

func TestRateLimitDeniesWithHeaders(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) { s.Tiers[0].Quota = 2 })
	for i := 0; i < 2; i++ {
		rec := f.do(http.MethodGet, "/v1/svc/x")
		expectStatus(t, rec, http.StatusOK)
		if rec.Header().Get("RateLimit-Limit") != "2" {
			t.Fatalf("RateLimit-Limit = %q", rec.Header().Get("RateLimit-Limit"))
		}
	}
	rec := f.do(http.MethodGet, "/v1/svc/x")
	expectStatus(t, rec, http.StatusTooManyRequests)
	if problemOf(t, rec).Code != "rate_limited" || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("denial: %v %s", rec.Header(), rec.Body)
	}
}

func TestHubOutageWithoutSafetyLimitFailsClosed(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.hub.err = status.Error(codes.Unavailable, "down")
	rec := f.do(http.MethodGet, "/v1/svc/x")
	expectStatus(t, rec, http.StatusServiceUnavailable)
	if problemOf(t, rec).Code != "rate_limit_unavailable" {
		t.Fatal(rec.Body)
	}
}

func TestHubOutageUsesSafetyLimit(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.SafetyTier = &types.TierConfig{TierID: "safety", Quota: 2, GetCost: 1}
		s.Failure.Hub.AllowOnRateServiceError = true
	})
	f.hub.err = status.Error(codes.Unavailable, "down")
	got := []int{f.do("GET", "/v1/svc/x").Code, f.do("GET", "/v1/svc/x").Code, f.do("GET", "/v1/svc/x").Code}
	if got[0] != 200 || got[1] != 200 || got[2] != 429 {
		t.Fatalf("statuses under safety limit 2: %v", got)
	}
}

func TestHubRejectionIsForbidden(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.SafetyTier = &types.TierConfig{TierID: "safety", Quota: 2, GetCost: 1}
		s.Failure.Hub.AllowOnRateServiceError = true
	})
	f.hub.err = status.Error(codes.PermissionDenied, "no")
	expectStatus(t, f.do("GET", "/v1/svc/x"), http.StatusForbidden)
}

// --- proxying --------------------------------------------------------------

func TestProxyRewritesRequest(t *testing.T) {
	var got *http.Request
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		w.Header().Set("X-Internal", "secret")
		w.Header().Set("X-Request-Id", "upstream-id")
		_, _ = io.WriteString(w, "ok")
	}, func(s *types.ServiceConfig) {
		s.TargetURL += "/base"
		s.Transform.AddHeaders = map[string]string{"X-Service": "svc"}
		s.Transform.HideHeaders = []string{"X-Internal"}
	})
	rec := f.do(http.MethodGet, "/v1/svc/users/1?q=a", "X-Forwarded-For", "203.0.113.9", "X-Gate-Tier", "spoofed", "X-Internal", "x", "X-Request-Id", "rid-1")
	expectStatus(t, rec, http.StatusOK)
	h := got.Header
	switch {
	case got.URL.Path != "/base/users/1" || got.URL.RawQuery != "q=a":
		t.Fatalf("upstream URL %s", got.URL)
	case h.Get("X-API-Key") != "" || h.Get("X-Gate-Tier") != "" || h.Get("X-Internal") != "":
		t.Fatalf("leaked headers: %v", h)
	case h.Get("X-Service") != "svc":
		t.Fatal("added header missing")
	case !strings.HasPrefix(h.Get("X-Forwarded-For"), "203.0.113.9, "):
		t.Fatalf("X-Forwarded-For = %q", h.Get("X-Forwarded-For"))
	case h.Get("X-Request-Id") != "rid-1":
		t.Fatalf("request ID not propagated: %q", h.Get("X-Request-Id"))
	}
	if rec.Header().Get("X-Internal") != "" || rec.Header().Values("X-Request-Id")[0] != "rid-1" || len(rec.Header().Values("X-Request-Id")) != 1 {
		t.Fatalf("response headers: %v", rec.Header())
	}
}

func TestProxyRetriesConfiguredStatusesWithBody(t *testing.T) {
	var calls atomic.Int32
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write(body)
	}, func(s *types.ServiceConfig) {
		s.Failure.Upstream.MaxRetries = 2
		s.Failure.Upstream.RetryBackoff = time.Millisecond
		s.Failure.Upstream.RetryOnStatuses = []int{http.StatusBadGateway}
	})
	rec := f.send(httptest.NewRequest(http.MethodPut, "/v1/svc/x", strings.NewReader("payload")))
	expectStatus(t, rec, http.StatusOK)
	if calls.Load() != 3 || rec.Body.String() != "payload" {
		t.Fatalf("calls=%d body=%q", calls.Load(), rec.Body)
	}
}

func TestProxyDoesNotRetryNonIdempotentMethods(t *testing.T) {
	var calls atomic.Int32
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}, func(s *types.ServiceConfig) {
		s.Failure.Upstream.MaxRetries = 2
		s.Failure.Upstream.RetryOnStatuses = []int{http.StatusBadGateway}
	})
	rec := f.send(httptest.NewRequest(http.MethodPost, "/v1/svc/x", strings.NewReader("once")))
	expectStatus(t, rec, http.StatusBadGateway)
	if calls.Load() != 1 {
		t.Fatalf("POST attempted %d times", calls.Load())
	}
}

func TestUpstreamFailureFailsClosed(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) { s.TargetURL = "http://127.0.0.1:1" })
	rec := f.do(http.MethodGet, "/v1/svc/x")
	expectStatus(t, rec, http.StatusBadGateway)
	if problemOf(t, rec).Code != "upstream_unavailable" {
		t.Fatal(rec.Body)
	}
	if e := f.sink.last(); e == nil || !e.UpstreamError || e.ResponseCode != 502 {
		t.Fatalf("analytics entry: %+v", e)
	}
}

func TestUpstreamFailureFailsOpen(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.TargetURL = "http://127.0.0.1:1"
		s.Failure.Upstream = types.UpstreamFailurePolicy{Mode: "fail-open", FallbackStatusCode: 202, FallbackBody: "degraded", FallbackHeaders: map[string]string{"Content-Type": "text/plain"}}
	})
	rec := f.do(http.MethodGet, "/v1/svc/x")
	expectStatus(t, rec, http.StatusAccepted)
	if rec.Body.String() != "degraded" || rec.Header().Get("X-Upstream-Error") != "true" {
		t.Fatalf("fallback: %v %q", rec.Header(), rec.Body)
	}
}

func TestRequestBodyIsBounded(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body) }, nil, Options{MaxRequestBodyBytes: 8})
	rec := f.send(httptest.NewRequest(http.MethodPost, "/v1/svc/x", strings.NewReader(strings.Repeat("x", 64))))
	expectStatus(t, rec, http.StatusRequestEntityTooLarge)
}

// Upgrades pass through every middleware wrapper (they expose Unwrap).
func TestProxySupportsUpgrades(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "echo" {
			http.Error(w, "want upgrade", 400)
			return
		}
		conn, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
		_ = brw.Flush()
		line, _ := brw.ReadString('\n')
		_, _ = brw.WriteString(line)
		_ = brw.Flush()
	}, nil)
	edge := httptest.NewServer(f.server)
	defer edge.Close()
	conn, err := net.Dial("tcp", edge.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "GET /v1/svc/ws HTTP/1.1\r\nHost: edge\r\nX-API-Key: testkey\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %v %v", resp, err)
	}
	_, _ = io.WriteString(conn, "ping\n")
	if line, _ := br.ReadString('\n'); line != "ping\n" {
		t.Fatalf("echo over upgraded connection = %q", line)
	}
}

// --- caching ---------------------------------------------------------------

func cachingService(s *types.ServiceConfig) {
	s.Cache = &types.CacheConfig{Enabled: true, TTL: time.Minute, CacheKey: "$path:$query:$key"}
}

func TestCacheServesRepeatsWithoutQuotaOrUpstream(t *testing.T) {
	var calls atomic.Int32
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"n":1}`)
	}, func(s *types.ServiceConfig) { cachingService(s); s.Tiers[0].Quota = 2 })

	first := f.do("GET", "/v1/svc/a")
	if first.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("first: %v", first.Header())
	}
	for i := 0; i < 3; i++ { // beyond the quota of 2: hits are free
		rec := f.do("GET", "/v1/svc/a")
		expectStatus(t, rec, http.StatusOK)
		if rec.Header().Get("X-Cache") != "HIT" || rec.Body.String() != `{"n":1}` || rec.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("hit %d: %v %q", i, rec.Header(), rec.Body)
		}
	}
	if e := f.sink.last(); e == nil || !e.CacheHit {
		t.Fatalf("analytics entry: %+v", e)
	}
	f.do("GET", "/v1/svc/missing")
	if rec := f.do("GET", "/v1/svc/missing"); rec.Code != http.StatusNotFound || rec.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("cached 404: %d %v", rec.Code, rec.Header())
	}
	if calls.Load() != 2 {
		t.Fatalf("upstream called %d times, want 2", calls.Load())
	}
}

func TestCacheSkipsOversizedResponses(t *testing.T) {
	var calls atomic.Int32
	body := strings.Repeat("b", 80)
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, body)
	}, cachingService, Options{MaxCacheableBytes: 64})
	for i := 0; i < 2; i++ {
		rec := f.do("GET", "/v1/svc/big")
		if rec.Header().Get("X-Cache") != "MISS" || rec.Body.String() != body {
			t.Fatalf("request %d: %v len=%d", i, rec.Header(), rec.Body.Len())
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("oversized response was cached")
	}
}

// --- analytics and admin -----------------------------------------------------

func TestAnalyticsCapture(t *testing.T) {
	t.Run("records the request", func(t *testing.T) {
		f := newFixture(t, nil, nil)
		f.do("GET", "/v1/svc/x")
		e := f.sink.last()
		if e == nil || e.Prefix != "v1" || e.Service != "svc" || e.Tier != "free" || e.ResponseCode != 200 || e.ResponseSize != 2 {
			t.Fatalf("entry: %+v", e)
		}
	})
	for name, cfg := range map[string]types.AnalyticsConfig{
		"disabled":      {Enabled: false, SamplingRate: 1},
		"zero sampling": {Enabled: true, SamplingRate: 0},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil, func(s *types.ServiceConfig) { s.Analytics = cfg })
			expectStatus(t, f.do("GET", "/v1/svc/x"), http.StatusOK)
			if f.sink.last() != nil {
				t.Fatal("captured an entry")
			}
		})
	}
}

func TestAnalyticsMetricsEndpointNeedsAdminToken(t *testing.T) {
	mgr := NewAnalyticsManager(4)
	mgr.Capture(&types.AnalyticsEntry{})
	s := NewServer(Deps{Analytics: mgr}, Options{AdminToken: "admin-secret"})

	anon := httptest.NewRecorder()
	s.ServeHTTP(anon, httptest.NewRequest(http.MethodGet, "/analytics/metrics", nil))
	expectStatus(t, anon, http.StatusNotFound)

	req := httptest.NewRequest(http.MethodGet, "/analytics/metrics", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	expectStatus(t, rec, http.StatusOK)
	var stats AnalyticsManagerStats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil || stats.Captured != 1 {
		t.Fatalf("stats %+v %v", stats, err)
	}
}

func TestStartupGateHoldsTraffic(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.server.SetStartupReady(false)
	f.server.SetPendingDependency("hub_readyz")
	rec := f.do("GET", "/v1/svc/x")
	expectStatus(t, rec, http.StatusServiceUnavailable)
	if p := problemOf(t, rec); p.Code != "starting" || !strings.Contains(p.Detail, "hub_readyz") {
		t.Fatalf("problem %+v", p)
	}
	ready := httptest.NewRecorder()
	f.server.ServeHTTP(ready, httptest.NewRequest("GET", "/readyz", nil))
	expectStatus(t, ready, http.StatusServiceUnavailable)
	f.server.SetStartupReady(true)
	expectStatus(t, f.do("GET", "/v1/svc/x"), http.StatusOK)
}

func TestUpstreamTransportUsesPolicyDefaults(t *testing.T) {
	s := NewServer(Deps{}, Options{})
	def := types.DefaultRuntimePolicy().Edge
	tr := s.transport
	if tr.ResponseHeaderTimeout != def.UpstreamClientTimeout || tr.MaxIdleConns != def.UpstreamMaxIdleConns || tr.IdleConnTimeout != def.UpstreamIdleConnTimeout {
		t.Fatalf("transport %+v", tr)
	}
}

func TestConfigManager_Lookups(t *testing.T) {
	cfg := &types.GatewayConfig{Prefixes: []types.PrefixConfig{{Prefix: "v1", Services: []types.ServiceConfig{{ServiceID: "svc"}}}}}
	cm := NewConfigManager(cfg)
	if p, ok := cm.GetPrefix("v1"); !ok || p.Prefix != "v1" {
		t.Fatal("GetPrefix(v1)")
	}
	if _, ok := cm.GetPrefix("missing"); ok {
		t.Fatal("GetPrefix found a missing prefix")
	}
	if svc, err := cm.GetServiceConfig("v1", "svc"); err != nil || svc.ServiceID != "svc" {
		t.Fatalf("GetServiceConfig: %v %v", svc, err)
	}
	if _, err := cm.GetServiceConfig("v1", "missing"); err == nil {
		t.Fatal("found a missing service")
	}
	if _, err := cm.GetServiceConfig("missing", "svc"); err == nil {
		t.Fatal("found a service under a missing prefix")
	}
}
