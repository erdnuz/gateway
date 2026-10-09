package edge

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gateway/packages/common/types"

	"github.com/golang-jwt/jwt/v5"
)

func replica(t *testing.T, name string, hits *atomic.Int32, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, name)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestUpstreamPool_RoundRobin(t *testing.T) {
	var a, b atomic.Int32
	ra, rb := replica(t, "a", &a, 200), replica(t, "b", &b, 200)
	f := newFixture(t, nil, func(s *types.ServiceConfig) { s.Upstreams = []string{ra.URL, rb.URL} })
	for i := 0; i < 10; i++ {
		expectStatus(t, f.do("GET", "/v1/svc/x"), http.StatusOK)
	}
	if a.Load() != 5 || b.Load() != 5 {
		t.Fatalf("round robin split %d/%d, want 5/5", a.Load(), b.Load())
	}
}

func TestUpstreamPool_EjectsFailingReplicaAndRetriesElsewhere(t *testing.T) {
	var good, bad atomic.Int32
	rg, rb := replica(t, "good", &good, 200), replica(t, "bad", &bad, 502)
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Upstreams = []string{rb.URL, rg.URL}
		s.CircuitBreaker = &types.CircuitBreakerConfig{Failures: 2, Cooldown: time.Minute}
		s.Failure.Upstream = types.UpstreamFailurePolicy{MaxRetries: 1, RetryOnStatuses: []int{502}, RetryBackoff: time.Millisecond}
	})
	for i := 0; i < 20; i++ {
		rec := f.do("GET", "/v1/svc/x")
		if rec.Code != http.StatusOK || rec.Body.String() != "good" {
			t.Fatalf("request %d: %d %q (a retry should reach the healthy replica)", i, rec.Code, rec.Body)
		}
	}
	if n := bad.Load(); n != 2 {
		t.Fatalf("failing replica got %d requests, want 2 (then ejected)", n)
	}
}

func TestUpstreamPool_AllReplicasEjected(t *testing.T) {
	var hits atomic.Int32
	r := replica(t, "down", &hits, 503)
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Upstreams = []string{r.URL}
		s.CircuitBreaker = &types.CircuitBreakerConfig{Failures: 1, Cooldown: time.Minute}
	})
	f.do("GET", "/v1/svc/x") // 503 passes through and ejects the only replica
	rec := f.do("GET", "/v1/svc/x")
	expectStatus(t, rec, http.StatusServiceUnavailable)
	if problemOf(t, rec).Code != "upstream_circuit_open" || hits.Load() != 1 {
		t.Fatalf("got %s after %d upstream hits", rec.Body, hits.Load())
	}
}

func TestUpstreamPool_LeastRequests(t *testing.T) {
	release := make(chan struct{})
	var slowHits, fastHits atomic.Int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		slowHits.Add(1)
		<-release
	}))
	defer slow.Close()
	fast := replica(t, "fast", &fastHits, 200)
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Upstreams = []string{slow.URL, fast.URL}
		s.LoadBalancing = "least_requests"
	})
	// Park one request on the slow replica (both start idle, so it may take
	// a couple of tries to land there).
	var wg sync.WaitGroup
	for slowHits.Load() == 0 {
		wg.Add(1)
		go func() { defer wg.Done(); f.do("GET", "/v1/svc/hold") }()
		time.Sleep(20 * time.Millisecond)
	}
	before := fastHits.Load()
	for i := 0; i < 5; i++ {
		f.do("GET", "/v1/svc/x")
	}
	close(release)
	wg.Wait()
	if n := fastHits.Load() - before; n != 5 {
		t.Fatalf("least_requests sent %d of 5 to the idle replica", n)
	}
}

func TestMatch_MethodsAndHost(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Methods = []string{"GET"}
		s.Host = "api.example.com"
	})
	req := httptest.NewRequest("GET", "http://api.example.com:8080/v1/svc/x", nil)
	expectStatus(t, f.send(req), http.StatusOK)
	req = httptest.NewRequest("POST", "http://api.example.com/v1/svc/x", nil)
	rec := f.send(req)
	expectStatus(t, rec, http.StatusMethodNotAllowed)
	if rec.Header().Get("Allow") != "GET" {
		t.Fatalf("Allow = %q", rec.Header().Get("Allow"))
	}
	expectStatus(t, f.send(httptest.NewRequest("GET", "http://other.example.com/v1/svc/x", nil)), http.StatusNotFound)
}

func TestAuthNone_LimitsByClientIP(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Auth = types.AuthConfig{Type: "none"}
		s.Tiers[0].Quota = 2
	})
	ip := func(addr string) int {
		req := httptest.NewRequest("GET", "/v1/svc/x", nil)
		req.RemoteAddr = addr
		rec := httptest.NewRecorder()
		f.server.ServeHTTP(rec, req)
		return rec.Code
	}
	got := []int{ip("10.0.0.1:1"), ip("10.0.0.1:2"), ip("10.0.0.1:3"), ip("10.0.0.2:1")}
	if got[0] != 200 || got[1] != 200 || got[2] != 429 || got[3] != 200 {
		t.Fatalf("statuses %v: each client IP has its own quota of 2", got)
	}
}

func TestMaxConcurrent(t *testing.T) {
	release := make(chan struct{})
	var started atomic.Int32
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		started.Add(1)
		<-release
	}, func(s *types.ServiceConfig) { s.MaxConcurrent = 1 })
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); f.do("GET", "/v1/svc/hold") }()
	for started.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	rec := f.do("GET", "/v1/svc/x")
	close(release)
	wg.Wait()
	expectStatus(t, rec, http.StatusTooManyRequests)
	if problemOf(t, rec).Code != "concurrency_limited" {
		t.Fatal(rec.Body)
	}
	expectStatus(t, f.do("GET", "/v1/svc/x"), http.StatusOK) // the slot was released
}

func TestServiceTimeoutAndBodyLimit(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			time.Sleep(200 * time.Millisecond)
		}
		_, _ = io.Copy(io.Discard, r.Body)
	}, func(s *types.ServiceConfig) {
		s.Timeout = 50 * time.Millisecond
		s.MaxBodyBytes = 4
	})
	expectStatus(t, f.do("GET", "/v1/svc/slow"), http.StatusGatewayTimeout)
	expectStatus(t, f.send(httptest.NewRequest("POST", "/v1/svc/x", strings.NewReader("12345"))), http.StatusRequestEntityTooLarge)
	expectStatus(t, f.send(httptest.NewRequest("POST", "/v1/svc/x", strings.NewReader("1234"))), http.StatusOK)
}

// --- JWT ---------------------------------------------------------------------

func jwksServer(t *testing.T, key *rsa.PrivateKey, kid string) *httptest.Server {
	t.Helper()
	b64 := base64.RawURLEncoding.EncodeToString
	doc := map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
		"n": b64(key.N.Bytes()), "e": b64(big.NewInt(int64(key.E)).Bytes()),
	}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(doc) }))
	t.Cleanup(srv.Close)
	return srv
}

func TestAuthJWT(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	jwks := jwksServer(t, key, "k1")
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Auth = types.AuthConfig{Type: "jwt", JWT: &types.JWTConfig{JWKSURL: jwks.URL, Issuer: "https://idp.example", Audience: "gate", TierClaim: "plan"}}
		s.Tiers = append(s.Tiers, types.TierConfig{TierID: "pro", Quota: 1000, GetCost: 1})
		s.Failure.Hub.DefaultTier = "free"
	})
	sign := func(claims jwt.MapClaims, kid string) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		tok.Header["kid"] = kid
		s, err := tok.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	valid := jwt.MapClaims{"iss": "https://idp.example", "aud": "gate", "sub": "u1", "plan": "pro", "exp": time.Now().Add(time.Hour).Unix()}
	call := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/v1/svc/x", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		f.server.ServeHTTP(rec, req)
		return rec
	}

	rec := call(sign(valid, "k1"))
	expectStatus(t, rec, http.StatusOK)
	if e := f.sink.last(); e == nil || e.Tier != "pro" {
		t.Fatalf("tier from claim: %+v", e)
	}
	noPlan := jwt.MapClaims{"iss": "https://idp.example", "aud": "gate", "sub": "u2", "exp": time.Now().Add(time.Hour).Unix()}
	expectStatus(t, call(sign(noPlan, "k1")), http.StatusOK)
	if e := f.sink.last(); e.Tier != "free" {
		t.Fatalf("default tier: %+v", e)
	}

	for name, token := range map[string]string{
		"missing":      "",
		"garbage":      "not.a.jwt",
		"wrong issuer": sign(jwt.MapClaims{"iss": "evil", "aud": "gate", "sub": "u", "exp": time.Now().Add(time.Hour).Unix()}, "k1"),
		"expired":      sign(jwt.MapClaims{"iss": "https://idp.example", "aud": "gate", "sub": "u", "exp": time.Now().Add(-time.Hour).Unix()}, "k1"),
		"unknown key":  sign(valid, "k2"),
	} {
		if rec := call(token); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s token: %d %s", name, rec.Code, rec.Body)
		}
	}
}
