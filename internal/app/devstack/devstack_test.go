package devstack

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// startStack runs a dev stack on random ports with its upstream pointed at
// the given server, and returns its endpoints.
func startStack(t *testing.T, upstream string, analytics bool) Endpoints {
	t.Helper()
	cfg := strings.Replace(string(defaultConfig), "http://127.0.0.1:9000", upstream, 1)
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := Options{
		ConfigPath: path,
		EdgeAddr:   "127.0.0.1:0",
		HubAddr:    "127.0.0.1:0",
		Out:        io.Discard,
		// The decision service runs too; its gRPC port is picked at random.
		LimiterAddr: "127.0.0.1:0",
	}
	if analytics {
		opts.AnalyticsAddr = "127.0.0.1:0"
	}
	ready := make(chan Endpoints, 1)
	opts.OnReady = func(e Endpoints) { ready <- e }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, opts) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("dev stack did not stop")
		}
	})
	select {
	case e := <-ready:
		return e
	case err := <-done:
		t.Fatalf("dev stack exited: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("dev stack not ready after 30s")
	}
	return Endpoints{}
}

func get(t *testing.T, url, key string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-API-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestDevStack_ProxiesLimitsAndAuthenticates(t *testing.T) {
	var gotKeyHeader atomic.Value
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKeyHeader.Store(r.Header.Get("X-API-Key"))
		_, _ = io.WriteString(w, "upstream:"+r.URL.Path)
	}))
	defer upstream.Close()

	e := startStack(t, upstream.URL, false)
	if e.Keys["freekey"] != "free" || e.Keys["prokey"] != "pro" {
		t.Fatalf("unexpected seeded keys: %v", e.Keys)
	}

	resp := get(t, e.Edge+"/v1/echo/hello", "freekey")
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != "upstream:/hello" {
		t.Fatalf("proxy: status %d body %q", resp.StatusCode, body)
	}
	if gotKeyHeader.Load() != "" {
		t.Fatal("API key leaked to upstream")
	}
	if resp.Header.Get("RateLimit-Limit") != "20" {
		t.Fatalf("RateLimit-Limit = %q", resp.Header.Get("RateLimit-Limit"))
	}

	// free tier: 20 per minute in total (one used above).
	codes := map[int]int{}
	for i := 0; i < 25; i++ {
		codes[get(t, e.Edge+"/v1/echo/n"+string(rune('a'+i)), "freekey").StatusCode]++
	}
	if codes[200] != 19 || codes[429] != 6 {
		t.Fatalf("free tier codes = %v, want 19×200 and 6×429", codes)
	}
	if c := get(t, e.Edge+"/v1/echo/p", "prokey").StatusCode; c != 200 {
		t.Fatalf("pro key: %d", c)
	}
	if c := get(t, e.Edge+"/v1/echo/p", "unknownkey").StatusCode; c != 401 {
		t.Fatalf("unknown key: %d, want 401", c)
	}
}

func TestDevStack_AnalyticsIngestsEvents(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()
	e := startStack(t, upstream.URL, true)

	get(t, e.Edge+"/v1/echo/a", "prokey")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(e.Analytics + "/analytics/events?limit=10")
		if err == nil {
			var out struct {
				Count int `json:"count"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&out)
			resp.Body.Close()
			if out.Count > 0 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("analytics recorded no events")
}

// The decision service answers from the same hub ledger: the built-in demo
// domain allows each user 5 checks a minute.
func TestDevStack_DecisionService(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer upstream.Close()
	e := startStack(t, upstream.URL, false)
	if e.Limiter == "" || e.LimiterGRPC == "" {
		t.Fatalf("limiter endpoints missing: %+v", e)
	}
	check := func(user string) bool {
		body := `{"domain":"demo","descriptors":[{"entries":[{"key":"user","value":"` + user + `"}]}]}`
		resp, err := http.Post(e.Limiter+"/v1/check", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Allowed bool `json:"allowed"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("check: %d %v", resp.StatusCode, err)
		}
		return out.Allowed
	}
	for i := 0; i < 5; i++ {
		if !check("alice") {
			t.Fatalf("check %d denied", i)
		}
	}
	if check("alice") {
		t.Fatal("sixth check in a minute admitted")
	}
	if !check("bob") {
		t.Fatal("bob shares alice's limit")
	}
}
