package config

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gateway/packages/common/types"
)

const basic = `
version: 2
services:
  - name: orders
    match: { path_prefix: /v1/orders }
    upstream: http://orders:8080
    limits:
      window: 1m
      default_tier: free
      tiers:
        free: { requests: 20 }
        pro:  { requests: 1000, cost: { POST: 2, "*": 3 } }
      when_hub_unavailable: { allow: 10 }
    cache: { ttl: 2s, key: "{method}:{path}:{encoding}" }
    retries: { attempts: 3, statuses: [502, 503], backoff: 100ms }
  - name: users
    match: { path_prefix: /v1/users }
    upstream: http://users:8080/api
    strip_path_prefix: false
    limits:
      window: 1m
      tiers: { free: { requests: 5 } }
    on_upstream_error: { mode: fail-open, status: 200, body: "{}" }
`

func TestCompile_Basic(t *testing.T) {
	cfg, err := ParseAndCompile([]byte(basic), "basic.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Prefixes) != 1 || cfg.Prefixes[0].Prefix != "v1" || cfg.Prefixes[0].QuotaPeriod != time.Minute {
		t.Fatalf("prefix grouping: %+v", cfg.Prefixes)
	}
	orders := cfg.Prefixes[0].Services[0]
	if orders.ServiceID != "orders" || !orders.Transform.StripPrefix {
		t.Fatalf("orders: %+v", orders)
	}
	pro := orders.Tiers[1]
	if pro.TierID != "pro" || pro.Quota != 1000 || pro.GetCost != 3 || pro.PostCost != 2 || pro.OtherCost != 3 {
		t.Fatalf("pro tier costs: %+v", pro)
	}
	if orders.SafetyTier == nil || orders.SafetyTier.Quota != 10 || !orders.Failure.Hub.AllowOnRateServiceError {
		t.Fatalf("when_hub_unavailable not compiled: %+v", orders.Failure.Hub)
	}
	if orders.Failure.Hub.TierLookupStrategy != "stale-or-default" || orders.Failure.Hub.DefaultTier != "free" {
		t.Fatalf("default tier: %+v", orders.Failure.Hub)
	}
	if orders.Cache.CacheKey != "$method:$path:$enc" || orders.Cache.TTL != 2*time.Second {
		t.Fatalf("cache: %+v", orders.Cache)
	}
	if up := orders.Failure.Upstream; up.MaxRetries != 2 || up.RetryBackoff != 100*time.Millisecond {
		t.Fatalf("retries: %+v", up)
	}
	users := cfg.Prefixes[0].Services[1]
	if users.Transform.StripPrefix || users.Failure.Upstream.Mode != "fail-open" || !users.Failure.FailOpen {
		t.Fatalf("users: %+v", users)
	}
	if cfg.Runtime.Edge.RateLeaseSize != 100 {
		t.Fatalf("runtime defaults not applied: %+v", cfg.Runtime.Edge)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("compiled config fails runtime validation: %v", err)
	}
}

func TestParse_AcceptsJSON(t *testing.T) {
	doc := `{"version": 2, "services": [{"name": "a", "match": {"path_prefix": "/g/a"},
	  "upstream": "http://a", "limits": {"window": "10s", "tiers": {"t": {"requests": 1}}}}]}`
	if _, err := ParseAndCompile([]byte(doc), "a.json"); err != nil {
		t.Fatal(err)
	}
}

func problems(t *testing.T, doc string) []Problem {
	t.Helper()
	_, err := Parse([]byte(doc), "t.yaml")
	var ce *Error
	if !errors.As(err, &ce) {
		t.Fatalf("want *Error, got %v", err)
	}
	return ce.Problems
}

func TestParse_ReportsEveryProblemWithLines(t *testing.T) {
	doc := `version: 2
services:
  - name: orders
    match: { path_prefix: /v1/orders }
    upstrem: http://orders
    limits:
      window: 60
      tiers:
        free: { requests: 0 }
`
	ps := problems(t, doc)
	want := []struct {
		line int
		sub  string
	}{
		{5, `unknown field "upstrem" (did you mean "upstream"?)`},
		{7, `duration 60 has no unit`},
	}
	for _, w := range want {
		found := false
		for _, p := range ps {
			if p.Line == w.line && strings.Contains(p.Msg, w.sub) {
				found = true
			}
		}
		if !found {
			t.Errorf("missing problem at line %d containing %q; got %v", w.line, w.sub, ps)
		}
	}
}

func TestParse_SemanticProblemsPointAtFields(t *testing.T) {
	doc := `version: 2
services:
  - name: orders
    match: { path_prefix: /v1/orders }
    upstream: orders:8080
    limits:
      window: 1m
      default_tier: gold
      tiers:
        free: { requests: 0 }
  - name: users
    match: { path_prefix: /v1/people }
    upstream: http://users
    limits:
      window: 1h
      tiers: { free: { requests: 1 } }
`
	got := map[string]int{}
	for _, p := range problems(t, doc) {
		got[p.Path] = p.Line
	}
	for path, line := range map[string]int{
		"services[0].upstream":                   5,
		"services[0].limits.default_tier":        8,
		"services[0].limits.tiers.free.requests": 10,
		"services[1].match.path_prefix":          12,
		"services[1].limits.window":              15,
	} {
		if got[path] != line {
			t.Errorf("%s: line %d, want %d (all: %v)", path, got[path], line, got)
		}
	}
}

func TestParse_DetectsVersion1(t *testing.T) {
	ps := problems(t, `{"prefixes": [{"prefix": "v1"}]}`)
	if len(ps) != 1 || !strings.Contains(ps[0].Msg, "gate config migrate") {
		t.Fatalf("got %v", ps)
	}
}

func TestDuration_String(t *testing.T) {
	for d, want := range map[time.Duration]string{time.Minute: "1m", time.Hour: "1h", 90 * time.Minute: "1h30m", 1500 * time.Millisecond: "1.5s"} {
		if got := Duration(d).String(); got != want {
			t.Errorf("%v: %q, want %q", d, got, want)
		}
	}
}

// Every v1 config in the repository migrates to a valid v2 file that compiles
// back to the same routing, tiers and limits.
func TestMigrateV1_RoundTrip(t *testing.T) {
	files := []string{
		"testdata/v1-minimal.json",
		"testdata/v1-full.json",
	}
	for _, path := range files {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Skipf("not present: %v", err)
			}
			f, err := MigrateV1(data)
			if err != nil {
				t.Fatal(err)
			}
			out, err := Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ParseAndCompile(out, "migrated")
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			var orig types.GatewayConfig
			if v1, err := MigrateV1(data); err == nil {
				orig = *Compile(v1)
			}
			if !reflect.DeepEqual(summarize(got), summarize(&orig)) {
				t.Fatalf("round trip changed config:\n got  %v\n want %v", summarize(got), summarize(&orig))
			}
		})
	}
}

func summarize(c *types.GatewayConfig) []string {
	var out []string
	for _, p := range c.Prefixes {
		for _, s := range p.Services {
			line := p.Prefix + "/" + s.ServiceID + " " + s.TargetURL + " " + p.QuotaPeriod.String()
			for _, t := range s.Tiers {
				line += " " + t.TierID + "=" + string(rune('0'+t.Quota%10))
			}
			out = append(out, line)
		}
	}
	return out
}

var update = flag.Bool("update", false, "rewrite golden files")

// The published schema must match the Go types.
func TestJSONSchema_Golden(t *testing.T) {
	got, err := JSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	golden := "../../schema/gate.schema.json"
	if *update {
		_ = os.MkdirAll(filepath.Dir(golden), 0o755)
		if err := os.WriteFile(golden, append(got, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run: go test ./packages/config -run Golden -update)", err)
	}
	if strings.TrimSpace(strings.ReplaceAll(string(want), "\r\n", "\n")) != strings.TrimSpace(string(got)) {
		t.Fatal("schema/gate.schema.json is stale; run: go test ./packages/config -run Golden -update")
	}
}

// Every config shipped in the repository is valid.
func TestShippedConfigsAreValid(t *testing.T) {
	files, _ := filepath.Glob("../../examples/*.yaml")
	files = append(files, "../../deploy/gateway.yaml", "../../internal/app/devstack/default-config.yaml",
		"../../testing/loadtest/gateway-config.loadtest.yaml")
	if len(files) < 5 {
		t.Fatalf("expected example configs, found %v", files)
	}
	for _, f := range files {
		if _, err := Load(f); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}

func TestCompile_ReplicasAuthAndMatching(t *testing.T) {
	cfg, err := Load("../../examples/advanced.yaml")
	if err != nil {
		t.Fatal(err)
	}
	orders := cfg.Prefixes[0].Services[0]
	if len(orders.Upstreams) != 3 || orders.CircuitBreaker == nil || orders.CircuitBreaker.Failures != 5 ||
		orders.MaxConcurrent != 20 || orders.Timeout != 5*time.Second || len(orders.Methods) != 2 {
		t.Fatalf("orders: %+v", orders)
	}
	reports := cfg.Prefixes[0].Services[1]
	if reports.AuthType() != "jwt" || reports.Auth.JWT.TierClaim != "plan" || reports.LoadBalancing != "least_requests" || reports.CircuitBreaker.Cooldown != 30*time.Second {
		t.Fatalf("reports: %+v", reports)
	}
	status := cfg.Prefixes[1].Services[0]
	if status.AuthType() != "none" || status.MaxBodyBytes != 1024 {
		t.Fatalf("status: %+v", status)
	}
	// The v2 form survives a round trip through the runtime model.
	out, err := Marshal(FromInternal(cfg))
	if err != nil {
		t.Fatal(err)
	}
	again, err := ParseAndCompile(out, "roundtrip")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !reflect.DeepEqual(again.Prefixes, cfg.Prefixes) {
		t.Fatalf("round trip changed services:\n%s", out)
	}
}

func TestParse_ReplicasAndAuthProblems(t *testing.T) {
	doc := `version: 2
services:
  - name: a
    match: { path_prefix: /v1/a, methods: [get] }
    upstream: http://a
    upstreams: [http://b]
    limits: { window: 1m, tiers: { x: { requests: 1 }, y: { requests: 2 } } }
  - name: b
    match: { path_prefix: /v1/b }
    upstreams: [http://b1/api, http://b2/other]
    load_balancing: random
    auth: { type: none }
    limits: { window: 1m, tiers: { x: { requests: 1 }, y: { requests: 2 } } }
  - name: c
    match: { path_prefix: /v1/c }
    upstream: http://c
    auth: { type: jwt }
    limits: { window: 1m, tiers: { x: { requests: 1 }, y: { requests: 2 } } }
`
	got := map[string]bool{}
	for _, p := range problems(t, doc) {
		got[p.Path] = true
	}
	for _, path := range []string{
		"services[0].match.methods[0]",
		"services[0].upstreams",
		"services[1].upstreams[1]",
		"services[1].load_balancing",
		"services[1].limits.default_tier",
		"services[2].auth.jwt.jwks_url",
		"services[2].limits.default_tier",
	} {
		if !got[path] {
			t.Errorf("no problem reported at %s (got %v)", path, got)
		}
	}
}
