package decide_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/config"
	"gateway/packages/decide"
	"gateway/packages/hub"
	"gateway/packages/ledger"
	"gateway/packages/limiter"
	"gateway/packages/rls"

	rlscommon "github.com/envoyproxy/go-control-plane/envoy/extensions/common/ratelimit/v3"
	rlsv3 "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
)

const cfgYAML = `version: 2
descriptors:
  - domain: ingress
    entries:
      - key: remote_address
        limit: { requests: 2, window: 1h }
      - key: generic_key
        value: checkout
        limit: { requests: 3, window: 1h }
        entries:
          - key: user_id
            limit: { requests: 1, window: 1h }
            mode: shadow
      - key: generic_key
        limit: { requests: 50, window: 1h }
      - key: path
        entries:
          - key: method
            value: POST
            limit: { requests: 10, window: 1h }
`

// inproc calls the hub's lease server directly.
type inproc struct{ srv *hub.QuotaLeaseServer }

func (c inproc) RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	return c.srv.RequestQuotaLease(ctx, in)
}

func (c inproc) ReturnLeases(ctx context.Context, in *types.ReturnLeasesRequest) (*types.ReturnLeasesResponse, error) {
	return c.srv.ReturnLeases(ctx, in)
}

func setup(t *testing.T) (*decide.Decider, *ledger.Mem, *types.GatewayConfig) {
	t.Helper()
	cfg, err := config.ParseAndCompile([]byte(cfgYAML), "t.yaml")
	if err != nil {
		t.Fatal(err)
	}
	mem := ledger.NewMem()
	client := inproc{hub.NewQuotaLeaseServer(mem, hub.NewStaticConfigManager(cfg), hub.NewInMemoryTierStore())}
	d := &decide.Decider{
		Limiter: limiter.NewLease(client, limiter.LeaseOptions{LeaseSize: 1}),
		Config:  func() *types.GatewayConfig { return cfg },
		Hub:     client,
		Secret:  []byte("s3cret"),
	}
	return d, mem, cfg
}

func entries(kv ...string) []decide.Entry {
	var out []decide.Entry
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, decide.Entry{Key: kv[i], Value: kv[i+1]})
	}
	return out
}

func TestResolve(t *testing.T) {
	_, _, cfg := setup(t)
	cases := []struct {
		entries []decide.Entry
		limit   string
		ok      bool
	}{
		{entries("remote_address", "10.0.0.1"), "remote_address", true},
		{entries("generic_key", "checkout"), "generic_key=checkout", true}, // exact beats wildcard
		{entries("generic_key", "search"), "generic_key", true},
		{entries("generic_key", "checkout", "user_id", "u1"), "generic_key=checkout/user_id", true},
		{entries("path", "/x"), "", false}, // the entry has no limit of its own
		{entries("path", "/x", "method", "POST"), "path/method=POST", true},
		{entries("path", "/x", "method", "GET"), "", false},
		{entries("nope", "x"), "", false},
	}
	for _, c := range cases {
		m, ok := decide.Resolve(cfg, "ingress", c.entries)
		if ok != c.ok || m.LimitID != c.limit {
			t.Errorf("%v: got %q %v, want %q %v", c.entries, m.LimitID, ok, c.limit, c.ok)
			continue
		}
		if ok {
			if spec, found := decide.Find(cfg, "ingress", m.LimitID); !found || spec.Quota != m.Spec.Quota {
				t.Errorf("Find(%q) = %v %v", m.LimitID, spec, found)
			}
		}
	}
	a, _ := decide.Resolve(cfg, "ingress", entries("remote_address", "10.0.0.1"))
	b, _ := decide.Resolve(cfg, "ingress", entries("remote_address", "10.0.0.2"))
	if a.Subject == b.Subject || strings.Contains(a.Subject, "10.0.0.1") {
		t.Fatalf("per-value subjects: %q %q", a.Subject, b.Subject)
	}
}

func TestCheck_PerValueLimitsAndRetryHint(t *testing.T) {
	d, _, _ := setup(t)
	ctx := context.Background()
	ip := func(v string) []decide.Descriptor {
		return []decide.Descriptor{{Entries: entries("remote_address", v)}}
	}
	for i := 0; i < 2; i++ {
		if out, err := d.Check(ctx, "ingress", ip("10.0.0.1"), 0, false); err != nil || !out.Allowed {
			t.Fatalf("request %d denied: %v %+v", i, err, out)
		}
	}
	out, _ := d.Check(ctx, "ingress", ip("10.0.0.1"), 0, false)
	if out.Allowed || out.Statuses[0].Allowed || out.RetryAfter <= 0 {
		t.Fatalf("third request: %+v", out)
	}
	if out, _ := d.Check(ctx, "ingress", ip("10.0.0.2"), 0, false); !out.Allowed {
		t.Fatal("another address shares the first one's limit")
	}
	if out, _ := d.Check(ctx, "ingress", []decide.Descriptor{{Entries: entries("unknown", "x")}}, 0, false); !out.Allowed || out.Statuses[0].Matched {
		t.Fatalf("an unmatched descriptor was limited: %+v", out)
	}
}

func TestCheck_AllOfRollsBackAndShadowNeverDenies(t *testing.T) {
	d, _, _ := setup(t)
	ctx := context.Background()
	checkout := decide.Descriptor{Entries: entries("generic_key", "checkout", "user_id", "u1")} // shadow, limit 1
	ip := decide.Descriptor{Entries: entries("remote_address", "10.9.9.9")}                     // limit 2
	for i := 0; i < 2; i++ {
		out, _ := d.Check(ctx, "ingress", []decide.Descriptor{checkout, ip}, 0, false)
		if !out.Allowed {
			t.Fatalf("request %d denied by a shadow limit: %+v", i, out)
		}
		if i == 1 && !out.Statuses[0].ShadowDenied {
			t.Fatalf("shadow denial not reported: %+v", out.Statuses[0])
		}
	}
	// generic_key=checkout allows 3; remote_address is now spent, so the
	// pair is denied and checkout's charge refunded.
	pair := []decide.Descriptor{{Entries: entries("generic_key", "checkout")}, ip}
	if out, _ := d.Check(ctx, "ingress", pair, 0, false); out.Allowed {
		t.Fatal("pair admitted past remote_address's limit")
	}
	for i := 0; i < 3; i++ {
		if out, _ := d.Check(ctx, "ingress", pair[:1], 0, false); !out.Allowed {
			t.Fatalf("checkout request %d denied: the denied pair's charge was not refunded", i)
		}
	}
}

func TestSettle_RefundsTheLedgerOnce(t *testing.T) {
	d, mem, cfg := setup(t)
	ctx := context.Background()
	desc := []decide.Descriptor{{Entries: entries("path", "/x", "method", "POST"), Cost: 8}}
	out, err := d.Check(ctx, "ingress", desc, 0, true)
	if err != nil || !out.Allowed || out.Reservation == "" {
		t.Fatalf("reserve: %v %+v", err, out)
	}
	m, _ := decide.Resolve(cfg, "ingress", desc[0].Entries)
	ref := ledger.Ref{Scope: decide.Scope, Service: "ingress", Limit: m.LimitID, Subject: m.Subject}
	spec := ledger.Spec{Limit: 10, Window: time.Hour}
	before := mem.Used(ref, spec)
	for i := 0; i < 2; i++ { // a retried settle applies once
		if err := d.Settle(ctx, out.Reservation, 3); err != nil {
			t.Fatal(err)
		}
	}
	if after := mem.Used(ref, spec); before-after != 5 {
		t.Fatalf("ledger used %d -> %d, want a refund of 5", before, after)
	}
	if err := d.Settle(ctx, out.Reservation+"x", 1); !errors.Is(err, decide.ErrBadReservation) {
		t.Fatalf("tampered reservation: %v", err)
	}
}

func TestRLS_ShouldRateLimit(t *testing.T) {
	d, _, _ := setup(t)
	s := &rls.Server{Decider: d}
	req := &rlsv3.RateLimitRequest{Domain: "ingress", Descriptors: []*rlscommon.RateLimitDescriptor{
		{Entries: []*rlscommon.RateLimitDescriptor_Entry{{Key: "remote_address", Value: "192.0.2.7"}}},
	}}
	var codes []rlsv3.RateLimitResponse_Code
	var last *rlsv3.RateLimitResponse
	for i := 0; i < 3; i++ {
		resp, err := s.ShouldRateLimit(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		codes = append(codes, resp.OverallCode)
		last = resp
	}
	if codes[0] != rlsv3.RateLimitResponse_OK || codes[1] != rlsv3.RateLimitResponse_OK || codes[2] != rlsv3.RateLimitResponse_OVER_LIMIT {
		t.Fatalf("codes = %v", codes)
	}
	st := last.Statuses[0]
	if st.Code != rlsv3.RateLimitResponse_OVER_LIMIT || st.CurrentLimit.RequestsPerUnit != 2 || st.CurrentLimit.Unit != rlsv3.RateLimitResponse_RateLimit_HOUR {
		t.Fatalf("status: %+v", st)
	}
	found := false
	for _, h := range last.ResponseHeadersToAdd {
		found = found || h.Key == "Retry-After"
	}
	if !found {
		t.Fatalf("no Retry-After header for a denial: %v", last.ResponseHeadersToAdd)
	}
}
