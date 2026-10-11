package edge

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"gateway/packages/common/types"
	"gateway/packages/keys"
)

func TestAdditionalLimitPerClientIP(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Limits = []types.LimitSpec{{ID: "per-ip", Per: types.PerClientIP, Quota: 2}}
	})
	f.hub.limits = map[string]int64{"per-ip": 2}
	for i := 0; i < 2; i++ {
		expectStatus(t, f.do("GET", "/v1/svc/x"), http.StatusOK)
	}
	rec := f.do("GET", "/v1/svc/x")
	expectStatus(t, rec, http.StatusTooManyRequests)
	if p := problemOf(t, rec); !strings.Contains(p.Detail, "per-ip") {
		t.Fatalf("denial does not name the limit: %+v", p)
	}
	// The per-IP limit's subject is the client address (hashed), not the key.
	f.hub.mu.Lock()
	defer f.hub.mu.Unlock()
	if _, ok := f.hub.used["per-ip/"+keys.Hash("ip:192.0.2.1")]; !ok {
		t.Fatalf("per-ip limit not keyed by the client address: %v", f.hub.used)
	}
}

func TestShadowModeReportsButAdmits(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Tiers[0].Quota = 1
		s.Shadow = true
	})
	f.hub.quota = 1
	expectStatus(t, f.do("GET", "/v1/svc/x"), http.StatusOK)
	for i := 0; i < 3; i++ {
		rec := f.do("GET", "/v1/svc/x")
		expectStatus(t, rec, http.StatusOK)
		if rec.Header().Get("Gate-Shadow-Denied") != "tier" {
			t.Fatalf("request %d: Gate-Shadow-Denied = %q", i, rec.Header().Get("Gate-Shadow-Denied"))
		}
	}
}

func usageUpstream(trailer bool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if trailer {
			w.Header().Set("Trailer", "X-Usage")
			_, _ = io.WriteString(w, "streamed answer")
			w.Header().Set("X-Usage", "3")
			return
		}
		w.Header().Set("X-Usage", "3")
		_, _ = io.WriteString(w, "answer")
	}
}

// With an estimate of 10 and an actual cost of 3, a budget of 30 serves
// about ten requests, not three.
func TestPostHocCostSettlesActualCost(t *testing.T) {
	for _, trailer := range []bool{false, true} {
		name := "header"
		if trailer {
			name = "trailer"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, usageUpstream(trailer), func(s *types.ServiceConfig) {
				s.Tiers[0].Quota = 30
				s.Cost = &types.CostPolicy{Estimate: 10, ActualHeader: "X-Usage"}
			})
			f.hub.quota = 30
			admitted := 0
			for i := 0; i < 20; i++ {
				if f.do("GET", "/v1/svc/x").Code == http.StatusOK {
					admitted++
				}
			}
			if admitted < 7 {
				t.Fatalf("admitted %d; actual costs of 3 against a budget of 30 should admit about 10", admitted)
			}
		})
	}
}

func TestPostHocCostWithoutReportKeepsEstimate(t *testing.T) {
	f := newFixture(t, nil, func(s *types.ServiceConfig) {
		s.Tiers[0].Quota = 30
		s.Cost = &types.CostPolicy{Estimate: 10, ActualHeader: "X-Usage"}
	})
	f.hub.quota = 30
	admitted := 0
	for i := 0; i < 10; i++ {
		if f.do("GET", "/v1/svc/x").Code == http.StatusOK {
			admitted++
		}
	}
	if admitted != 3 {
		t.Fatalf("admitted %d at an estimated 10 each from 30, want 3", admitted)
	}
}
