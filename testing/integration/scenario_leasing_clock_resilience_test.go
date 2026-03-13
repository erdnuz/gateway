//go:build integration

package integration

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"gateway/packages/common/types"
)

type scriptedLeaseClient struct {
	mu          sync.Mutex
	calls       int
	ttlSeconds  int64
	grants      []int64
	waitForCall map[int]bool
	retryAfter  int64
}

func (f *scriptedLeaseClient) RequestQuotaLease(_ context.Context, _ *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++

	if f.waitForCall[f.calls] {
		retry := f.retryAfter
		if retry <= 0 {
			retry = 100
		}
		return &types.QuotaLeaseResponse{WaitingForCapacity: true, RetryAfterMs: retry}, nil
	}

	grant := int64(0)
	if len(f.grants) > 0 {
		grant = f.grants[0]
		f.grants = f.grants[1:]
	}
	ttl := f.ttlSeconds
	if ttl <= 0 {
		ttl = 2
	}
	return &types.QuotaLeaseResponse{GrantedTokens: grant, LeaseTtlSeconds: ttl}, nil
}

func (f *scriptedLeaseClient) Close() error { return nil }

func (f *scriptedLeaseClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// T-CLK-1 (duration semantics): validate that the edge lease key follows lease_ttl
// duration from local receipt time (not absolute hub timestamp assumptions).
func TestIntegration_TCLK1DurationBasedExpiry(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-clk-1", "free")
	fake := &scriptedLeaseClient{ttlSeconds: 2, grants: []int64{5}}
	h.setLeaseClient(fake)

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/clk-1", "k-clk-1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initial request expected 200 got=%d", resp.StatusCode)
	}

	key := "edge:lease:tokens:v1:k-clk-1"
	pttl, err := h.rdb.PTTL(h.ctx, key).Result()
	if err != nil {
		t.Fatalf("read pttl: %v", err)
	}
	if pttl < 1500*time.Millisecond || pttl > 2200*time.Millisecond {
		t.Fatalf("expected lease pttl near 2s, got %s", pttl)
	}

	// Before 2s TTL elapses, requests should still pass.
	time.Sleep(1200 * time.Millisecond)
	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/clk-1-mid", "k-clk-1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("request before ttl expiry expected 200 got=%d", resp.StatusCode)
	}
}

// T-CLK-2 (hub ahead simulation): with small ttl, temporal renewal buffer should
// renew before hard expiry and keep traffic flowing.
func TestIntegration_TCLK2RenewalBufferPreventsReclaimGap(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-clk-2", "free")
	fake := &scriptedLeaseClient{ttlSeconds: 2, grants: []int64{5, 5}}
	h.setLeaseClient(fake)

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/clk-2-a", "k-clk-2")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request expected 200 got=%d", resp.StatusCode)
	}

	// Enter temporal-renewal window and trigger proactive renewal.
	time.Sleep(1100 * time.Millisecond)
	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/clk-2-b", "k-clk-2")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("trigger request expected 200 got=%d", resp.StatusCode)
	}

	// Wait for async renewal and pass beyond the first lease's original TTL.
	time.Sleep(1300 * time.Millisecond)
	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/clk-2-c", "k-clk-2")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("request after original ttl expected 200 (renewed lease), got=%d", resp.StatusCode)
	}
	if fake.callCount() < 2 {
		t.Fatalf("expected renewal lease call before expiry, calls=%d", fake.callCount())
	}
}

// T-RES-1: micro-grants should not create internal lease polling storms.
// We assert hub call count stays proportional to real request count.
func TestIntegration_TRES1MicroGrantNoSpin(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-res-1", "free")
	grants := make([]int64, 40)
	for i := range grants {
		grants[i] = 1
	}
	fake := &scriptedLeaseClient{ttlSeconds: 3, grants: grants}
	h.setLeaseClient(fake)

	const n = 20
	for i := 0; i < n; i++ {
		_, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/res-1", "k-res-1")
	}

	calls := fake.callCount()
	if calls > n+5 {
		t.Fatalf("expected no polling spin; calls should be proportional to requests. calls=%d requests=%d", calls, n)
	}
}

// T-RES-3: edge should recover from repeated WAITING (ghost 202) once a later
// poll yields tokens; it must not stay stuck in WAITING forever.
func TestIntegration_TRES3Ghost202Recovery(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-res-3", "free")
	fake := &scriptedLeaseClient{
		ttlSeconds: 2,
		grants:     []int64{0, 0, 5},
		waitForCall: map[int]bool{
			1: true,
			2: true,
		},
		retryAfter: 100,
	}
	h.setLeaseClient(fake)

	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/res-3", "k-res-3")
		if resp.StatusCode == http.StatusOK {
			break
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("expected 429/200 during recovery, got=%d", resp.StatusCode)
		}
		if time.Now().After(deadline) {
			t.Fatalf("edge remained stuck in waiting state too long; calls=%d", fake.callCount())
		}
		time.Sleep(120 * time.Millisecond)
	}

	if fake.callCount() < 3 {
		t.Fatalf("expected multiple polls before recovery grant, calls=%d", fake.callCount())
	}
}
