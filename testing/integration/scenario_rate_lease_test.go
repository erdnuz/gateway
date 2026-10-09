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

func TestIntegration_RateLeaseRefillAndExhaustion(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "krate", "free")
	allowed := 0

	for i := 1; i <= 20; i++ {
		resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "krate")
		if resp.StatusCode == http.StatusOK {
			allowed++
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			break
		}
		t.Fatalf("request %d expected 200/429 got=%d", i, resp.StatusCode)
	}

	if allowed < 15 || allowed > 20 {
		t.Fatalf("expected 15-20 successful requests before exhaustion, got %d", allowed)
	}

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "krate")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after quota exhaustion got=%d", resp.StatusCode)
	}

	if hits := h.upstreamHits.Load(); hits != int64(allowed) {
		t.Fatalf("expected upstream hits=%d, got %d", allowed, hits)
	}
}

type fakeLeaseClientIntegration struct {
	mu         sync.Mutex
	grants     []int64
	ttlSeconds int64
	calls      int
}

func (f *fakeLeaseClientIntegration) RequestQuotaLease(_ context.Context, _ *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	grant := int64(0)
	if len(f.grants) > 0 {
		grant = f.grants[0]
		f.grants = f.grants[1:]
	}
	ttl := f.ttlSeconds
	if ttl <= 0 {
		ttl = 10
	}
	return &types.QuotaLeaseResponse{GrantedTokens: grant, LeaseTtlSeconds: ttl}, nil
}

func (f *fakeLeaseClientIntegration) Close() error { return nil }

func (f *fakeLeaseClientIntegration) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type fakeWaitingLeaseClientIntegration struct {
	mu           sync.Mutex
	calls        int
	retryAfterMs int64
}

func (f *fakeWaitingLeaseClientIntegration) RequestQuotaLease(_ context.Context, _ *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return &types.QuotaLeaseResponse{WaitingForCapacity: true, RetryAfterMs: f.retryAfterMs}, nil
}

func (f *fakeWaitingLeaseClientIntegration) Close() error { return nil }

func (f *fakeWaitingLeaseClientIntegration) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestIntegration_T1TemporalOverlapTopUp(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()
	// Lease balances and TTLs are asserted across time; stay inside one quota window.
	h.requireWindowHeadroom(15 * time.Second)

	h.setTier("v1", "kt1", "free")
	fake := &fakeLeaseClientIntegration{grants: []int64{100, 100}, ttlSeconds: 10}
	h.setLeaseClient(fake)

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "kt1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request expected 200 got=%d", resp.StatusCode)
	}

	if b1, _ := h.leaseHolding("kt1"); b1 != 99 {
		t.Fatalf("expected 99 tokens after first consume, got %d", b1)
	}

	// Wait until we are inside the 2s temporal renewal buffer for a 10s lease.
	time.Sleep(8300 * time.Millisecond)

	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "kt1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second request expected 200 got=%d", resp.StatusCode)
	}

	// Allow the async renewal to complete and merge.
	time.Sleep(250 * time.Millisecond)
	if b2, _ := h.leaseHolding("kt1"); b2 < 190 {
		t.Fatalf("expected merged balance around 198 after overlap top-up, got %d", b2)
	}
	if fake.callCount() < 2 {
		t.Fatalf("expected 2 lease calls (A and B), got %d", fake.callCount())
	}
}

func TestIntegration_T2LowWaterTopUpUnderPressure(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()
	// Lease balances and TTLs are asserted across time; stay inside one quota window.
	h.requireWindowHeadroom(15 * time.Second)

	h.setTier("v1", "kt2", "free")
	fake := &fakeLeaseClientIntegration{grants: []int64{10, 10}, ttlSeconds: 30}
	h.setLeaseClient(fake)

	for i := 0; i < 9; i++ {
		resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "kt2")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d expected 200 got=%d", i+1, resp.StatusCode)
		}
	}

	// Give the async LWM renewal a small window to complete.
	time.Sleep(250 * time.Millisecond)
	if b, _ := h.leaseHolding("kt2"); b < 10 {
		t.Fatalf("expected balance to be topped up (>=10), got %d", b)
	}
	if fake.callCount() < 2 {
		t.Fatalf("expected at least 2 lease calls after low-water trigger, got %d", fake.callCount())
	}
}

func TestIntegration_T3GlobalExhaustionBackoff(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "kt3", "free")
	fake := &fakeWaitingLeaseClientIntegration{retryAfterMs: 500}
	h.setLeaseClient(fake)

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "kt3")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("first request expected 429 got=%d", resp.StatusCode)
	}

	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "kt3")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second request expected 429 while waiting got=%d", resp.StatusCode)
	}
	if fake.callCount() > 2 {
		t.Fatalf("expected at most one immediate retry attempt while entering waiting, calls=%d", fake.callCount())
	}

	time.Sleep(800 * time.Millisecond) // retry_after_ms with up to 50% jitter
	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "kt3")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("post-retry request expected 429 got=%d", resp.StatusCode)
	}
	if fake.callCount() < 2 {
		t.Fatalf("expected second poll after retry_after_ms, calls=%d", fake.callCount())
	}
}

func TestIntegration_T4StaleLeaseHardExpiry(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()
	// Lease balances and TTLs are asserted across time; stay inside one quota window.
	h.requireWindowHeadroom(15 * time.Second)

	h.setTier("v1", "kt4", "free")
	fake := &fakeLeaseClientIntegration{grants: []int64{5}, ttlSeconds: 2}
	h.setLeaseClient(fake)

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "kt4")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request expected 200 got=%d", resp.StatusCode)
	}

	if n, exp := h.leaseHolding("kt4"); n <= 0 || time.Until(exp) <= 0 {
		t.Fatalf("expected an unexpired lease, tokens=%d expires in %v", n, time.Until(exp))
	}

	time.Sleep(2300 * time.Millisecond)
	if n, _ := h.leaseHolding("kt4"); n != 0 {
		t.Fatalf("expected the lease to hard-expire, still holding %d tokens", n)
	}

	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "kt4")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("request after hard expiry expected 429 got=%d", resp.StatusCode)
	}
}
