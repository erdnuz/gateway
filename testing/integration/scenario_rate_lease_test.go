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

	h.setTier("v1", "k-rate", "free")
	allowed := 0

	for i := 1; i <= 20; i++ {
		resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-rate")
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

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-rate")
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

	h.setTier("v1", "k-t1", "free")
	fake := &fakeLeaseClientIntegration{grants: []int64{100, 100}, ttlSeconds: 10}
	h.setLeaseClient(fake)

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-t1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request expected 200 got=%d", resp.StatusCode)
	}

	key := "edge:lease:tokens:v1:k-t1"
	b1, err := h.rdb.Get(h.ctx, key).Int64()
	if err != nil {
		t.Fatalf("read local token balance: %v", err)
	}
	if b1 != 99 {
		t.Fatalf("expected 99 tokens after first consume, got %d", b1)
	}

	// Wait until we are inside the 2s temporal renewal buffer for a 10s lease.
	time.Sleep(8300 * time.Millisecond)

	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-t1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second request expected 200 got=%d", resp.StatusCode)
	}

	// Allow async renewal to complete and apply INCRBY + PEXPIRE.
	time.Sleep(250 * time.Millisecond)
	b2, err := h.rdb.Get(h.ctx, key).Int64()
	if err != nil {
		t.Fatalf("read merged balance: %v", err)
	}
	if b2 < 190 {
		t.Fatalf("expected merged balance around 198 after overlap top-up, got %d", b2)
	}
	if fake.callCount() < 2 {
		t.Fatalf("expected 2 lease calls (A and B), got %d", fake.callCount())
	}
}

func TestIntegration_T2LowWaterTopUpUnderPressure(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-t2", "free")
	fake := &fakeLeaseClientIntegration{grants: []int64{10, 10}, ttlSeconds: 30}
	h.setLeaseClient(fake)

	for i := 0; i < 9; i++ {
		resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-t2")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d expected 200 got=%d", i+1, resp.StatusCode)
		}
	}

	// Give the async LWM renewal a small window to complete.
	time.Sleep(250 * time.Millisecond)
	key := "edge:lease:tokens:v1:k-t2"
	b, err := h.rdb.Get(h.ctx, key).Int64()
	if err != nil {
		t.Fatalf("read low-water merged balance: %v", err)
	}
	if b < 10 {
		t.Fatalf("expected balance to be topped up (>=10), got %d", b)
	}
	if fake.callCount() < 2 {
		t.Fatalf("expected at least 2 lease calls after low-water trigger, got %d", fake.callCount())
	}
}

func TestIntegration_T3GlobalExhaustionBackoff(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-t3", "free")
	fake := &fakeWaitingLeaseClientIntegration{retryAfterMs: 500}
	h.setLeaseClient(fake)

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-t3")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("first request expected 429 got=%d", resp.StatusCode)
	}

	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-t3")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second request expected 429 while waiting got=%d", resp.StatusCode)
	}
	if fake.callCount() > 2 {
		t.Fatalf("expected at most one immediate retry attempt while entering waiting, calls=%d", fake.callCount())
	}

	time.Sleep(600 * time.Millisecond)
	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-t3")
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

	h.setTier("v1", "k-t4", "free")
	fake := &fakeLeaseClientIntegration{grants: []int64{5}, ttlSeconds: 2}
	h.setLeaseClient(fake)

	resp, _ := h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-t4")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request expected 200 got=%d", resp.StatusCode)
	}

	key := "edge:lease:tokens:v1:k-t4"
	if ttl, err := h.rdb.PTTL(h.ctx, key).Result(); err != nil || ttl <= 0 {
		t.Fatalf("expected positive ttl on local key, ttl=%v err=%v", ttl, err)
	}

	time.Sleep(2300 * time.Millisecond)
	exists, err := h.rdb.Exists(h.ctx, key).Result()
	if err != nil {
		t.Fatalf("check expiry key existence: %v", err)
	}
	if exists != 0 {
		t.Fatalf("expected local lease key to hard-expire, still exists=%d", exists)
	}

	resp, _ = h.edgeRequest(http.MethodPost, "/v1/auth-api/rate", "k-t4")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("request after hard expiry expected 429 got=%d", resp.StatusCode)
	}
}
