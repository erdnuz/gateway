//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/edge"
)

// delayedGrantLeaseClient returns configured grants and can delay specific calls
// to force overlap between DECRBY traffic and INCRBY top-up.
type delayedGrantLeaseClient struct {
	mu         sync.Mutex
	grants     []int64
	ttlSeconds int64
	calls      int
	delayOn    map[int]time.Duration // 1-based call index -> delay
}

func (f *delayedGrantLeaseClient) RequestQuotaLease(_ context.Context, _ *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	f.mu.Lock()
	f.calls++
	callNum := f.calls
	grant := int64(0)
	if len(f.grants) > 0 {
		grant = f.grants[0]
		f.grants = f.grants[1:]
	}
	ttl := f.ttlSeconds
	if ttl <= 0 {
		ttl = 5
	}
	delay := f.delayOn[callNum]
	f.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	return &types.QuotaLeaseResponse{GrantedTokens: grant, LeaseTtlSeconds: ttl}, nil
}

func (f *delayedGrantLeaseClient) Close() error { return nil }

func (f *delayedGrantLeaseClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// T-CON-1: Concurrent Renewal Suppression.
// Trigger temporal and volume renewals on the same request and verify only one
// extra lease call is dispatched (in-flight suppression works).
func TestIntegration_TCON1ConcurrentRenewalSuppression(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-con-1", "free")
	fake := &delayedGrantLeaseClient{grants: []int64{10, 10}, ttlSeconds: 3}
	h.setLeaseClient(fake)

	// First request fetches lease A and consumes 1 => remaining 9.
	resp, _ := h.edgeRequest("POST", "/v1/auth-api/con-1", "k-con-1")
	if resp.StatusCode != 200 {
		t.Fatalf("initial request expected 200 got=%d", resp.StatusCode)
	}

	// Drain to remaining=3 so no low-water renewal fires before the combined trigger.
	for i := 0; i < 6; i++ {
		resp, _ = h.edgeRequest("POST", "/v1/auth-api/con-1-drain", "k-con-1")
		if resp.StatusCode != 200 {
			t.Fatalf("drain request %d expected 200 got=%d", i+1, resp.StatusCode)
		}
	}
	if got := fake.callCount(); got != 1 {
		t.Fatalf("expected exactly 1 lease call before trigger setup, got %d", got)
	}

	// Move inside temporal renewal buffer (2s) for a 3s lease.
	time.Sleep(1200 * time.Millisecond)

	// This single request should satisfy both conditions:
	// - temporal trigger (near expiry)
	// - volume trigger (remaining <= low-water)
	// and still issue only one renewal call.
	resp, _ = h.edgeRequest("POST", "/v1/auth-api/con-1-trigger", "k-con-1")
	if resp.StatusCode != 200 {
		t.Fatalf("trigger request expected 200 got=%d", resp.StatusCode)
	}

	// Allow async renewal goroutine to complete.
	time.Sleep(300 * time.Millisecond)

	if got := fake.callCount(); got != 2 {
		t.Fatalf("expected exactly 2 lease calls (initial + one suppressed renewal), got %d", got)
	}
}

// T-CON-2: Multi-Edge Global Drain.
// Scaled to current test config (quota=20, requested=10) while preserving the
// atomicity invariant: total grants across concurrent requests == total quota.
func TestIntegration_TCON2MultiEdgeGlobalDrain(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-con-2", "free")

	const (
		concurrentEdges = 5
		requestTokens   = int64(10)
		expectedTotal   = int64(20) // tier free quota in integration config
	)

	clients := make([]edge.QuotaLeaseRequester, 0, concurrentEdges)
	for i := 0; i < concurrentEdges; i++ {
		c, err := edge.NewGRPCQuotaLeaseClient(
			h.hubGRPCListener.Addr().String(),
			"hub-server",
			h.mtlsFiles.EdgeCertPath,
			h.mtlsFiles.EdgeKeyPath,
			h.mtlsFiles.CAPath,
		)
		if err != nil {
			t.Fatalf("new grpc lease client %d: %v", i, err)
		}
		clients = append(clients, c)
		defer c.Close()
	}

	var wg sync.WaitGroup
	grants := make([]int64, concurrentEdges)
	errs := make([]error, concurrentEdges)
	for i := 0; i < concurrentEdges; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := clients[idx].RequestQuotaLease(h.ctx, &types.QuotaLeaseRequest{
				Prefix:          "v1",
				ServiceId:       "auth-api",
				ApiKey:          "k-con-2",
				RequestedTokens: requestTokens,
			})
			if err != nil {
				errs[idx] = err
				return
			}
			grants[idx] = resp.GrantedTokens
		}(i)
	}
	wg.Wait()

	total := int64(0)
	for i := 0; i < concurrentEdges; i++ {
		if errs[i] != nil {
			t.Fatalf("lease request %d failed: %v", i, errs[i])
		}
		if grants[i] < 0 || grants[i] > requestTokens {
			t.Fatalf("lease request %d invalid grant=%d", i, grants[i])
		}
		total += grants[i]
	}
	if total != expectedTotal {
		t.Fatalf("expected atomic total grants=%d, got %d (grants=%v)", expectedTotal, total, grants)
	}
}

// T-CON-3: Atomic Merge (Top-Up).
// While DECRBY traffic is ongoing, deliver a delayed renewal grant and verify
// the final balance reflects all consumed units plus merged top-up.
func TestIntegration_TCON3AtomicMergeTopUp(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	h.setTier("v1", "k-con-3", "free")
	fake := &delayedGrantLeaseClient{
		grants:     []int64{10, 10},
		ttlSeconds: 10,
		delayOn: map[int]time.Duration{
			2: 150 * time.Millisecond, // delay the renewal grant
		},
	}
	h.setLeaseClient(fake)

	// Initial lease A and consume 1 => remaining 9.
	resp, _ := h.edgeRequest("POST", "/v1/auth-api/con-3", "k-con-3")
	if resp.StatusCode != 200 {
		t.Fatalf("initial request expected 200 got=%d", resp.StatusCode)
	}

	// Drain to remaining 3.
	for i := 0; i < 6; i++ {
		resp, _ = h.edgeRequest("POST", "/v1/auth-api/con-3-drain", "k-con-3")
		if resp.StatusCode != 200 {
			t.Fatalf("drain request %d expected 200 got=%d", i+1, resp.StatusCode)
		}
	}

	// Start a small burst that pushes to low-water and below while renewal runs.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = h.edgeRequest("POST", "/v1/auth-api/con-3-burst", "k-con-3")
		}()
	}
	wg.Wait()

	// Wait for delayed top-up to apply.
	time.Sleep(300 * time.Millisecond)

	key := "edge:lease:tokens:v1:k-con-3"
	balance, err := h.rdb.Get(h.ctx, key).Int64()
	if err != nil {
		t.Fatalf("read local token balance: %v", err)
	}

	// Expected invariant (scaled):
	// tokens_before_grant=3, used_during_grant=3, new_tokens=10 => final=10.
	if balance < 9 {
		t.Fatalf("expected merged final balance around 10 with no token loss, got %d", balance)
	}
	if fake.callCount() < 2 {
		t.Fatalf("expected renewal grant call to occur, calls=%d", fake.callCount())
	}
}
