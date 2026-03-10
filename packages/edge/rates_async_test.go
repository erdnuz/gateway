package edge

import (
	"context"
	"sync"
	"testing"

	"gateway/packages/common/types"
)

type fakeLeaseClient struct {
	mu     sync.Mutex
	grants []int64
	calls  int
}

func (f *fakeLeaseClient) RequestQuotaLease(_ context.Context, _ *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	grant := int64(0)
	if len(f.grants) > 0 {
		grant = f.grants[0]
		f.grants = f.grants[1:]
	}
	return &types.QuotaLeaseResponse{GrantedTokens: grant, LeaseTtlSeconds: 60}, nil
}

func (f *fakeLeaseClient) Close() error { return nil }

func TestEdgeRateManager_RequestsLeaseAndConsumesTokens(t *testing.T) {
	rm := NewRateManagerWithOptions("", nil, 100, RateManagerOptions{LeaseSize: 5, LowWaterPct: 0.2})
	client := &fakeLeaseClient{grants: []int64{5, 5}}
	rm.SetLeaseClient(client)

	for i := 0; i < 7; i++ {
		usage, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 100, 1)
		if err != nil {
			t.Fatalf("increment failed: %v", err)
		}
		if usage != int64(i+1) {
			t.Fatalf("unexpected usage at step %d: %d", i, usage)
		}
	}

	if client.calls < 2 {
		t.Fatalf("expected at least 2 lease calls, got %d", client.calls)
	}
}

func TestEdgeRateManager_DeniesWhenNoLeaseAvailable(t *testing.T) {
	rm := NewRateManagerWithOptions("", nil, 100, RateManagerOptions{LeaseSize: 2, LowWaterPct: 0.2})
	client := &fakeLeaseClient{grants: []int64{2, 0}}
	rm.SetLeaseClient(client)

	if _, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 2, 1); err != nil {
		t.Fatal(err)
	}

	usage, err := rm.IncrementWithService(context.Background(), "v1", "svc", "k1", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if usage <= 2 {
		t.Fatalf("expected synthetic over-limit usage, got %d", usage)
	}
}
