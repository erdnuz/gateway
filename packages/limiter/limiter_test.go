package limiter

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gateway/packages/common/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// windowedHub grants up to quota tokens per epoch-aligned window, mimicking
// the hub's accounting.
type windowedHub struct {
	mu     sync.Mutex
	quota  int64
	period time.Duration
	used   map[int64]int64
	calls  atomic.Int64
	err    error
}

func (h *windowedHub) RequestQuotaLease(_ context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	h.calls.Add(1)
	if h.err != nil {
		return nil, h.err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	wid := time.Now().Unix() / int64(h.period/time.Second)
	if h.used == nil {
		h.used = map[int64]int64{}
	}
	grant := min(in.RequestedTokens, h.quota-h.used[wid])
	if grant <= 0 {
		return &types.QuotaLeaseResponse{WaitingForCapacity: true, RetryAfterMs: 50}, nil
	}
	h.used[wid] += grant
	return &types.QuotaLeaseResponse{GrantedTokens: grant, RemainingGlobal: h.quota - h.used[wid], LeaseTtlSeconds: int64(h.period / time.Second)}, nil
}

// scriptedHub returns the next scripted grant on each call (0 when exhausted).
type scriptedHub struct {
	mu     sync.Mutex
	grants []int64
	ttl    int64
	calls  atomic.Int64
}

func (h *scriptedHub) RequestQuotaLease(context.Context, *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	h.calls.Add(1)
	h.mu.Lock()
	defer h.mu.Unlock()
	var g int64
	if len(h.grants) > 0 {
		g, h.grants = h.grants[0], h.grants[1:]
	}
	ttl := h.ttl
	if ttl == 0 {
		ttl = 60
	}
	return &types.QuotaLeaseResponse{GrantedTokens: g, LeaseTtlSeconds: ttl}, nil
}

type unlimitedHub struct{}

func (unlimitedHub) RequestQuotaLease(_ context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	return &types.QuotaLeaseResponse{GrantedTokens: in.RequestedTokens, RemainingGlobal: 1 << 40, LeaseTtlSeconds: 3600}, nil
}

func allowN(t testing.TB, l Limiter, req Request, n int) int {
	t.Helper()
	ok := 0
	for i := 0; i < n; i++ {
		d, err := l.Allow(context.Background(), req)
		if err != nil {
			t.Fatalf("allow: %v", err)
		}
		if d.Allowed {
			ok++
		}
	}
	return ok
}

func waitForWindowStart(period time.Duration) {
	p := int64(period)
	now := time.Now().UnixNano()
	time.Sleep(time.Duration((now/p+1)*p-now) + 5*time.Millisecond)
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var req = Request{Scope: "p", Service: "s", Key: "k", Limit: 1 << 30}

func TestLease_SpendsLeasesAndRenews(t *testing.T) {
	hub := &scriptedHub{grants: []int64{5, 5}}
	l := NewLease(hub, LeaseOptions{LeaseSize: 5})
	if got := allowN(t, l, req, 7); got != 7 {
		t.Fatalf("admitted %d of 7", got)
	}
	if hub.calls.Load() < 2 {
		t.Fatalf("expected a renewal, got %d lease calls", hub.calls.Load())
	}
}

func TestLease_DeniesWhenHubGrantsNothing(t *testing.T) {
	l := NewLease(&scriptedHub{grants: []int64{2}}, LeaseOptions{LeaseSize: 2})
	if got := allowN(t, l, req, 3); got != 2 {
		t.Fatalf("admitted %d, want 2", got)
	}
}

func TestLease_ExpiredLeaseIsNotSpent(t *testing.T) {
	l := NewLease(&scriptedHub{grants: []int64{100}}, LeaseOptions{LeaseSize: 100})
	allowN(t, l, req, 1)
	c := l.counter(req)
	c.expiresAt.Store(time.Now().Add(-time.Millisecond).UnixMilli())
	if got := allowN(t, l, req, 1); got != 0 {
		t.Fatal("spent a token from an expired lease")
	}
	if c.tokens.Load() != 0 {
		t.Fatalf("expired lease kept %d tokens", c.tokens.Load())
	}
}

func TestLease_RenewsAtLowWater(t *testing.T) {
	hub := &scriptedHub{grants: []int64{1000, 1000}}
	l := NewLease(hub, LeaseOptions{LeaseSize: 1000, LowWaterPct: 20})
	allowN(t, l, req, 850) // crosses the 200-token low-water mark
	eventually(t, func() bool { n, _ := l.Holding("p", "s", "k"); return n >= 1000 })
	if hub.calls.Load() < 2 {
		t.Fatalf("lease calls = %d, want initial + low water", hub.calls.Load())
	}
}

func TestLease_RenewsBeforeExpiryAndMergesBalance(t *testing.T) {
	hub := &scriptedHub{grants: []int64{100, 100}, ttl: 10}
	l := NewLease(hub, LeaseOptions{LeaseSize: 100, LowWaterPct: 1, RenewBefore: 2 * time.Second})
	allowN(t, l, req, 1)
	l.counter(req).expiresAt.Store(time.Now().Add(1800 * time.Millisecond).UnixMilli())
	allowN(t, l, req, 1)
	eventually(t, func() bool { n, _ := l.Holding("p", "s", "k"); return n == 198 })
}

func TestLease_WaitingBacksOffThenPolls(t *testing.T) {
	hub := &windowedHub{quota: 0, period: time.Hour}
	l := NewLease(hub, LeaseOptions{LeaseSize: 10})
	r := req
	r.Limit = 10
	if got := allowN(t, l, r, 6); got != 0 {
		t.Fatalf("admitted %d with no global quota", got)
	}
	if n := hub.calls.Load(); n != 1 {
		t.Fatalf("hub called %d times while WAITING, want 1", n)
	}
	eventually(t, func() bool { allowN(t, l, r, 1); return hub.calls.Load() >= 2 })
}

func TestLease_WaitingStillSpendsHeldTokens(t *testing.T) {
	hub := &windowedHub{quota: 1 << 30, period: time.Hour}
	l := NewLease(hub, LeaseOptions{LeaseSize: 10})
	r := req
	r.Limit, r.Window = 100, time.Hour
	allowN(t, l, r, 1)
	c := l.counter(r)
	held := c.tokens.Load()
	c.retryAt.Store(time.Now().Add(time.Hour).UnixMilli())
	calls := hub.calls.Load()
	if got := allowN(t, l, r, int(held)+1); got != int(held) {
		t.Fatalf("admitted %d while WAITING, want the %d held tokens", got, held)
	}
	if hub.calls.Load() != calls {
		t.Fatal("hub was called while WAITING")
	}
}

// Regression: a key that hit its quota once was denied forever.
func TestLease_QuotaResetsEachWindow(t *testing.T) {
	l := NewLease(&windowedHub{quota: 30, period: time.Second}, LeaseOptions{LeaseSize: 10})
	r := req
	r.Limit, r.Window = 30, time.Second
	waitForWindowStart(time.Second)
	if got := allowN(t, l, r, 50); got != 30 {
		t.Fatalf("window 1 admitted %d, want 30", got)
	}
	waitForWindowStart(time.Second)
	if got := allowN(t, l, r, 50); got != 30 {
		t.Fatalf("window 2 admitted %d, want 30", got)
	}
}

// Regression: tokens leased in window N must not be spent in N+1.
func TestLease_DropsLeftoverTokensOnWindowRoll(t *testing.T) {
	l := NewLease(&windowedHub{quota: 30, period: time.Second}, LeaseOptions{LeaseSize: 20})
	r := req
	r.Limit, r.Window = 30, time.Second
	waitForWindowStart(time.Second)
	allowN(t, l, r, 1) // leases 20, spends 1
	waitForWindowStart(time.Second)
	if got := allowN(t, l, r, 100); got != 30 {
		t.Fatalf("next window admitted %d, want exactly 30", got)
	}
}

// Regression: counters were keyed by scope+key, charging one service's usage
// to another.
func TestLease_ServicesAreIsolated(t *testing.T) {
	l := NewLease(&windowedHub{quota: 1 << 30, period: time.Hour}, LeaseOptions{LeaseSize: 5})
	a := Request{Scope: "p", Service: "a", Key: "k", Limit: 5, Window: time.Hour}
	b := a
	b.Service = "b"
	if got := allowN(t, l, a, 10); got != 5 {
		t.Fatalf("service a admitted %d, want 5", got)
	}
	if got := allowN(t, l, b, 3); got != 3 {
		t.Fatalf("service b admitted %d after a was exhausted, want 3", got)
	}
}

func TestLease_HubOutageBacksOff(t *testing.T) {
	hub := &windowedHub{err: status.Error(codes.Unavailable, "hub down"), period: time.Second}
	l := NewLease(hub, LeaseOptions{})
	for i := 0; i < 100; i++ {
		if _, err := l.Allow(context.Background(), req); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("want ErrUnavailable, got %v", err)
		}
	}
	if n := hub.calls.Load(); n != 1 {
		t.Fatalf("hub called %d times during backoff, want 1", n)
	}
}

func TestLease_RejectionIsNotAnOutage(t *testing.T) {
	l := NewLease(&windowedHub{err: status.Error(codes.PermissionDenied, "no tier"), period: time.Second}, LeaseOptions{})
	if _, err := l.Allow(context.Background(), req); !errors.Is(err, ErrRejected) {
		t.Fatalf("want ErrRejected, got %v", err)
	}
}

type slowHub struct{ delay time.Duration }

func (h slowHub) RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	select {
	case <-time.After(h.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &types.QuotaLeaseResponse{GrantedTokens: in.RequestedTokens, LeaseTtlSeconds: 60}, nil
}

func TestLease_CancelledCallerDoesNotFailPeers(t *testing.T) {
	l := NewLease(slowHub{delay: 100 * time.Millisecond}, LeaseOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := l.Allow(ctx, req); done <- err }()
	time.Sleep(10 * time.Millisecond)
	peer := make(chan error, 1)
	go func() {
		d, err := l.Allow(context.Background(), req)
		if err == nil && !d.Allowed {
			err = errors.New("peer denied")
		}
		peer <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller: %v", err)
	}
	if err := <-peer; err != nil {
		t.Fatalf("peer failed because another caller cancelled: %v", err)
	}
}

// Regression: under contention peers could drain a refill before a waiter
// consumed, denying it although the hub had quota.
func TestLease_HotKeyContentionNeverDeniesWithQuota(t *testing.T) {
	l := NewLease(unlimitedHub{}, LeaseOptions{LeaseSize: 100})
	r := req
	r.Window = time.Hour
	var denied atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20000; i++ {
				if d, err := l.Allow(context.Background(), r); err != nil || !d.Allowed {
					denied.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if n := denied.Load(); n != 0 {
		t.Fatalf("%d requests denied despite unlimited hub quota", n)
	}
}

func TestLease_EvictsIdleKeys(t *testing.T) {
	l := NewLease(unlimitedHub{}, LeaseOptions{})
	allowN(t, l, req, 1)
	eventually(t, func() bool {
		l.counters.evict(func(c *leaseCounter) bool { return !c.renewing.Load() })
		return l.counters.len() == 0
	})
}

func TestLocal_EnforcesLimitPerWindow(t *testing.T) {
	l := NewLocal()
	r := Request{Scope: "p", Service: "s", Key: "k", Limit: 5, Window: time.Second}
	waitForWindowStart(time.Second)
	if got := allowN(t, l, r, 50); got != 5 {
		t.Fatalf("admitted %d, want 5", got)
	}
	waitForWindowStart(time.Second)
	if got := allowN(t, l, r, 1); got != 1 {
		t.Fatal("quota did not reset in the next window")
	}
}

func benchAllow(b *testing.B, keys int) {
	l := NewLease(unlimitedHub{}, LeaseOptions{LeaseSize: 1000})
	reqs := make([]Request, keys)
	for i := range reqs {
		reqs[i] = Request{Scope: "p", Service: "s", Key: "key" + strconv.Itoa(i), Limit: 1 << 40, Window: time.Hour}
	}
	var seq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(seq.Add(1)) % keys
			if d, err := l.Allow(context.Background(), reqs[i]); err != nil || !d.Allowed {
				b.Fatalf("denied: %+v %v", d, err)
			}
		}
	})
}

func BenchmarkLeaseHotKey(b *testing.B)   { benchAllow(b, 1) }
func BenchmarkLeaseManyKeys(b *testing.B) { benchAllow(b, 1000) }
