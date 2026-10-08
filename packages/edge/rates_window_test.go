package edge

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// windowedLeaseClient grants up to quota tokens per epoch-aligned window,
// mimicking the hub's lease-used accounting.
type windowedLeaseClient struct {
	mu     sync.Mutex
	quota  int64
	period time.Duration
	used   map[int64]int64
	calls  atomic.Int64
	err    error
}

func (c *windowedLeaseClient) RequestQuotaLease(_ context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	c.calls.Add(1)
	if c.err != nil {
		return nil, c.err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	wid := time.Now().Unix() / int64(c.period/time.Second)
	if c.used == nil {
		c.used = map[int64]int64{}
	}
	left := c.quota - c.used[wid]
	grant := min(in.RequestedTokens, left)
	if grant <= 0 {
		return &types.QuotaLeaseResponse{WaitingForCapacity: true, RetryAfterMs: 50}, nil
	}
	c.used[wid] += grant
	return &types.QuotaLeaseResponse{GrantedTokens: grant, RemainingGlobal: c.quota - c.used[wid], LeaseTtlSeconds: int64(c.period / time.Second)}, nil
}

func (c *windowedLeaseClient) Close() error { return nil }

func countAllowed(t *testing.T, rm *RateManager, req RateRequest, n int) int {
	t.Helper()
	ok := 0
	for i := 0; i < n; i++ {
		d, err := rm.Allow(context.Background(), req)
		if err != nil {
			t.Fatalf("allow: %v", err)
		}
		if d.Allowed {
			ok++
		}
	}
	return ok
}

// Regression for C1: usage used to be a lifetime counter, so a key that hit
// its quota once was denied forever.
func TestAllow_QuotaResetsEachWindow(t *testing.T) {
	rm := NewRateManagerWithOptions(nil, RateManagerOptions{LeaseSize: 10, MinTokens: -1})
	client := &windowedLeaseClient{quota: 30, period: time.Second}
	rm.SetLeaseClient(client)
	req := RateRequest{Prefix: "p", Service: "s", APIKey: "k", Limit: 30, Period: time.Second}

	waitForWindowStart(time.Second)
	if got := countAllowed(t, rm, req, 50); got != 30 {
		t.Fatalf("window 1 admitted %d, want 30", got)
	}
	waitForWindowStart(time.Second)
	if got := countAllowed(t, rm, req, 50); got != 30 {
		t.Fatalf("window 2 admitted %d, want 30 (quota must reset)", got)
	}
}

// Regression for H3: tokens leased in window N must not be spent in N+1.
func TestAllow_DropsLeftoverTokensOnWindowRoll(t *testing.T) {
	rm := NewRateManagerWithOptions(nil, RateManagerOptions{LeaseSize: 20, MinTokens: -1})
	client := &windowedLeaseClient{quota: 30, period: time.Second}
	rm.SetLeaseClient(client)
	req := RateRequest{Prefix: "p", Service: "s", APIKey: "k", Limit: 30, Period: time.Second}

	waitForWindowStart(time.Second)
	if got := countAllowed(t, rm, req, 1); got != 1 { // leases 20, uses 1
		t.Fatalf("warm-up admitted %d", got)
	}
	waitForWindowStart(time.Second)
	if got := countAllowed(t, rm, req, 100); got != 30 {
		t.Fatalf("next window admitted %d, want exactly 30 (no carry-over)", got)
	}
}

// Regression for C4: counters were keyed by prefix+key, so one service's
// usage was charged to another.
func TestAllow_ServicesAreIsolated(t *testing.T) {
	rm := NewRateManagerWithOptions(nil, RateManagerOptions{LeaseSize: 5, MinTokens: -1})
	rm.SetLeaseClient(&windowedLeaseClient{quota: 1 << 30, period: time.Hour})
	a := RateRequest{Prefix: "p", Service: "a", APIKey: "k", Limit: 5, Period: time.Hour}
	b := RateRequest{Prefix: "p", Service: "b", APIKey: "k", Limit: 5, Period: time.Hour}
	if got := countAllowed(t, rm, a, 10); got != 5 {
		t.Fatalf("service a admitted %d, want 5", got)
	}
	if got := countAllowed(t, rm, b, 3); got != 3 {
		t.Fatalf("service b admitted %d after a exhausted, want 3", got)
	}
}

// Regression for C2: the safety tier refilled whenever it hit zero.
func TestAllowSafety_EnforcesLimitPerWindow(t *testing.T) {
	rm := NewRateManagerWithOptions(nil, DefaultRateManagerOptions())
	req := RateRequest{Prefix: "p", Service: "s", APIKey: "k", Limit: 5, Period: time.Second}
	waitForWindowStart(time.Second)
	ok := 0
	for i := 0; i < 50; i++ {
		if rm.AllowSafety(req).Allowed {
			ok++
		}
	}
	if ok != 5 {
		t.Fatalf("safety admitted %d, want 5", ok)
	}
	waitForWindowStart(time.Second)
	if !rm.AllowSafety(req).Allowed {
		t.Fatal("safety quota did not reset in the next window")
	}
	if usage, _ := rm.IncrementSafety(context.Background(), "x", "k", 2, 1); usage > 2 {
		t.Fatalf("legacy IncrementSafety denied first request: %d", usage)
	}
}

func TestAllow_HubOutageBacksOffAndReportsUnavailable(t *testing.T) {
	rm := NewRateManagerWithOptions(nil, DefaultRateManagerOptions())
	client := &windowedLeaseClient{err: status.Error(codes.Unavailable, "hub down"), period: time.Second}
	rm.SetLeaseClient(client)
	req := RateRequest{Prefix: "p", Service: "s", APIKey: "k", Limit: 10}
	for i := 0; i < 100; i++ {
		_, err := rm.Allow(context.Background(), req)
		if !errors.Is(err, ErrLeaseUnavailable) {
			t.Fatalf("want ErrLeaseUnavailable, got %v", err)
		}
	}
	if c := client.calls.Load(); c != 1 {
		t.Fatalf("hub called %d times during backoff, want 1", c)
	}
}

func TestAllow_HubRejectionIsNotAnOutage(t *testing.T) {
	rm := NewRateManagerWithOptions(nil, DefaultRateManagerOptions())
	rm.SetLeaseClient(&windowedLeaseClient{err: status.Error(codes.PermissionDenied, "no tier"), period: time.Second})
	_, err := rm.Allow(context.Background(), RateRequest{Prefix: "p", Service: "s", APIKey: "k", Limit: 10})
	if !errors.Is(err, ErrLeaseRejected) {
		t.Fatalf("want ErrLeaseRejected, got %v", err)
	}
}

func TestAllow_CancelledCallerDoesNotFailPeers(t *testing.T) {
	rm := NewRateManagerWithOptions(nil, DefaultRateManagerOptions())
	slow := &slowLeaseClient{delay: 100 * time.Millisecond}
	rm.SetLeaseClient(slow)
	req := RateRequest{Prefix: "p", Service: "s", APIKey: "k", Limit: 100}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rm.Allow(ctx, req)
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	peer := make(chan error, 1)
	go func() {
		d, err := rm.Allow(context.Background(), req)
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

type slowLeaseClient struct{ delay time.Duration }

func (c *slowLeaseClient) RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &types.QuotaLeaseResponse{GrantedTokens: in.RequestedTokens, LeaseTtlSeconds: 60}, nil
}

func (c *slowLeaseClient) Close() error { return nil }

// Regression: under contention a refill could be drained by peers before a
// waiter consumed, and the waiter was denied although the hub had quota.
func TestAllow_HotKeyContentionNeverDeniesWithQuota(t *testing.T) {
	rm := NewRateManagerWithOptions(nil, RateManagerOptions{LeaseSize: 100})
	rm.SetLeaseClient(unlimitedLeaseClient{})
	req := RateRequest{Prefix: "p", Service: "s", APIKey: "hot", Limit: 1 << 40, Period: time.Hour}
	var denied atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20000; i++ {
				if d, err := rm.Allow(context.Background(), req); err != nil || !d.Allowed {
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

// Regression: a WAITING answer to a low-water renewal used to strand the
// tokens the edge still held (each edge lost its low-water mark per window).
func TestAllow_WaitingStillSpendsHeldTokens(t *testing.T) {
	client := &windowedLeaseClient{quota: 1 << 30, period: time.Hour}
	rm := NewRateManagerWithOptions(nil, RateManagerOptions{LeaseSize: 10})
	rm.SetLeaseClient(client)
	req := RateRequest{Prefix: "p", Service: "s", APIKey: "k", Limit: 100, Period: time.Hour}
	if d, _ := rm.Allow(context.Background(), req); !d.Allowed {
		t.Fatal("first request denied")
	}
	c := rm.getLeaseCounter("p", "s", "k")
	held := c.remaining.Load()
	c.state.Store(int32(leaseStateWaiting))
	c.nextRetryUnixMs.Store(time.Now().Add(time.Hour).UnixMilli())
	calls := client.calls.Load()
	for i := int64(0); i < held; i++ {
		if d, err := rm.Allow(context.Background(), req); err != nil || !d.Allowed {
			t.Fatalf("held token %d/%d not spendable while WAITING: %+v %v", i+1, held, d, err)
		}
	}
	if d, _ := rm.Allow(context.Background(), req); d.Allowed {
		t.Fatal("admitted beyond held tokens while WAITING")
	}
	if client.calls.Load() != calls {
		t.Fatal("hub was called while WAITING")
	}
}

func TestEvictIdle_RemovesQuietCounters(t *testing.T) {
	rm := NewRateManagerWithOptions(nil, DefaultRateManagerOptions())
	rm.SetLeaseClient(&windowedLeaseClient{quota: 100, period: time.Hour})
	_, _ = rm.Allow(context.Background(), RateRequest{Prefix: "p", Service: "s", APIKey: "k", Limit: 10})
	rm.evictIdle(time.Now().Add(time.Second))
	n := 0
	rm.leaseMap.Range(func(any, any) bool { n++; return true })
	if n != 0 {
		t.Fatalf("expected idle counter evicted, %d remain", n)
	}
}

func waitForWindowStart(period time.Duration) {
	p := int64(period)
	now := time.Now().UnixNano()
	time.Sleep(time.Duration((now/p+1)*p-now) + 5*time.Millisecond)
}

// --- benchmarks -----------------------------------------------------------

type unlimitedLeaseClient struct{}

func (unlimitedLeaseClient) RequestQuotaLease(_ context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	return &types.QuotaLeaseResponse{GrantedTokens: in.RequestedTokens, RemainingGlobal: 1 << 40, LeaseTtlSeconds: 3600}, nil
}
func (unlimitedLeaseClient) Close() error { return nil }

// Redis-store throughput is measured end to end (testing/loadtest with
// EDGE_LEASE_STORE=redis); miniredis runs Lua in-process and is not
// representative of a real Redis round trip.
func benchAllow(b *testing.B, rdb *redis.Client, keys int) {
	rm := NewRateManagerWithOptions(rdb, RateManagerOptions{LeaseSize: 1000})
	rm.SetLeaseClient(unlimitedLeaseClient{})
	ctx := context.Background()
	reqs := make([]RateRequest, keys)
	for i := range reqs {
		reqs[i] = RateRequest{Prefix: "p", Service: "s", APIKey: "key" + itoa(i), Limit: 1 << 40, Period: time.Hour}
	}
	var seq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(seq.Add(1)) % keys
			if d, err := rm.Allow(ctx, reqs[i]); err != nil || !d.Allowed {
				b.Fatalf("denied: %+v %v", d, err)
			}
		}
	})
}

func BenchmarkAllowMemoryHotKey(b *testing.B)   { benchAllow(b, nil, 1) }
func BenchmarkAllowMemoryManyKeys(b *testing.B) { benchAllow(b, nil, 1000) }
