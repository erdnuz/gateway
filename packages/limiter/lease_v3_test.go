package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gateway/packages/common/types"
)

// The hub's clock defines windows. With the hub's clock 400 ms ahead of or
// behind the edges', no hub window ever admits more than its quota: tokens
// expire with the hub window they were granted in, not the edge's.
func TestLease_HubClockSkewNeverOvershoots(t *testing.T) {
	for _, skew := range []time.Duration{-400 * time.Millisecond, 400 * time.Millisecond} {
		t.Run(skew.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				hub := &windowedHub{quota: 100, period: time.Second, skew: skew}
				edges := []*Lease{NewLease(hub, LeaseOptions{LeaseSize: 10}), NewLease(hub, LeaseOptions{LeaseSize: 10})}
				r := req
				r.Limit, r.Window = 100, time.Second
				perWindow := map[int64]int{}
				for step := 0; step < 5000; step++ {
					for _, l := range edges {
						hubWindow := time.Now().Add(skew).UnixMilli() / 1000
						if d, err := l.Allow(context.Background(), r); err == nil && d.Allowed {
							perWindow[hubWindow]++
						}
					}
					time.Sleep(time.Millisecond)
				}
				total := 0
				for w, n := range perWindow {
					total += n
					if n > 100 {
						t.Errorf("hub window %d admitted %d > quota 100", w, n)
					}
				}
				if total < 400 { // five windows at 2x demand: nearly all quota used
					t.Errorf("admitted %d over 5 windows, want >= 400", total)
				}
			})
		})
	}
}

func TestReserve_RollsBackWhenALaterLimitDenies(t *testing.T) {
	l := NewLocal()
	a := Request{Scope: "p", Service: "s", LimitID: "a", Key: "k", Limit: 2, Window: time.Hour}
	b := Request{Scope: "p", Service: "s", LimitID: "b", Key: "k", Limit: 1, Window: time.Hour}
	ctx := context.Background()
	if r, _ := Reserve(ctx, l, []Request{a, b}); !r.Allowed {
		t.Fatal("first request denied")
	}
	r, _ := Reserve(ctx, l, []Request{a, b})
	if r.Allowed || r.BindingIndex != 1 {
		t.Fatalf("second request: allowed=%v binding=%d, want denied by b", r.Allowed, r.BindingIndex)
	}
	// a was charged and refunded, so it still has one left.
	if r, _ := Reserve(ctx, l, []Request{a}); !r.Allowed {
		t.Fatal("a's charge for the denied request was not rolled back")
	}
}

func TestReserve_ShadowLimitsNeverDeny(t *testing.T) {
	l := NewLocal()
	enforced := Request{Scope: "p", Service: "s", Key: "k", Limit: 100, Window: time.Hour}
	shadow := Request{Scope: "p", Service: "s", LimitID: "dry", Key: "k", Limit: 1, Window: time.Hour, Shadow: true}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		r, err := Reserve(ctx, l, []Request{enforced, shadow})
		if err != nil || !r.Allowed {
			t.Fatalf("request %d denied by a shadow limit", i)
		}
		if want := i > 0; (len(r.ShadowDenied) == 1) != want {
			t.Fatalf("request %d: shadow denials %v", i, r.ShadowDenied)
		}
		if r.BindingIndex != 0 {
			t.Fatalf("a shadow limit was reported as binding")
		}
	}
}

func TestReservation_SettleRefundsAndCharges(t *testing.T) {
	l := NewLocal()
	r := Request{Scope: "p", Service: "s", Key: "k", Limit: 10, Window: time.Hour, Cost: 5}
	ctx := context.Background()
	res, _ := Reserve(ctx, l, []Request{r})
	res.Settle(2) // reserved 5, used 2
	res.Settle(9) // only the first settle counts
	r.Cost = 8
	if res, _ := Reserve(ctx, l, []Request{r}); !res.Allowed {
		t.Fatal("the refund from settling 5 down to 2 was not applied")
	}
}

func TestLease_SettleOverdrawsIntoBoundedDebt(t *testing.T) {
	hub := &scriptedHub{grants: []int64{10}}
	l := NewLease(hub, LeaseOptions{LeaseSize: 10})
	r := req
	r.Cost, r.MaxDebt = 5, 4
	res, _ := Reserve(context.Background(), l, []Request{r})
	if !res.Allowed {
		t.Fatal("denied")
	}
	res.Settle(20) // 10 granted, 20 used: 10 over, capped at 4 of debt
	if n, _ := l.Holding("p", "s", "k"); n != -4 {
		t.Fatalf("balance after overdraw = %d, want -4 (max debt)", n)
	}
	r.Cost = 1
	if d, _ := l.Allow(context.Background(), r); d.Allowed {
		t.Fatal("admitted while in debt with no new grant")
	}
}

func TestLease_DrainReturnsUnspentTokens(t *testing.T) {
	hub := &windowedHub{quota: 1000, period: time.Hour}
	l := NewLease(hub, LeaseOptions{LeaseSize: 10, EdgeID: "e1"})
	r := req
	r.Limit, r.Window = 1000, time.Hour
	allowN(t, l, r, 3)
	if err := l.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(hub.returned) != 1 || hub.returned[0].Tokens != 7 || hub.returned[0].WindowId == 0 {
		t.Fatalf("returned %+v, want 7 tokens tagged with the hub window", hub.returned)
	}
	if n, _ := l.Holding("p", "s", "k"); n != 0 {
		t.Fatalf("still holding %d after drain", n)
	}
}

func TestLease_ReportsObservedDemand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hub := &windowedHub{quota: 1 << 30, period: time.Hour}
		l := NewLease(hub, LeaseOptions{LeaseSize: 20, EdgeID: "e1"})
		r := req
		r.Window = time.Hour
		for i := 0; i < 1000; i++ { // 100 per second for 10 s
			allowN(t, l, r, 1)
			time.Sleep(10 * time.Millisecond)
		}
		synctest.Wait()
		hub.mu.Lock()
		last := hub.reqs[len(hub.reqs)-1]
		hub.mu.Unlock()
		if last.EdgeId != "e1" || last.ObservedRate < 70 || last.ObservedRate > 130 {
			t.Fatalf("reported edge %q rate %.1f, want e1 at about 100/s", last.EdgeId, last.ObservedRate)
		}
	})
}

// gatedBatchHub blocks its first batch until released, counting batches.
type gatedBatchHub struct {
	unlimitedHub
	gate    chan struct{}
	batches atomic.Int64
	items   atomic.Int64
	once    sync.Once
}

func (h *gatedBatchHub) RequestQuotaLeases(ctx context.Context, in *types.QuotaLeaseBatchRequest) (*types.QuotaLeaseBatchResponse, error) {
	h.batches.Add(1)
	h.items.Add(int64(len(in.Requests)))
	h.once.Do(func() { <-h.gate })
	out := &types.QuotaLeaseBatchResponse{}
	for _, r := range in.Requests {
		resp, _ := h.RequestQuotaLease(ctx, r)
		out.Results = append(out.Results, &types.QuotaLeaseResult{Response: resp})
	}
	return out, nil
}

func TestBatcher_CoalescesConcurrentLeases(t *testing.T) {
	hub := &gatedBatchHub{gate: make(chan struct{})}
	b := NewBatcher(hub)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := b.RequestQuotaLease(context.Background(), &types.QuotaLeaseRequest{Prefix: "p", ServiceId: "s", ApiKey: "k", RequestedTokens: 5})
			if err != nil || resp.GrantedTokens != 5 {
				t.Errorf("lease: %v %v", resp, err)
			}
		}()
	}
	eventually(t, func() bool { return hub.batches.Load() >= 1 })
	time.Sleep(50 * time.Millisecond) // let the rest queue behind the stalled batch
	close(hub.gate)
	wg.Wait()
	if n := hub.batches.Load(); n > 10 {
		t.Fatalf("100 concurrent leases took %d RPCs, want a few batches", n)
	}
	if hub.items.Load() != 100 {
		t.Fatalf("batched %d items, want 100", hub.items.Load())
	}
}
