package ledger

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestConformance drives Mem and the Redis scripts (on miniredis) through
// the same random operations on the same clock and requires identical
// answers, so the simulation's ledger is the production ledger.
func TestConformance(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	red := NewRedis(rdb)

	clock := time.UnixMilli(1_760_000_000_123)
	mem := NewMem()
	mem.Now = func() time.Time { return clock }
	mr.SetTime(clock)

	specs := []Spec{
		{Algo: FixedWindow, Limit: 50, Window: time.Second},
		{Algo: FixedWindow, Limit: 7, Window: 3 * time.Second},
		{Algo: SlidingWindow, Limit: 40, Window: 2 * time.Second},
		{Algo: TokenBucket, Limit: 30, Rate: 12.5},
		{Algo: TokenBucket, Limit: 3, Rate: 0.7},
	}
	parent := &Spec{Algo: FixedWindow, Limit: 60, Window: 5 * time.Second}
	edges := []string{"", "e1", "e2", "e3"}
	rng := rand.New(rand.NewPCG(1, 2))
	ctx := context.Background()

	type held struct {
		ref  Ref
		spec Spec
		par  *Spec
		g    Grant
	}
	var grants []held

	for i := 0; i < 4000; i++ {
		clock = clock.Add(time.Duration(rng.IntN(120)) * time.Millisecond)
		mem.Now = func() time.Time { return clock }
		mr.SetTime(clock)

		spec := specs[rng.IntN(len(specs))]
		ref := Ref{Scope: "v1", Service: "svc", Subject: fmt.Sprintf("k%d", rng.IntN(3))}
		var par *Spec
		if spec.Algo == FixedWindow && rng.IntN(3) == 0 {
			ref.Parent, par = "acme", parent
		}

		if len(grants) > 0 && rng.IntN(5) == 0 {
			h := grants[rng.IntN(len(grants))]
			n := int64(rng.IntN(int(h.g.Granted)+3)) - 1 // occasionally negative (debt)
			ret := []Return{{Ref: h.ref, Spec: h.spec, Parent: h.par, WindowID: h.g.WindowID, ParentWindowID: h.g.ParentWindowID, Tokens: n}}
			dedupe := ""
			if rng.IntN(2) == 0 {
				dedupe = fmt.Sprintf("d%d", rng.IntN(20))
			}
			if err := mem.Return(ctx, dedupe, ret); err != nil {
				t.Fatal(err)
			}
			if err := red.Return(ctx, dedupe, ret); err != nil {
				t.Fatal(err)
			}
			continue
		}

		args := GrantArgs{Requested: int64(1 + rng.IntN(15)), EdgeID: edges[rng.IntN(len(edges))], Parent: par}
		if rng.IntN(2) == 0 {
			args.Horizon = time.Duration(200+rng.IntN(1000)) * time.Millisecond
			args.ObservedRate = rng.Float64() * 40
		}
		gm, err := mem.Grant(ctx, ref, spec, args)
		if err != nil {
			t.Fatal(err)
		}
		gr, err := red.Grant(ctx, ref, spec, args)
		if err != nil {
			t.Fatal(err)
		}
		if gm != gr {
			t.Fatalf("op %d %s %+v: mem %+v, redis %+v", i, spec.Algo, args, gm, gr)
		}
		if gm.Granted > 0 {
			grants = append(grants, held{ref, spec, par, gm})
		}
	}
}

func TestGrantBatchMatchesSingleGrants(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	red := NewRedis(rdb)
	mr.SetTime(time.UnixMilli(1_760_000_000_000))
	spec := Spec{Algo: FixedWindow, Limit: 10, Window: time.Minute}
	var items []BatchItem
	for i := 0; i < 6; i++ {
		items = append(items, BatchItem{Ref: Ref{Scope: "v1", Service: "s", Subject: "k"}, Spec: spec, Args: GrantArgs{Requested: 3}})
	}
	items = append(items, BatchItem{Spec: Spec{Limit: 0}}) // invalid
	got := GrantAll(context.Background(), red, items)
	var total int64
	for i, r := range got[:6] {
		if r.Err != nil {
			t.Fatalf("item %d: %v", i, r.Err)
		}
		total += r.Grant.Granted
	}
	if total != 10 {
		t.Fatalf("batch granted %d of a quota of 10", total)
	}
	if got[6].Err == nil {
		t.Fatal("invalid spec in a batch was not reported")
	}
}

func TestFixedWindowNeverExceedsQuota(t *testing.T) {
	clock := time.UnixMilli(1_760_000_000_000)
	m := NewMem()
	m.Now = func() time.Time { return clock }
	spec := Spec{Algo: FixedWindow, Limit: 100, Window: time.Second}
	ref := Ref{Scope: "v1", Service: "s", Subject: "k"}
	perWindow := map[int64]int64{}
	for i := 0; i < 5000; i++ {
		clock = clock.Add(time.Millisecond)
		g, err := m.Grant(context.Background(), ref, spec, GrantArgs{Requested: 7, EdgeID: fmt.Sprintf("e%d", i%4)})
		if err != nil {
			t.Fatal(err)
		}
		perWindow[g.WindowID] += g.Granted
		if g.Granted > 0 && clock.Add(g.ExpiresIn).UnixMilli() > (g.WindowID+1)*1000 {
			t.Fatalf("tokens outlive their window: expires %v after window %d", g.ExpiresIn, g.WindowID)
		}
	}
	for w, n := range perWindow {
		if n > 100 {
			t.Fatalf("window %d granted %d > 100", w, n)
		}
	}
}

func TestEndgameSplitsTheRemainder(t *testing.T) {
	clock := time.UnixMilli(1_760_000_000_000)
	m := NewMem()
	m.Now = func() time.Time { return clock }
	spec := Spec{Algo: FixedWindow, Limit: 1000, Window: time.Minute}
	ref := Ref{Scope: "v1", Service: "s", Subject: "k"}
	grant := func(edge string, want int64) int64 {
		g, err := m.Grant(context.Background(), ref, spec, GrantArgs{Requested: want, EdgeID: edge})
		if err != nil {
			t.Fatal(err)
		}
		return g.Granted
	}
	// Edges register once the quota is within 64 grants of running out.
	// With 100 of 1000 left, four edges ask; then one asks for 50: it gets
	// a quarter of what is left, not half of it.
	grant("", 900)
	for _, e := range []string{"a", "b", "c", "d"} {
		grant(e, 2)
	}
	if got := grant("a", 50); got != 23 {
		t.Fatalf("endgame grant = %d, want ceil(92/4) = 23", got)
	}
	// Quiet edges age out of the split.
	clock = clock.Add(3 * time.Second)
	if got := grant("a", 20); got != 20 {
		t.Fatalf("after peers went quiet, grant = %d, want 20", got)
	}
}

func TestTokenBucketRefillsAtRate(t *testing.T) {
	clock := time.UnixMilli(1_760_000_000_000)
	m := NewMem()
	m.Now = func() time.Time { return clock }
	spec := Spec{Algo: TokenBucket, Limit: 10, Rate: 5} // burst 10, 5/s
	ref := Ref{Scope: "v1", Service: "s", Subject: "k"}
	g, _ := m.Grant(context.Background(), ref, spec, GrantArgs{Requested: 100})
	if g.Granted != 2 { // the banker cap: an ask above the burst gets 25% of it
		t.Fatalf("burst grant = %d, want 2", g.Granted)
	}
	var total int64
	for i := 0; i < 2000; i++ { // 20 s
		clock = clock.Add(10 * time.Millisecond)
		g, _ := m.Grant(context.Background(), ref, spec, GrantArgs{Requested: 1})
		total += g.Granted
	}
	// 20 s at 5/s plus the remaining burst, within rounding.
	if total < 100 || total > 109 {
		t.Fatalf("granted %d over 20s, want about 100 + burst", total)
	}
}

func TestReturnCreditsCurrentWindowOnce(t *testing.T) {
	clock := time.UnixMilli(1_760_000_000_000)
	m := NewMem()
	m.Now = func() time.Time { return clock }
	spec := Spec{Algo: FixedWindow, Limit: 10, Window: time.Minute}
	ref := Ref{Scope: "v1", Service: "s", Subject: "k"}
	g, _ := m.Grant(context.Background(), ref, spec, GrantArgs{Requested: 10})
	if g.Granted != 10 { // the banker cap applies only above the whole quota
		t.Fatalf("granted %d, want 10", g.Granted)
	}
	ret := []Return{{Ref: ref, Spec: spec, WindowID: g.WindowID, Tokens: 4}}
	_ = m.Return(context.Background(), "x", ret)
	_ = m.Return(context.Background(), "x", ret) // retried: applied once
	if used := m.Used(ref, spec); used != 6 {
		t.Fatalf("used after return = %d, want 6", used)
	}
	// A return for a window that has ended is discarded.
	clock = clock.Add(time.Minute)
	_ = m.Return(context.Background(), "y", ret)
	if used := m.Used(ref, spec); used != 0 {
		t.Fatalf("stale return touched the new window: used %d", used)
	}
}

// TestConformanceRegional keeps regional grants, Lend and Credit identical
// between Mem and the Lua.
func TestConformanceRegional(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	red := NewRedis(rdb)
	clock := time.UnixMilli(1_760_000_000_321)
	mem := NewMem()
	mem.Now = func() time.Time { return clock }
	mr.SetTime(clock)
	spec := Spec{Algo: FixedWindow, Limit: 100, Window: 2 * time.Second}
	rng := rand.New(rand.NewPCG(7, 8))
	ctx := context.Background()
	for i := 0; i < 2000; i++ {
		clock = clock.Add(time.Duration(rng.IntN(50)) * time.Millisecond)
		mr.SetTime(clock)
		ref := Ref{Scope: "v1", Service: "svc", Subject: fmt.Sprintf("k%d", rng.IntN(2))}
		wid := WindowOf(spec, clock) - int64(rng.IntN(2)) // sometimes a past window
		id := fmt.Sprintf("t%d", rng.IntN(300))
		switch rng.IntN(3) {
		case 0:
			args := GrantArgs{Requested: int64(1 + rng.IntN(20)), Regional: true, AllocBase: int64(rng.IntN(60))}
			a, err1 := mem.Grant(ctx, ref, spec, args)
			b, err2 := red.Grant(ctx, ref, spec, args)
			if err1 != nil || err2 != nil || a != b {
				t.Fatalf("op %d grant: mem %+v %v, redis %+v %v", i, a, err1, b, err2)
			}
		case 1:
			base, want, margin := int64(rng.IntN(60)), int64(1+rng.IntN(30)), int64(rng.IntN(10))
			a, err1 := mem.Lend(ctx, ref, spec, base, wid, want, margin, id)
			b, err2 := red.Lend(ctx, ref, spec, base, wid, want, margin, id)
			if err1 != nil || err2 != nil || a != b {
				t.Fatalf("op %d lend: mem %d %v, redis %d %v", i, a, err1, b, err2)
			}
		default:
			amount := int64(1 + rng.IntN(30))
			a, err1 := mem.Credit(ctx, ref, spec, wid, amount, id)
			b, err2 := red.Credit(ctx, ref, spec, wid, amount, id)
			if err1 != nil || err2 != nil || a != b {
				t.Fatalf("op %d credit: mem %v %v, redis %v %v", i, a, err1, b, err2)
			}
		}
	}
}
