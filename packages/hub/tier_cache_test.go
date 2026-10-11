package hub

import (
	"context"
	"testing"
	"time"
)

type countingTiers struct {
	*InMemoryTierStore
	reads int
}

func (c *countingTiers) GetTier(ctx context.Context, prefix, key string) (string, error) {
	c.reads++
	return c.InMemoryTierStore.GetTier(ctx, prefix, key)
}

func TestCachedTiers_CachesUntilInvalidated(t *testing.T) {
	ctx := context.Background()
	inner := &countingTiers{InMemoryTierStore: NewInMemoryTierStore()}
	_ = inner.SetTier(ctx, "v1", "k", "pro")
	c := NewCachedTiers(inner, time.Minute)
	for i := 0; i < 3; i++ {
		if tier, _ := c.GetTier(ctx, "v1", "k"); tier != "pro" {
			t.Fatalf("tier %q", tier)
		}
	}
	if inner.reads != 1 {
		t.Fatalf("store read %d times, want 1 (then served from memory)", inner.reads)
	}
	// A change on another hub arrives as an invalidation.
	_ = inner.SetTier(ctx, "v1", "k", "free")
	c.Invalidate("v1", "k")
	if tier, _ := c.GetTier(ctx, "v1", "k"); tier != "free" {
		t.Fatalf("after invalidation: %q, want free", tier)
	}
	// Unknown keys are never cached: issuing one takes effect at once.
	if tier, _ := c.GetTier(ctx, "v1", "new"); tier != "" {
		t.Fatal(tier)
	}
	_ = inner.SetTier(ctx, "v1", "new", "pro")
	if tier, _ := c.GetTier(ctx, "v1", "new"); tier != "pro" {
		t.Fatalf("newly issued key: %q", tier)
	}
}

// A lookup that began before an invalidation must not store its answer.
func TestCachedTiers_InvalidationWinsARacingLookup(t *testing.T) {
	ctx := context.Background()
	inner := NewInMemoryTierStore()
	_ = inner.SetTier(ctx, "v1", "k", "pro")
	c := NewCachedTiers(inner, time.Minute)
	gen := c.gen.Load()                                                        // the lookup starts...
	c.Invalidate("v1", "k")                                                    // ...a change lands...
	c.store1(cacheKey("v1", "k"), gen, func(e *cachedTier) { e.tier = "pro" }) // ...then the stale answer
	if _, ok := c.lookup(cacheKey("v1", "k")); ok {
		t.Fatal("a stale lookup was cached after an invalidation")
	}
}
