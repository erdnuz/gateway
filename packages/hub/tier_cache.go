package hub

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// CachedTiers keeps key tiers and owners in hub memory, so a lease for an
// API key costs no Redis read for the key's tier. Every tier change reaches
// every hub over the control channel and invalidates the entry
// (Invalidate); the TTL bounds staleness if a change is ever missed.
type CachedTiers struct {
	store TierStore
	ttl   time.Duration
	max   int

	mu sync.RWMutex
	m  map[string]cachedTier
	// gen counts invalidations: a lookup that started before one does not
	// store its (possibly stale) answer.
	gen atomic.Uint64
}

type cachedTier struct {
	tier, owner string
	hasOwner    bool
	at          time.Time
}

// NewCachedTiers caches store's lookups for up to ttl.
func NewCachedTiers(store TierStore, ttl time.Duration) *CachedTiers {
	return &CachedTiers{store: store, ttl: ttl, max: 200_000, m: map[string]cachedTier{}}
}

func cacheKey(prefix, apiKey string) string { return prefix + "\x00" + apiKey }

func (c *CachedTiers) lookup(k string) (cachedTier, bool) {
	c.mu.RLock()
	e, ok := c.m[k]
	c.mu.RUnlock()
	return e, ok && time.Since(e.at) < c.ttl
}

func (c *CachedTiers) store1(k string, gen uint64, update func(*cachedTier)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen.Load() != gen {
		return
	}
	if len(c.m) >= c.max {
		c.m = map[string]cachedTier{} // crude but bounded
	}
	e := c.m[k]
	if time.Since(e.at) >= c.ttl {
		e = cachedTier{}
	}
	update(&e)
	e.at = time.Now()
	c.m[k] = e
}

func (c *CachedTiers) GetTier(ctx context.Context, prefix, apiKey string) (string, error) {
	k := cacheKey(prefix, apiKey)
	if e, ok := c.lookup(k); ok && e.tier != "" {
		return e.tier, nil
	}
	gen := c.gen.Load()
	tier, err := c.store.GetTier(ctx, prefix, apiKey)
	if err == nil && tier != "" { // unknown keys are not cached: issuing one must take effect at once
		c.store1(k, gen, func(e *cachedTier) { e.tier = tier })
	}
	return tier, err
}

// GetOwner returns the key's owner, cached like its tier.
func (c *CachedTiers) GetOwner(ctx context.Context, prefix, apiKey string) (string, error) {
	owners, ok := c.store.(OwnerStore)
	if !ok {
		return "", nil
	}
	k := cacheKey(prefix, apiKey)
	if e, ok := c.lookup(k); ok && e.hasOwner {
		return e.owner, nil
	}
	gen := c.gen.Load()
	owner, err := owners.GetOwner(ctx, prefix, apiKey)
	if err == nil {
		c.store1(k, gen, func(e *cachedTier) { e.owner, e.hasOwner = owner, true })
	}
	return owner, err
}

func (c *CachedTiers) SetTier(ctx context.Context, prefix, apiKey, tierID string) error {
	defer c.Invalidate(prefix, apiKey)
	return c.store.SetTier(ctx, prefix, apiKey, tierID)
}

func (c *CachedTiers) DeleteTier(ctx context.Context, prefix, apiKey string) error {
	defer c.Invalidate(prefix, apiKey)
	return c.store.DeleteTier(ctx, prefix, apiKey)
}

func (c *CachedTiers) SetOwner(ctx context.Context, prefix, apiKey, owner string) error {
	defer c.Invalidate(prefix, apiKey)
	if owners, ok := c.store.(OwnerStore); ok {
		return owners.SetOwner(ctx, prefix, apiKey, owner)
	}
	return nil
}

// Invalidate forgets a key, after a tier or owner change on any hub.
func (c *CachedTiers) Invalidate(prefix, apiKey string) {
	c.gen.Add(1)
	c.mu.Lock()
	delete(c.m, cacheKey(prefix, apiKey))
	c.mu.Unlock()
}
