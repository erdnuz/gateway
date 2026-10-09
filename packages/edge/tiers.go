package edge

import (
	"context"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	tierTTL        = time.Hour        // pushed changes keep known tiers current
	unknownKeyTTL  = 30 * time.Second // bounds what a flood of bogus keys costs
	resolveTimeout = 500 * time.Millisecond
	tierCacheKeys  = 1 << 20
)

// KeyResolver asks the hub for a key's tier ("" when the key is unknown).
type KeyResolver func(ctx context.Context, prefix, apiKey string) (string, error)

// TierCache maps API keys to tiers in process memory. Misses ask the hub once
// per key however many requests miss together, and the hub pushes changes
// (Set) so revocations apply without waiting for expiry. When the hub is
// unreachable, Stale serves the last known tier.
type TierCache struct {
	resolve KeyResolver
	cache   *ttlCache[tierEntry]
	gen     atomic.Uint64 // bumped by Reset; older entries need revalidating
	flights singleflight.Group
}

type tierEntry struct {
	tier string
	gen  uint64
}

func NewTierCache(resolve KeyResolver) *TierCache {
	return &TierCache{resolve: resolve, cache: newTTLCache[tierEntry](tierCacheKeys)}
}

func tierKey(prefix, apiKey string) string { return prefix + "\x00" + apiKey }

// Get returns the key's tier, asking the hub on a miss.
func (t *TierCache) Get(ctx context.Context, prefix, apiKey string) (string, error) {
	key := tierKey(prefix, apiKey)
	if e, _, fresh, ok := t.cache.get(key); ok && fresh && e.gen == t.gen.Load() {
		return e.tier, nil
	}
	ch := t.flights.DoChan(key, func() (any, error) {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), resolveTimeout)
		defer cancel()
		gen := t.gen.Load()
		tier, err := t.resolve(rctx, prefix, apiKey)
		if err != nil {
			return "", err
		}
		t.store(key, tier, gen)
		return tier, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return "", res.Err
		}
		return res.Val.(string), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (t *TierCache) store(key, tier string, gen uint64) {
	ttl := tierTTL
	if tier == "" {
		ttl = unknownKeyTTL
	}
	t.cache.set(key, tierEntry{tier: tier, gen: gen}, ttl, 1)
}

// Stale returns the last tier seen for a key if it is no older than maxAge.
func (t *TierCache) Stale(prefix, apiKey string, maxAge time.Duration) (string, bool) {
	e, storedAt, _, ok := t.cache.get(tierKey(prefix, apiKey))
	if !ok || e.tier == "" || (maxAge > 0 && time.Since(storedAt) > maxAge) {
		return "", false
	}
	return e.tier, true
}

// Set applies a tier change pushed by the hub ("" revokes the key).
func (t *TierCache) Set(prefix, apiKey, tier string) {
	t.store(tierKey(prefix, apiKey), tier, t.gen.Load())
}

// Reset marks every cached tier for revalidation (after a reconnect, when
// changes may have been missed). Entries stay usable as stale fallbacks.
func (t *TierCache) Reset() { t.gen.Add(1) }
