package edge

import (
	"hash/fnv"
	"sync"
	"time"
)

const cacheShards = 32

// ttlCache is a bounded in-process map with per-entry expiry. It is sharded
// so lock contention stays flat as cores and request rates grow. When a
// shard is over budget it evicts by sampling a few entries and preferring
// expired ones, which is cheap and good enough for caches that refill on
// demand. Expired entries stay readable (reported as not fresh) until
// evicted, so callers can fall back to stale data.
type ttlCache[V any] struct {
	shards    [cacheShards]cacheShard[V]
	shardCost int64 // budget per shard; cost is 1 per entry unless given
}

type cacheShard[V any] struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry[V]
	cost    int64
}

type cacheEntry[V any] struct {
	value    V
	cost     int64
	storedAt int64 // unix nanoseconds
	expires  int64 // unix nanoseconds
}

func newTTLCache[V any](budget int64) *ttlCache[V] {
	c := &ttlCache[V]{shardCost: max(budget/cacheShards, 1)}
	for i := range c.shards {
		c.shards[i].entries = map[string]cacheEntry[V]{}
	}
	return c
}

func (c *ttlCache[V]) shard(key string) *cacheShard[V] {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &c.shards[h.Sum32()%cacheShards]
}

// get returns the entry for key, whether it is still fresh, and when it was
// stored.
func (c *ttlCache[V]) get(key string) (v V, storedAt time.Time, fresh, ok bool) {
	s := c.shard(key)
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return v, time.Time{}, false, false
	}
	return e.value, time.Unix(0, e.storedAt), time.Now().UnixNano() < e.expires, true
}

func (c *ttlCache[V]) set(key string, v V, ttl time.Duration, cost int64) {
	cost = max(cost, 1)
	if cost > c.shardCost {
		return // larger than a whole shard: not worth caching
	}
	now := time.Now().UnixNano()
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.entries[key]; ok {
		s.cost -= old.cost
	}
	for s.cost+cost > c.shardCost && len(s.entries) > 0 {
		s.evictOne(now)
	}
	s.entries[key] = cacheEntry[V]{value: v, cost: cost, storedAt: now, expires: now + int64(ttl)}
	s.cost += cost
}

// evictOne removes an expired entry among a small sample, or else the first
// sampled entry (map iteration order is random).
func (s *cacheShard[V]) evictOne(now int64) {
	victim, n := "", 0
	for k, e := range s.entries {
		if victim == "" || e.expires <= now {
			victim = k
			if e.expires <= now {
				break
			}
		}
		if n++; n == 8 {
			break
		}
	}
	s.cost -= s.entries[victim].cost
	delete(s.entries, victim)
}

func (c *ttlCache[V]) delete(key string) {
	s := c.shard(key)
	s.mu.Lock()
	if e, ok := s.entries[key]; ok {
		s.cost -= e.cost
		delete(s.entries, key)
	}
	s.mu.Unlock()
}

// deleteFunc removes every entry whose key matches.
func (c *ttlCache[V]) deleteFunc(match func(key string) bool) {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		for k, e := range s.entries {
			if match(k) {
				s.cost -= e.cost
				delete(s.entries, k)
			}
		}
		s.mu.Unlock()
	}
}
