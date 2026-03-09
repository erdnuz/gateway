package edge

import (
	"context"
	"gateway/packages/common/types"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestTierManager_AppliesTierUpdatePayload(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	tm := NewTierManager("", rdb, types.DefaultHubUpdatesChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// pre-populate tier cache for two users
	key1 := tm.getCacheKey("api/v1", "user1")
	key2 := tm.getCacheKey("api/v1", "user2")

	if err := rdb.Set(ctx, key1, "premium", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, key2, "free", 0).Err(); err != nil {
		t.Fatal(err)
	}

	// verify both keys exist before update
	if _, err := rdb.Get(ctx, key1).Result(); err != nil {
		t.Fatal("expected tier key1 to exist before update")
	}

	// apply a tier update for user1 only
	tm.applyTierUpdatePayload(ctx, "TIER_UPDATE:api/v1:user1:enterprise")

	// user1 cache entry should be updated
	if val, err := rdb.Get(ctx, key1).Result(); err != nil || val != "enterprise" {
		t.Errorf("expected tier key1 to be updated, got val=%q err=%v", val, err)
	}

	// user2 cache entry must remain untouched
	if val, err := rdb.Get(ctx, key2).Result(); err != nil || val != "free" {
		t.Errorf("expected tier key2 to be untouched, got val=%q err=%v", val, err)
	}
}

func TestTierManager_IgnoresNonTierUpdatePayload(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	tm := NewTierManager("", rdb, types.DefaultHubUpdatesChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheKey := tm.getCacheKey("api/v1", "user1")
	if err := rdb.Set(ctx, cacheKey, "premium", 0).Err(); err != nil {
		t.Fatal(err)
	}

	// unrecognised payload must not affect cache
	tm.applyTierUpdatePayload(ctx, "RELOAD:all")

	if val, err := rdb.Get(ctx, cacheKey).Result(); err != nil || val != "premium" {
		t.Errorf("expected cache to be untouched, got val=%q err=%v", val, err)
	}
}

func TestTierManager_EmptyTierDeletesCache(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	tm := NewTierManager("", rdb, types.DefaultHubUpdatesChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheKey := tm.getCacheKey("api/v1", "user1")
	if err := rdb.Set(ctx, cacheKey, "premium", 0).Err(); err != nil {
		t.Fatal(err)
	}

	tm.applyTierUpdatePayload(ctx, "TIER_UPDATE:api/v1:user1:")

	if err := rdb.Get(ctx, cacheKey).Err(); err != redis.Nil {
		t.Errorf("expected tier key to be deleted on empty update, got err=%v", err)
	}
}
