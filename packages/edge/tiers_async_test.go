package edge

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestTierManager_HubUpdateInvalidation(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	tm := NewTierManager("", rdb)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := tm.StartHubUpdateListener(ctx); err != nil {
		t.Fatalf("StartHubUpdateListener: %v", err)
	}

	// pre-populate the tier cache for two users
	key1 := tm.getCacheKey("api/v1", "user1")
	key2 := tm.getCacheKey("api/v1", "user2")

	if err := rdb.Set(ctx, key1, "premium", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, key2, "free", 0).Err(); err != nil {
		t.Fatal(err)
	}

	// verify both keys exist before invalidation
	if _, err := rdb.Get(ctx, key1).Result(); err != nil {
		t.Fatal("expected tier key1 to exist before invalidation")
	}

	// publish an INVALIDATE message for user1 only
	if err := rdb.Publish(ctx, "hub_updates", "INVALIDATE:api/v1:user1").Err(); err != nil {
		t.Fatal(err)
	}

	// give the goroutine time to process the message
	time.Sleep(50 * time.Millisecond)

	// user1 cache entry should be gone
	if err := rdb.Get(ctx, key1).Err(); err != redis.Nil {
		t.Errorf("expected tier key1 to be invalidated, got err: %v", err)
	}

	// user2 cache entry must remain untouched
	if val, err := rdb.Get(ctx, key2).Result(); err != nil || val != "free" {
		t.Errorf("expected tier key2 to be untouched, got val=%q err=%v", val, err)
	}
}

func TestTierManager_HubUpdateIgnoresNonInvalidate(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	tm := NewTierManager("", rdb)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := tm.StartHubUpdateListener(ctx); err != nil {
		t.Fatalf("StartHubUpdateListener: %v", err)
	}

	cacheKey := tm.getCacheKey("api/v1", "user1")
	if err := rdb.Set(ctx, cacheKey, "premium", 0).Err(); err != nil {
		t.Fatal(err)
	}

	// publish a message with an unrecognised format — must not affect the cache
	if err := rdb.Publish(ctx, "hub_updates", "RELOAD:all").Err(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	if val, err := rdb.Get(ctx, cacheKey).Result(); err != nil || val != "premium" {
		t.Errorf("expected cache to be untouched, got val=%q err=%v", val, err)
	}
}
