package hub

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisTierStoreLifecycle(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis run: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := NewTierManager(rdb)
	ctx := context.Background()

	tier, err := store.GetTier(ctx, "v1", "k1")
	if err != nil {
		t.Fatalf("GetTier unexpected err: %v", err)
	}
	if tier != "" {
		t.Fatalf("expected empty tier, got %q", tier)
	}

	if err := store.SetTier(ctx, "v1", "k1", "pro"); err != nil {
		t.Fatalf("SetTier unexpected err: %v", err)
	}

	tier, err = store.GetTier(ctx, "v1", "k1")
	if err != nil {
		t.Fatalf("GetTier unexpected err: %v", err)
	}
	if tier != "pro" {
		t.Fatalf("expected tier pro, got %q", tier)
	}

	ttl := mr.TTL("hub:tier:v1:k1")
	if ttl != 0 {
		t.Fatalf("expected persistent key with no TTL, got ttl=%v", ttl)
	}

	if err := store.DeleteTier(ctx, "v1", "k1"); err != nil {
		t.Fatalf("DeleteTier unexpected err: %v", err)
	}

	tier, err = store.GetTier(ctx, "v1", "k1")
	if err != nil {
		t.Fatalf("GetTier unexpected err: %v", err)
	}
	if tier != "" {
		t.Fatalf("expected empty tier after delete, got %q", tier)
	}
}
