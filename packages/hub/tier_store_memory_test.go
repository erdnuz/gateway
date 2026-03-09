package hub

import (
	"context"
	"testing"
)

func TestInMemoryTierStoreLifecycle(t *testing.T) {
	store := NewInMemoryTierStore()
	ctx := context.Background()

	tier, err := store.GetTier(ctx, "v1", "key1")
	if err != nil {
		t.Fatalf("GetTier unexpected err: %v", err)
	}
	if tier != "" {
		t.Fatalf("expected empty tier for missing key, got %q", tier)
	}

	if err := store.SetTier(ctx, "v1", "key1", "gold"); err != nil {
		t.Fatalf("SetTier unexpected err: %v", err)
	}

	tier, err = store.GetTier(ctx, "v1", "key1")
	if err != nil {
		t.Fatalf("GetTier unexpected err: %v", err)
	}
	if tier != "gold" {
		t.Fatalf("expected tier gold, got %q", tier)
	}

	if err := store.DeleteTier(ctx, "v1", "key1"); err != nil {
		t.Fatalf("DeleteTier unexpected err: %v", err)
	}

	tier, err = store.GetTier(ctx, "v1", "key1")
	if err != nil {
		t.Fatalf("GetTier unexpected err: %v", err)
	}
	if tier != "" {
		t.Fatalf("expected empty tier after delete, got %q", tier)
	}
}
