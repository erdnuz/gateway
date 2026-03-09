package edge

import (
	"net/http/httptest"
	"testing"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
)

func TestGenerateCacheKeySupportsLowercasePlaceholders(t *testing.T) {
	cm := NewCacheManager(
		redis.NewClient(&redis.Options{Addr: "localhost:0"}),
		types.CacheConfig{Enabled: true, TTL: time.Minute, CacheKey: "$method:$path:$query:$key:$enc"},
	)

	reqA := httptest.NewRequest("GET", "http://example.local/v1/svc/resource?b=2&a=1", nil)
	reqA.Header.Set("Accept-Encoding", "gzip")
	reqB := httptest.NewRequest("POST", "http://example.local/v1/svc/resource?b=2&a=1", nil)
	reqB.Header.Set("Accept-Encoding", "gzip")

	keyA1 := cm.generateCacheKey(reqA, "test-key")
	keyA2 := cm.generateCacheKey(reqA, "test-key")
	keyB := cm.generateCacheKey(reqB, "test-key")

	if keyA1 != keyA2 {
		t.Fatalf("expected stable cache key generation, got %q and %q", keyA1, keyA2)
	}
	if keyA1 == keyB {
		t.Fatalf("expected method to influence cache key, got identical key %q", keyA1)
	}
}
