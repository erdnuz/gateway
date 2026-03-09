package edge

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
)

type CacheManager struct {
	rdb *redis.Client
	cfg types.CacheConfig
}

func NewCacheManager(rdb *redis.Client, cfg types.CacheConfig) *CacheManager {
	return &CacheManager{
		rdb: rdb,
		cfg: cfg,
	}
}

// CheckCache retrieves data from Redis.
func (cm *CacheManager) CheckCache(ctx context.Context, request *http.Request, apiKey string) ([]byte, bool, error) {
	key := cm.generateCacheKey(request, apiKey)
	val, err := cm.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

// SetCache stores the upstream response bytes using the configured TTL.
func (cm *CacheManager) SetCache(ctx context.Context, request *http.Request, apiKey string, data []byte) error {
	key := cm.generateCacheKey(request, apiKey)
	return cm.rdb.Set(ctx, key, data, cm.cfg.TTL).Err()
}

func (cm *CacheManager) generateCacheKey(r *http.Request, apiKey string) string {
	// Canonicalize Query: Sort keys alphabetically so ?a=1&b=2 is same as ?b=2&a=1
	query := r.URL.Query().Encode()

	replacements := map[string]string{
		"$PATH":  r.URL.Path,
		"$QUERY": query,
		"$KEY":   apiKey,
		"$ENC":   r.Header.Get("Accept-Encoding"),
	}

	generated := cm.cfg.CacheKey
	for keyword, value := range replacements {
		generated = strings.ReplaceAll(generated, keyword, value)
	}

	hash := sha256.Sum256([]byte(generated))
	return fmt.Sprintf("c:%s:%x", r.URL.Path, hash[:16])
}
