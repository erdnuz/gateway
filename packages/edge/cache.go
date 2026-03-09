package edge

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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

type CachedResponse struct {
	StatusCode  int    `json:"status_code"`
	ContentType string `json:"content_type,omitempty"`
	Body        []byte `json:"body"`
}

func NewCacheManager(rdb *redis.Client, cfg types.CacheConfig) *CacheManager {
	return &CacheManager{
		rdb: rdb,
		cfg: cfg,
	}
}

// CheckCache retrieves cached response metadata from Redis.
func (cm *CacheManager) CheckCache(ctx context.Context, request *http.Request, apiKey string) (*CachedResponse, bool, error) {
	key := cm.generateCacheKey(request, apiKey)
	val, err := cm.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var cached CachedResponse
	if err := json.Unmarshal(val, &cached); err != nil {
		// Backward compatibility with older cache format (raw body only).
		return &CachedResponse{StatusCode: http.StatusOK, ContentType: "application/json", Body: val}, true, nil
	}
	if len(cached.Body) == 0 && cached.StatusCode == 0 && cached.ContentType == "" {
		// Backward compatibility for valid JSON payloads that are not cache envelopes.
		return &CachedResponse{StatusCode: http.StatusOK, ContentType: "application/json", Body: val}, true, nil
	}
	if cached.StatusCode == 0 {
		cached.StatusCode = http.StatusOK
	}
	if cached.ContentType == "" {
		cached.ContentType = "application/json"
	}
	return &cached, true, nil
}

// SetCache stores the upstream response metadata using the configured TTL.
func (cm *CacheManager) SetCache(ctx context.Context, request *http.Request, apiKey string, cached *CachedResponse) error {
	key := cm.generateCacheKey(request, apiKey)
	if cached == nil {
		return nil
	}
	b, err := json.Marshal(cached)
	if err != nil {
		return err
	}
	return cm.rdb.Set(ctx, key, b, cm.cfg.TTL).Err()
}

func (cm *CacheManager) generateCacheKey(r *http.Request, apiKey string) string {
	// Canonicalize Query: Sort keys alphabetically so ?a=1&b=2 is same as ?b=2&a=1
	query := r.URL.Query().Encode()

	replacements := map[string]string{
		"$PATH":   r.URL.Path,
		"$path":   r.URL.Path,
		"$QUERY":  query,
		"$query":  query,
		"$KEY":    apiKey,
		"$key":    apiKey,
		"$ENC":    r.Header.Get("Accept-Encoding"),
		"$enc":    r.Header.Get("Accept-Encoding"),
		"$METHOD": r.Method,
		"$method": r.Method,
	}

	generated := cm.cfg.CacheKey
	for keyword, value := range replacements {
		generated = strings.ReplaceAll(generated, keyword, value)
	}

	hash := sha256.Sum256([]byte(generated))
	return fmt.Sprintf("c:%s:%x", r.URL.Path, hash[:16])
}
