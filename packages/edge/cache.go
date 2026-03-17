package edge

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
)

type CacheManager struct {
	rdb          *redis.Client
	cfg          types.CacheConfig
	templatePlan []cacheTemplatePart
}

type cacheToken int

const (
	tokenLiteral cacheToken = iota
	tokenPath
	tokenQuery
	tokenAPIKey
	tokenEncoding
	tokenMethod
)

type cacheTemplatePart struct {
	token   cacheToken
	literal string
}

const edgeCacheRedisTimeout = 150 * time.Millisecond

func withCacheRedisTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.WithTimeout(context.Background(), edgeCacheRedisTimeout)
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, edgeCacheRedisTimeout)
}

type CachedResponse struct {
	StatusCode  int    `json:"status_code"`
	ContentType string `json:"content_type,omitempty"`
	Body        []byte `json:"body"`
}

func NewCacheManager(rdb *redis.Client, cfg types.CacheConfig) *CacheManager {
	return &CacheManager{
		rdb:          rdb,
		cfg:          cfg,
		templatePlan: compileCacheTemplate(cfg.CacheKey),
	}
}

// CheckCache retrieves cached response metadata from Redis.
func (cm *CacheManager) CheckCache(ctx context.Context, request *http.Request, apiKey string) (*CachedResponse, bool, error) {
	key := cm.generateCacheKey(request, apiKey)
	redisCtx, cancel := withCacheRedisTimeout(ctx)
	defer cancel()
	val, err := cm.rdb.Get(redisCtx, key).Bytes()
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
	redisCtx, cancel := withCacheRedisTimeout(ctx)
	defer cancel()
	return cm.rdb.Set(redisCtx, key, b, cm.cfg.TTL).Err()
}

func (cm *CacheManager) generateCacheKey(r *http.Request, apiKey string) string {
	h := fnv.New64a()
	path := r.URL.Path
	method := r.Method
	encoding := r.Header.Get("Accept-Encoding")
	var query string
	queryResolved := false
	for _, part := range cm.templatePlan {
		switch part.token {
		case tokenLiteral:
			_, _ = h.Write([]byte(part.literal))
		case tokenPath:
			_, _ = h.Write([]byte(path))
		case tokenMethod:
			_, _ = h.Write([]byte(method))
		case tokenEncoding:
			_, _ = h.Write([]byte(encoding))
		case tokenAPIKey:
			_, _ = h.Write([]byte(apiKey))
		case tokenQuery:
			if !queryResolved {
				// Canonicalized query preserves stable keys regardless of param ordering.
				query = r.URL.Query().Encode()
				queryResolved = true
			}
			_, _ = h.Write([]byte(query))
		}
	}
	var b strings.Builder
	b.Grow(3 + len(path) + 1 + 16)
	b.WriteString("c:")
	b.WriteString(path)
	b.WriteByte(':')
	b.WriteString(strconv.FormatUint(h.Sum64(), 16))
	return b.String()
}

func compileCacheTemplate(tpl string) []cacheTemplatePart {
	if tpl == "" {
		tpl = "$method:$path"
	}
	parts := make([]cacheTemplatePart, 0, 8)
	for len(tpl) > 0 {
		dollar := strings.IndexByte(tpl, '$')
		if dollar < 0 {
			parts = append(parts, cacheTemplatePart{token: tokenLiteral, literal: tpl})
			break
		}
		if dollar > 0 {
			parts = append(parts, cacheTemplatePart{token: tokenLiteral, literal: tpl[:dollar]})
			tpl = tpl[dollar:]
		}
		matched, token, size := matchCacheToken(tpl)
		if !matched {
			parts = append(parts, cacheTemplatePart{token: tokenLiteral, literal: "$"})
			tpl = tpl[1:]
			continue
		}
		parts = append(parts, cacheTemplatePart{token: token})
		tpl = tpl[size:]
	}
	return parts
}

func matchCacheToken(s string) (bool, cacheToken, int) {
	switch {
	case strings.HasPrefix(s, "$PATH"), strings.HasPrefix(s, "$path"):
		return true, tokenPath, 5
	case strings.HasPrefix(s, "$QUERY"), strings.HasPrefix(s, "$query"):
		return true, tokenQuery, 6
	case strings.HasPrefix(s, "$KEY"), strings.HasPrefix(s, "$key"):
		return true, tokenAPIKey, 4
	case strings.HasPrefix(s, "$ENC"), strings.HasPrefix(s, "$enc"):
		return true, tokenEncoding, 4
	case strings.HasPrefix(s, "$METHOD"), strings.HasPrefix(s, "$method"):
		return true, tokenMethod, 7
	default:
		return false, tokenLiteral, 0
	}
}

func InvalidateResponseCacheScopes(ctx context.Context, rdb *redis.Client, scopes []types.ConfigInvalidationScope) error {
	if rdb == nil || len(scopes) == 0 {
		return nil
	}
	for _, scope := range scopes {
		if strings.TrimSpace(scope.Prefix) == "" {
			continue
		}
		pattern := "c:/" + strings.TrimPrefix(scope.Prefix, "/")
		if scope.ServiceID != "" {
			pattern += "/" + scope.ServiceID
		}
		pattern += "*"
		if err := deleteKeysByPattern(ctx, rdb, pattern); err != nil {
			return err
		}
	}
	return nil
}

func deleteKeysByPattern(ctx context.Context, rdb *redis.Client, pattern string) error {
	var cursor uint64
	for {
		redisCtx, cancel := withCacheRedisTimeout(ctx)
		keys, next, err := rdb.Scan(redisCtx, cursor, pattern, 256).Result()
		cancel()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			redisCtx, delCancel := withCacheRedisTimeout(ctx)
			if err := rdb.Del(redisCtx, keys...).Err(); err != nil {
				delCancel()
				return err
			}
			delCancel()
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}
