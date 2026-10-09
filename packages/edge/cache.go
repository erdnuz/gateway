package edge

import (
	"bytes"
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

// serveFromCache answers GETs from the edge's response cache, and stores
// cacheable upstream responses that fit the size bound. Hits are served
// before rate limiting, so they cost the key no quota.
func (s *Server) serveFromCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := stateOf(r)
		cfg := st.svc.Cache
		if r.Method != http.MethodGet || s.Redis == nil || cfg == nil || !cfg.Enabled {
			next.ServeHTTP(w, r)
			return
		}
		cm := NewCacheManager(s.Redis, *cfg)
		if cached, ok, err := cm.CheckCache(r.Context(), r, st.key); err == nil && ok {
			if st.entry != nil {
				st.entry.CacheHit = true
			}
			w.Header().Set("X-Cache", "HIT")
			if cached.ContentType != "" {
				w.Header().Set("Content-Type", cached.ContentType)
			}
			w.WriteHeader(cached.StatusCode)
			_, _ = w.Write(cached.Body)
			return
		}
		w.Header().Set("X-Cache", "MISS")
		cw := &cacheWriter{ResponseWriter: w, limit: s.opts.MaxCacheableBytes}
		next.ServeHTTP(cw, r)
		if cacheableStatus[cw.status] && !cw.overflow && !st.upstreamFailed {
			_ = cm.SetCache(r.Context(), r, st.key, &CachedResponse{
				StatusCode:  cw.status,
				ContentType: w.Header().Get("Content-Type"),
				Body:        cw.buf.Bytes(),
			})
		}
	})
}

// cacheableStatus lists the statuses RFC 9111 §4.2.2 allows caching by
// default.
var cacheableStatus = map[int]bool{200: true, 203: true, 204: true, 300: true, 301: true, 308: true, 404: true, 405: true, 410: true, 414: true, 501: true}

// cacheWriter streams the response to the client while keeping a copy of
// up to limit bytes.
type cacheWriter struct {
	http.ResponseWriter
	status   int
	buf      bytes.Buffer
	limit    int64
	overflow bool
}

func (w *cacheWriter) WriteHeader(code int) {
	if w.status == 0 && code >= 200 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *cacheWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if !w.overflow {
		if int64(w.buf.Len()+len(b)) > w.limit {
			w.overflow = true
			w.buf = bytes.Buffer{}
		} else {
			w.buf.Write(b)
		}
	}
	return w.ResponseWriter.Write(b)
}

func (w *cacheWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

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

const cacheRedisTimeout = 150 * time.Millisecond

type CachedResponse struct {
	StatusCode  int    `json:"status_code"`
	ContentType string `json:"content_type,omitempty"`
	Body        []byte `json:"body"`
}

func NewCacheManager(rdb *redis.Client, cfg types.CacheConfig) *CacheManager {
	return &CacheManager{rdb: rdb, cfg: cfg, templatePlan: compileCacheTemplate(cfg.CacheKey)}
}

// CheckCache looks up the cached response for a request.
func (cm *CacheManager) CheckCache(ctx context.Context, r *http.Request, apiKey string) (*CachedResponse, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, cacheRedisTimeout)
	defer cancel()
	val, err := cm.rdb.Get(ctx, cm.generateCacheKey(r, apiKey)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var cached CachedResponse
	if err := json.Unmarshal(val, &cached); err != nil || cached.StatusCode == 0 {
		return nil, false, err
	}
	return &cached, true, nil
}

// SetCache stores a response for the configured TTL.
func (cm *CacheManager) SetCache(ctx context.Context, r *http.Request, apiKey string, cached *CachedResponse) error {
	b, err := json.Marshal(cached)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, cacheRedisTimeout)
	defer cancel()
	return cm.rdb.Set(ctx, cm.generateCacheKey(r, apiKey), b, cm.cfg.TTL).Err()
}

func (cm *CacheManager) generateCacheKey(r *http.Request, apiKey string) string {
	h := fnv.New64a()
	var query string
	queryResolved := false
	for _, part := range cm.templatePlan {
		switch part.token {
		case tokenLiteral:
			_, _ = h.Write([]byte(part.literal))
		case tokenPath:
			_, _ = h.Write([]byte(r.URL.Path))
		case tokenMethod:
			_, _ = h.Write([]byte(r.Method))
		case tokenEncoding:
			_, _ = h.Write([]byte(r.Header.Get("Accept-Encoding")))
		case tokenAPIKey:
			_, _ = h.Write([]byte(apiKey))
		case tokenQuery:
			if !queryResolved {
				// Canonical order keeps keys stable regardless of param order.
				query = r.URL.Query().Encode()
				queryResolved = true
			}
			_, _ = h.Write([]byte(query))
		}
	}
	// The path prefix lets a config reload invalidate one service's entries.
	return "c:" + r.URL.Path + ":" + strconv.FormatUint(h.Sum64(), 16)
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
		token, size := matchCacheToken(tpl)
		if size == 0 {
			parts = append(parts, cacheTemplatePart{token: tokenLiteral, literal: "$"})
			tpl = tpl[1:]
			continue
		}
		parts = append(parts, cacheTemplatePart{token: token})
		tpl = tpl[size:]
	}
	return parts
}

func matchCacheToken(s string) (cacheToken, int) {
	for _, t := range []struct {
		name  string
		token cacheToken
	}{{"path", tokenPath}, {"query", tokenQuery}, {"key", tokenAPIKey}, {"enc", tokenEncoding}, {"method", tokenMethod}} {
		if n := len(t.name) + 1; len(s) >= n && strings.EqualFold(s[1:n], t.name) {
			return t.token, n
		}
	}
	return tokenLiteral, 0
}

// InvalidateResponseCacheScopes deletes cached responses for reloaded
// services.
func InvalidateResponseCacheScopes(ctx context.Context, rdb *redis.Client, scopes []types.ConfigInvalidationScope) error {
	if rdb == nil {
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
		if err := deleteKeysByPattern(ctx, rdb, pattern+"*"); err != nil {
			return err
		}
	}
	return nil
}

func deleteKeysByPattern(ctx context.Context, rdb *redis.Client, pattern string) error {
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, pattern, 256).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		if cursor = next; cursor == 0 {
			return nil
		}
	}
}
