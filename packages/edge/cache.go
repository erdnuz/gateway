package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gateway/packages/common/telemetry"
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
		if r.Method != http.MethodGet || s.Responses == nil || cfg == nil || !cfg.Enabled {
			next.ServeHTTP(w, r)
			return
		}
		cm := NewCacheManager(s.Responses, *cfg)
		cached, ok, err := cm.CheckCache(r.Context(), r, st.key)
		if err == nil && ok {
			telemetry.CacheLookups.WithLabelValues(st.label(), "hit").Inc()
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
		telemetry.CacheLookups.WithLabelValues(st.label(), "miss").Inc()
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

// ResponseStore holds cached upstream responses. The default keeps them in
// edge memory (NewMemoryResponses); Redis (NewRedisResponses) suits caches
// larger than an edge's memory budget.
type ResponseStore interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Invalidate drops every entry under /prefix (and /service when set).
	Invalidate(ctx context.Context, prefix, service string) error
	Ping(ctx context.Context) error
}

// NewMemoryResponses keeps up to budget bytes of responses in process.
func NewMemoryResponses(budget int64) ResponseStore {
	return memoryResponses{newTTLCache[[]byte](budget)}
}

type memoryResponses struct{ c *ttlCache[[]byte] }

func (m memoryResponses) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, _, fresh, ok := m.c.get(key)
	return v, ok && fresh, nil
}

func (m memoryResponses) Set(_ context.Context, key string, v []byte, ttl time.Duration) error {
	m.c.set(key, v, ttl, int64(len(v)+len(key)))
	return nil
}

func (m memoryResponses) Invalidate(_ context.Context, prefix, service string) error {
	scope := scopePattern(prefix, service)
	m.c.deleteFunc(func(k string) bool { return strings.HasPrefix(k, scope) })
	return nil
}

func (memoryResponses) Ping(context.Context) error { return nil }

// NewRedisResponses keeps responses in the edge's own Redis.
func NewRedisResponses(rdb *redis.Client) ResponseStore { return redisResponses{rdb} }

type redisResponses struct{ rdb *redis.Client }

func (r redisResponses) Get(ctx context.Context, key string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, cacheRedisTimeout)
	defer cancel()
	v, err := r.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	return v, err == nil, err
}

func (r redisResponses) Set(ctx context.Context, key string, v []byte, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, cacheRedisTimeout)
	defer cancel()
	return r.rdb.Set(ctx, key, v, ttl).Err()
}

func (r redisResponses) Invalidate(ctx context.Context, prefix, service string) error {
	var cursor uint64
	for {
		keys, next, err := r.rdb.Scan(ctx, cursor, scopePattern(prefix, service)+"*", 256).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := r.rdb.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		if cursor = next; cursor == 0 {
			return nil
		}
	}
}

func (r redisResponses) Ping(ctx context.Context) error { return r.rdb.Ping(ctx).Err() }

// scopePattern is the key prefix shared by a service's cached responses.
func scopePattern(prefix, service string) string {
	p := "c:/" + strings.TrimPrefix(prefix, "/")
	if service != "" {
		p += "/" + service
	}
	return p
}

type CacheManager struct {
	store        ResponseStore
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

func NewCacheManager(store ResponseStore, cfg types.CacheConfig) *CacheManager {
	return &CacheManager{store: store, cfg: cfg, templatePlan: compileCacheTemplate(cfg.CacheKey)}
}

// CheckCache looks up the cached response for a request.
func (cm *CacheManager) CheckCache(ctx context.Context, r *http.Request, apiKey string) (*CachedResponse, bool, error) {
	val, ok, err := cm.store.Get(ctx, cm.generateCacheKey(r, apiKey))
	if !ok || err != nil {
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
	return cm.store.Set(ctx, cm.generateCacheKey(r, apiKey), b, cm.cfg.TTL)
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
