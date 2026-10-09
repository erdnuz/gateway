package edge

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gateway/packages/common/httpx"
	"gateway/packages/common/types"
)

// request is the per-request state the middleware chain fills in.
type request struct {
	scope   string // route group: the first path segment
	service string
	svc     *types.ServiceConfig
	prefix  *types.PrefixConfig
	target  *url.URL
	key     string
	tier    *types.TierConfig
	entry   *types.AnalyticsEntry // nil when analytics is off

	upstreamFailed bool // the response is a failure-policy fallback
}

type requestKey struct{}

// label names the routed service for metrics ("unrouted" before routing).
func (st *request) label() string {
	if st.svc == nil {
		return "unrouted"
	}
	return st.scope + "/" + st.service
}

func stateOf(r *http.Request) *request {
	st, _ := r.Context().Value(requestKey{}).(*request)
	return st
}

func (s *Server) withState(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := &request{}
		if s.Analytics != nil {
			st.entry = &types.AnalyticsEntry{Timestamp: time.Now(), Method: r.Method, EdgeID: s.opts.EdgeID}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestKey{}, st)))
	})
}

// route resolves /{group}/{service}/... to a configured service.
func (s *Server) route(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := stateOf(r)
		scope, service := splitRoute(r.URL.Path)
		if scope == "" {
			httpx.WriteProblem(w, r, http.StatusNotFound, "no_route", "paths have the form /{group}/{service}/...")
			return
		}
		svc, err := s.Config.GetServiceConfig(scope, service)
		if err != nil {
			httpx.WriteProblem(w, r, http.StatusNotFound, "no_route", "no service is configured at /"+scope+"/"+service)
			return
		}
		st.scope, st.service, st.svc = scope, service, svc
		st.prefix, _ = s.Config.GetPrefix(scope)
		if st.entry != nil {
			st.entry.Prefix, st.entry.Service = scope, service
		}
		next.ServeHTTP(w, r)
	})
}

func splitRoute(path string) (scope, service string) {
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 3)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", ""
	}
	return parts[0], parts[1]
}

// cors answers preflights and sets the allowed origin. Preflights carry no
// credentials, so this runs before authentication.
func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := stateOf(r).svc.CORS
		if c == nil {
			next.ServeHTTP(w, r)
			return
		}
		h := w.Header()
		if origin, ok := allowedOrigin(r.Header.Get("Origin"), c.AllowedOrigins); ok {
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin")
		}
		if r.Method != http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		allowHeaders := c.AllowedHeaders
		if len(allowHeaders) == 0 {
			allowHeaders = types.DefaultRuntimePolicy().Hub.CORSAllowedHeaders
		}
		h.Set("Access-Control-Allow-Methods", strings.Join(c.AllowedMethods, ", "))
		h.Set("Access-Control-Allow-Headers", strings.Join(allowHeaders, ", "))
		h.Set("Access-Control-Max-Age", strconv.Itoa(int(c.MaxAge.Seconds())))
		w.WriteHeader(http.StatusNoContent)
	})
}

func allowedOrigin(origin string, allowed []string) (string, bool) {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return "", false
	}
	for _, a := range allowed {
		if a = strings.TrimSpace(a); a == "*" || a == origin {
			return origin, true
		}
	}
	return "", false
}
