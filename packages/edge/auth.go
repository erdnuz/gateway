package edge

import (
	"context"
	"net/http"
	"regexp"

	"gateway/packages/common/httpx"
	"gateway/packages/common/types"
)

// authenticate resolves the caller's API key to a tier of the routed service.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := stateOf(r)
		key := r.Header.Get("X-API-Key")
		if key == "" {
			httpx.WriteProblem(w, r, http.StatusUnauthorized, "missing_api_key", "send your API key in the X-API-Key header")
			return
		}
		if !s.validKey(key) {
			httpx.WriteProblem(w, r, http.StatusUnauthorized, "invalid_api_key", "")
			return
		}
		tierID, err := s.resolveTier(r.Context(), st, key)
		if err != nil {
			httpx.Logger(r.Context()).Warn("tier lookup failed", "scope", st.scope, "key", httpx.RedactKey(key), "err", err)
			httpx.WriteProblem(w, r, http.StatusServiceUnavailable, "tier_lookup_failed", "the key's tier could not be resolved; retry shortly")
			return
		}
		if tierID == "" { // never issued, or revoked
			httpx.WriteProblem(w, r, http.StatusUnauthorized, "invalid_api_key", "")
			return
		}
		tier, ok := GetTier(st.svc, tierID)
		if !ok {
			httpx.WriteProblem(w, r, http.StatusForbidden, "tier_not_allowed", "the key's tier has no access to this service")
			return
		}
		st.key, st.tier = key, tier
		if st.entry != nil {
			st.entry.Tier = tierID
		}
		next.ServeHTTP(w, r)
	})
}

// resolveTier asks the tier cache (backed by the hub) and, when the hub is
// unreachable, falls back as the service's policy allows.
func (s *Server) resolveTier(ctx context.Context, st *request, key string) (string, error) {
	tier, err := s.Tiers.GetUserTier(ctx, st.scope, key)
	if err == nil {
		return tier, nil
	}
	p := st.svc.Failure.EffectiveHubPolicy()
	switch p.TierLookupStrategy {
	case "default-tier":
		tier = p.DefaultTier
	case "stale-or-default":
		if stale, ok := s.Tiers.GetStaleTier(st.scope, key, p.StaleTierMaxAge); ok {
			return stale, nil
		}
		tier = p.DefaultTier
	}
	if tier == "" {
		return "", err
	}
	return tier, nil
}

// keyMatcher caches the compiled runtime.hub.api_key_pattern.
type keyMatcher struct {
	src string
	re  *regexp.Regexp
}

// validKey applies the hub's key pattern at the edge, so keys the hub would
// reject (or mis-route, e.g. "key/suffix") never reach tier lookup or leasing.
func (s *Server) validKey(key string) bool {
	src := ""
	if cfg := s.Config.Get(); cfg != nil {
		src = cfg.Runtime.Hub.APIKeyPattern
	}
	src = orDefault(src, types.DefaultRuntimePolicy().Hub.APIKeyPattern)
	m := s.keys.Load()
	if m == nil || m.src != src {
		re, err := regexp.Compile(src)
		if err != nil {
			re = regexp.MustCompile(types.DefaultRuntimePolicy().Hub.APIKeyPattern)
		}
		m = &keyMatcher{src: src, re: re}
		s.keys.Store(m)
	}
	return m.re.MatchString(key)
}
