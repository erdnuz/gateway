package edge

import (
	"context"
	"net"
	"net/http"
	"regexp"

	"gateway/packages/common/httpx"
	"gateway/packages/common/types"
	"gateway/packages/keys"
)

// authenticate identifies the caller and resolves their tier for the routed
// service. Identities are hashed here, on arrival: tier lookups, leases,
// cache keys and logs past this point only ever see the hash.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := stateOf(r)
		var id, tierID string
		var ok bool
		switch st.svc.AuthType() {
		case "jwt":
			id, tierID, ok = s.bearerToken(w, r, st)
		case "none":
			id, tierID, ok = keys.Hash("ip:"+clientIP(r)), defaultTier(st.svc), true
		default:
			id, tierID, ok = s.apiKey(w, r, st)
		}
		if !ok {
			return
		}
		tier, found := GetTier(st.svc, tierID)
		if !found {
			httpx.WriteProblem(w, r, http.StatusForbidden, "tier_not_allowed", "the caller's tier has no access to this service")
			return
		}
		st.key, st.tier = id, tier
		if st.entry != nil {
			st.entry.Tier = tierID
		}
		next.ServeHTTP(w, r)
	})
}

// apiKey authenticates X-API-Key; the hub assigns its tier.
func (s *Server) apiKey(w http.ResponseWriter, r *http.Request, st *request) (id, tier string, ok bool) {
	key := r.Header.Get("X-API-Key")
	if key == "" {
		httpx.WriteProblem(w, r, http.StatusUnauthorized, "missing_api_key", "send your API key in the X-API-Key header")
		return "", "", false
	}
	if !s.validKey(key) {
		httpx.WriteProblem(w, r, http.StatusUnauthorized, "invalid_api_key", "")
		return "", "", false
	}
	id = keys.Hash(key)
	tier, err := s.resolveTier(r.Context(), st, id)
	if err != nil {
		httpx.Logger(r.Context()).Warn("tier lookup failed", "scope", st.scope, "key", keys.Short(id), "err", err)
		httpx.WriteProblem(w, r, http.StatusServiceUnavailable, "tier_lookup_failed", "the key's tier could not be resolved; retry shortly")
		return "", "", false
	}
	if tier == "" { // never issued, or revoked
		httpx.WriteProblem(w, r, http.StatusUnauthorized, "invalid_api_key", "")
		return "", "", false
	}
	return id, tier, true
}

// bearerToken authenticates a JWT. The caller is its issuer and subject; the
// tier comes from the configured claim, else the service's default tier.
func (s *Server) bearerToken(w http.ResponseWriter, r *http.Request, st *request) (id, tier string, ok bool) {
	raw, found := httpx.BearerToken(r)
	if !found || raw == "" {
		w.Header().Set("WWW-Authenticate", "Bearer")
		httpx.WriteProblem(w, r, http.StatusUnauthorized, "missing_token", "send a bearer token in the Authorization header")
		return "", "", false
	}
	cfg := st.svc.Auth.JWT
	claims, err := s.jwt.verify(r.Context(), cfg, raw)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
		httpx.WriteProblem(w, r, http.StatusUnauthorized, "invalid_token", "the bearer token could not be verified")
		return "", "", false
	}
	sub, _ := claims["sub"].(string)
	iss, _ := claims["iss"].(string)
	if sub == "" {
		httpx.WriteProblem(w, r, http.StatusUnauthorized, "invalid_token", "the token has no subject")
		return "", "", false
	}
	tier = defaultTier(st.svc)
	if cfg.TierClaim != "" {
		if t, _ := claims[cfg.TierClaim].(string); t != "" {
			tier = t
		}
	}
	return keys.Hash("jwt:" + iss + ":" + sub), tier, true
}

// defaultTier is the service's default tier, or its only tier.
func defaultTier(svc *types.ServiceConfig) string {
	if t := svc.Failure.Hub.DefaultTier; t != "" {
		return t
	}
	if len(svc.Tiers) == 1 {
		return svc.Tiers[0].TierID
	}
	return ""
}

// clientIP is the connection's peer address. Behind a load balancer, put the
// edge on a network where that address identifies the client, or use api_key
// or jwt auth: X-Forwarded-For can be forged.
func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// resolveTier asks the tier cache (backed by the hub) and, when the hub is
// unreachable, falls back as the service's policy allows.
func (s *Server) resolveTier(ctx context.Context, st *request, key string) (string, error) {
	tier, err := s.Tiers.Get(ctx, st.scope, key)
	if err == nil {
		return tier, nil
	}
	p := st.svc.Failure.EffectiveHubPolicy()
	switch p.TierLookupStrategy {
	case "default-tier":
		tier = p.DefaultTier
	case "stale-or-default":
		if stale, ok := s.Tiers.Stale(st.scope, key, p.StaleTierMaxAge); ok {
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
