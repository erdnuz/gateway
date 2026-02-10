package middleware

import (
	"gate/internal/state"
	"net"
	"net/http"
)

type RateLimitMiddleware struct {
	State *state.LocalState
}

func (m *RateLimitMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		svcID := r.Header.Get("X-Service-ID")
		if svcID == "" {
			http.Error(w, "Missing X-Service-ID", http.StatusBadRequest)
			return
		}

		// 1. Fetch Service Config
		m.State.RLock()
		cfg, svcExists := m.State.Configs[svcID]
		m.State.RUnlock()

		if !svcExists {
			http.Error(w, "Service configuration not found", http.StatusNotFound)
			return
		}

		// 2. Resolve Identity based on AuthType (API_KEY or IP)
		identifier := m.resolveIdentity(r, cfg.AuthType)
		if identifier == "" {
			http.Error(w, "Unauthorized: Identity could not be verified", http.StatusUnauthorized)
			return
		}

		// 3. Fetch User Tier
		m.State.RLock()
		userTier := m.State.UserTiers[svcID][identifier]
		m.State.RUnlock()

		// 4. Determine current Tier rules
		currentTier := cfg.Tiers[userTier] // Assuming userTier is an index; adjust if it's a name

		// 5. Calculate Weight (Token Costs)
		weight := uint64(1)
		if cost, ok := currentTier.TokenCosts[r.Method]; ok {
			weight = cost
		}

		// 6. Check Projected Usage
		usage := m.State.GetProjectedUsage(svcID, identifier)
		if usage+int64(weight) > int64(currentTier.Quota) {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("Rate limit exceeded"))
			return
		}

		// 7. Success: Update state and proceed
		m.State.IncrementLocal(svcID, identifier, int64(weight))
		next.ServeHTTP(w, r)
	})
}

// resolveIdentity switches identification logic based on ServiceConfig.AuthType
func (m *RateLimitMiddleware) resolveIdentity(r *http.Request, authType string) string {
	switch authType {
	case "IP":
		// Handle X-Forwarded-For if behind a proxy, otherwise RemoteAddr
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return r.RemoteAddr
		}
		return ip
	case "API_KEY":
		return r.Header.Get("X-API-Key")
	default:
		// Default to API_KEY if not specified
		return r.Header.Get("X-API-Key")
	}
}
