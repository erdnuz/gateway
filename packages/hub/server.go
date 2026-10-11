package hub

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gateway/packages/common/httpx"
	"gateway/packages/common/types"
	"gateway/packages/keys"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9" // Updated to v9
)

type corsPolicy struct {
	allowed           map[string]struct{}
	allowHeaders      []string
	allowMethods      []string
	maxAge            time.Duration
	allowHeadersValue string
	allowMethodsValue string
	maxAgeValue       string
}

func newCORSPolicy(origins map[string]struct{}, allowedHeaders, allowedMethods []string, maxAge time.Duration) *corsPolicy {
	if origins == nil {
		origins = map[string]struct{}{}
	}
	originsCopy := make(map[string]struct{}, len(origins))
	for k, v := range origins {
		originsCopy[k] = v
	}
	headersCopy := append([]string(nil), allowedHeaders...)
	methodsCopy := append([]string(nil), allowedMethods...)

	return &corsPolicy{
		allowed:           originsCopy,
		allowHeaders:      headersCopy,
		allowMethods:      methodsCopy,
		maxAge:            maxAge,
		allowHeadersValue: strings.Join(headersCopy, ", "),
		allowMethodsValue: strings.Join(methodsCopy, ", "),
		maxAgeValue:       strconv.FormatInt(int64(maxAge.Seconds()), 10),
	}
}

type HubServer struct {
	rdb               redis.UniversalClient
	cfgManager        ConfigRegistry
	tierManager       TierStore
	authToken         string        // admin: every endpoint
	readToken         string        // read-only: GET endpoints
	control           *ControlPlane // nil: changes are not pushed to edges
	v2                *http.ServeMux
	corsPolicy        atomic.Pointer[corsPolicy]
	apiKeyPattern     atomic.Pointer[regexp.Regexp]
	readyHealthy      atomic.Bool
	pendingDependency atomic.Value
}

type configFileLoader interface {
	LoadFromFile() (*types.GatewayConfig, error)
}

type configPayloadReloader interface {
	ReloadFromPayload(cfg *types.GatewayConfig) error
}

type configUpdateReport struct {
	Mode     string               `json:"mode"`
	Valid    bool                 `json:"valid"`
	Analysis configUpdateAnalysis `json:"analysis"`
}

// NewHubServer builds the hub's admin HTTP API. Config and tier changes made
// through it are pushed to edges by control.
func NewHubServer(rdb redis.UniversalClient, cfg ConfigRegistry, tierStore TierStore, control *ControlPlane, authToken string) *HubServer {
	server := &HubServer{
		rdb:         rdb,
		cfgManager:  cfg,
		tierManager: tierStore,
		authToken:   authToken,
		control:     control,
	}
	server.v2 = server.keyRoutes()
	server.readyHealthy.Store(false)
	server.corsPolicy.Store(newCORSPolicy(
		map[string]struct{}{},
		[]string{"Authorization", "Content-Type", "X-API-Key"},
		[]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		5*time.Minute,
	))
	server.apiKeyPattern.Store(regexp.MustCompile(types.DefaultRuntimePolicy().Hub.APIKeyPattern))
	server.pendingDependency.Store("redis")
	return server
}

// SetReadToken enables a second bearer token that may only read.
func (s *HubServer) SetReadToken(token string) { s.readToken = token }

func (s *HubServer) SetStartupReady(ready bool) {
	s.readyHealthy.Store(ready)
}

func (s *HubServer) SetPendingDependency(dependency string) {
	v := strings.TrimSpace(dependency)
	if v == "" {
		v = "unknown"
	}
	s.pendingDependency.Store(v)
}

func (s *HubServer) pendingDependencyValue() string {
	v := s.pendingDependency.Load()
	dep, _ := v.(string)
	dep = strings.TrimSpace(dep)
	if dep == "" {
		return "unknown"
	}
	return dep
}

func (s *HubServer) currentCORSPolicy() *corsPolicy {
	policy := s.corsPolicy.Load()
	if policy != nil {
		return policy
	}
	fallback := newCORSPolicy(
		map[string]struct{}{},
		[]string{"Authorization", "Content-Type", "X-API-Key"},
		[]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		5*time.Minute,
	)
	s.corsPolicy.Store(fallback)
	return fallback
}

func (s *HubServer) SetCORSAllowedOrigins(origins []string) {
	current := s.currentCORSPolicy()
	allowed := make(map[string]struct{}, len(origins))
	for _, origin := range origins {
		v := strings.TrimSpace(strings.TrimSuffix(origin, "/"))
		if v != "" {
			allowed[v] = struct{}{}
		}
	}
	s.corsPolicy.Store(newCORSPolicy(allowed, current.allowHeaders, current.allowMethods, current.maxAge))
}

func newBootID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

func (s *HubServer) SetCORSPreflightPolicy(allowedHeaders, allowedMethods []string, maxAge time.Duration) {
	current := s.currentCORSPolicy()
	nextHeaders := current.allowHeaders
	nextMethods := current.allowMethods
	nextMaxAge := current.maxAge
	if len(allowedHeaders) > 0 {
		nextHeaders = allowedHeaders
	}
	if len(allowedMethods) > 0 {
		nextMethods = allowedMethods
	}
	if maxAge > 0 {
		nextMaxAge = maxAge
	}
	s.corsPolicy.Store(newCORSPolicy(current.allowed, nextHeaders, nextMethods, nextMaxAge))
}

func (s *HubServer) SetAPIKeyPattern(pattern string) error {
	v := strings.TrimSpace(pattern)
	if v == "" {
		return nil
	}
	re, err := regexp.Compile(v)
	if err != nil {
		return err
	}
	s.apiKeyPattern.Store(re)
	return nil
}

func (s *HubServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := r.URL.Path
	if s.handleCORS(w, r) {
		return
	}

	// Standardize: Remove leading/trailing slashes for easier splitting
	trimmedPath := strings.Trim(path, "/")
	parts := strings.Split(trimmedPath, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	switch parts[0] {
	case "healthz":
		s.handleHealth(w, r)
		return
	case "readyz":
		s.handleReady(w, r)
		return
	}

	role, ok := s.authorize(r)
	if !ok {
		httpx.Error(w, r, http.StatusUnauthorized, "send a hub token as Authorization: Bearer <token>")
		return
	}
	if role == "read-only" && r.Method != http.MethodGet && r.Method != http.MethodHead {
		httpx.WriteProblem(w, r, http.StatusForbidden, "read_only_token", "this token may only read")
		return
	}
	r = r.WithContext(context.WithValue(ctx, roleKey{}, role))
	ctx = r.Context()

	switch parts[0] {
	case "config":
		s.handleConfig(w, r)
	case "config-reload":
		if len(parts) > 1 && parts[1] == "dry-run" {
			s.handleConfigReloadDryRun(w, r)
			return
		}
		s.handleConfigReload(w, r)
	case "status":
		s.handleStatus(w, r)
	case "tiers":
		s.handleTiers(w, r, ctx, parts[1:])
	case "v2":
		s.v2.ServeHTTP(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *HubServer) handleCORS(w http.ResponseWriter, r *http.Request) bool {
	policy := s.currentCORSPolicy()
	origin := strings.TrimSpace(strings.TrimSuffix(r.Header.Get("Origin"), "/"))
	if origin == "" {
		return false
	}
	if !s.isCORSOriginAllowed(origin) {
		httpx.Error(w, r, http.StatusForbidden, "forbidden origin")
		return true
	}
	w.Header().Set("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", policy.allowMethodsValue)
		w.Header().Set("Access-Control-Allow-Headers", policy.allowHeadersValue)
		w.Header().Set("Access-Control-Max-Age", policy.maxAgeValue)
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

func (s *HubServer) isCORSOriginAllowed(origin string) bool {
	policy := s.currentCORSPolicy()
	if len(policy.allowed) == 0 {
		return false
	}
	_, ok := policy.allowed[origin]
	return ok
}

// --- Handler Implementations ---

func (s *HubServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.Error(w, r, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		httpx.Error(w, r, http.StatusInternalServerError, err.Error())
	}
}

func (s *HubServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.Error(w, r, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	config := s.cfgManager.Snapshot()
	if err := json.NewEncoder(w).Encode(config); err != nil {
		s.writeInternalError(w, r, "handleConfig", err)
	}

}

func (s *HubServer) handleConfigReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		httpx.Error(w, r, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	nextCfg, mode, err := s.resolveReloadTargetConfig(r)
	if err != nil {
		httpx.Error(w, r, http.StatusBadRequest, err.Error())
		return
	}
	current := s.cfgManager.Snapshot()
	analysis := analyzeConfigUpdate(current, nextCfg)

	if mode == "payload" {
		reloader, ok := s.cfgManager.(configPayloadReloader)
		if !ok {
			httpx.Error(w, r, http.StatusNotImplemented, "config payload reload unsupported")
			return
		}
		if err := reloader.ReloadFromPayload(nextCfg); err != nil {
			httpx.Error(w, r, http.StatusBadRequest, err.Error())
			return
		}
	} else {
		reloader, ok := s.cfgManager.(interface{ ReloadFromFile() error })
		if !ok {
			httpx.Error(w, r, http.StatusNotImplemented, "config reload unsupported")
			return
		}
		if err := reloader.ReloadFromFile(); err != nil {
			httpx.Error(w, r, http.StatusBadRequest, err.Error())
			return
		}
	}

	if s.control != nil {
		s.control.PublishConfig(s.cfgManager.Snapshot(), analysis.Invalidations.ResponseCache)
	}
	s.audit(r, "config.reload", mode, strings.Join(analysis.ChangedSections, ","))
	w.WriteHeader(http.StatusNoContent)
}

func (s *HubServer) handleConfigReloadDryRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		httpx.Error(w, r, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	nextCfg, mode, err := s.resolveReloadTargetConfig(r)
	if err != nil {
		httpx.Error(w, r, http.StatusBadRequest, err.Error())
		return
	}
	analysis := analyzeConfigUpdate(s.cfgManager.Snapshot(), nextCfg)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(configUpdateReport{
		Mode:     mode,
		Valid:    true,
		Analysis: analysis,
	})
}

func (s *HubServer) resolveReloadTargetConfig(r *http.Request) (*types.GatewayConfig, string, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		return nil, "", err
	}
	payloadBody := strings.TrimSpace(string(body))
	if payloadBody != "" {
		cfg, err := decodeAndValidateConfig(body, "config-reload payload")
		if err != nil {
			return nil, "", err
		}
		return cfg, "payload", nil
	}
	loader, ok := s.cfgManager.(configFileLoader)
	if !ok {
		return nil, "", fmt.Errorf("config file reload unsupported")
	}
	cfg, err := loader.LoadFromFile()
	if err != nil {
		return nil, "", err
	}
	return cfg, "file", nil
}

func (s *HubServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.Error(w, r, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.readyHealthy.Load() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "pending", "missing_dependency": s.pendingDependencyValue()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

// handleStatus reports how many edges are watching this hub.
func (s *HubServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.Error(w, r, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	edges := 0
	if s.control != nil {
		edges = s.control.Watchers()
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"edges_connected": edges, "ready": s.readyHealthy.Load()})
}

func (s *HubServer) handleTiers(w http.ResponseWriter, r *http.Request, ctx context.Context, params []string) {
	// Expecting: {prefix}/{api_key}
	if len(params) < 2 {
		httpx.Error(w, r, http.StatusBadRequest, "invalid path: expected /tiers/{prefix}/{api_key}")
		return
	}

	prefix := params[0]
	apiKey := params[1]
	if !s.isAllowedPrefix(prefix) || !s.matchesAPIKey(apiKey) {
		httpx.Error(w, r, http.StatusBadRequest, "invalid prefix or api key")
		return
	}
	// Operator-chosen keys are stored and pushed by hash, like issued keys.
	hash := keys.Hash(apiKey)

	switch r.Method {
	case http.MethodGet:
		tier, err := s.tierManager.GetTier(ctx, prefix, hash)
		if err != nil {
			s.writeInternalError(w, r, "handleTiers.GetTier", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"tier": tier})

	case http.MethodPost, http.MethodPut:
		var req struct {
			TierID string `json:"tier_id"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&req); err != nil {
			httpx.Error(w, r, http.StatusBadRequest, err.Error())
			return
		}
		req.TierID = strings.TrimSpace(req.TierID)
		if !s.tierDefinedInPrefix(prefix, req.TierID) {
			httpx.Error(w, r, http.StatusBadRequest, "unknown tier_id for prefix")
			return
		}

		if err := s.assign(ctx, prefix, hash, req.TierID, nil); err != nil {
			s.writeInternalError(w, r, "handleTiers.SetTier", err)
			return
		}
		s.audit(r, "tier.set", prefix+"/"+keys.Short(hash), "tier="+req.TierID)
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		if err := s.assign(ctx, prefix, hash, "", nil); err != nil {
			s.writeInternalError(w, r, "handleTiers.DeleteTier", err)
			return
		}
		s.audit(r, "tier.revoke", prefix+"/"+keys.Short(hash), "")
		w.WriteHeader(http.StatusNoContent)

	default:
		httpx.Error(w, r, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *HubServer) writeInternalError(w http.ResponseWriter, r *http.Request, op string, err error) {
	httpx.Logger(r.Context()).Error("hub internal error", "op", op, "err", err)
	httpx.Error(w, r, http.StatusInternalServerError, "internal server error")
}

// authorize checks the bearer token in constant time and returns its role:
// "admin" or "read-only". An empty authToken disables auth; `gate hub`
// refuses to start without HUB_AUTH_TOKEN, so that only happens for
// embedded/test servers.
func (s *HubServer) authorize(r *http.Request) (string, bool) {
	if s.authToken == "" {
		return "admin", true
	}
	got, ok := httpx.BearerToken(r)
	switch {
	case !ok:
		return "", false
	case subtle.ConstantTimeCompare([]byte(got), []byte(s.authToken)) == 1:
		return "admin", true
	case s.readToken != "" && subtle.ConstantTimeCompare([]byte(got), []byte(s.readToken)) == 1:
		return "read-only", true
	}
	return "", false
}

// tierDefinedInPrefix reports whether any service under prefix defines tierID
// (as a regular or safety tier). Assigning an undefined tier would make every
// request for the key fail at the edge.
func (s *HubServer) tierDefinedInPrefix(prefix, tierID string) bool {
	if tierID == "" {
		return false
	}
	p, ok := s.cfgManager.FindPrefix(prefix)
	if !ok {
		return false
	}
	for i := range p.Services {
		if _, ok := findTier(&p.Services[i], tierID); ok {
			return true
		}
	}
	return false
}

func (s *HubServer) isAllowedPrefix(prefix string) bool {
	_, ok := s.cfgManager.FindPrefix(prefix)
	return ok
}

func (s *HubServer) matchesAPIKey(apiKey string) bool {
	pattern := s.apiKeyPattern.Load()
	if pattern == nil {
		pattern = regexp.MustCompile(types.DefaultRuntimePolicy().Hub.APIKeyPattern)
		s.apiKeyPattern.Store(pattern)
	}
	return pattern.MatchString(apiKey)
}

// publishTier pushes a key's new tier to every edge, so revocations take
// effect without waiting for cached tiers to expire.
func (s *HubServer) publishTier(prefix, apiKey, tierID string) {
	if s.control != nil {
		s.control.PublishTier(prefix, apiKey, tierID)
	}
}
