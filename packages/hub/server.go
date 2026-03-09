package hub

import (
	"context"
	"encoding/json"
	"gateway/packages/common/types"
	"net/http"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9" // Updated to v9
	"go.mongodb.org/mongo-driver/mongo"
)

type HubServer struct {
	mongoDB          *mongo.Database
	rdb              *redis.Client
	cfgManager       *ConfigManager
	tierManager      *TierManager
	rateManager      *RateManager
	analyticsManager *AnalyticsManager
}

func NewHubServer(mdb *mongo.Database, rdb *redis.Client, configFilePath, analyticsURL, analyticsToken, analyticsOrg, analyticsBucket string) *HubServer {
	cfg, err := NewConfigManager(configFilePath) // Load initial config from file
	if err != nil {
		panic("Failed to load config: " + err.Error())
	}

	hs := &HubServer{
		mongoDB:          mdb,
		rdb:              rdb,
		cfgManager:       cfg,
		tierManager:      NewTierManager(mdb, rdb),
		rateManager:      NewRateManager(rdb, cfg),
		analyticsManager: NewAnalyticsManager(analyticsURL, analyticsToken, analyticsOrg, analyticsBucket),
	}

	// begin listening for asynchronous rate updates from edges
	if hs.rateManager != nil {
		_ = hs.rateManager.StartDeltaListener(context.Background())
	}

	return hs
}

func (s *HubServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := r.URL.Path

	// Standardize: Remove leading/trailing slashes for easier splitting
	trimmedPath := strings.Trim(path, "/")
	parts := strings.Split(trimmedPath, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	switch parts[0] {
	case "config":
		s.handleConfig(w, r)
	case "health":
		s.handleHealth(w, r)
	case "tiers":
		s.handleTiers(w, r, ctx, parts[1:])
	case "rate":
		s.handleRate(w, r, ctx, parts[1:])
	case "analytics":
		s.handleAnalytics(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *HubServer) handleRate(w http.ResponseWriter, r *http.Request, ctx context.Context, params []string) {
	// Expecting: {prefix}/{api_key}
	if len(params) < 2 {
		http.Error(w, "invalid path: expected /rate/{prefix}/{api_key}", http.StatusBadRequest)
		return
	}

	prefix := params[0]
	apiKey := params[1]

	var total int64
	var err error

	switch r.Method {
	case http.MethodPost:
		// 1. Parse Delta for Increment
		delta := int64(1)
		if dStr := r.URL.Query().Get("delta"); dStr != "" {
			d, err := strconv.ParseInt(dStr, 10, 64)
			if err != nil || d <= 0 {
				http.Error(w, "invalid delta", http.StatusBadRequest)
				return
			}
			delta = d
		}
		total, err = s.rateManager.Increment(ctx, prefix, apiKey, delta)

	case http.MethodGet:
		// 2. Pure retrieval (Weighted Calculation only)
		total, err = s.rateManager.Increment(ctx, prefix, apiKey, 0)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. Clean integer response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(total)
}

// --- Handler Implementations ---

func (s *HubServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *HubServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	config := s.cfgManager.Get()
	json.NewEncoder(w).Encode(config)

}

func (s *HubServer) handleTiers(w http.ResponseWriter, r *http.Request, ctx context.Context, params []string) {
	// Expecting: {prefix}/{api_key}
	if len(params) < 2 {
		http.Error(w, "invalid path: expected /tiers/{prefix}/{api_key}", http.StatusBadRequest)
		return
	}

	prefix := params[0]
	apiKey := params[1]

	switch r.Method {
	case http.MethodGet:
		tier, err := s.tierManager.GetTier(ctx, prefix, apiKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"tier": tier})

	case http.MethodPost, http.MethodPut:
		var req struct {
			TierID string `json:"tier_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := s.tierManager.SetTier(ctx, prefix, apiKey, req.TierID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Broadcast invalidation so Edges drop stale cache for this user
		s.rdb.Publish(ctx, "hub_updates", "INVALIDATE:"+prefix+":"+apiKey)
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		if err := s.tierManager.DeleteTier(ctx, prefix, apiKey); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		s.rdb.Publish(ctx, "hub_updates", "INVALIDATE:"+prefix+":"+apiKey)
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HubServer) handleAnalytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. Decode the batch of entries from the Edge
	var entries []types.AnalyticsEntry
	if err := json.NewDecoder(r.Body).Decode(&entries); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(entries) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	s.analyticsManager.IngestBatch(entries)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"count":   len(entries),
		"message": "batch processed successfully",
	})
}
