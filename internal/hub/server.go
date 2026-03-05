package hub

import (
	"context"
	"encoding/json"
	"gateway/packages/common/types"
	"net/http"
	"time"

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

func NewHubServer(mdb *mongo.Database, rdb *redis.Client, analyticsURL, analyticsToken, analyticsOrg, analyticsBucket string) *HubServer {
	return &HubServer{
		mongoDB:     mdb,
		rdb:         rdb,
		cfgManager:  NewConfigManager(mdb, rdb),
		tierManager: NewTierManager(mdb, rdb),
		// RateManager now primarily uses Redis + Background Sync
		rateManager:      NewRateManager(rdb),
		analyticsManager: NewAnalyticsManager(analyticsURL, analyticsToken, analyticsOrg, analyticsBucket),
	}
}

func (s *HubServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Use request context to ensure cancellations propagate to DB/Redis
	ctx := r.Context()

	switch r.URL.Path {
	case "/config":
		s.handleConfig(w, r, ctx)
	case "/tiers":
		s.handleTiers(w, r, ctx)
	case "/rate":
		s.handleRate(w, r) // Rate has internal context handling
	case "/analytics":
		s.handleAnalytics(w, r) // Analytics has internal context handling
	default:
		http.NotFound(w, r)
	}
}

// --- Handler Implementations ---

func (s *HubServer) handleConfig(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	switch r.Method {
	case http.MethodGet:
		config, err := s.cfgManager.GetConfig(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(config)
	case http.MethodPost, http.MethodPut:
		var config types.GatewayConfig
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.cfgManager.UpdateConfig(ctx, &config); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Notify Edges to refresh their local memory cache
		s.rdb.Publish(ctx, "hub_updates", "CONFIG_UPDATED")
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HubServer) handleTiers(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	switch r.Method {
	case http.MethodGet:
		prefixID := r.URL.Query().Get("prefix_id")
		apiKey := r.URL.Query().Get("api_key")
		if prefixID == "" || apiKey == "" {
			http.Error(w, "missing prefix_id or api_key", http.StatusBadRequest)
			return
		}
		tier, err := s.tierManager.GetTier(ctx, prefixID, apiKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"tier": tier})

	case http.MethodPost, http.MethodPut:
		var req struct {
			PrefixID string `json:"prefix_id"`
			APIKey   string `json:"api_key"`
			TierID   string `json:"tier_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.tierManager.SetTier(ctx, req.PrefixID, req.APIKey, req.TierID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Notify Edges to invalidate local tier cache for this specific user
		s.rdb.Publish(ctx, "hub_updates", "INVALIDATE:"+req.APIKey)
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		prefixID := r.URL.Query().Get("prefix_id")
		apiKey := r.URL.Query().Get("api_key")
		if err := s.tierManager.DeleteTier(ctx, prefixID, apiKey); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ... handleRate and handleAnalytics remain as previously updated ...

func (s *HubServer) handleRate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	switch r.Method {
	case http.MethodGet:
		// Get the current true value for a key
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key parameter", http.StatusBadRequest)
			return
		}

		val, err := s.rateManager.Get(ctx, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(map[string]int64{"current_total": val})

	case http.MethodPost:
		// Increment and get the new total back
		var req struct {
			Key    string `json:"key"`
			Delta  int64  `json:"delta"`
			Expiry int64  `json:"expiry_seconds"` // Sent as seconds
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Key == "" || req.Delta <= 0 {
			http.Error(w, "invalid key or delta", http.StatusBadRequest)
			return
		}

		// Convert expiry seconds to time.Duration
		expiry := time.Duration(req.Expiry) * time.Second

		// 1. Perform the persistent increment
		newTotal, err := s.rateManager.Increment(ctx, req.Key, req.Delta, expiry)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 2. Respond with the "True Value" immediately
		// The Edge uses this to reset its localDelta and update its globalBase
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int64{
			"new_total": newTotal,
		})

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

	// 2. Ingest into the TSDB (TimescaleDB/Mongo/Influx)
	// This uses the optimized batching logic we discussed previously
	s.analyticsManager.IngestBatch(entries)

	// 3. Respond with confirmation
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"count":   len(entries),
		"message": "batch processed successfully",
	})
}
