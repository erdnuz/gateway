package main

import (
	"context"
	"gateway/packages/common/config"
	"gateway/packages/common/types"
	"gateway/packages/edge"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. Context & Signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Infrastructure (Redis)
	rdb := redis.NewClient(&redis.Options{Addr: config.String("REDIS_ADDR", "localhost:6379")})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis Ping Error: %v", err)
	}
	defer rdb.Close()

	// 3. Initialize Managers
	hubAddr := config.String("HUB_ADDR", "http://localhost:8081")
	hubToken := config.String("HUB_AUTH_TOKEN", "")
	hubUpdatesChannel := config.String("HUB_UPDATES_CHANNEL", types.DefaultHubUpdatesChannel)
	maxDelta := config.Int64("EDGE_MAX_DELTA", 100)
	analyticsBufferSize := config.Int("ANALYTICS_BUFFER_SIZE", 1000)
	analyticsEnabled := config.Bool("GATE_ANALYTICS_ENABLED", config.Bool("ANALYTICS_ENABLED", true))
	natsAnalyticsSubject := config.String("NATS_ANALYTICS_SUBJECT", "analytics.events")
	hubHTTPTimeout := config.DurationSeconds("HUB_HTTP_TIMEOUT_SECONDS", 5)
	hubHTTPClient := edge.NewHubHTTPClient(hubHTTPTimeout)
	configRefreshSeconds := config.Int("EDGE_CONFIG_REFRESH_SECONDS", 0)
	configReloadChannel := config.String("EDGE_CONFIG_RELOAD_CHANNEL", types.DefaultConfigReloadChannel)
	bootstrapConfigFile := config.String("EDGE_BOOTSTRAP_CONFIG_FILE", "")
	rateHardThresholdPctRaw := config.Int("RATE_HARD_THRESHOLD_PERCENT", 90)
	rateHardThresholdPct := float64(rateHardThresholdPctRaw) / 100.0
	leaseSize := config.Int64("EDGE_LEASE_SIZE", 100)
	leaseLowWaterPctRaw := config.Int("EDGE_LEASE_LOW_WATER_PERCENT", 20)
	leaseLowWaterPct := float64(leaseLowWaterPctRaw) / 100.0
	hubGRPCAddr := config.String("HUB_GRPC_ADDR", "localhost:9090")
	hubGRPCServerName := config.String("HUB_GRPC_SERVER_NAME", "")
	edgeTLSCertFile := config.String("EDGE_TLS_CERT_FILE", "")
	edgeTLSKeyFile := config.String("EDGE_TLS_KEY_FILE", "")
	edgeTLSCAFile := config.String("EDGE_TLS_CA_FILE", "")
	natsURL := config.String("NATS_URL", "nats://localhost:4222")
	natsTierUpdatesSubject := config.String("NATS_TIER_UPDATES_SUBJECT", "tier.updates")
	natsEdgeQueue := config.String("NATS_EDGE_QUEUE", "edge-tier-updates")

	// ConfigManager performs initial hydration from Hub
	configMgr, err := edge.NewConfigManagerWithFallback(hubAddr, hubToken, hubHTTPClient, bootstrapConfigFile)
	if err != nil {
		log.Fatalf("Failed to initialize ConfigManager: %v", err)
	}
	if configRefreshSeconds > 0 {
		configMgr.StartAutoRefresh(ctx, time.Duration(configRefreshSeconds)*time.Second)
	}
	configMgr.StartConfigReloadSubscriber(ctx, rdb, configReloadChannel)

	// TierManager caches user tier information
	tierMgr := edge.NewTierManagerWithOptions(
		hubAddr,
		rdb,
		hubUpdatesChannel,
		hubToken,
		hubHTTPClient,
		edge.TierManagerOptions{
			NATSURL:   natsURL,
			NATSSubj:  natsTierUpdatesSubject,
			NATSQueue: natsEdgeQueue,
		},
	)

	if edgeTLSCertFile == "" || edgeTLSKeyFile == "" || edgeTLSCAFile == "" || hubGRPCServerName == "" {
		log.Fatal("EDGE_TLS_CERT_FILE, EDGE_TLS_KEY_FILE, EDGE_TLS_CA_FILE and HUB_GRPC_SERVER_NAME must be set")
	}
	leaseClient, err := edge.NewGRPCQuotaLeaseClient(hubGRPCAddr, hubGRPCServerName, edgeTLSCertFile, edgeTLSKeyFile, edgeTLSCAFile)
	if err != nil {
		log.Fatalf("Failed to initialize lease gRPC client: %v", err)
	}
	defer leaseClient.Close()

	// RateManager handles local rate limiting with hub synchronization
	rateMgr := edge.NewRateManagerWithOptions(
		hubAddr,
		rdb,
		maxDelta,
		edge.RateManagerOptions{
			HardThresholdPct: rateHardThresholdPct,
			LeaseSize:        leaseSize,
			LowWaterPct:      leaseLowWaterPct,
		},
		"",
	)
	rateMgr.SetLeaseClient(leaseClient)

	// Analytics sink can be no-op to keep request path lightweight when disabled.
	var analyticsSink edge.AnalyticsSink = edge.NoOpAnalyticsSink{}
	var analyticsMgr *edge.AnalyticsManager
	if analyticsEnabled {
		analyticsMgr = edge.NewAnalyticsManagerWithNATS(analyticsBufferSize, natsURL, natsAnalyticsSubject)
		analyticsSink = analyticsMgr
	}

	// 4. Initialize Edge Server with all managers
	edgeServer := edge.NewEdgeServer(configMgr, tierMgr, rateMgr, analyticsSink, rdb)
	edgeServer.StartBackgroundWorkers(ctx)

	// 5. Start analytics publisher — forwards captured events to NATS immediately.
	if analyticsEnabled && analyticsMgr != nil {
		analyticsMgr.StartPublisher(ctx)
	}

	// 6. Configure HTTP Server
	port := config.String("PORT", ":8080")
	httpServer := &http.Server{
		Addr:         port,
		Handler:      edgeServer,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 6. Start Server
	go func() {
		log.Printf("Edge Gateway listening on %s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server Error: %v", err)
		}
	}()

	// 7. Wait for shutdown signal
	<-ctx.Done()
	log.Println("Shutting down Edge Gateway...")

	// 8. Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Shutdown Error: %v", err)
	}

	log.Println("Edge Gateway shutdown complete")
}
