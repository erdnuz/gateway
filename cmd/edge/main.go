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
	analyticsEnabled := config.Bool("GATE_ANALYTICS_ENABLED", config.Bool("ANALYTICS_ENABLED", true))
	natsAnalyticsSubject := config.String("NATS_ANALYTICS_SUBJECT", "")
	hubHTTPTimeout := config.DurationSeconds("HUB_HTTP_TIMEOUT_SECONDS", int(types.DefaultRuntimePolicy().Edge.HubHTTPTimeout.Seconds()))
	hubHTTPClient := edge.NewHubHTTPClient(hubHTTPTimeout)
	configRefreshSeconds := config.Int("EDGE_CONFIG_REFRESH_SECONDS", 0)
	configReloadChannel := config.String("EDGE_CONFIG_RELOAD_CHANNEL", types.DefaultConfigReloadChannel)
	bootstrapConfigFile := config.String("EDGE_BOOTSTRAP_CONFIG_FILE", "")
	rateHardThresholdPctRaw := config.Int("RATE_HARD_THRESHOLD_PERCENT", 0)
	rateHardThresholdPct := float64(rateHardThresholdPctRaw) / 100.0
	leaseSize := config.Int64("EDGE_LEASE_SIZE", 0)
	leaseLowWaterPctRaw := config.Int("EDGE_LEASE_LOW_WATER_PERCENT", 0)
	leaseLowWaterPct := float64(leaseLowWaterPctRaw) / 100.0
	hubGRPCAddr := config.String("HUB_GRPC_ADDR", "localhost:9090")
	hubGRPCServerName := config.String("HUB_GRPC_SERVER_NAME", "")
	edgeTLSCertFile := config.String("EDGE_TLS_CERT_FILE", "")
	edgeTLSKeyFile := config.String("EDGE_TLS_KEY_FILE", "")
	edgeTLSCAFile := config.String("EDGE_TLS_CA_FILE", "")
	natsURL := config.String("NATS_URL", "nats://localhost:4222")
	natsTierUpdatesSubject := config.String("NATS_TIER_UPDATES_SUBJECT", "")
	natsEdgeQueue := config.String("NATS_EDGE_QUEUE", "edge-tier-updates")

	// ConfigManager performs initial hydration from Hub
	configMgr, err := edge.NewConfigManagerWithFallback(hubAddr, hubToken, hubHTTPClient, bootstrapConfigFile)
	if err != nil {
		log.Fatalf("Failed to initialize ConfigManager: %v", err)
	}
	runtimePolicy := configMgr.Get().Runtime.Effective()
	runtimeDefaults := types.DefaultRuntimePolicy()
	if configMgr.Get().Runtime.Edge.HubHTTPTimeout <= 0 {
		log.Printf("warning: runtime.edge.hub_http_timeout missing; using safe default %s", runtimeDefaults.Edge.HubHTTPTimeout)
	}
	if configMgr.Get().Runtime.Edge.AnalyticsBufferSize <= 0 {
		log.Printf("warning: runtime.edge.analytics_buffer_size missing; using safe default %d", runtimeDefaults.Edge.AnalyticsBufferSize)
	}
	if configMgr.Get().Runtime.Edge.AnalyticsPublishTimeout <= 0 {
		log.Printf("warning: runtime.edge.analytics_publish_timeout missing; using safe default %s", runtimeDefaults.Edge.AnalyticsPublishTimeout)
	}
	if configMgr.Get().Runtime.Edge.RateHardThresholdPct <= 0 {
		log.Printf("warning: runtime.edge.rate_hard_threshold_pct missing; using safe default %.2f", runtimeDefaults.Edge.RateHardThresholdPct)
	}
	if configMgr.Get().Runtime.Edge.RateLeaseSize <= 0 {
		log.Printf("warning: runtime.edge.rate_lease_size missing; using safe default %d", runtimeDefaults.Edge.RateLeaseSize)
	}
	if configMgr.Get().Runtime.Edge.RateLowWaterPct <= 0 {
		log.Printf("warning: runtime.edge.rate_low_water_pct missing; using safe default %.2f", runtimeDefaults.Edge.RateLowWaterPct)
	}
	if configMgr.Get().Runtime.Edge.UpstreamClientTimeout <= 0 || configMgr.Get().Runtime.Edge.CacheMaxObjectBytes <= 0 {
		log.Printf("warning: runtime.edge upstream/cache limits missing; using safe defaults where needed")
	}
	if configMgr.Get().Runtime.Edge.UpstreamMaxIdleConns <= 0 || configMgr.Get().Runtime.Edge.UpstreamIdleConnTimeout <= 0 {
		log.Printf("warning: runtime.edge upstream connection pool settings missing; using safe defaults where needed")
	}
	if configMgr.Get().Runtime.Edge.HTTPReadTimeout <= 0 || configMgr.Get().Runtime.Edge.HTTPWriteTimeout <= 0 || configMgr.Get().Runtime.Edge.HTTPIdleTimeout <= 0 {
		log.Printf("warning: runtime.edge http timeouts missing; using safe defaults where needed")
	}
	if natsAnalyticsSubject == "" {
		natsAnalyticsSubject = runtimePolicy.Analytics.NATSSubject
		if configMgr.Get().Runtime.Analytics.NATSSubject == "" {
			log.Printf("warning: runtime.analytics.nats_subject missing; using safe default %s", runtimeDefaults.Analytics.NATSSubject)
		}
	}
	if natsTierUpdatesSubject == "" {
		natsTierUpdatesSubject = runtimePolicy.Hub.TierUpdatesSubject
		if configMgr.Get().Runtime.Hub.TierUpdatesSubject == "" {
			log.Printf("warning: runtime.hub.tier_updates_subject missing; using safe default %s", runtimeDefaults.Hub.TierUpdatesSubject)
		}
	}
	hubHTTPTimeout = runtimePolicy.Edge.HubHTTPTimeout
	hubHTTPClient = edge.NewHubHTTPClient(hubHTTPTimeout)
	configMgr.SetHTTPClient(hubHTTPClient)
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
			HardThresholdPct: chooseRatePct(rateHardThresholdPct, runtimePolicy.Edge.RateHardThresholdPct),
			LeaseSize:        chooseLeaseSize(leaseSize, runtimePolicy.Edge.RateLeaseSize),
			LowWaterPct:      chooseRatePct(leaseLowWaterPct, runtimePolicy.Edge.RateLowWaterPct),
		},
		"",
	)
	rateMgr.SetLeaseClient(leaseClient)

	// Analytics sink can be no-op to keep request path lightweight when disabled.
	var analyticsSink edge.AnalyticsSink = edge.NoOpAnalyticsSink{}
	var analyticsMgr *edge.AnalyticsManager
	if analyticsEnabled {
		analyticsMgr = edge.NewAnalyticsManagerWithNATSOptions(
			runtimePolicy.Edge.AnalyticsBufferSize,
			natsURL,
			natsAnalyticsSubject,
			edge.AnalyticsManagerOptions{PublishTimeout: runtimePolicy.Edge.AnalyticsPublishTimeout},
		)
		analyticsSink = analyticsMgr
	}

	// 4. Initialize Edge Server with all managers
	edgeServer := edge.NewEdgeServerWithOptions(
		configMgr,
		tierMgr,
		rateMgr,
		analyticsSink,
		rdb,
		edge.EdgeServerOptions{
			UpstreamClientTimeout:     runtimePolicy.Edge.UpstreamClientTimeout,
			MaxCacheableResponseBytes: runtimePolicy.Edge.CacheMaxObjectBytes,
			UpstreamMaxIdleConns:      runtimePolicy.Edge.UpstreamMaxIdleConns,
			UpstreamIdleConnTimeout:   runtimePolicy.Edge.UpstreamIdleConnTimeout,
		},
	)
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
		ReadTimeout:  runtimePolicy.Edge.HTTPReadTimeout,
		WriteTimeout: runtimePolicy.Edge.HTTPWriteTimeout,
		IdleTimeout:  runtimePolicy.Edge.HTTPIdleTimeout,
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

func chooseRatePct(envValue, policyValue float64) float64 {
	if envValue > 0 && envValue < 1 {
		return envValue
	}
	return policyValue
}

func chooseLeaseSize(envValue, policyValue int64) int64 {
	if envValue > 0 {
		return envValue
	}
	return policyValue
}
