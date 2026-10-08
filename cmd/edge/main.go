package main

import (
	"context"
	"fmt"
	"gateway/packages/common/config"
	"gateway/packages/common/ready"
	"gateway/packages/common/startup"
	"gateway/packages/common/types"
	"gateway/packages/edge"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. Context & Signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupChecks := newEdgeStartupChecks()
	if err := startupChecks.ValidateConfig(ctx); err != nil {
		startup.FailFast("edge", "config_validation", err, "fix edge env/config before startup")
	}

	// 2. Infrastructure (Redis)
	rdb := redis.NewClient(&redis.Options{Addr: startupChecks.redisAddr})
	defer rdb.Close()

	// 3. Initialize Managers
	hubAddr := config.String("HUB_ADDR", "http://localhost:8081")
	hubToken := config.String("HUB_AUTH_TOKEN", "")
	hubUpdatesChannel := config.String("HUB_UPDATES_CHANNEL", types.DefaultHubUpdatesChannel)
	analyticsEnabled := config.Bool("GATE_ANALYTICS_ENABLED", config.Bool("ANALYTICS_ENABLED", true))
	natsAnalyticsSubject := config.String("NATS_ANALYTICS_SUBJECT", "")
	hubHTTPTimeout := config.DurationSeconds("HUB_HTTP_TIMEOUT_SECONDS", int(types.DefaultRuntimePolicy().Edge.HubHTTPTimeout.Seconds()))
	hubHTTPClient := edge.NewHubHTTPClient(hubHTTPTimeout)
	configRefreshSeconds := config.Int("EDGE_CONFIG_REFRESH_SECONDS", 0)
	configReloadChannel := config.String("EDGE_CONFIG_RELOAD_CHANNEL", types.DefaultConfigReloadChannel)
	bootstrapConfigFile := config.String("EDGE_BOOTSTRAP_CONFIG_FILE", "")
	leaseSize := config.Int64("EDGE_LEASE_SIZE", 0)
	leaseLowWaterPctRaw := config.Int("EDGE_LEASE_LOW_WATER_PERCENT", 0)
	leaseLowWaterPct := float64(leaseLowWaterPctRaw) // 0–100 percentage
	hubGRPCAddr := config.String("HUB_GRPC_ADDR", "localhost:9090")
	hubGRPCServerName := config.String("HUB_GRPC_SERVER_NAME", "")
	edgeTLSCertFile := config.String("EDGE_TLS_CERT_FILE", "")
	edgeTLSKeyFile := config.String("EDGE_TLS_KEY_FILE", "")
	edgeTLSCAFile := config.String("EDGE_TLS_CA_FILE", "")
	natsURL := config.String("NATS_URL", "nats://localhost:4222")
	natsTierURL := config.String("NATS_TIER_URL", natsURL)
	natsAnalyticsURL := config.String("NATS_ANALYTICS_URL", natsURL)
	natsTierUpdatesSubject := config.String("NATS_TIER_UPDATES_SUBJECT", "")
	configReloadSubject := config.String("EDGE_CONFIG_RELOAD_SUBJECT", types.DefaultConfigReloadSubject)
	edgeID := config.String("EDGE_ID", "")
	edgeAdminToken := config.String("EDGE_ADMIN_TOKEN", "")
	// memory (default): lease tokens live in process — each edge owns its
	// quota slice, so Redis adds a network hop and nothing else.
	// redis: share lease tokens between edge replicas that share one Redis.
	leaseStore := strings.ToLower(strings.TrimSpace(config.String("EDGE_LEASE_STORE", "memory")))

	// ConfigManager performs initial hydration from Hub
	configMgr, err := edge.NewConfigManagerWithFallback(hubAddr, hubToken, hubHTTPClient, bootstrapConfigFile)
	if err != nil {
		log.Fatalf("Failed to initialize ConfigManager: %v", err)
	}
	edgePolicy := configMgr.EdgePolicy()
	analyticsPolicy := configMgr.AnalyticsPolicy()
	if natsAnalyticsSubject == "" {
		natsAnalyticsSubject = analyticsPolicy.NATSSubject
	}
	if natsTierUpdatesSubject == "" {
		natsTierUpdatesSubject = configMgr.TierUpdatesSubject()
	}
	hubHTTPTimeout = edgePolicy.HubHTTPTimeout
	hubHTTPClient = edge.NewHubHTTPClient(hubHTTPTimeout)
	configMgr.SetHTTPClient(hubHTTPClient)
	if configRefreshSeconds > 0 {
		configMgr.StartAutoRefresh(ctx, time.Duration(configRefreshSeconds)*time.Second)
	}
	configMgr.StartConfigReloadSubscriber(ctx, rdb, configReloadChannel)
	if reloadNC, err := nats.Connect(natsTierURL, nats.MaxReconnects(-1)); err != nil {
		log.Printf("edge config reload nats connect failed url=%s err=%v (reloads will not propagate)", natsTierURL, err)
	} else {
		defer reloadNC.Close()
		if err := configMgr.StartConfigReloadNATSSubscriber(ctx, reloadNC, configReloadSubject, rdb); err != nil {
			log.Printf("edge config reload nats subscribe failed subject=%s err=%v", configReloadSubject, err)
		}
	}

	// TierManager caches user tier information
	tierMgr := edge.NewTierManagerWithOptions(
		hubAddr,
		rdb,
		hubUpdatesChannel,
		hubToken,
		hubHTTPClient,
		edge.TierManagerOptions{
			NATSURL:  natsTierURL,
			NATSSubj: natsTierUpdatesSubject,
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

	// RateManager handles local rate limiting with hub-granted leases. Lease
	// size and quota window are resolved per prefix on each request.
	var leaseRedis *redis.Client
	switch leaseStore {
	case "memory", "":
	case "redis":
		leaseRedis = rdb
	default:
		log.Fatalf("invalid EDGE_LEASE_STORE=%q (valid: memory, redis)", leaseStore)
	}
	rateMgr := edge.NewRateManagerWithOptions(
		leaseRedis,
		edge.RateManagerOptions{
			LeaseSize:   chooseLeaseSize(leaseSize, edgePolicy.RateLeaseSize),
			LowWaterPct: chooseLowWaterPct(leaseLowWaterPct, edgePolicy.RateLowWaterPct),
			MinTokens:   1,
		},
	)
	rateMgr.SetLeaseClient(leaseClient)

	// Analytics sink can be no-op to keep request path lightweight when disabled.
	var analyticsSink edge.AnalyticsSink = edge.NoOpAnalyticsSink{}
	var analyticsMgr *edge.AnalyticsManager
	if analyticsEnabled {
		analyticsMgr = edge.NewAnalyticsManagerWithNATSOptions(
			edgePolicy.AnalyticsBufferSize,
			natsAnalyticsURL,
			natsAnalyticsSubject,
			edge.AnalyticsManagerOptions{PublishTimeout: edgePolicy.AnalyticsPublishTimeout},
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
			UpstreamClientTimeout:     edgePolicy.UpstreamClientTimeout,
			MaxCacheableResponseBytes: edgePolicy.CacheMaxObjectBytes,
			UpstreamMaxIdleConns:      edgePolicy.UpstreamMaxIdleConns,
			UpstreamIdleConnTimeout:   edgePolicy.UpstreamIdleConnTimeout,
			EdgeID:                    edgeID,
			AdminToken:                edgeAdminToken,
		},
	)
	edgeServer.StartBackgroundWorkers(ctx)
	edgeServer.SetStartupReady(false)
	edgeServer.SetPendingDependency("hub_readyz")

	// 5. Start analytics publisher — forwards captured events to NATS immediately.
	if analyticsEnabled && analyticsMgr != nil {
		analyticsMgr.StartPublisher(ctx)
	}

	// 6. Configure HTTP Server
	port := config.String("PORT", ":8080")
	httpServer := &http.Server{
		Addr:              port,
		Handler:           edgeServer,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       edgePolicy.HTTPReadTimeout,
		WriteTimeout:      edgePolicy.HTTPWriteTimeout,
		IdleTimeout:       edgePolicy.HTTPIdleTimeout,
	}

	// 6. Start Server
	go func() {
		log.Printf("Edge Gateway listening on %s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server Error: %v", err)
		}
	}()

	hubReadyURL := strings.TrimSuffix(startupChecks.hubAddr, "/") + "/readyz"
	httpClient := &http.Client{Timeout: 2 * time.Second}
	edgeWatcher := ready.NewReadyWatcher("edge", 500*time.Millisecond, 5*time.Second)
	checks := []ready.Check{
		{
			Name: "hub_readyz",
			URL:  hubReadyURL,
			Probe: func(probeCtx context.Context) error {
				req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, hubReadyURL, nil)
				if err != nil {
					edgeServer.SetPendingDependency("hub_readyz")
					return err
				}
				if startupChecks.hubToken != "" {
					req.Header.Set("Authorization", "Bearer "+startupChecks.hubToken)
				}
				resp, err := httpClient.Do(req)
				if err != nil {
					edgeServer.SetPendingDependency("hub_readyz")
					return err
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					edgeServer.SetPendingDependency("hub_readyz")
					return fmt.Errorf("status=%d", resp.StatusCode)
				}
				return nil
			},
		},
	}
	// Hub readiness gates startup only. Afterwards /readyz tracks edge-local
	// dependencies: a hub or analytics outage is handled per request by the
	// failure policies (safety tier, stale tiers) instead of taking every edge
	// out of rotation at once.
	watchCtx, stopWatch := context.WithCancel(ctx)
	go edgeWatcher.WatchAll(watchCtx, checks, func(isReady bool) {
		if !isReady {
			return
		}
		stopWatch()
		edgeServer.SetPendingDependency("")
		edgeServer.SetStartupReady(true)
		edgeServer.StartReadinessProbe(ctx)
		log.Printf("edge startup complete; readiness now tracks edge-local dependencies")
	})

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

func chooseLowWaterPct(envValue, policyValue float64) float64 {
	if envValue > 0 && envValue <= 100 {
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
