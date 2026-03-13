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
	natsEdgeQueue := config.String("NATS_EDGE_QUEUE", "edge-tier-updates")
	edgeID := config.String("EDGE_ID", "")

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

	// TierManager caches user tier information
	tierMgr := edge.NewTierManagerWithOptions(
		hubAddr,
		rdb,
		hubUpdatesChannel,
		hubToken,
		hubHTTPClient,
		edge.TierManagerOptions{
			NATSURL:   natsTierURL,
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
	prefixLeasing := resolvePrefixLeasing(configMgr.Prefixes(), edgePolicy)
	rateMgr := edge.NewRateManagerWithOptions(
		hubAddr,
		rdb,
		maxDelta,
		edge.RateManagerOptions{
			HardThresholdPct: chooseRatePct(rateHardThresholdPct, edgePolicy.RateHardThresholdPct),
			LeaseSize:        chooseLeaseSize(leaseSize, prefixLeasing.LeaseQuantum),
			LowWaterPct:      chooseLowWaterPct(leaseLowWaterPct, edgePolicy.RateLowWaterPct),
			LeaseTTL:         prefixLeasing.LeaseTTL,
			RenewalBuffer:    prefixLeasing.RenewalBuffer,
		},
		"",
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
		Addr:         port,
		Handler:      edgeServer,
		ReadTimeout:  edgePolicy.HTTPReadTimeout,
		WriteTimeout: edgePolicy.HTTPWriteTimeout,
		IdleTimeout:  edgePolicy.HTTPIdleTimeout,
	}

	// 6. Start Server
	go func() {
		log.Printf("Edge Gateway listening on %s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server Error: %v", err)
		}
	}()

	analyticsReadyURL := strings.TrimSuffix(config.String("ANALYTICS_ADDR", "http://localhost:8091"), "/") + "/readyz"
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
	if startupChecks.analyticsEnabled {
		checks = append(checks, ready.Check{
			Name: "analytics_readyz",
			URL:  analyticsReadyURL,
			Probe: func(probeCtx context.Context) error {
				req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, analyticsReadyURL, nil)
				if err != nil {
					edgeServer.SetPendingDependency("analytics_readyz")
					return err
				}
				resp, err := httpClient.Do(req)
				if err != nil {
					edgeServer.SetPendingDependency("analytics_readyz")
					return err
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					edgeServer.SetPendingDependency("analytics_readyz")
					return fmt.Errorf("status=%d", resp.StatusCode)
				}
				return nil
			},
		})
	}
	go edgeWatcher.WatchAll(ctx, checks, func(isReady bool) {
		edgeServer.SetStartupReady(isReady)
		if isReady {
			edgeServer.SetPendingDependency("")
		}
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

func chooseRatePct(envValue, policyValue float64) float64 {
	if envValue > 0 && envValue < 1 {
		return envValue
	}
	return policyValue
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

// resolvePrefixLeasing returns the effective LeasingConfig for the first prefix
// in the gateway config, falling back to global EdgeRuntimePolicy defaults.
func resolvePrefixLeasing(prefixes []types.PrefixConfig, edgePolicy types.EdgeRuntimePolicy) types.LeasingConfig {
	if len(prefixes) == 0 {
		var zero types.LeasingConfig
		return zero.Effective(edgePolicy, 0)
	}
	p := &prefixes[0]
	return p.Leasing.Effective(edgePolicy, p.QuotaPeriod)
}
