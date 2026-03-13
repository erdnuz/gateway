package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	analyticsapi "gateway/packages/analytics"
	"gateway/packages/common/config"
	"gateway/packages/common/ready"
	"gateway/packages/common/startup"
	"gateway/packages/common/types"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runtimePolicy, err := loadAnalyticsRuntimePolicy(ctx)
	if err != nil {
		log.Printf("analytics startup pending: runtime policy fetch failed (%v); using defaults", err)
		runtimePolicy = types.DefaultRuntimePolicy().Analytics
	}

	checks, err := newAnalyticsStartupChecks(runtimePolicy)
	if err != nil {
		startup.FailFast("analytics", "config", err, "ensure analytics .env is complete and valid")
	}
	if err := checks.ValidateConfig(ctx); err != nil {
		startup.FailFast("analytics", "config_validation", err, "ensure analytics env and URLs are valid")
	}

	store, err := analyticsapi.NewClickHouseAnalyticsStore(
		checks.clickHouseDSN,
		checks.clickHouseTable,
	)
	if err != nil {
		startup.FailFast("analytics", "clickhouse_config", err, "verify CLICKHOUSE_DSN and ANALYTICS_CLICKHOUSE_TABLE")
	}
	defer store.Close()

	server := analyticsapi.NewServerWithAnalyticsStore(
		store,
		config.String("ANALYTICS_API_TOKEN", ""),
		runtimePolicy,
	)
	server.SetHubReady(false)
	server.SetPendingDependency("hub_readyz")

	httpServer := &http.Server{
		Addr:         config.String("PORT", ":8091"),
		Handler:      server,
		ReadTimeout:  runtimePolicy.ReadTimeout,
		WriteTimeout: runtimePolicy.WriteTimeout,
		IdleTimeout:  runtimePolicy.IdleTimeout,
	}

	go func() {
		log.Printf("analytics api listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			startup.FailFast("analytics", "http_server", err, "check PORT binding and process permissions")
		}
	}()

	var subscriberStarted atomic.Bool
	analyticsWatcher := ready.NewReadyWatcher("analytics", 500*time.Millisecond, 5*time.Second)
	go analyticsWatcher.WatchAll(ctx, []ready.Check{
		{
			Name: "hub_readyz",
			URL:  strings.TrimSuffix(checks.hubAddr, "/") + "/readyz",
			Probe: func(probeCtx context.Context) error {
				req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, strings.TrimSuffix(checks.hubAddr, "/")+"/readyz", nil)
				if err != nil {
					server.SetPendingDependency("hub_readyz")
					return err
				}
				if checks.hubToken != "" {
					req.Header.Set("Authorization", "Bearer "+checks.hubToken)
				}
				resp, err := (&http.Client{Timeout: checks.hubHTTPTimeout}).Do(req)
				if err != nil {
					server.SetPendingDependency("hub_readyz")
					return err
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					server.SetPendingDependency("hub_readyz")
					return fmt.Errorf("status=%d", resp.StatusCode)
				}
				return nil
			},
		},
		{
			Name: "clickhouse",
			URL:  checks.clickHouseDSN,
			Probe: func(probeCtx context.Context) error {
				if err := store.Ping(probeCtx); err != nil {
					server.SetPendingDependency("clickhouse")
					return err
				}
				return nil
			},
		},
		{
			Name: "nats",
			URL:  checks.natsURL,
			Probe: func(probeCtx context.Context) error {
				_ = probeCtx
				if subscriberStarted.Load() {
					return nil
				}
				if err := server.StartNATSSubscriber(ctx, checks.natsURL, checks.natsSubject, checks.natsQueue); err != nil {
					server.SetPendingDependency("nats")
					return err
				}
				subscriberStarted.Store(true)
				server.StartBackgroundAggregator(ctx, 10*time.Second)
				return nil
			},
		},
	}, func(isReady bool) {
		server.SetHubReady(isReady)
		if isReady {
			server.SetPendingDependency("")
		}
	})

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), runtimePolicy.ShutdownTimeout)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

func loadAnalyticsRuntimePolicy(ctx context.Context) (types.AnalyticsRuntimePolicy, error) {
	defaults := types.DefaultRuntimePolicy().Analytics
	hubAddr := strings.TrimSpace(config.String("HUB_ADDR", ""))
	hubToken := strings.TrimSpace(config.String("HUB_AUTH_TOKEN", ""))
	if hubAddr == "" {
		return defaults, fmt.Errorf("HUB_ADDR is required to fetch runtime policy")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(hubAddr, "/")+"/config", nil)
	if err != nil {
		return defaults, fmt.Errorf("runtime policy request build failed: %w", err)
	}
	if hubToken != "" {
		req.Header.Set("Authorization", "Bearer "+hubToken)
	}

	resp, err := (&http.Client{Timeout: defaults.ConfigFetchTimeout}).Do(req)
	if err != nil {
		return defaults, fmt.Errorf("runtime policy fetch failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return defaults, fmt.Errorf("runtime policy fetch returned status=%d", resp.StatusCode)
	}

	var snapshot analyticsConfigSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return defaults, fmt.Errorf("runtime policy decode failed: %w", err)
	}
	return snapshot.AnalyticsPolicy(), nil
}

type analyticsConfigSnapshot struct {
	Runtime struct {
		Analytics types.AnalyticsRuntimePolicy `json:"analytics"`
	} `json:"runtime"`
}

func (s analyticsConfigSnapshot) AnalyticsPolicy() types.AnalyticsRuntimePolicy {
	return types.RuntimePolicy{Analytics: s.Runtime.Analytics}.Effective().Analytics
}
