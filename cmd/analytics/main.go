package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	analyticsapi "gateway/packages/analytics"
	"gateway/packages/common/config"
	"gateway/packages/common/types"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runtimePolicy := loadAnalyticsRuntimePolicy(ctx)
	store, err := analyticsapi.NewClickHouseAnalyticsStore(
		config.String("CLICKHOUSE_DSN", "clickhouse://localhost:9000/default"),
		config.String("ANALYTICS_CLICKHOUSE_TABLE", "analytics_events"),
	)
	if err != nil {
		log.Fatalf("clickhouse store init failed: %v", err)
	}
	defer store.Close()
	if err := store.Ping(ctx); err != nil {
		log.Fatalf("clickhouse ping failed: %v", err)
	}

	server := analyticsapi.NewServerWithAnalyticsStore(
		store,
		config.String("ANALYTICS_API_TOKEN", ""),
		runtimePolicy,
	)

	natsURL := config.String("NATS_URL", "nats://localhost:4222")
	natsAnalyticsURL := config.String("NATS_ANALYTICS_URL", natsURL)
	natsSubject := config.String("NATS_ANALYTICS_SUBJECT", runtimePolicy.NATSSubject)
	natsQueue := config.String("NATS_ANALYTICS_QUEUE", runtimePolicy.NATSQueue)
	if err := server.StartNATSSubscriber(ctx, natsAnalyticsURL, natsSubject, natsQueue); err != nil {
		log.Fatalf("analytics nats subscriber start failed: %v", err)
	}
	server.StartBackgroundAggregator(ctx, 10*time.Second)

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
			log.Fatalf("analytics api error: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), runtimePolicy.ShutdownTimeout)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

func loadAnalyticsRuntimePolicy(ctx context.Context) types.AnalyticsRuntimePolicy {
	defaults := types.DefaultRuntimePolicy().Analytics
	hubAddr := strings.TrimSpace(config.String("HUB_ADDR", ""))
	hubToken := strings.TrimSpace(config.String("HUB_AUTH_TOKEN", ""))
	if hubAddr == "" {
		log.Printf("warning: HUB_ADDR missing; using safe analytics runtime defaults")
		return defaults
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(hubAddr, "/")+"/config", nil)
	if err != nil {
		log.Printf("warning: analytics runtime policy request build failed: %v; using defaults", err)
		return defaults
	}
	if hubToken != "" {
		req.Header.Set("Authorization", "Bearer "+hubToken)
	}

	resp, err := (&http.Client{Timeout: defaults.ConfigFetchTimeout}).Do(req)
	if err != nil {
		log.Printf("warning: analytics runtime policy fetch failed: %v; using defaults", err)
		return defaults
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("warning: analytics runtime policy fetch returned status=%d; using defaults", resp.StatusCode)
		return defaults
	}

	var snapshot analyticsConfigSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		log.Printf("warning: analytics runtime policy decode failed: %v; using defaults", err)
		return defaults
	}
	return snapshot.AnalyticsPolicy()
}

type analyticsConfigSnapshot struct {
	Runtime struct {
		Analytics types.AnalyticsRuntimePolicy `json:"analytics"`
	} `json:"runtime"`
}

func (s analyticsConfigSnapshot) AnalyticsPolicy() types.AnalyticsRuntimePolicy {
	return types.RuntimePolicy{Analytics: s.Runtime.Analytics}.Effective().Analytics
}
