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

	analyticsapi "gateway/packages/analytics"
	"gateway/packages/common/config"
	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: config.String("REDIS_ADDR", "localhost:6379")})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	defer rdb.Close()

	runtimePolicy := loadAnalyticsRuntimePolicy(ctx)
	server := analyticsapi.NewServerWithPolicy(
		rdb,
		config.String("ANALYTICS_REDIS_KEY", types.DefaultAnalyticsKey),
		config.String("ANALYTICS_API_TOKEN", ""),
		runtimePolicy,
	)

	natsURL := config.String("NATS_URL", "nats://localhost:4222")
	natsSubject := config.String("NATS_ANALYTICS_SUBJECT", runtimePolicy.NATSSubject)
	natsQueue := config.String("NATS_ANALYTICS_QUEUE", runtimePolicy.NATSQueue)
	if err := server.StartNATSSubscriber(ctx, natsURL, natsSubject, natsQueue); err != nil {
		log.Fatalf("analytics nats subscriber start failed: %v", err)
	}

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

	var cfg types.GatewayConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		log.Printf("warning: analytics runtime policy decode failed: %v; using defaults", err)
		return defaults
	}

	effective := cfg.Runtime.Effective().Analytics
	if cfg.Runtime.Analytics.ReadTimeout <= 0 || cfg.Runtime.Analytics.WriteTimeout <= 0 || cfg.Runtime.Analytics.IdleTimeout <= 0 {
		log.Printf("warning: runtime.analytics timeouts missing/invalid; using safe defaults where needed")
	}
	if cfg.Runtime.Analytics.DefaultEventsLimit <= 0 || cfg.Runtime.Analytics.MaxEventsLimit <= 0 || cfg.Runtime.Analytics.DefaultSummaryLimit <= 0 || cfg.Runtime.Analytics.MaxSummaryLimit <= 0 {
		log.Printf("warning: runtime.analytics limits missing/invalid; using safe defaults where needed")
	}
	if strings.TrimSpace(cfg.Runtime.Analytics.NATSSubject) == "" || strings.TrimSpace(cfg.Runtime.Analytics.NATSQueue) == "" {
		log.Printf("warning: runtime.analytics nats subject/queue missing; using safe defaults where needed")
	}
	return effective
}
