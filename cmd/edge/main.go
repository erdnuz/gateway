package main

import (
	"context"
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
	rdb := redis.NewClient(&redis.Options{Addr: getEnv("REDIS_ADDR", "localhost:6379")})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis Ping Error: %v", err)
	}
	defer rdb.Close()

	// 3. Initialize Managers
	// ConfigManager performs initial hydration from Hub
	configMgr, err := edge.NewConfigManager(getEnv("HUB_ADDR", "http://localhost:8081"))
	if err != nil {
		log.Fatalf("Failed to initialize ConfigManager: %v", err)
	}

	// TierManager caches user tier information
	tierMgr := edge.NewTierManager(getEnv("HUB_ADDR", "http://localhost:8081"), rdb)

	// RateManager handles local rate limiting with hub synchronization
	rateMgr := edge.NewRateManager(
		getEnv("HUB_ADDR", "http://localhost:8081"),
		rdb,
		100, // maxDelta: sync after this many local increments
	)

	// AnalyticsManager buffers and sends analytics to the hub
	analyticsMgr := edge.NewAnalyticsManager(1000)

	// 4. Initialize Edge Server with all managers
	edgeServer := edge.NewEdgeServer(configMgr, tierMgr, rateMgr, analyticsMgr, rdb)

	// 5. Configure HTTP Server
	port := getEnv("PORT", ":8080")
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

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
