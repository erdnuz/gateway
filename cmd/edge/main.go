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
	"strings"
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
	rateUpdateChannel := config.String("RATE_UPDATE_CHANNEL", types.DefaultRateUpdateChannel)
	rateSOTChannel := config.String("RATE_SOT_CHANNEL", types.DefaultRateSOTChannel)
	hubUpdatesChannel := config.String("HUB_UPDATES_CHANNEL", types.DefaultHubUpdatesChannel)
	maxDelta := config.Int64("EDGE_MAX_DELTA", 100)
	analyticsBufferSize := config.Int("ANALYTICS_BUFFER_SIZE", 1000)
	analyticsEnabled := config.Bool("ANALYTICS_ENABLED", true)
	kafkaAnalyticsTopic := config.String("KAFKA_ANALYTICS_TOPIC", "analytics-events")
	hubHTTPTimeout := config.DurationSeconds("HUB_HTTP_TIMEOUT_SECONDS", 5)
	hubHTTPClient := edge.NewHubHTTPClient(hubHTTPTimeout)
	configRefreshSeconds := config.Int("EDGE_CONFIG_REFRESH_SECONDS", 0)
	bootstrapConfigFile := config.String("EDGE_BOOTSTRAP_CONFIG_FILE", "")
	rateQueueCapacity := config.Int("RATE_QUEUE_CAPACITY", 512)
	rateQueueWorkers := config.Int("RATE_QUEUE_WORKERS", 1)
	rateQueueSubmitTimeout := config.Duration("RATE_QUEUE_SUBMIT_TIMEOUT", 25*time.Millisecond)
	rateQueueRetryBackoff := config.Duration("RATE_QUEUE_RETRY_BACKOFF", 10*time.Millisecond)
	rateHardThresholdPctRaw := config.Int("RATE_HARD_THRESHOLD_PERCENT", 90)
	rateHardThresholdPct := float64(rateHardThresholdPctRaw) / 100.0
	rateQueueRetryMax := config.Int("RATE_QUEUE_RETRY_MAX", 1)
	kafkaBrokers := strings.Split(config.String("KAFKA_BROKERS", "kafka:9092"), ",")
	kafkaRateTopic := config.String("KAFKA_RATE_TOPIC", "rate-updates")
	kafkaSOTTopic := config.String("KAFKA_SOT_TOPIC", "rate-sot")
	kafkaTierUpdatesTopic := config.String("KAFKA_TIER_UPDATES_TOPIC", "tier-updates")
	kafkaEdgeGroup := config.String("KAFKA_EDGE_GROUP", "edge-consumers")

	// ConfigManager performs initial hydration from Hub
	configMgr, err := edge.NewConfigManagerWithFallback(hubAddr, hubToken, hubHTTPClient, bootstrapConfigFile)
	if err != nil {
		log.Fatalf("Failed to initialize ConfigManager: %v", err)
	}
	if configRefreshSeconds > 0 {
		configMgr.StartAutoRefresh(ctx, time.Duration(configRefreshSeconds)*time.Second)
	}

	// TierManager caches user tier information
	tierMgr := edge.NewTierManagerWithOptions(
		hubAddr,
		rdb,
		hubUpdatesChannel,
		hubToken,
		hubHTTPClient,
		edge.TierManagerOptions{
			KafkaBrokers: kafkaBrokers,
			KafkaTopic:   kafkaTierUpdatesTopic,
			KafkaGroupID: kafkaEdgeGroup + "-tier",
		},
	)

	// RateManager handles local rate limiting with hub synchronization
	rateMgr := edge.NewRateManagerWithOptions(
		hubAddr,
		rdb,
		maxDelta,
		edge.RateManagerOptions{
			QueueCapacity:    rateQueueCapacity,
			QueueWorkers:     rateQueueWorkers,
			SubmitTimeout:    rateQueueSubmitTimeout,
			RetryMax:         rateQueueRetryMax,
			RetryBackoff:     rateQueueRetryBackoff,
			HardThresholdPct: rateHardThresholdPct,
			HubAuthToken:     hubToken,
			HubHTTPClient:    hubHTTPClient,
			KafkaBrokers:     kafkaBrokers,
			KafkaTopic:       kafkaRateTopic,
			KafkaSOTTopic:    kafkaSOTTopic,
			KafkaGroupID:     kafkaEdgeGroup + "-sot",
		},
		rateUpdateChannel,
		rateSOTChannel,
	)

	// Analytics sink can be no-op to keep request path lightweight when disabled.
	var analyticsSink edge.AnalyticsSink = edge.NoOpAnalyticsSink{}
	var analyticsMgr *edge.AnalyticsManager
	if analyticsEnabled {
		analyticsMgr = edge.NewAnalyticsManagerWithKafka(analyticsBufferSize, kafkaBrokers, kafkaAnalyticsTopic)
		analyticsSink = analyticsMgr
	}

	// 4. Initialize Edge Server with all managers
	edgeServer := edge.NewEdgeServer(configMgr, tierMgr, rateMgr, analyticsSink, rdb)
	edgeServer.StartBackgroundWorkers(ctx)

	// 5. Start analytics publisher — forwards captured events to Kafka immediately.
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
