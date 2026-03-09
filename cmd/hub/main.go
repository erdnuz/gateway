package main

import (
	"context"
	"gateway/packages/common/config"
	"gateway/packages/common/types"
	"gateway/packages/hub"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9" // Updated to v9
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// 1. Context & Signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	configFilePath := config.String("CONFIG_FILE_PATH", "/cmd/config.json")
	cfgManager, err := hub.NewConfigManager(configFilePath)
	if err != nil {
		log.Fatalf("Config load error: %v", err)
	}

	// 3. Infrastructure (Redis)
	rdb := redis.NewClient(&redis.Options{Addr: config.String("REDIS_ADDR", "localhost:6379")})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis Ping Error: %v", err)
	}

	hubAuthToken := strings.TrimSpace(config.String("HUB_AUTH_TOKEN", ""))
	if hubAuthToken == "" {
		log.Fatal("HUB_AUTH_TOKEN must be set: hub only accepts authenticated edge requests")
	}
	rateUpdateChannel := config.String("RATE_UPDATE_CHANNEL", types.DefaultRateUpdateChannel)
	rateSOTChannel := config.String("RATE_SOT_CHANNEL", types.DefaultRateSOTChannel)
	hubUpdatesChannel := config.String("HUB_UPDATES_CHANNEL", types.DefaultHubUpdatesChannel)
	kafkaBrokers := strings.Split(config.String("KAFKA_BROKERS", "kafka:9092"), ",")
	kafkaRateTopic := config.String("KAFKA_RATE_TOPIC", "rate-updates")
	kafkaRateGroup := config.String("KAFKA_RATE_GROUP", "hub-rate-consumers")
	kafkaSOTTopic := config.String("KAFKA_SOT_TOPIC", "rate-sot")
	kafkaTierUpdatesTopic := config.String("KAFKA_TIER_UPDATES_TOPIC", "tier-updates")

	tierStoreMode := strings.TrimSpace(config.String("HUB_TIER_STORE", "mongo"))
	var tierStore hub.TierStore
	if tierStoreMode == "memory" {
		tierStore = hub.NewInMemoryTierStore()
	} else {
		mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(config.String("MONGO_URI", "mongodb://localhost:27017")))
		if err != nil {
			log.Fatalf("Mongo Connect Error: %v", err)
		}
		if err := mongoClient.Ping(ctx, nil); err != nil {
			log.Fatalf("Mongo Ping Error: %v", err)
		}
		defer mongoClient.Disconnect(context.Background())
		db := mongoClient.Database(config.String("DB_NAME", "gateway_db"))
		tierStore = hub.NewTierManager(db, rdb)
	}

	rateManager := hub.NewRateManagerWithOptions(
		rdb,
		cfgManager,
		hub.RateManagerOptions{
			KafkaBrokers:  kafkaBrokers,
			KafkaTopic:    kafkaRateTopic,
			KafkaGroupID:  kafkaRateGroup,
			KafkaSOTTopic: kafkaSOTTopic,
		},
		rateUpdateChannel,
		rateSOTChannel,
	)

	// 5. Initialize Server
	server := hub.NewHubServerWithManagers(
		rdb,
		cfgManager,
		tierStore,
		rateManager,
		hubAuthToken,
		config.Int64("MAX_DELTA", 10000),
		hubUpdatesChannel,
	)
	server.SetAsyncQueueConfig(
		config.Int("HUB_QUEUE_WORKERS", 2),
		config.Duration("HUB_QUEUE_SUBMIT_TIMEOUT", 25*time.Millisecond),
		config.Int("HUB_QUEUE_RETRY_MAX", 1),
		config.Duration("HUB_QUEUE_RETRY_BACKOFF", 10*time.Millisecond),
	)
	server.SetTierUpdateMessaging(kafkaBrokers, kafkaTierUpdatesTopic)
	server.StartBackgroundWorkers(ctx)

	// 7. Server Execution
	httpServer := &http.Server{
		Addr:    ":" + config.String("PORT", "8080"),
		Handler: server,
		// Set timeouts so hung clients don't eat your resources
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Hub Server starting on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// 8. Graceful Shutdown
	<-ctx.Done()
	log.Println("Shutdown signal received...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Graceful shutdown failed: %v", err)
	}
	log.Println("Hub exited cleanly")
}
