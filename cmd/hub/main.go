package main

import (
	"context"
	"gateway/internal/hub"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	// 2. Infrastructure (Mongo)
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(getEnv("MONGO_URI", "mongodb://localhost:27017")))
	if err != nil {
		log.Fatalf("❌ Mongo Connect Error: %v", err)
	}
	// CRITICAL: Ensure Mongo is actually reachable
	if err := mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("❌ Mongo Ping Error: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())
	db := mongoClient.Database(getEnv("DB_NAME", "gateway_db"))

	// 3. Infrastructure (Redis)
	rdb := redis.NewClient(&redis.Options{Addr: getEnv("REDIS_ADDR", "localhost:6379")})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis Ping Error: %v", err)
	}

	// 5. Initialize Server
	server := hub.NewHubServer(db, rdb,
		os.Getenv("ANALYTICS_URL"),
		os.Getenv("ANALYTICS_TOKEN"),
		os.Getenv("ANALYTICS_ORG"),
		os.Getenv("ANALYTICS_BUCKET"),
	)

	// 7. Server Execution
	httpServer := &http.Server{
		Addr:    ":" + getEnv("PORT", "8080"),
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
func getEnv(key, fallback string) string {

	if value, ok := os.LookupEnv(key); ok {

		return value

	}

	return fallback

}
