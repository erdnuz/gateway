package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"gate/internal/limiter"
	"gate/internal/middleware"
	"gate/internal/state"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	ctx := context.Background()

	// 1. Initialize Infrastructure Connections
	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"), // e.g., "gate-redis:6379"
	})

	mClient, err := mongo.Connect(options.Client().ApplyURI(os.Getenv("MONGO_URI")))
	if err != nil {
		log.Fatal("Mongo Connect Error:", err)
	}

	// 2. Initialize Shared Local State (The In-Memory Mirror)
	localState := state.NewLocalState()

	// 3. Initialize Limiter & Sync Worker
	redisLimiter := limiter.NewRedisLimiter(rdb)
	syncWorker := limiter.NewSyncWorker(localState, redisLimiter)

	// Start the Counter Batcher (Syncs every 500ms)
	go syncWorker.Start(ctx, 500*time.Millisecond)

	// 4. Start Redis Subscriber (Listens for Config/User updates)
	go listenForUpdates(ctx, rdb, localState, mClient)

	// 5. Setup Middleware & Server
	mw := &middleware.RateLimitMiddleware{State: localState}

	// Create a simple proxy or dummy handler for testing
	finalHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Request Allowed - Forwarding to Downstream"))
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mw.Handler(finalHandler),
	}

	log.Println("Gate Wrapper starting on :8080...")
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// listenForUpdates watches Redis for "config_reload" or "user_updates" signals
func listenForUpdates(ctx context.Context, rdb *redis.Client, ls *state.LocalState, mc *mongo.Client) {
	pubsub := rdb.Subscribe(ctx, "config_reload", "user_updates")
	defer pubsub.Close()

	for msg := range pubsub.Channel() {
		log.Printf("Received sync signal: %s", msg.Payload)
		// logic to re-fetch specific config from Mongo and update ls.Configs
		// or re-fetch user tier and update ls.UserTiers
	}
}
