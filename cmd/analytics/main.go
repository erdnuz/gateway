package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	server := analyticsapi.NewServer(
		rdb,
		config.String("ANALYTICS_REDIS_KEY", types.DefaultAnalyticsKey),
		config.String("ANALYTICS_API_TOKEN", ""),
	)

	natsURL := config.String("NATS_URL", "nats://localhost:4222")
	natsSubject := config.String("NATS_ANALYTICS_SUBJECT", "analytics.events")
	natsQueue := config.String("NATS_ANALYTICS_QUEUE", "analytics-subscribers")
	if err := server.StartNATSSubscriber(ctx, natsURL, natsSubject, natsQueue); err != nil {
		log.Fatalf("analytics nats subscriber start failed: %v", err)
	}

	httpServer := &http.Server{
		Addr:         config.String("PORT", ":8091"),
		Handler:      server,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	go func() {
		log.Printf("analytics api listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("analytics api error: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}
