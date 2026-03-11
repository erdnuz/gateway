package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"gateway/packages/common/config"
	"gateway/packages/common/types"
	"gateway/packages/hub"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9" // Updated to v9
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	// 1. Context & Signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	configFilePath := config.String("CONFIG_FILE_PATH", "/cmd/config.json")
	if _, statErr := os.Stat(configFilePath); statErr != nil {
		for _, candidate := range []string{"config/policies.json", "cmd/config.json", "/cmd/config.json"} {
			if _, err := os.Stat(candidate); err == nil {
				log.Printf("CONFIG_FILE_PATH not found (%s), falling back to %s", configFilePath, candidate)
				configFilePath = candidate
				break
			}
		}
	}
	cfgManager, err := hub.NewConfigManager(configFilePath)
	if err != nil {
		log.Fatalf("Config load error: %v", err)
	}
	runtimePolicy := cfgManager.Get().Runtime.Effective()
	defaults := types.DefaultRuntimePolicy()
	if len(cfgManager.Get().Runtime.Hub.CORSAllowedHeaders) == 0 {
		log.Printf("warning: runtime.hub.cors_allowed_headers missing; using safe default %v", defaults.Hub.CORSAllowedHeaders)
	}
	if cfgManager.Get().Runtime.Hub.CORSPreflightMaxAge <= 0 {
		log.Printf("warning: runtime.hub.cors_preflight_max_age missing; using safe default %s", defaults.Hub.CORSPreflightMaxAge)
	}
	if len(cfgManager.Get().Runtime.Hub.CORSAllowedMethods) == 0 {
		log.Printf("warning: runtime.hub.cors_allowed_methods missing; using safe default %v", defaults.Hub.CORSAllowedMethods)
	}
	if strings.TrimSpace(cfgManager.Get().Runtime.Hub.APIKeyPattern) == "" {
		log.Printf("warning: runtime.hub.api_key_pattern missing; using safe default %q", defaults.Hub.APIKeyPattern)
	}
	if cfgManager.Get().Runtime.Hub.MaxDelta <= 0 {
		log.Printf("warning: runtime.hub.max_delta missing; using safe default %d", defaults.Hub.MaxDelta)
	}

	// 3. Infrastructure (Redis)
	rdb := redis.NewClient(&redis.Options{Addr: config.String("REDIS_ADDR", "localhost:6379")})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis Ping Error: %v", err)
	}
	if config.Bool("HUB_ENFORCE_REDIS_NOEVICTION", true) {
		if err := enforceNoEvictionPolicy(ctx, rdb); err != nil {
			log.Fatalf("Redis policy check failed: %v", err)
		}
	}

	hubAuthToken := strings.TrimSpace(config.String("HUB_AUTH_TOKEN", ""))
	if hubAuthToken == "" {
		log.Fatal("HUB_AUTH_TOKEN must be set: hub only accepts authenticated edge requests")
	}
	hubUpdatesChannel := config.String("HUB_UPDATES_CHANNEL", types.DefaultHubUpdatesChannel)
	natsURL := config.String("NATS_URL", "nats://localhost:4222")
	natsTierUpdatesSubject := config.String("NATS_TIER_UPDATES_SUBJECT", runtimePolicy.Hub.TierUpdatesSubject)

	tierStore := hub.NewTierManager(rdb)

	rateManager := hub.NewRateManagerWithOptions(
		rdb,
		cfgManager,
		hub.RateManagerOptions{},
	)
	_ = rateManager

	// 5. Initialize Server
	server := hub.NewHubServerWithManagers(
		rdb,
		cfgManager,
		tierStore,
		rateManager,
		hubAuthToken,
		config.Int64("MAX_DELTA", runtimePolicy.Hub.MaxDelta),
		hubUpdatesChannel,
	)
	server.SetCORSAllowedOrigins(splitCSV(config.String("HUB_EDGE_ALLOWED_ORIGINS", "http://localhost:8082")))
	server.SetCORSPreflightPolicy(runtimePolicy.Hub.CORSAllowedHeaders, runtimePolicy.Hub.CORSAllowedMethods, runtimePolicy.Hub.CORSPreflightMaxAge)
	if err := server.SetAPIKeyPattern(runtimePolicy.Hub.APIKeyPattern); err != nil {
		log.Fatalf("invalid runtime.hub.api_key_pattern: %v", err)
	}
	server.SetConfigReloadChannel(config.String("HUB_CONFIG_RELOAD_CHANNEL", types.DefaultConfigReloadChannel))
	server.SetAsyncQueueConfig(
		config.Int("HUB_QUEUE_WORKERS", runtimePolicy.Hub.QueueWorkers),
		config.Duration("HUB_QUEUE_SUBMIT_TIMEOUT", runtimePolicy.Hub.QueueSubmitTimeout),
		config.Int("HUB_QUEUE_RETRY_MAX", runtimePolicy.Hub.QueueRetryMax),
		config.Duration("HUB_QUEUE_RETRY_BACKOFF", runtimePolicy.Hub.QueueRetryBackoff),
	)
	server.SetTierUpdateMessaging(natsURL, natsTierUpdatesSubject)
	server.StartBackgroundWorkers(ctx)

	grpcAddr := ":" + config.String("HUB_GRPC_PORT", "9090")
	hubTLSCertFile := config.String("HUB_TLS_CERT_FILE", "")
	hubTLSKeyFile := config.String("HUB_TLS_KEY_FILE", "")
	hubTLSCAFile := config.String("HUB_TLS_CA_FILE", "")
	hubGRPCClientAuthMode := strings.TrimSpace(config.String("HUB_GRPC_CLIENT_AUTH_MODE", "require-and-verify"))
	if hubTLSCertFile == "" || hubTLSKeyFile == "" || hubTLSCAFile == "" {
		log.Fatal("HUB_TLS_CERT_FILE, HUB_TLS_KEY_FILE and HUB_TLS_CA_FILE must be set")
	}
	grpcServer, grpcErr := newHubGRPCServer(hubTLSCertFile, hubTLSKeyFile, hubTLSCAFile, hubGRPCClientAuthMode)
	if grpcErr != nil {
		log.Fatalf("Failed to create hub grpc server: %v", grpcErr)
	}
	leaseServer := hub.NewQuotaLeaseServer(rdb, cfgManager, tierStore)
	types.RegisterQuotaLeaseServiceServer(grpcServer, leaseServer)
	grpcListener, grpcErr := net.Listen("tcp", grpcAddr)
	if grpcErr != nil {
		log.Fatalf("Failed to listen on grpc addr %s: %v", grpcAddr, grpcErr)
	}
	go func() {
		log.Printf("Hub gRPC lease server listening on %s", grpcAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Printf("hub grpc server stopped: %v", err)
		}
	}()

	// 7. Server Execution
	port := strings.TrimSpace(config.String("PORT", "8080"))
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}
	httpServer := &http.Server{
		Addr:    port,
		Handler: server,
		// Set timeouts so hung clients don't eat your resources
		ReadTimeout:  runtimePolicy.Hub.HTTPReadTimeout,
		WriteTimeout: runtimePolicy.Hub.HTTPWriteTimeout,
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), runtimePolicy.Hub.HTTPShutdownTimeout)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Graceful shutdown failed: %v", err)
	}
	grpcServer.GracefulStop()
	log.Println("Hub exited cleanly")
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func enforceNoEvictionPolicy(ctx context.Context, rdb *redis.Client) error {
	res, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		return fmt.Errorf("config get maxmemory-policy: %w", err)
	}
	policy := strings.TrimSpace(res["maxmemory-policy"])
	if policy == "noeviction" {
		return nil
	}
	if err := rdb.ConfigSet(ctx, "maxmemory-policy", "noeviction").Err(); err != nil {
		return fmt.Errorf("expected maxmemory-policy=noeviction, got=%q and failed to set: %w", policy, err)
	}
	log.Printf("redis maxmemory-policy changed from %q to noeviction", policy)
	return nil
}

func newHubGRPCServer(certFile, keyFile, caFile, clientAuthMode string) (*grpc.Server, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to parse hub CA certificate")
	}
	clientAuth := tls.RequireAndVerifyClientCert
	switch strings.ToLower(strings.TrimSpace(clientAuthMode)) {
	case "", "require-and-verify":
		clientAuth = tls.RequireAndVerifyClientCert
	case "require-any":
		log.Printf("warning: HUB_GRPC_CLIENT_AUTH_MODE=require-any (development mode, certificate chain/EKU not verified)")
		clientAuth = tls.RequireAnyClientCert
	case "none", "no-client-cert":
		log.Printf("warning: HUB_GRPC_CLIENT_AUTH_MODE=none (development mode, client certificate auth disabled)")
		clientAuth = tls.NoClientCert
	default:
		return nil, fmt.Errorf("invalid HUB_GRPC_CLIENT_AUTH_MODE=%q (valid: require-and-verify, require-any, none)", clientAuthMode)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   clientAuth,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}
	return grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCfg)),
	), nil
}
