// Package hubapp assembles and runs the hub (control plane): config, tier
// store, admin HTTP API and the quota-lease gRPC service.
package hubapp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"gateway/internal/app/appkit"
	"gateway/packages/common/config"
	"gateway/packages/common/ready"
	"gateway/packages/common/types"
	"gateway/packages/hub"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Config is everything the hub needs to start. FromEnv fills it from the
// process environment; gate dev builds it directly.
type Config struct {
	HTTPAddr   string // admin/control HTTP API
	GRPCAddr   string // quota-lease gRPC; empty disables it (in-process leasing)
	ConfigPath string
	AuthToken  string

	RedisAddr         string
	EnforceNoEviction bool

	NATSURL             string
	TierUpdatesSubject  string // empty: runtime.hub.tier_updates_subject
	UpdatesChannel      string
	ConfigReloadChannel string
	ConfigReloadSubject string
	EdgeOrigins         []string

	TLSCertFile        string
	TLSKeyFile         string
	TLSCAFile          string
	GRPCClientAuthMode string
}

// FromEnv reads the hub configuration from environment variables.
func FromEnv() Config {
	natsURL := config.String("NATS_URL", nats.DefaultURL)
	return Config{
		HTTPAddr:            appkit.ListenAddr(config.String("PORT", "8080")),
		GRPCAddr:            appkit.ListenAddr(config.String("HUB_GRPC_PORT", "9090")),
		ConfigPath:          strings.TrimSpace(config.String("CONFIG_FILE_PATH", "")),
		AuthToken:           strings.TrimSpace(config.String("HUB_AUTH_TOKEN", "")),
		RedisAddr:           config.String("REDIS_ADDR", "localhost:6379"),
		EnforceNoEviction:   config.Bool("HUB_ENFORCE_REDIS_NOEVICTION", true),
		NATSURL:             config.String("NATS_TIER_URL", natsURL),
		TierUpdatesSubject:  strings.TrimSpace(config.String("NATS_TIER_UPDATES_SUBJECT", "")),
		UpdatesChannel:      config.String("HUB_UPDATES_CHANNEL", types.DefaultHubUpdatesChannel),
		ConfigReloadChannel: config.String("HUB_CONFIG_RELOAD_CHANNEL", types.DefaultConfigReloadChannel),
		ConfigReloadSubject: config.String("HUB_CONFIG_RELOAD_SUBJECT", types.DefaultConfigReloadSubject),
		EdgeOrigins:         appkit.SplitCSV(config.String("HUB_EDGE_ALLOWED_ORIGINS", "")),
		TLSCertFile:         strings.TrimSpace(config.String("HUB_TLS_CERT_FILE", "")),
		TLSKeyFile:          strings.TrimSpace(config.String("HUB_TLS_KEY_FILE", "")),
		TLSCAFile:           strings.TrimSpace(config.String("HUB_TLS_CA_FILE", "")),
		GRPCClientAuthMode:  strings.TrimSpace(config.String("HUB_GRPC_CLIENT_AUTH_MODE", "require-and-verify")),
	}
}

// Validate checks the configuration without touching the network.
func (c Config) Validate() error {
	var errs []error
	if c.AuthToken == "" {
		errs = append(errs, errors.New("HUB_AUTH_TOKEN is required"))
	}
	if c.ConfigPath == "" {
		errs = append(errs, errors.New("CONFIG_FILE_PATH is required"))
	} else if _, err := hub.NewConfigManager(c.ConfigPath); err != nil {
		errs = append(errs, fmt.Errorf("gateway config %s: %w", c.ConfigPath, err))
	}
	if c.GRPCAddr != "" {
		if c.TLSCertFile == "" || c.TLSKeyFile == "" || c.TLSCAFile == "" {
			errs = append(errs, errors.New("HUB_TLS_CERT_FILE, HUB_TLS_KEY_FILE and HUB_TLS_CA_FILE are required for the gRPC lease service"))
		} else if _, err := newGRPCServer(c); err != nil {
			errs = append(errs, fmt.Errorf("grpc tls: %w", err))
		}
	}
	return errors.Join(errs...)
}

// Hub is a running hub.
type Hub struct {
	// Leases serves quota leases; gate dev calls it in-process.
	Leases *hub.QuotaLeaseServer

	httpServer *http.Server
	httpLn     net.Listener
	grpcServer *grpc.Server
	rdb        *redis.Client
}

// Addr is the address the admin HTTP API listens on.
func (h *Hub) Addr() string { return h.httpLn.Addr().String() }

// Start validates cfg, binds listeners and starts serving. It returns once
// the hub accepts connections; readiness converges in the background.
func Start(ctx context.Context, cfg Config) (*Hub, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfgManager, err := hub.NewConfigManager(cfg.ConfigPath)
	if err != nil {
		return nil, err
	}
	policy := cfgManager.HubPolicy()

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  300 * time.Millisecond,
		WriteTimeout: 300 * time.Millisecond,
		PoolTimeout:  500 * time.Millisecond,
		PoolSize:     64,
		MinIdleConns: 16,
	})

	tierStore := hub.NewTierManager(rdb)
	server := hub.NewHubServerWithManagers(rdb, cfgManager, tierStore, hub.NewRateManager(rdb, cfgManager),
		cfg.AuthToken, policy.MaxDelta, cfg.UpdatesChannel)
	server.SetCORSAllowedOrigins(cfg.EdgeOrigins)
	server.SetCORSPreflightPolicy(policy.CORSAllowedHeaders, policy.CORSAllowedMethods, policy.CORSPreflightMaxAge)
	if err := server.SetAPIKeyPattern(policy.APIKeyPattern); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("invalid runtime.hub.api_key_pattern: %w", err)
	}
	server.SetConfigReloadChannel(cfg.ConfigReloadChannel)
	server.SetConfigReloadSubject(cfg.ConfigReloadSubject)
	server.SetAsyncQueueConfig(policy.QueueWorkers, policy.QueueSubmitTimeout, policy.QueueRetryMax, policy.QueueRetryBackoff)
	server.SetStartupReady(false)
	server.SetPendingDependency("redis")
	server.StartBackgroundWorkers(ctx)

	h := &Hub{Leases: hub.NewQuotaLeaseServer(rdb, cfgManager, tierStore), rdb: rdb}

	if cfg.GRPCAddr != "" {
		h.grpcServer, err = newGRPCServer(cfg)
		if err != nil {
			rdb.Close()
			return nil, err
		}
		types.RegisterQuotaLeaseServiceServer(h.grpcServer, h.Leases)
		grpcLn, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			rdb.Close()
			return nil, fmt.Errorf("grpc listen %s: %w", cfg.GRPCAddr, err)
		}
		go func() {
			log.Printf("hub gRPC lease service listening on %s", grpcLn.Addr())
			if err := h.grpcServer.Serve(grpcLn); err != nil {
				log.Printf("hub grpc server stopped: %v", err)
			}
		}()
	}

	h.httpLn, err = net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		h.stopGRPC()
		rdb.Close()
		return nil, fmt.Errorf("http listen %s: %w", cfg.HTTPAddr, err)
	}
	h.httpServer = &http.Server{
		Handler:           server,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       policy.HTTPReadTimeout,
		WriteTimeout:      policy.HTTPWriteTimeout,
	}
	go func() {
		log.Printf("hub HTTP API listening on %s", h.httpLn.Addr())
		if err := h.httpServer.Serve(h.httpLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("hub http server stopped: %v", err)
		}
	}()

	subject := cfg.TierUpdatesSubject
	if subject == "" {
		subject = policy.TierUpdatesSubject
	}
	go watchReadiness(ctx, cfg, server, rdb, subject)
	return h, nil
}

// watchReadiness keeps /readyz in step with Redis and NATS, and connects tier
// update messaging once NATS is reachable.
func watchReadiness(ctx context.Context, cfg Config, server *hub.HubServer, rdb *redis.Client, subject string) {
	messagingConfigured := false
	watcher := ready.NewReadyWatcher("hub", 500*time.Millisecond, 5*time.Second)
	watcher.WatchAll(ctx, []ready.Check{
		{
			Name: "redis",
			URL:  cfg.RedisAddr,
			Probe: func(probeCtx context.Context) error {
				if err := rdb.Ping(probeCtx).Err(); err != nil {
					server.SetPendingDependency("redis")
					return err
				}
				if cfg.EnforceNoEviction {
					if err := checkNoEviction(probeCtx, rdb); err != nil {
						server.SetPendingDependency("redis")
						return err
					}
				}
				return nil
			},
		},
		{
			Name: "nats",
			URL:  cfg.NATSURL,
			Probe: func(context.Context) error {
				nc, err := nats.Connect(cfg.NATSURL)
				if err != nil {
					server.SetPendingDependency("nats")
					return err
				}
				defer nc.Close()
				if err := nc.Flush(); err != nil {
					server.SetPendingDependency("nats")
					return err
				}
				return nc.LastError()
			},
		},
	}, func(isReady bool) {
		server.SetStartupReady(isReady)
		if !isReady || messagingConfigured {
			return
		}
		if err := server.SetTierUpdateMessaging(cfg.NATSURL, subject); err != nil {
			server.SetPendingDependency("nats")
			server.SetStartupReady(false)
			log.Printf("hub readiness pending dependency=nats_messaging url=%s err=%v", cfg.NATSURL, err)
			return
		}
		server.SetPendingDependency("")
		messagingConfigured = true
	})
}

// Shutdown stops accepting requests and waits for in-flight ones.
func (h *Hub) Shutdown(ctx context.Context) error {
	err := h.httpServer.Shutdown(ctx)
	h.stopGRPC()
	h.rdb.Close()
	return err
}

func (h *Hub) stopGRPC() {
	if h.grpcServer != nil {
		h.grpcServer.GracefulStop()
	}
}

// Run starts the hub and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	h, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	<-ctx.Done()
	log.Println("hub shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return h.Shutdown(shutdownCtx)
}

// checkNoEviction verifies that hub Redis will never evict lease or tier
// keys. It only checks: server configuration belongs to whoever runs Redis.
func checkNoEviction(ctx context.Context, rdb *redis.Client) error {
	res, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		return fmt.Errorf("config get maxmemory-policy: %w", err)
	}
	if policy := strings.TrimSpace(res["maxmemory-policy"]); policy != "noeviction" {
		return fmt.Errorf("redis maxmemory-policy is %q; hub requires noeviction (set it on the Redis server, or HUB_ENFORCE_REDIS_NOEVICTION=false to skip)", policy)
	}
	return nil
}

func newGRPCServer(cfg Config) (*grpc.Server, error) {
	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(cfg.TLSCAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to parse hub CA certificate")
	}
	var clientAuth tls.ClientAuthType
	switch strings.ToLower(strings.TrimSpace(cfg.GRPCClientAuthMode)) {
	case "", "require-and-verify":
		clientAuth = tls.RequireAndVerifyClientCert
	case "require-any":
		log.Printf("warning: HUB_GRPC_CLIENT_AUTH_MODE=require-any (development mode, certificate chain/EKU not verified)")
		clientAuth = tls.RequireAnyClientCert
	case "none", "no-client-cert":
		log.Printf("warning: HUB_GRPC_CLIENT_AUTH_MODE=none (development mode, client certificate auth disabled)")
		clientAuth = tls.NoClientCert
	default:
		return nil, fmt.Errorf("invalid HUB_GRPC_CLIENT_AUTH_MODE=%q (valid: require-and-verify, require-any, none)", cfg.GRPCClientAuthMode)
	}
	return grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   clientAuth,
			ClientCAs:    pool,
			MinVersion:   tls.VersionTLS13,
		})),
		// Edges ping every 10s (packages/edge/grpc_client.go) to keep the lease
		// connection warm. gRPC's default policy allows one ping per 5 minutes
		// and answers faster pings with GOAWAY "too_many_pings", dropping the
		// connection and failing in-flight lease calls.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	), nil
}
