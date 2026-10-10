// Package hubapp assembles and runs the hub (control plane): config, tier
// store, admin HTTP API, and the gRPC services edges use (quota leases and
// the control-plane stream).
package hubapp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"gateway/internal/app/appkit"
	"gateway/packages/common/config"
	"gateway/packages/common/httpx"
	"gateway/packages/common/ready"
	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"
	"gateway/packages/edge"
	"gateway/packages/hub"
	"gateway/packages/ledger"
	"gateway/packages/region"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Config is everything the hub needs to start. FromEnv fills it from the
// process environment; gate dev builds it directly.
type Config struct {
	HTTPAddr   string // admin HTTP API
	GRPCAddr   string // edge-facing gRPC (leases, control plane)
	ConfigPath string
	AuthToken  string
	ReadToken  string // optional read-only admin token

	RedisAddr string
	// RedisAddrs, when it lists several addresses, selects Redis Cluster:
	// the ledger's hash tags keep every grant on one shard.
	RedisAddrs        []string
	EnforceNoEviction bool
	EdgeOrigins       []string
	MetricsAddr       string // Prometheus /metrics; empty disables it

	// TLS for the gRPC listener. Without it the hub only serves plaintext on
	// a loopback address (gate dev).
	TLSCertFile        string
	TLSKeyFile         string
	TLSCAFile          string
	GRPCClientAuthMode string

	// Region names this hub's region in a multi-region deployment; peers
	// are the other regions' hubs (name=address), reached over mTLS with
	// this hub's certificate. Services with limits.global split their
	// quotas between the regions.
	Region           string
	RegionPeers      map[string]string
	RegionServerName string // TLS server name the peers' certificates carry
}

// FromEnv reads the hub configuration from environment variables.
func FromEnv() Config {
	return Config{
		HTTPAddr:           appkit.ListenAddr(config.String("PORT", "8080")),
		GRPCAddr:           appkit.ListenAddr(config.String("HUB_GRPC_PORT", "9090")),
		ConfigPath:         strings.TrimSpace(config.String("CONFIG_FILE_PATH", "")),
		AuthToken:          strings.TrimSpace(config.String("HUB_AUTH_TOKEN", "")),
		ReadToken:          strings.TrimSpace(config.String("HUB_READ_TOKEN", "")),
		RedisAddr:          config.String("REDIS_ADDR", "localhost:6379"),
		RedisAddrs:         appkit.SplitCSV(config.String("REDIS_ADDRS", "")),
		EnforceNoEviction:  config.Bool("HUB_ENFORCE_REDIS_NOEVICTION", true),
		EdgeOrigins:        appkit.SplitCSV(config.String("HUB_EDGE_ALLOWED_ORIGINS", "")),
		MetricsAddr:        config.String("METRICS_ADDR", ":9100"),
		TLSCertFile:        strings.TrimSpace(config.String("HUB_TLS_CERT_FILE", "")),
		TLSKeyFile:         strings.TrimSpace(config.String("HUB_TLS_KEY_FILE", "")),
		TLSCAFile:          strings.TrimSpace(config.String("HUB_TLS_CA_FILE", "")),
		GRPCClientAuthMode: strings.TrimSpace(config.String("HUB_GRPC_CLIENT_AUTH_MODE", "require-and-verify")),
		Region:             strings.TrimSpace(config.String("HUB_REGION", "")),
		RegionPeers:        parsePeers(config.String("HUB_REGION_PEERS", "")),
		RegionServerName:   strings.TrimSpace(config.String("HUB_REGION_SERVER_NAME", "hub-server")),
	}
}

// parsePeers reads "eu=hub.eu:9090,us=hub.us:9090".
func parsePeers(v string) map[string]string {
	out := map[string]string{}
	for _, p := range appkit.SplitCSV(v) {
		if name, addr, ok := strings.Cut(p, "="); ok && name != "" && addr != "" {
			out[strings.TrimSpace(name)] = strings.TrimSpace(addr)
		}
	}
	return out
}

func (c Config) plaintext() bool {
	return c.TLSCertFile == "" && c.TLSKeyFile == "" && c.TLSCAFile == ""
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
	switch {
	case c.GRPCAddr == "":
		errs = append(errs, errors.New("HUB_GRPC_PORT is required: edges reach the hub over gRPC"))
	case c.plaintext() && !appkit.IsLoopback(c.GRPCAddr):
		errs = append(errs, errors.New("HUB_TLS_CERT_FILE, HUB_TLS_KEY_FILE and HUB_TLS_CA_FILE are required unless gRPC listens on loopback"))
	case !c.plaintext():
		if _, err := serverTLS(c); err != nil {
			errs = append(errs, fmt.Errorf("grpc tls: %w", err))
		}
	}
	return errors.Join(errs...)
}

// Hub is a running hub.
type Hub struct {
	httpServer *http.Server
	httpLn     net.Listener
	grpcServer *grpc.Server
	grpcLn     net.Listener
	rdb        redis.UniversalClient
}

// Addr is the address the admin HTTP API listens on.
func (h *Hub) Addr() string { return h.httpLn.Addr().String() }

// GRPCAddr is the address edges connect to.
func (h *Hub) GRPCAddr() string { return h.grpcLn.Addr().String() }

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

	h := &Hub{rdb: newRedis(cfg)}
	ok := false
	defer func() {
		if !ok {
			h.close()
		}
	}()

	go func() {
		mctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := hub.MigrateKeyLayout(mctx, h.rdb); err != nil {
			slog.Warn("migrating key records to the Cluster layout failed; retried on the next start", "err", err)
		}
	}()
	tierStore := hub.NewTierManager(h.rdb)
	control := hub.NewControlPlane(cfgManager, tierStore, h.rdb)
	go control.Run(ctx)
	telemetry.RegisterWatchers(control.Watchers)
	telemetry.ServeMetrics(ctx, cfg.MetricsAddr)

	server := hub.NewHubServer(h.rdb, cfgManager, tierStore, control, cfg.AuthToken)
	server.SetReadToken(cfg.ReadToken)
	server.SetCORSAllowedOrigins(cfg.EdgeOrigins)
	server.SetCORSPreflightPolicy(policy.CORSAllowedHeaders, policy.CORSAllowedMethods, policy.CORSPreflightMaxAge)
	if err := server.SetAPIKeyPattern(policy.APIKeyPattern); err != nil {
		return nil, fmt.Errorf("invalid runtime.hub.api_key_pattern: %w", err)
	}
	server.SetStartupReady(false)
	server.SetPendingDependency("redis")

	if h.grpcServer, err = newGRPCServer(cfg); err != nil {
		return nil, err
	}
	leases := hub.NewQuotaLeaseServer(ledger.NewRedis(h.rdb), cfgManager, tierStore)
	types.RegisterQuotaLeaseServiceServer(h.grpcServer, leases)
	if cfg.Region != "" {
		node, err := startRegion(ctx, cfg, ledger.NewRedis(h.rdb))
		if err != nil {
			return nil, err
		}
		leases.SetRegion(node)
		types.RegisterRegionPeerServer(h.grpcServer, &hub.RegionServer{Leases: leases})
		slog.Info("hub region", "region", cfg.Region, "peers", len(cfg.RegionPeers))
	}
	types.RegisterControlPlaneServer(h.grpcServer, control)
	if h.grpcLn, err = net.Listen("tcp", cfg.GRPCAddr); err != nil {
		return nil, fmt.Errorf("grpc listen %s: %w", cfg.GRPCAddr, err)
	}
	go func() {
		slog.Info("hub gRPC listening", "addr", h.grpcLn.Addr().String(), "tls", !cfg.plaintext())
		if err := h.grpcServer.Serve(h.grpcLn); err != nil {
			slog.Error("hub grpc server stopped", "err", err)
		}
	}()

	if h.httpLn, err = net.Listen("tcp", cfg.HTTPAddr); err != nil {
		return nil, fmt.Errorf("http listen %s: %w", cfg.HTTPAddr, err)
	}
	h.httpServer = &http.Server{
		Handler:           telemetry.HTTPHandler(httpx.WithRequestID(server), "hub"),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       policy.HTTPReadTimeout,
		WriteTimeout:      policy.HTTPWriteTimeout,
	}
	go func() {
		slog.Info("hub HTTP API listening", "addr", h.httpLn.Addr().String())
		if err := h.httpServer.Serve(h.httpLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("hub http server stopped", "err", err)
		}
	}()

	go watchReadiness(ctx, cfg, server, h.rdb)
	ok = true
	return h, nil
}

// watchReadiness keeps /readyz in step with Redis, the hub's only dependency.
func watchReadiness(ctx context.Context, cfg Config, server *hub.HubServer, rdb redis.UniversalClient) {
	ready.NewReadyWatcher("hub", 500*time.Millisecond, 5*time.Second).WatchAll(ctx, []ready.Check{{
		Name: "redis",
		URL:  cfg.RedisAddr,
		Probe: func(probeCtx context.Context) error {
			if err := ping(probeCtx, rdb); err != nil {
				return err
			}
			if cfg.EnforceNoEviction {
				return checkNoEviction(probeCtx, rdb)
			}
			return nil
		},
	}}, func(isReady bool) {
		server.SetStartupReady(isReady)
		if isReady {
			server.SetPendingDependency("")
		} else {
			server.SetPendingDependency("redis")
		}
	})
}

// startRegion connects to the peer regions' hubs and starts rebalancing.
func startRegion(ctx context.Context, cfg Config, l *ledger.Redis) (*region.Node, error) {
	node := &region.Node{Name: cfg.Region, Ledger: l, Peers: map[string]region.Peer{}}
	creds := insecure.NewCredentials()
	if !cfg.plaintext() {
		tlsCfg, err := edge.ClientTLS(cfg.RegionServerName, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("region peer TLS: %w", err)
		}
		creds = credentials.NewTLS(tlsCfg)
	}
	for name, addr := range cfg.RegionPeers {
		if name == cfg.Region {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, fmt.Errorf("region peer %s: %w", name, err)
		}
		go func() { <-ctx.Done(); _ = conn.Close() }()
		node.Peers[name] = peerClient{types.NewRegionPeerClient(conn)}
	}
	go node.Run(ctx)
	return node, nil
}

type peerClient struct{ c types.RegionPeerClient }

func (p peerClient) Borrow(ctx context.Context, in *types.BorrowRequest) (*types.BorrowResponse, error) {
	return p.c.Borrow(ctx, in)
}

// newRedis connects to one Redis, or to a Redis Cluster when REDIS_ADDRS
// lists several seed nodes.
func newRedis(cfg Config) redis.UniversalClient {
	opts := &redis.UniversalOptions{
		Addrs:        []string{cfg.RedisAddr},
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  300 * time.Millisecond,
		WriteTimeout: 300 * time.Millisecond,
		PoolTimeout:  500 * time.Millisecond,
		PoolSize:     64,
		MinIdleConns: 16,
	}
	if len(cfg.RedisAddrs) > 0 {
		opts.Addrs = cfg.RedisAddrs
	}
	if len(opts.Addrs) > 1 {
		return redis.NewClusterClient(opts.Cluster())
	}
	return redis.NewClient(opts.Simple())
}

// ping checks every shard of a cluster, or the one Redis.
func ping(ctx context.Context, rdb redis.UniversalClient) error {
	if c, ok := rdb.(*redis.ClusterClient); ok {
		return c.ForEachShard(ctx, func(ctx context.Context, s *redis.Client) error { return s.Ping(ctx).Err() })
	}
	return rdb.Ping(ctx).Err()
}

// Shutdown stops accepting requests and waits for in-flight ones.
func (h *Hub) Shutdown(ctx context.Context) error {
	err := h.httpServer.Shutdown(ctx)
	h.close()
	return err
}

func (h *Hub) close() {
	if h.grpcServer != nil {
		h.grpcServer.Stop() // watch streams never end on their own
	}
	_ = h.rdb.Close()
}

// Run starts the hub and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	h, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	<-ctx.Done()
	slog.Info("hub shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return h.Shutdown(shutdownCtx)
}

// checkNoEviction verifies that hub Redis will never evict lease or tier
// keys. It only checks: server configuration belongs to whoever runs Redis.
func checkNoEviction(ctx context.Context, rdb redis.UniversalClient) error {
	if c, ok := rdb.(*redis.ClusterClient); ok {
		return c.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error { return checkNoEviction(ctx, m) })
	}
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
	opts := append(telemetry.GRPCServerOptions(),
		// Edges ping every 10s to keep their connection warm. gRPC's default
		// policy allows one ping per 5 minutes and answers faster pings with
		// GOAWAY "too_many_pings", dropping in-flight calls and watch streams.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if cfg.plaintext() {
		slog.Warn("hub gRPC is plaintext; only acceptable on loopback (gate dev)", "addr", cfg.GRPCAddr)
		return grpc.NewServer(opts...), nil
	}
	tlsCfg, err := serverTLS(cfg)
	if err != nil {
		return nil, err
	}
	return grpc.NewServer(append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))...), nil
}

func serverTLS(cfg Config) (*tls.Config, error) {
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
		slog.Warn("HUB_GRPC_CLIENT_AUTH_MODE=require-any: client certificate chains are not verified (development only)")
		clientAuth = tls.RequireAnyClientCert
	case "none", "no-client-cert":
		slog.Warn("HUB_GRPC_CLIENT_AUTH_MODE=none: client certificates are not required (development only)")
		clientAuth = tls.NoClientCert
	default:
		return nil, fmt.Errorf("invalid HUB_GRPC_CLIENT_AUTH_MODE=%q (valid: require-and-verify, require-any, none)", cfg.GRPCClientAuthMode)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: clientAuth, ClientCAs: pool, MinVersion: tls.VersionTLS13}, nil
}
