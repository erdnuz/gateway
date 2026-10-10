// Package edgeapp assembles and runs the edge (data plane): routing, auth,
// rate limiting, caching, proxying and analytics capture.
package edgeapp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"gateway/internal/app/appkit"
	"gateway/packages/common/config"
	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"
	gateconfig "gateway/packages/config"
	"gateway/packages/edge"
	"gateway/packages/limiter"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

// Config is everything the edge needs to start.
type Config struct {
	HTTPAddr string
	EdgeID   string

	// The hub, reached over one gRPC connection. Without TLS files the edge
	// only connects in plaintext to a loopback hub (gate dev).
	HubGRPCAddr       string
	HubGRPCServerName string
	TLSCertFile       string
	TLSKeyFile        string
	TLSCAFile         string

	// RedisAddr, when set, keeps cached responses in that Redis instead of
	// edge memory (CacheBytes).
	RedisAddr  string
	CacheBytes int64

	LeaseSize        int64   // 0: runtime.edge.rate_lease_size
	LeaseLowWaterPct float64 // 0: runtime.edge.rate_low_water_pct

	AnalyticsEnabled bool
	NATSURL          string // analytics events
	AnalyticsSubject string // empty: runtime.analytics.nats_subject

	BootstrapConfigFile string // serve this config until the hub sends one
	AdminToken          string
	MetricsAddr         string // Prometheus /metrics; empty disables it
}

const defaultCacheBytes = 64 << 20

// FromEnv reads the edge configuration from environment variables.
func FromEnv() Config {
	return Config{
		HTTPAddr:            appkit.ListenAddr(config.String("PORT", "8080")),
		EdgeID:              config.String("EDGE_ID", ""),
		HubGRPCAddr:         strings.TrimSpace(config.String("HUB_GRPC_ADDR", "localhost:9090")),
		HubGRPCServerName:   strings.TrimSpace(config.String("HUB_GRPC_SERVER_NAME", "")),
		TLSCertFile:         strings.TrimSpace(config.String("EDGE_TLS_CERT_FILE", "")),
		TLSKeyFile:          strings.TrimSpace(config.String("EDGE_TLS_KEY_FILE", "")),
		TLSCAFile:           strings.TrimSpace(config.String("EDGE_TLS_CA_FILE", "")),
		RedisAddr:           strings.TrimSpace(config.String("REDIS_ADDR", "")),
		CacheBytes:          config.Int64("EDGE_CACHE_BYTES", defaultCacheBytes),
		LeaseSize:           config.Int64("EDGE_LEASE_SIZE", 0),
		LeaseLowWaterPct:    float64(config.Int("EDGE_LEASE_LOW_WATER_PERCENT", 0)),
		AnalyticsEnabled:    config.Bool("GATE_ANALYTICS_ENABLED", true),
		NATSURL:             config.String("NATS_URL", nats.DefaultURL),
		AnalyticsSubject:    config.String("NATS_ANALYTICS_SUBJECT", ""),
		BootstrapConfigFile: config.String("EDGE_BOOTSTRAP_CONFIG_FILE", ""),
		AdminToken:          config.String("EDGE_ADMIN_TOKEN", ""),
		MetricsAddr:         config.String("METRICS_ADDR", ":9100"),
	}
}

func (c Config) plaintext() bool {
	return c.TLSCertFile == "" && c.TLSKeyFile == "" && c.TLSCAFile == ""
}

// Validate checks the configuration without touching the network.
func (c Config) Validate() error {
	var errs []error
	switch {
	case c.HubGRPCAddr == "":
		errs = append(errs, errors.New("HUB_GRPC_ADDR is required"))
	case c.plaintext() && !appkit.IsLoopback(c.HubGRPCAddr):
		errs = append(errs, errors.New("EDGE_TLS_CERT_FILE, EDGE_TLS_KEY_FILE and EDGE_TLS_CA_FILE are required unless the hub is on loopback"))
	case !c.plaintext() && c.HubGRPCServerName == "":
		errs = append(errs, errors.New("HUB_GRPC_SERVER_NAME is required with TLS"))
	}
	if c.AnalyticsEnabled && c.NATSURL == "" {
		errs = append(errs, errors.New("NATS_URL is required when analytics is enabled"))
	}
	return errors.Join(errs...)
}

// Edge is a running edge.
type Edge struct {
	httpServer *http.Server
	ln         net.Listener
	hub        *edge.HubClient
	lease      *limiter.Lease
	closers    []func()
}

// Addr is the address the gateway listens on.
func (e *Edge) Addr() string { return e.ln.Addr().String() }

// Start validates cfg, connects to the hub and starts serving. Proxy traffic
// is held (503) until the hub sends the first config, unless a bootstrap
// config is given.
func Start(ctx context.Context, cfg Config) (*Edge, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	e := &Edge{}
	ok := false
	defer func() {
		if !ok {
			e.close()
		}
	}()

	var tlsCfg *tls.Config
	if !cfg.plaintext() {
		var err error
		if tlsCfg, err = edge.ClientTLS(cfg.HubGRPCServerName, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile); err != nil {
			return nil, fmt.Errorf("hub TLS: %w", err)
		}
	}
	hub, err := edge.DialHub(cfg.HubGRPCAddr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("hub connection: %w", err)
	}
	e.hub = hub

	var bootstrap *types.GatewayConfig
	if cfg.BootstrapConfigFile != "" {
		if bootstrap, err = gateconfig.Load(cfg.BootstrapConfigFile); err != nil {
			return nil, fmt.Errorf("bootstrap config: %w", err)
		}
	}
	defaults := types.DefaultRuntimePolicy().Edge

	// Quota state lives in this process only: edges share nothing but the
	// hub, so they scale out without coordinating with each other.
	// Concurrent lease requests share batched RPCs, and the hub splits the
	// last of a quota between the edges it knows are spending it.
	edgeID := cfg.EdgeID
	if edgeID == "" {
		edgeID = defaultEdgeID()
	}
	lease := limiter.NewLease(limiter.NewBatcher(hub), limiter.LeaseOptions{LeaseSize: cfg.LeaseSize, LowWaterPct: cfg.LeaseLowWaterPct, EdgeID: edgeID})
	e.lease = lease

	responses := edge.NewMemoryResponses(cfg.CacheBytes)
	if cfg.RedisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
		e.closers = append(e.closers, func() { _ = rdb.Close() })
		responses = edge.NewRedisResponses(rdb)
	}

	var sink edge.AnalyticsSink
	if cfg.AnalyticsEnabled {
		mgr := edge.NewAnalyticsManagerWithNATSOptions(defaults.AnalyticsBufferSize, cfg.NATSURL, cfg.AnalyticsSubject,
			edge.AnalyticsManagerOptions{PublishTimeout: defaults.AnalyticsPublishTimeout})
		mgr.StartPublisher(ctx)
		sink = mgr
	}

	server := edge.NewServer(edge.Deps{
		Config:    edge.NewConfigManager(bootstrap),
		Tiers:     edge.NewTierCache(hub.ResolveKey),
		Limiter:   lease,
		Analytics: sink,
		Responses: responses,
	}, edge.Options{EdgeID: edgeID, AdminToken: cfg.AdminToken})
	server.StartBackgroundWorkers(ctx)
	server.StartReadinessProbe(ctx)
	if bootstrap == nil {
		server.SetStartupReady(false)
		server.SetPendingDependency("hub_config")
	}
	go server.FollowHub(ctx, hub)
	telemetry.ServeMetrics(ctx, cfg.MetricsAddr)

	if e.ln, err = net.Listen("tcp", cfg.HTTPAddr); err != nil {
		return nil, fmt.Errorf("http listen %s: %w", cfg.HTTPAddr, err)
	}
	e.httpServer = &http.Server{
		Handler:           telemetry.HTTPHandler(server, "edge"),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       defaults.HTTPReadTimeout,
		WriteTimeout:      defaults.HTTPWriteTimeout,
		IdleTimeout:       defaults.HTTPIdleTimeout,
	}
	go func() {
		slog.Info("edge listening", "addr", e.ln.Addr().String(), "hub", cfg.HubGRPCAddr)
		if err := e.httpServer.Serve(e.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("edge http server stopped", "err", err)
		}
	}()
	ok = true
	return e, nil
}

// Shutdown stops accepting requests and waits for in-flight ones.
//
// Once no request can be admitted any more, the edge gives its unspent
// tokens back to the hub, so leaving mid-window strands no quota.
func (e *Edge) Shutdown(ctx context.Context) error {
	err := e.httpServer.Shutdown(ctx)
	if e.lease != nil {
		if derr := e.lease.Drain(ctx); derr != nil {
			slog.Warn("returning leases on shutdown failed; they expire with their window", "err", derr)
		}
	}
	e.close()
	return err
}

// defaultEdgeID names an edge that was not given EDGE_ID: its hostname (a
// container ID under Docker) plus a random suffix, unique per process.
func defaultEdgeID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "edge"
	}
	return host + "-" + strconv.FormatUint(rand.Uint64()%1e6, 36)
}

func (e *Edge) close() {
	for _, c := range e.closers {
		c()
	}
	if e.hub != nil {
		_ = e.hub.Close()
	}
}

// Run starts the edge and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	e, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	<-ctx.Done()
	slog.Info("edge shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return e.Shutdown(shutdownCtx)
}
