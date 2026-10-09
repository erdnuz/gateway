// Package edgeapp assembles and runs the edge (data plane): routing, auth,
// rate limiting, caching, proxying and analytics capture.
package edgeapp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gateway/internal/app/appkit"
	"gateway/packages/common/config"
	"gateway/packages/common/ready"
	"gateway/packages/common/types"
	"gateway/packages/edge"
	"gateway/packages/limiter"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

// Config is everything the edge needs to start.
type Config struct {
	HTTPAddr  string
	RedisAddr string
	EdgeID    string

	HubAddr  string
	HubToken string

	// Lease service. LeaseClient, when set, replaces the gRPC client (gate dev
	// leases in-process); otherwise the TLS fields are required.
	HubGRPCAddr       string
	HubGRPCServerName string
	TLSCertFile       string
	TLSKeyFile        string
	TLSCAFile         string
	LeaseClient       edge.QuotaLeaseRequester

	LeaseSize        int64   // 0: runtime.edge.rate_lease_size
	LeaseLowWaterPct float64 // 0: runtime.edge.rate_low_water_pct

	NATSTierURL         string
	NATSAnalyticsURL    string
	TierUpdatesSubject  string // empty: runtime.hub.tier_updates_subject
	AnalyticsSubject    string // empty: runtime.analytics.nats_subject
	AnalyticsEnabled    bool
	ConfigReloadSubject string
	ConfigRefresh       time.Duration
	BootstrapConfigFile string
	AdminToken          string
}

// FromEnv reads the edge configuration from environment variables.
func FromEnv() Config {
	natsURL := config.String("NATS_URL", nats.DefaultURL)
	return Config{
		HTTPAddr:            appkit.ListenAddr(config.String("PORT", "8080")),
		RedisAddr:           strings.TrimSpace(config.String("REDIS_ADDR", "localhost:6379")),
		EdgeID:              config.String("EDGE_ID", ""),
		HubAddr:             strings.TrimSpace(config.String("HUB_ADDR", "http://localhost:8081")),
		HubToken:            strings.TrimSpace(config.String("HUB_AUTH_TOKEN", "")),
		HubGRPCAddr:         strings.TrimSpace(config.String("HUB_GRPC_ADDR", "localhost:9090")),
		HubGRPCServerName:   strings.TrimSpace(config.String("HUB_GRPC_SERVER_NAME", "")),
		TLSCertFile:         strings.TrimSpace(config.String("EDGE_TLS_CERT_FILE", "")),
		TLSKeyFile:          strings.TrimSpace(config.String("EDGE_TLS_KEY_FILE", "")),
		TLSCAFile:           strings.TrimSpace(config.String("EDGE_TLS_CA_FILE", "")),
		LeaseSize:           config.Int64("EDGE_LEASE_SIZE", 0),
		LeaseLowWaterPct:    float64(config.Int("EDGE_LEASE_LOW_WATER_PERCENT", 0)),
		NATSTierURL:         config.String("NATS_TIER_URL", natsURL),
		NATSAnalyticsURL:    config.String("NATS_ANALYTICS_URL", natsURL),
		TierUpdatesSubject:  config.String("NATS_TIER_UPDATES_SUBJECT", ""),
		AnalyticsSubject:    config.String("NATS_ANALYTICS_SUBJECT", ""),
		AnalyticsEnabled:    config.Bool("GATE_ANALYTICS_ENABLED", true),
		ConfigReloadSubject: config.String("EDGE_CONFIG_RELOAD_SUBJECT", types.DefaultConfigReloadSubject),
		ConfigRefresh:       time.Duration(config.Int("EDGE_CONFIG_REFRESH_SECONDS", 0)) * time.Second,
		BootstrapConfigFile: config.String("EDGE_BOOTSTRAP_CONFIG_FILE", ""),
		AdminToken:          config.String("EDGE_ADMIN_TOKEN", ""),
	}
}

// Validate checks the configuration without touching the network.
func (c Config) Validate() error {
	var errs []error
	if c.RedisAddr == "" {
		errs = append(errs, errors.New("REDIS_ADDR is required"))
	}
	if u, err := url.ParseRequestURI(c.HubAddr); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, fmt.Errorf("HUB_ADDR must be an http(s) URL, got %q", c.HubAddr))
	}
	if c.LeaseClient == nil {
		if c.HubGRPCServerName == "" {
			errs = append(errs, errors.New("HUB_GRPC_SERVER_NAME is required"))
		}
		if c.TLSCertFile == "" || c.TLSKeyFile == "" || c.TLSCAFile == "" {
			errs = append(errs, errors.New("EDGE_TLS_CERT_FILE, EDGE_TLS_KEY_FILE and EDGE_TLS_CA_FILE are required"))
		}
	}
	if c.NATSTierURL == "" {
		errs = append(errs, errors.New("NATS_TIER_URL or NATS_URL is required"))
	}
	if c.AnalyticsEnabled && c.NATSAnalyticsURL == "" {
		errs = append(errs, errors.New("NATS_ANALYTICS_URL or NATS_URL is required when analytics is enabled"))
	}
	return errors.Join(errs...)
}

// Edge is a running edge.
type Edge struct {
	httpServer  *http.Server
	ln          net.Listener
	rdb         *redis.Client
	leaseClient edge.QuotaLeaseRequester
	closers     []func()
}

// Addr is the address the gateway listens on.
func (e *Edge) Addr() string { return e.ln.Addr().String() }

// Start validates cfg, hydrates config from the hub, and starts serving.
// Proxy traffic is held (503) until the hub reports ready once.
func Start(ctx context.Context, cfg Config) (*Edge, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	e := &Edge{rdb: redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})}
	ok := false
	defer func() {
		if !ok {
			e.close()
		}
	}()

	hubClient := edge.NewHubHTTPClient(types.DefaultRuntimePolicy().Edge.HubHTTPTimeout)
	configMgr, err := edge.NewConfigManager(cfg.HubAddr, cfg.HubToken, hubClient, cfg.BootstrapConfigFile)
	if err != nil {
		return nil, fmt.Errorf("load config from hub: %w", err)
	}
	policy := configMgr.EdgePolicy()
	hubClient = edge.NewHubHTTPClient(policy.HubHTTPTimeout)
	configMgr.SetHTTPClient(hubClient)
	if cfg.ConfigRefresh > 0 {
		configMgr.StartAutoRefresh(ctx, cfg.ConfigRefresh)
	}
	if nc, err := nats.Connect(cfg.NATSTierURL, nats.MaxReconnects(-1)); err != nil {
		log.Printf("edge config reload nats connect failed url=%s err=%v (reloads will not propagate)", cfg.NATSTierURL, err)
	} else {
		e.closers = append(e.closers, nc.Close)
		if err := configMgr.StartConfigReloadNATSSubscriber(ctx, nc, cfg.ConfigReloadSubject, e.rdb); err != nil {
			log.Printf("edge config reload nats subscribe failed subject=%s err=%v", cfg.ConfigReloadSubject, err)
		}
	}

	tierSubject := cfg.TierUpdatesSubject
	if tierSubject == "" {
		tierSubject = configMgr.TierUpdatesSubject()
	}
	tierMgr := edge.NewTierManager(cfg.HubAddr, e.rdb, edge.TierManagerOptions{
		AuthToken: cfg.HubToken, Client: hubClient, NATSURL: cfg.NATSTierURL, NATSSubj: tierSubject,
	})

	e.leaseClient = cfg.LeaseClient
	if e.leaseClient == nil {
		e.leaseClient, err = edge.NewGRPCQuotaLeaseClient(cfg.HubGRPCAddr, cfg.HubGRPCServerName, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("lease gRPC client: %w", err)
		}
	}

	// Quota state lives in this process only: edges share nothing but the
	// hub, so they scale out without coordinating with each other.
	lease := limiter.NewLease(e.leaseClient, limiter.LeaseOptions{
		LeaseSize:   firstPositive(cfg.LeaseSize, policy.RateLeaseSize),
		LowWaterPct: firstPositiveFloat(cfg.LeaseLowWaterPct, policy.RateLowWaterPct),
	})

	var sink edge.AnalyticsSink
	if cfg.AnalyticsEnabled {
		subject := cfg.AnalyticsSubject
		if subject == "" {
			subject = configMgr.AnalyticsPolicy().NATSSubject
		}
		mgr := edge.NewAnalyticsManagerWithNATSOptions(policy.AnalyticsBufferSize, cfg.NATSAnalyticsURL, subject,
			edge.AnalyticsManagerOptions{PublishTimeout: policy.AnalyticsPublishTimeout})
		mgr.StartPublisher(ctx)
		sink = mgr
	}

	server := edge.NewServer(edge.Deps{
		Config:    configMgr,
		Tiers:     tierMgr,
		Limiter:   lease,
		Analytics: sink,
		Redis:     e.rdb,
	}, edge.Options{
		EdgeID:                  cfg.EdgeID,
		UpstreamTimeout:         policy.UpstreamClientTimeout,
		UpstreamMaxIdleConns:    policy.UpstreamMaxIdleConns,
		UpstreamIdleConnTimeout: policy.UpstreamIdleConnTimeout,
		MaxCacheableBytes:       policy.CacheMaxObjectBytes,
		AdminToken:              cfg.AdminToken,
	})
	server.StartBackgroundWorkers(ctx)
	server.SetStartupReady(false)
	server.SetPendingDependency("hub_readyz")

	e.ln, err = net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("http listen %s: %w", cfg.HTTPAddr, err)
	}
	e.httpServer = &http.Server{
		Handler:           server,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       policy.HTTPReadTimeout,
		WriteTimeout:      policy.HTTPWriteTimeout,
		IdleTimeout:       policy.HTTPIdleTimeout,
	}
	go func() {
		log.Printf("edge gateway listening on %s", e.ln.Addr())
		if err := e.httpServer.Serve(e.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("edge http server stopped: %v", err)
		}
	}()

	go awaitHub(ctx, cfg, server)
	ok = true
	return e, nil
}

// awaitHub gates proxy traffic until the hub is ready once. Afterwards
// /readyz tracks edge-local dependencies only: a hub or analytics outage is
// handled per request by the failure policies instead of taking every edge
// out of rotation at once.
func awaitHub(ctx context.Context, cfg Config, server *edge.Server) {
	readyURL := strings.TrimSuffix(cfg.HubAddr, "/") + "/readyz"
	client := &http.Client{Timeout: 2 * time.Second}
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	ready.NewReadyWatcher("edge", 500*time.Millisecond, 5*time.Second).WatchAll(watchCtx, []ready.Check{{
		Name: "hub_readyz",
		URL:  readyURL,
		Probe: func(probeCtx context.Context) error {
			req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, readyURL, nil)
			if err != nil {
				return err
			}
			resp, err := client.Do(req)
			if err != nil {
				server.SetPendingDependency("hub_readyz")
				return err
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				server.SetPendingDependency("hub_readyz")
				return fmt.Errorf("status=%d", resp.StatusCode)
			}
			return nil
		},
	}}, func(isReady bool) {
		if !isReady {
			return
		}
		stop()
		server.SetPendingDependency("")
		server.SetStartupReady(true)
		server.StartReadinessProbe(ctx)
		log.Printf("edge startup complete; readiness now tracks edge-local dependencies")
	})
}

// Shutdown stops accepting requests and waits for in-flight ones.
func (e *Edge) Shutdown(ctx context.Context) error {
	err := e.httpServer.Shutdown(ctx)
	e.close()
	return err
}

func (e *Edge) close() {
	for _, c := range e.closers {
		c()
	}
	if e.leaseClient != nil {
		_ = e.leaseClient.Close()
	}
	_ = e.rdb.Close()
}

// Run starts the edge and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	e, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	<-ctx.Done()
	log.Println("edge shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return e.Shutdown(shutdownCtx)
}

func firstPositive(v, fallback int64) int64 {
	if v > 0 {
		return v
	}
	return fallback
}

func firstPositiveFloat(v, fallback float64) float64 {
	if v > 0 && v <= 100 {
		return v
	}
	return fallback
}
