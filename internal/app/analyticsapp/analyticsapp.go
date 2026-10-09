// Package analyticsapp assembles and runs the analytics service: event
// ingestion from NATS, storage, query API and dashboard.
package analyticsapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gateway/packages/common/httpx"
	"gateway/packages/common/telemetry"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"gateway/internal/app/appkit"
	analyticsapi "gateway/packages/analytics"
	"gateway/packages/common/config"
	"gateway/packages/common/ready"
	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
)

// Config is everything the analytics service needs to start.
type Config struct {
	HTTPAddr string
	HubAddr  string
	HubToken string
	APIToken string // guards the query API; empty leaves it open

	// Storage: ClickHouse when ClickHouseDSN is set, otherwise Redis
	// (RedisAddr), which suits development and demos.
	ClickHouseDSN   string
	ClickHouseTable string
	RetentionDays   int
	RedisAddr       string

	NATSURL     string
	NATSSubject string // empty: runtime.analytics.nats_subject
	NATSQueue   string // empty: runtime.analytics.nats_queue

	MetricsAddr string // Prometheus /metrics; empty disables it
}

// FromEnv reads the analytics configuration from environment variables.
func FromEnv() Config {
	return Config{
		HTTPAddr:        appkit.ListenAddr(config.String("PORT", "8091")),
		HubAddr:         strings.TrimSpace(config.String("HUB_ADDR", "")),
		HubToken:        strings.TrimSpace(config.String("HUB_AUTH_TOKEN", "")),
		APIToken:        config.String("ANALYTICS_API_TOKEN", ""),
		ClickHouseDSN:   strings.TrimSpace(config.String("CLICKHOUSE_DSN", "")),
		ClickHouseTable: strings.TrimSpace(config.String("ANALYTICS_CLICKHOUSE_TABLE", "analytics_events")),
		RetentionDays:   config.Int("ANALYTICS_RETENTION_DAYS", 30),
		RedisAddr:       strings.TrimSpace(config.String("REDIS_ADDR", "")),
		NATSURL:         strings.TrimSpace(config.String("NATS_ANALYTICS_URL", config.String("NATS_URL", ""))),
		NATSSubject:     strings.TrimSpace(config.String("NATS_ANALYTICS_SUBJECT", "")),
		NATSQueue:       strings.TrimSpace(config.String("NATS_ANALYTICS_QUEUE", "")),
		MetricsAddr:     config.String("METRICS_ADDR", ":9100"),
	}
}

// Validate checks the configuration without touching the network.
func (c Config) Validate() error {
	var errs []error
	if c.ClickHouseDSN == "" && c.RedisAddr == "" {
		errs = append(errs, errors.New("CLICKHOUSE_DSN (or REDIS_ADDR for a Redis-backed store) is required"))
	}
	if c.NATSURL == "" {
		errs = append(errs, errors.New("NATS_ANALYTICS_URL or NATS_URL is required"))
	}
	if c.HubAddr == "" {
		errs = append(errs, errors.New("HUB_ADDR is required"))
	} else if !strings.HasPrefix(c.HubAddr, "http://") && !strings.HasPrefix(c.HubAddr, "https://") {
		errs = append(errs, fmt.Errorf("HUB_ADDR must be an http(s) URL, got %q", c.HubAddr))
	}
	return errors.Join(errs...)
}

// Analytics is a running analytics service.
type Analytics struct {
	httpServer *http.Server
	ln         net.Listener
	closers    []func() error
}

// Addr is the address the API and dashboard listen on.
func (a *Analytics) Addr() string { return a.ln.Addr().String() }

// Start validates cfg and starts serving. Ingestion starts once the hub and
// the store are reachable.
func Start(ctx context.Context, cfg Config) (*Analytics, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	policy, err := fetchRuntimePolicy(ctx, cfg)
	if err != nil {
		log.Printf("analytics: runtime policy fetch failed (%v); using defaults", err)
		policy = types.DefaultRuntimePolicy().Analytics
	}
	if cfg.NATSSubject == "" {
		cfg.NATSSubject = policy.NATSSubject
	}
	if cfg.NATSQueue == "" {
		cfg.NATSQueue = policy.NATSQueue
	}

	a := &Analytics{}
	var server *analyticsapi.Server
	var ping func(context.Context) error
	if cfg.ClickHouseDSN != "" {
		store, err := analyticsapi.NewClickHouseAnalyticsStore(cfg.ClickHouseDSN, cfg.ClickHouseTable, cfg.RetentionDays)
		if err != nil {
			return nil, fmt.Errorf("clickhouse store: %w", err)
		}
		a.closers = append(a.closers, store.Close)
		server = analyticsapi.NewServerWithAnalyticsStore(store, cfg.APIToken, policy)
		ping = store.Ping
	} else {
		rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
		a.closers = append(a.closers, rdb.Close)
		server = analyticsapi.NewServerWithPolicy(rdb, types.DefaultAnalyticsKey, cfg.APIToken, policy)
		ping = func(ctx context.Context) error { return rdb.Ping(ctx).Err() }
	}
	server.SetHubReady(false)
	server.SetPendingDependency("hub_readyz")

	telemetry.ServeMetrics(ctx, cfg.MetricsAddr)
	a.ln, err = net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		a.close()
		return nil, fmt.Errorf("http listen %s: %w", cfg.HTTPAddr, err)
	}
	a.httpServer = &http.Server{
		Handler:           telemetry.HTTPHandler(httpx.WithRequestID(server), "analytics"),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       policy.ReadTimeout,
		WriteTimeout:      policy.WriteTimeout,
		IdleTimeout:       policy.IdleTimeout,
	}
	go func() {
		log.Printf("analytics API and dashboard listening on %s", a.ln.Addr())
		if err := a.httpServer.Serve(a.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("analytics http server stopped: %v", err)
		}
	}()

	go watchReadiness(ctx, cfg, server, ping)
	return a, nil
}

func watchReadiness(ctx context.Context, cfg Config, server *analyticsapi.Server, ping func(context.Context) error) {
	hubReady := strings.TrimSuffix(cfg.HubAddr, "/") + "/readyz"
	client := &http.Client{Timeout: 2 * time.Second}
	var subscribed atomic.Bool
	ready.NewReadyWatcher("analytics", 500*time.Millisecond, 5*time.Second).WatchAll(ctx, []ready.Check{
		{
			Name: "hub_readyz",
			URL:  hubReady,
			Probe: func(probeCtx context.Context) error {
				req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, hubReady, nil)
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
		},
		{
			Name: "store",
			Probe: func(probeCtx context.Context) error {
				if err := ping(probeCtx); err != nil {
					server.SetPendingDependency("store")
					return err
				}
				return nil
			},
		},
		{
			Name: "nats",
			URL:  cfg.NATSURL,
			Probe: func(context.Context) error {
				if subscribed.Load() {
					return nil
				}
				if err := server.StartNATSSubscriber(ctx, cfg.NATSURL, cfg.NATSSubject, cfg.NATSQueue); err != nil {
					server.SetPendingDependency("nats")
					return err
				}
				subscribed.Store(true)
				server.StartBackgroundAggregator(ctx, 10*time.Second)
				return nil
			},
		},
	}, func(isReady bool) {
		server.SetHubReady(isReady)
		if isReady {
			server.SetPendingDependency("")
		}
	})
}

// fetchRuntimePolicy reads runtime.analytics from the hub's config.
func fetchRuntimePolicy(ctx context.Context, cfg Config) (types.AnalyticsRuntimePolicy, error) {
	defaults := types.DefaultRuntimePolicy().Analytics
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(cfg.HubAddr, "/")+"/config", nil)
	if err != nil {
		return defaults, err
	}
	if cfg.HubToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.HubToken)
	}
	resp, err := (&http.Client{Timeout: defaults.ConfigFetchTimeout}).Do(req)
	if err != nil {
		return defaults, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return defaults, fmt.Errorf("hub /config returned status %d", resp.StatusCode)
	}
	var snapshot struct {
		Runtime struct {
			Analytics types.AnalyticsRuntimePolicy `json:"analytics"`
		} `json:"runtime"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return defaults, fmt.Errorf("decode hub /config: %w", err)
	}
	return types.RuntimePolicy{Analytics: snapshot.Runtime.Analytics}.Effective().Analytics, nil
}

// Shutdown stops accepting requests and waits for in-flight ones.
func (a *Analytics) Shutdown(ctx context.Context) error {
	err := a.httpServer.Shutdown(ctx)
	a.close()
	return err
}

func (a *Analytics) close() {
	for _, c := range a.closers {
		_ = c()
	}
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	a, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	<-ctx.Done()
	log.Println("analytics shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.Shutdown(shutdownCtx)
}
