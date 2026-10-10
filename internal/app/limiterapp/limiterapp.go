// Package limiterapp assembles and runs the decision service (gate
// limiter): Gate's rate limiter without the gateway, for Envoy and any other
// caller. It is stateless like an edge: it leases quota from the hub, follows
// the hub's config, and scales out behind a load balancer.
package limiterapp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gateway/internal/app/appkit"
	"gateway/packages/common/config"
	"gateway/packages/common/httpx"
	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"
	"gateway/packages/decide"
	"gateway/packages/edge"
	"gateway/packages/limiter"
	"gateway/packages/rls"

	rlsv3 "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	"google.golang.org/grpc"
)

// Config is everything the decision service needs to start.
type Config struct {
	HTTPAddr string // JSON API (/v1/check, /v1/settle) and health
	GRPCAddr string // Envoy's RateLimitService
	ID       string

	HubGRPCAddr       string
	HubGRPCServerName string
	TLSCertFile       string
	TLSKeyFile        string
	TLSCAFile         string

	// Secret signs reservations; replicas that settle each other's
	// reservations must share it. Without one, settle is disabled.
	Secret      string
	Token       string // bearer token for the JSON API; empty: open
	MetricsAddr string
}

// FromEnv reads the configuration from environment variables.
func FromEnv() Config {
	return Config{
		HTTPAddr:          appkit.ListenAddr(config.String("PORT", "8080")),
		GRPCAddr:          appkit.ListenAddr(config.String("LIMITER_GRPC_PORT", "8081")),
		ID:                config.String("LIMITER_ID", ""),
		HubGRPCAddr:       strings.TrimSpace(config.String("HUB_GRPC_ADDR", "localhost:9090")),
		HubGRPCServerName: strings.TrimSpace(config.String("HUB_GRPC_SERVER_NAME", "")),
		TLSCertFile:       strings.TrimSpace(config.String("LIMITER_TLS_CERT_FILE", "")),
		TLSKeyFile:        strings.TrimSpace(config.String("LIMITER_TLS_KEY_FILE", "")),
		TLSCAFile:         strings.TrimSpace(config.String("LIMITER_TLS_CA_FILE", "")),
		Secret:            config.String("LIMITER_SECRET", ""),
		Token:             config.String("LIMITER_TOKEN", ""),
		MetricsAddr:       config.String("METRICS_ADDR", ":9100"),
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
		errs = append(errs, errors.New("LIMITER_TLS_CERT_FILE, LIMITER_TLS_KEY_FILE and LIMITER_TLS_CA_FILE are required unless the hub is on loopback"))
	case !c.plaintext() && c.HubGRPCServerName == "":
		errs = append(errs, errors.New("HUB_GRPC_SERVER_NAME is required with TLS"))
	}
	if c.HTTPAddr == "" && c.GRPCAddr == "" {
		errs = append(errs, errors.New("set PORT or LIMITER_GRPC_PORT: the service needs something to serve"))
	}
	return errors.Join(errs...)
}

// Service is a running decision service.
type Service struct {
	hub     *edge.HubClient
	lease   *limiter.Lease
	http    *http.Server
	httpLn  net.Listener
	grpc    *grpc.Server
	grpcLn  net.Listener
	ready   atomic.Bool
	cfg     atomic.Pointer[types.GatewayConfig]
	decider *decide.Decider
}

// HTTPAddr is the JSON API's address ("" when disabled).
func (s *Service) HTTPAddr() string {
	if s.httpLn == nil {
		return ""
	}
	return s.httpLn.Addr().String()
}

// GRPCAddr is the RateLimitService's address ("" when disabled).
func (s *Service) GRPCAddr() string {
	if s.grpcLn == nil {
		return ""
	}
	return s.grpcLn.Addr().String()
}

// Start validates cfg, connects to the hub and starts serving.
func Start(ctx context.Context, cfg Config) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
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
	id := cfg.ID
	if id == "" {
		host, _ := os.Hostname()
		id = "limiter-" + host + "-" + strconv.FormatUint(rand.Uint64()%1e6, 36)
	}
	s := &Service{hub: hub}
	s.cfg.Store(&types.GatewayConfig{})
	s.lease = limiter.NewLease(limiter.NewBatcher(hub), limiter.LeaseOptions{EdgeID: id})
	go s.lease.Run(ctx)
	s.decider = &decide.Decider{Limiter: s.lease, Config: s.cfg.Load, Hub: hub, Secret: []byte(cfg.Secret), EdgeID: id}
	if cfg.Secret == "" {
		slog.Warn("LIMITER_SECRET is not set: reservations cannot be settled")
	}
	go hub.Watch(ctx, id, s.apply)
	telemetry.ServeMetrics(ctx, cfg.MetricsAddr)

	ok := false
	defer func() {
		if !ok {
			s.close()
		}
	}()
	if cfg.GRPCAddr != "" {
		s.grpc = grpc.NewServer(telemetry.GRPCServerOptions()...)
		rlsv3.RegisterRateLimitServiceServer(s.grpc, &rls.Server{Decider: s.decider})
		if s.grpcLn, err = net.Listen("tcp", cfg.GRPCAddr); err != nil {
			return nil, fmt.Errorf("grpc listen %s: %w", cfg.GRPCAddr, err)
		}
		go func() {
			slog.Info("limiter RateLimitService listening", "addr", s.grpcLn.Addr().String())
			if err := s.grpc.Serve(s.grpcLn); err != nil {
				slog.Error("limiter grpc server stopped", "err", err)
			}
		}()
	}
	if cfg.HTTPAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		})
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			if !s.ready.Load() {
				httpx.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "pending", "missing_dependency": "hub_config"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		})
		mux.Handle("/v1/", s.decider.HTTP(cfg.Token))
		if s.httpLn, err = net.Listen("tcp", cfg.HTTPAddr); err != nil {
			return nil, fmt.Errorf("http listen %s: %w", cfg.HTTPAddr, err)
		}
		s.http = &http.Server{Handler: telemetry.HTTPHandler(httpx.WithRequestID(mux), "limiter"), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			slog.Info("limiter JSON API listening", "addr", s.httpLn.Addr().String())
			if err := s.http.Serve(s.httpLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("limiter http server stopped", "err", err)
			}
		}()
	}
	ok = true
	return s, nil
}

// apply follows the hub's config.
func (s *Service) apply(ev *types.ControlEvent) {
	snap := ev.GetConfig()
	if snap == nil {
		return
	}
	var cfg types.GatewayConfig
	if err := json.Unmarshal(snap.GetConfigJson(), &cfg); err != nil {
		slog.Error("ignoring undecodable config snapshot", "err", err)
		return
	}
	s.cfg.Store(&cfg)
	if !s.ready.Swap(true) {
		slog.Info("limiter ready: config received from hub", "domains", len(cfg.Descriptors))
	}
}

// Shutdown stops serving, then returns unspent quota to the hub.
func (s *Service) Shutdown(ctx context.Context) error {
	var err error
	if s.http != nil {
		err = s.http.Shutdown(ctx)
	}
	if s.grpc != nil {
		s.grpc.GracefulStop()
	}
	if derr := s.lease.Drain(ctx); derr != nil {
		slog.Warn("returning leases on shutdown failed; they expire with their window", "err", derr)
	}
	s.close()
	return err
}

func (s *Service) close() {
	if s.grpc != nil {
		s.grpc.Stop()
	}
	_ = s.hub.Close()
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	s, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	<-ctx.Done()
	slog.Info("limiter shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return s.Shutdown(sctx)
}
