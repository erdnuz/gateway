package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gateway/packages/common/config"
	"gateway/packages/common/types"
	"gateway/packages/edge"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type edgeStartupChecks struct {
	redisAddr         string
	hubAddr           string
	hubToken          string
	analyticsEnabled  bool
	natsTierURL       string
	natsAnalyticsURL  string
	hubGRPCAddr       string
	hubGRPCServerName string
	edgeTLSCertFile   string
	edgeTLSKeyFile    string
	edgeTLSCAFile     string
	bootstrapConfig   string
}

func newEdgeStartupChecks() *edgeStartupChecks {
	natsURL := config.String("NATS_URL", nats.DefaultURL)
	return &edgeStartupChecks{
		redisAddr:         strings.TrimSpace(config.String("REDIS_ADDR", "localhost:6379")),
		hubAddr:           strings.TrimSpace(config.String("HUB_ADDR", "http://localhost:8081")),
		hubToken:          strings.TrimSpace(config.String("HUB_AUTH_TOKEN", "")),
		analyticsEnabled:  config.Bool("GATE_ANALYTICS_ENABLED", config.Bool("ANALYTICS_ENABLED", true)),
		natsTierURL:       strings.TrimSpace(config.String("NATS_TIER_URL", natsURL)),
		natsAnalyticsURL:  strings.TrimSpace(config.String("NATS_ANALYTICS_URL", natsURL)),
		hubGRPCAddr:       strings.TrimSpace(config.String("HUB_GRPC_ADDR", "localhost:9090")),
		hubGRPCServerName: strings.TrimSpace(config.String("HUB_GRPC_SERVER_NAME", "")),
		edgeTLSCertFile:   strings.TrimSpace(config.String("EDGE_TLS_CERT_FILE", "")),
		edgeTLSKeyFile:    strings.TrimSpace(config.String("EDGE_TLS_KEY_FILE", "")),
		edgeTLSCAFile:     strings.TrimSpace(config.String("EDGE_TLS_CA_FILE", "")),
		bootstrapConfig:   strings.TrimSpace(config.String("EDGE_BOOTSTRAP_CONFIG_FILE", "")),
	}
}

func (e *edgeStartupChecks) ValidateConfig(_ context.Context) error {
	if e.redisAddr == "" {
		return fmt.Errorf("REDIS_ADDR is required")
	}
	if e.hubAddr == "" {
		return fmt.Errorf("HUB_ADDR is required")
	}
	parsedHub, err := url.ParseRequestURI(e.hubAddr)
	if err != nil {
		return fmt.Errorf("HUB_ADDR must be a valid URL: %w", err)
	}
	if parsedHub.Scheme != "http" && parsedHub.Scheme != "https" {
		return fmt.Errorf("HUB_ADDR scheme must be http or https")
	}
	if strings.TrimSpace(parsedHub.Host) == "" {
		return fmt.Errorf("HUB_ADDR host is required")
	}
	if e.natsTierURL == "" {
		return fmt.Errorf("NATS_TIER_URL or NATS_URL is required")
	}
	if e.analyticsEnabled && e.natsAnalyticsURL == "" {
		return fmt.Errorf("NATS_ANALYTICS_URL or NATS_URL is required when analytics is enabled")
	}
	if e.hubGRPCServerName == "" {
		return fmt.Errorf("HUB_GRPC_SERVER_NAME must be set")
	}
	if e.edgeTLSCertFile == "" || e.edgeTLSKeyFile == "" || e.edgeTLSCAFile == "" {
		return fmt.Errorf("EDGE_TLS_CERT_FILE, EDGE_TLS_KEY_FILE, and EDGE_TLS_CA_FILE must be set")
	}
	if _, err := edge.NewGRPCQuotaLeaseClient(e.hubGRPCAddr, e.hubGRPCServerName, e.edgeTLSCertFile, e.edgeTLSKeyFile, e.edgeTLSCAFile); err != nil {
		return fmt.Errorf("edge lease grpc tls validation failed: %w", err)
	}
	if e.bootstrapConfig != "" {
		if err := validateBootstrapConfig(e.bootstrapConfig); err != nil {
			return err
		}
	}
	return nil
}

func (e *edgeStartupChecks) CheckDependencies(ctx context.Context) error {
	if err := e.checkRedis(ctx); err != nil {
		return err
	}
	if err := checkNATSCanary(e.natsTierURL); err != nil {
		return fmt.Errorf("tier nats connectivity check failed: %w", err)
	}
	if e.analyticsEnabled {
		if err := checkNATSCanary(e.natsAnalyticsURL); err != nil {
			return fmt.Errorf("analytics nats connectivity check failed: %w", err)
		}
	}
	if err := e.checkLeaseGRPC(ctx); err != nil {
		return err
	}
	return nil
}

func (e *edgeStartupChecks) checkRedis(ctx context.Context) error {
	rdb := redis.NewClient(&redis.Options{Addr: e.redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping failed at %s: %w", e.redisAddr, err)
	}
	key := "edge:start:canary"
	if err := rdb.Set(ctx, key, "ok", 5*time.Second).Err(); err != nil {
		return fmt.Errorf("redis set canary failed: %w", err)
	}
	if _, err := rdb.Get(ctx, key).Result(); err != nil {
		return fmt.Errorf("redis get canary failed: %w", err)
	}
	_ = rdb.Del(ctx, key).Err()
	return nil
}

func (e *edgeStartupChecks) checkLeaseGRPC(ctx context.Context) error {
	client, err := edge.NewGRPCQuotaLeaseClient(e.hubGRPCAddr, e.hubGRPCServerName, e.edgeTLSCertFile, e.edgeTLSKeyFile, e.edgeTLSCAFile)
	if err != nil {
		return fmt.Errorf("lease grpc client init failed: %w", err)
	}
	defer client.Close()
	_, err = client.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{
		Prefix:          "startup-canary",
		ServiceId:       "startup-canary",
		ApiKey:          "startup-canary",
		RequestedTokens: 1,
	})
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("lease grpc canary failed: %w", err)
	}
	if st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded {
		return fmt.Errorf("lease grpc connectivity failed: %w", err)
	}
	// Any non-transport RPC error still proves endpoint reachability.
	return nil
}

func validateBootstrapConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("bootstrap config read failed (%s): %w", path, err)
	}
	var cfg types.GatewayConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("bootstrap config decode failed (%s): %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("bootstrap config validation failed (%s): %w", path, err)
	}
	return nil
}

func checkNATSCanary(natsURL string) error {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("nats connect failed at %s: %w", natsURL, err)
	}
	defer nc.Close()
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return fmt.Errorf("nats subscribe canary failed: %w", err)
	}
	defer sub.Unsubscribe()
	if err := nc.Publish(inbox, []byte("edge-startup-canary")); err != nil {
		return fmt.Errorf("nats publish canary failed: %w", err)
	}
	if err := nc.Flush(); err != nil {
		return fmt.Errorf("nats flush canary failed: %w", err)
	}
	if err := nc.LastError(); err != nil {
		return fmt.Errorf("nats last error after canary: %w", err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		return fmt.Errorf("nats canary receive failed: %w", err)
	}
	return nil
}
