package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"gateway/packages/common/config"
	"gateway/packages/hub"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

type hubStartupChecks struct {
	configPath          string
	redisAddr           string
	redisDialTimeout    time.Duration
	redisReadTimeout    time.Duration
	redisWriteTimeout   time.Duration
	redisPoolTimeout    time.Duration
	redisPoolSize       int
	redisMinIdleConns   int
	enforceNoEviction   bool
	hubAuthToken        string
	tlsCertFile         string
	tlsKeyFile          string
	tlsCAFile           string
	grpcClientAuthMode  string
	natsTierURL         string
	natsTierUpdatesSubj string
}

func resolveConfigPath(raw string) (string, error) {
	configFilePath := strings.TrimSpace(raw)
	if configFilePath == "" {
		configFilePath = "/cmd/config.json"
	}
	if _, statErr := os.Stat(configFilePath); statErr == nil {
		return configFilePath, nil
	}
	for _, candidate := range []string{"config/policies.json", "cmd/config.json", "/cmd/config.json"} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("config file not found: %s", configFilePath)
}

func newHubStartupChecks() (*hubStartupChecks, error) {
	configPath, err := resolveConfigPath(config.String("CONFIG_FILE_PATH", "/cmd/config.json"))
	if err != nil {
		return nil, err
	}
	hubAuthToken, err := config.Required("HUB_AUTH_TOKEN")
	if err != nil {
		return nil, err
	}
	return &hubStartupChecks{
		configPath:          configPath,
		redisAddr:           config.String("REDIS_ADDR", "localhost:6379"),
		redisDialTimeout:    config.Duration("HUB_REDIS_DIAL_TIMEOUT", 200*time.Millisecond),
		redisReadTimeout:    config.Duration("HUB_REDIS_READ_TIMEOUT", 300*time.Millisecond),
		redisWriteTimeout:   config.Duration("HUB_REDIS_WRITE_TIMEOUT", 300*time.Millisecond),
		redisPoolTimeout:    config.Duration("HUB_REDIS_POOL_TIMEOUT", 500*time.Millisecond),
		redisPoolSize:       config.Int("HUB_REDIS_POOL_SIZE", 64),
		redisMinIdleConns:   config.Int("HUB_REDIS_MIN_IDLE_CONNS", 16),
		enforceNoEviction:   config.Bool("HUB_ENFORCE_REDIS_NOEVICTION", true),
		hubAuthToken:        strings.TrimSpace(hubAuthToken),
		tlsCertFile:         strings.TrimSpace(config.String("HUB_TLS_CERT_FILE", "")),
		tlsKeyFile:          strings.TrimSpace(config.String("HUB_TLS_KEY_FILE", "")),
		tlsCAFile:           strings.TrimSpace(config.String("HUB_TLS_CA_FILE", "")),
		grpcClientAuthMode:  strings.TrimSpace(config.String("HUB_GRPC_CLIENT_AUTH_MODE", "require-and-verify")),
		natsTierURL:         config.String("NATS_TIER_URL", config.String("NATS_URL", nats.DefaultURL)),
		natsTierUpdatesSubj: strings.TrimSpace(config.String("NATS_TIER_UPDATES_SUBJECT", "")),
	}, nil
}

func (h *hubStartupChecks) ValidateConfig(_ context.Context) error {
	if h.hubAuthToken == "" {
		return fmt.Errorf("HUB_AUTH_TOKEN must be set")
	}
	if h.configPath == "" {
		return fmt.Errorf("CONFIG_FILE_PATH must resolve to an existing file")
	}
	if _, err := hub.NewConfigManager(h.configPath); err != nil {
		return fmt.Errorf("gateway config validation failed: %w", err)
	}
	if h.tlsCertFile == "" || h.tlsKeyFile == "" || h.tlsCAFile == "" {
		return fmt.Errorf("HUB_TLS_CERT_FILE, HUB_TLS_KEY_FILE and HUB_TLS_CA_FILE must be set")
	}
	if _, err := newHubGRPCServer(h.tlsCertFile, h.tlsKeyFile, h.tlsCAFile, h.grpcClientAuthMode); err != nil {
		return fmt.Errorf("grpc tls validation failed: %w", err)
	}
	return nil
}

func (h *hubStartupChecks) CheckDependencies(ctx context.Context) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:         h.redisAddr,
		DialTimeout:  h.redisDialTimeout,
		ReadTimeout:  h.redisReadTimeout,
		WriteTimeout: h.redisWriteTimeout,
		PoolTimeout:  h.redisPoolTimeout,
		PoolSize:     h.redisPoolSize,
		MinIdleConns: h.redisMinIdleConns,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping failed at %s: %w", h.redisAddr, err)
	}
	if h.enforceNoEviction {
		if err := enforceNoEvictionPolicy(ctx, rdb); err != nil {
			return fmt.Errorf("redis no-eviction policy check failed: %w", err)
		}
	}

	nc, err := nats.Connect(h.natsTierURL)
	if err != nil {
		return fmt.Errorf("nats connect failed at %s: %w", h.natsTierURL, err)
	}
	defer nc.Close()
	if err := nc.Flush(); err != nil {
		return fmt.Errorf("nats flush failed at %s: %w", h.natsTierURL, err)
	}
	if err := nc.LastError(); err != nil {
		return fmt.Errorf("nats connectivity check failed at %s: %w", h.natsTierURL, err)
	}
	return nil
}
