package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestResolveConfigPathMissing(t *testing.T) {
	if _, err := resolveConfigPath("/definitely/missing/config.json"); err == nil {
		t.Fatal("expected error for missing config path")
	}
}

func TestHubStartupChecksValidateConfigMissingTLS(t *testing.T) {
	checks := &hubStartupChecks{
		configPath:   "../../cmd/config.json",
		hubAuthToken: "token",
	}
	if err := checks.ValidateConfig(context.Background()); err == nil {
		t.Fatal("expected missing TLS validation error")
	}
}

func TestHubStartupChecksCheckDependenciesNATSFailure(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start failed: %v", err)
	}
	defer mr.Close()

	checks := &hubStartupChecks{
		redisAddr:         mr.Addr(),
		redisDialTimeout:  200 * time.Millisecond,
		redisReadTimeout:  200 * time.Millisecond,
		redisWriteTimeout: 200 * time.Millisecond,
		redisPoolTimeout:  200 * time.Millisecond,
		redisPoolSize:     4,
		redisMinIdleConns: 1,
		enforceNoEviction: false,
		natsTierURL:       "nats://127.0.0.1:1",
	}

	err = checks.CheckDependencies(context.Background())
	if err == nil {
		t.Fatal("expected nats dependency failure")
	}
	if !strings.Contains(err.Error(), "nats") {
		t.Fatalf("expected nats error details, got %v", err)
	}
}
