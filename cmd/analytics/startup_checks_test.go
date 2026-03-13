package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gateway/packages/common/types"
)

func TestNewAnalyticsStartupChecksRequiresClickHouseDSN(t *testing.T) {
	t.Setenv("CLICKHOUSE_DSN", "")
	if _, err := newAnalyticsStartupChecks(types.DefaultRuntimePolicy().Analytics); err == nil {
		t.Fatal("expected required CLICKHOUSE_DSN error")
	}
}

func TestAnalyticsValidateConfigRequiresHubAddr(t *testing.T) {
	t.Setenv("CLICKHOUSE_DSN", "clickhouse://localhost:9000/default")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("NATS_ANALYTICS_SUBJECT", "analytics.events")
	t.Setenv("HUB_ADDR", "")
	checks, err := newAnalyticsStartupChecks(types.DefaultRuntimePolicy().Analytics)
	if err != nil {
		t.Fatalf("unexpected setup error: %v", err)
	}
	if err := checks.ValidateConfig(context.Background()); err == nil {
		t.Fatal("expected HUB_ADDR validation error")
	}
}

func TestAnalyticsValidateConfigRejectsBadHubURL(t *testing.T) {
	t.Setenv("CLICKHOUSE_DSN", "clickhouse://localhost:9000/default")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("NATS_ANALYTICS_SUBJECT", "analytics.events")
	t.Setenv("HUB_ADDR", "::bad-url")
	checks, err := newAnalyticsStartupChecks(types.DefaultRuntimePolicy().Analytics)
	if err != nil {
		t.Fatalf("unexpected setup error: %v", err)
	}
	if err := checks.ValidateConfig(context.Background()); err == nil {
		t.Fatal("expected invalid HUB_ADDR error")
	}
}

func TestAnalyticsCheckDependenciesFailsOnClickHouse(t *testing.T) {
	checks := &analyticsStartupChecks{
		clickHouseDSN:   "clickhouse://127.0.0.1:1/default",
		clickHouseTable: "analytics_events",
		natsURL:         "nats://127.0.0.1:1",
		natsSubject:     "analytics.events",
		hubAddr:         "http://127.0.0.1:65535",
		hubHTTPTimeout:  200 * time.Millisecond,
	}
	err := checks.CheckDependencies(context.Background())
	if err == nil {
		t.Fatal("expected dependency check failure")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "clickhouse") {
		t.Fatalf("expected clickhouse error, got %v", err)
	}
}

func TestLoadAnalyticsRuntimePolicyErrorsWithoutHubAddr(t *testing.T) {
	t.Setenv("HUB_ADDR", "")
	_, err := loadAnalyticsRuntimePolicy(context.Background())
	if err == nil {
		t.Fatal("expected HUB_ADDR error")
	}
}

func TestLoadAnalyticsRuntimePolicyErrorsOnNon200(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusServiceUnavailable)
	}))
	defer hub.Close()
	t.Setenv("HUB_ADDR", hub.URL)

	_, err := loadAnalyticsRuntimePolicy(context.Background())
	if err == nil {
		t.Fatal("expected non-200 policy fetch error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "status") {
		t.Fatalf("expected status error details, got %v", err)
	}
}
