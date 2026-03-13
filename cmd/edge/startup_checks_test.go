package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEdgeValidateConfigRejectsMissingHubServerName(t *testing.T) {
	checks := &edgeStartupChecks{
		redisAddr:         "localhost:6379",
		hubAddr:           "http://localhost:8080",
		natsTierURL:       "nats://localhost:4222",
		natsAnalyticsURL:  "nats://localhost:4222",
		analyticsEnabled:  true,
		hubGRPCAddr:       "localhost:9090",
		hubGRPCServerName: "",
		edgeTLSCertFile:   "cert",
		edgeTLSKeyFile:    "key",
		edgeTLSCAFile:     "ca",
	}
	if err := checks.ValidateConfig(context.Background()); err == nil {
		t.Fatal("expected HUB_GRPC_SERVER_NAME validation error")
	}
}

func TestWaitUntilReadyHubOnly(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer hub.Close()

	err := waitUntilReady(context.Background(), waitUntilReadyOptions{
		HubBaseURL:       hub.URL,
		AnalyticsEnabled: false,
		MaxAttempts:      3,
		InitialBackoff:   10 * time.Millisecond,
		MaxBackoff:       20 * time.Millisecond,
		RequestTimeout:   100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("expected handshake success, got %v", err)
	}
}

func TestWaitUntilReadyRetriesUntilSuccess(t *testing.T) {
	attempt := 0
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		attempt++
		if attempt < 3 {
			http.Error(w, "warming", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer hub.Close()

	err := waitUntilReady(context.Background(), waitUntilReadyOptions{
		HubBaseURL:       hub.URL,
		AnalyticsEnabled: false,
		MaxAttempts:      5,
		InitialBackoff:   10 * time.Millisecond,
		MaxBackoff:       20 * time.Millisecond,
		RequestTimeout:   100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
}

func TestWaitUntilReadyFailsAfterExhaustion(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer hub.Close()

	err := waitUntilReady(context.Background(), waitUntilReadyOptions{
		HubBaseURL:       hub.URL,
		AnalyticsEnabled: false,
		MaxAttempts:      2,
		InitialBackoff:   10 * time.Millisecond,
		MaxBackoff:       20 * time.Millisecond,
		RequestTimeout:   100 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected handshake failure")
	}
}

func TestWaitUntilReadyWithAnalytics(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer hub.Close()
	analytics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer analytics.Close()

	err := waitUntilReady(context.Background(), waitUntilReadyOptions{
		HubBaseURL:       hub.URL,
		AnalyticsBaseURL: analytics.URL,
		AnalyticsEnabled: true,
		MaxAttempts:      3,
		InitialBackoff:   10 * time.Millisecond,
		MaxBackoff:       20 * time.Millisecond,
		RequestTimeout:   100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("expected handshake success with analytics enabled, got %v", err)
	}
}

func TestCheckNATSCanaryFails(t *testing.T) {
	err := checkNATSCanary("nats://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected nats canary failure")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "nats") {
		t.Fatalf("expected nats error details, got %v", err)
	}
}
