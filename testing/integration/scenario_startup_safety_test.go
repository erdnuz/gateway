//go:build integration

package integration

import (
	"encoding/json"
	"gateway/packages/common/types"
	"gateway/packages/edge"
	"gateway/packages/hub"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestIntegration_StartupSafety_EdgeFailsWhenHubIsUnavailableWithoutBootstrap(t *testing.T) {
	_, err := edge.NewConfigManagerWithFallback("http://127.0.0.1:1", "", nil, "")
	if err == nil {
		t.Fatal("expected edge startup config hydration failure when hub is unavailable and no bootstrap is provided")
	}
}

func TestIntegration_StartupSafety_EdgeRecoversWhenHubStartsLate(t *testing.T) {
	workDir := t.TempDir()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	_, cfg, err := writeTestConfig(workDir, upstream.URL)
	if err != nil {
		t.Fatalf("write test config: %v", err)
	}

	_, err = edge.NewConfigManagerWithFallback("http://127.0.0.1:1", "", nil, "")
	if err == nil {
		t.Fatal("expected initial startup failure before hub is available")
	}

	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/config" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg)
	}))
	defer hub.Close()

	cm, err := edge.NewConfigManagerWithFallback(hub.URL, "", nil, "")
	if err != nil {
		t.Fatalf("expected edge startup to recover after hub becomes available, got: %v", err)
	}
	if cm.Get() == nil || len(cm.Prefixes()) == 0 {
		t.Fatal("expected hydrated config after hub startup")
	}
}

func TestIntegration_StartupSafety_EdgeBootstrapMisconfigThenValidFallback(t *testing.T) {
	workDir := t.TempDir()
	badPath := filepath.Join(workDir, "bad-config.json")
	if err := os.WriteFile(badPath, []byte(`{"prefixes":[{"prefix":"v1"}`), 0o600); err != nil {
		t.Fatalf("write bad config: %v", err)
	}

	_, err := edge.NewConfigManagerWithFallback("http://127.0.0.1:1", "", nil, badPath)
	if err == nil {
		t.Fatal("expected startup failure with invalid bootstrap config")
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	goodPath, cfg, err := writeTestConfig(workDir, upstream.URL)
	if err != nil {
		t.Fatalf("write good config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected generated config to be valid, got: %v", err)
	}

	cm, err := edge.NewConfigManagerWithFallback("http://127.0.0.1:1", "", nil, goodPath)
	if err != nil {
		t.Fatalf("expected startup success with valid bootstrap fallback, got: %v", err)
	}
	if cm.Get() == nil {
		t.Fatal("expected non-nil config after valid bootstrap fallback")
	}

	if _, ok := cm.GetPrefix("v1"); !ok {
		t.Fatal("expected prefix v1 in hydrated fallback config")
	}
}

func TestIntegration_StartupSafety_HubConfigValidationRejectsInvalidConfig(t *testing.T) {
	cfg := &types.GatewayConfig{}
	if err := hub.ValidateGatewayConfig(cfg); err == nil {
		t.Fatal("expected invalid empty gateway config to fail validation")
	}
}
