//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/config"
	"gateway/packages/edge"
	"gateway/packages/hub"

	"google.golang.org/grpc"
)

// An edge started before its hub holds traffic, then serves once the hub
// comes up and streams the config, with no restart.
func TestIntegration_StartupSafety_EdgeWaitsForLateHub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer upstream.Close()
	cfgPath, _, err := writeTestConfig(t.TempDir(), upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hubAddr := ln.Addr().String()
	ln.Close() // the hub is not up yet

	hubClient, err := edge.DialHub(hubAddr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hubClient.Close()
	es := edge.NewServer(edge.Deps{Config: edge.NewConfigManager(nil), Tiers: edge.NewTierCache(hubClient.ResolveKey)}, edge.Options{})
	es.SetStartupReady(false)
	go es.FollowHub(ctx, hubClient)
	edgeSrv := httptest.NewServer(es)
	defer edgeSrv.Close()

	if code := status(t, edgeSrv.URL+"/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("edge without a hub reported readyz %d", code)
	}

	cfgMgr, err := hub.NewConfigManager(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	control := hub.NewControlPlane(cfgMgr, hub.NewInMemoryTierStore(), nil)
	grpcSrv := grpc.NewServer()
	types.RegisterControlPlaneServer(grpcSrv, control)
	ln, err = net.Listen("tcp", hubAddr)
	if err != nil {
		t.Fatalf("relisten %s: %v", hubAddr, err)
	}
	go func() { _ = grpcSrv.Serve(ln) }()
	defer grpcSrv.Stop()

	deadline := time.Now().Add(10 * time.Second)
	for status(t, edgeSrv.URL+"/readyz") != http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatal("edge never became ready after the hub started")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, ok := es.Config.GetPrefix("v1"); !ok {
		t.Fatal("edge is ready without the hub's config")
	}
}

func status(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestIntegration_StartupSafety_BootstrapConfigIsValidated(t *testing.T) {
	workDir := t.TempDir()
	badPath := filepath.Join(workDir, "bad-config.json")
	if err := os.WriteFile(badPath, []byte(`{"prefixes":[{"prefix":"v1"}`), 0o600); err != nil {
		t.Fatalf("write bad config: %v", err)
	}
	if _, err := config.Load(badPath); err == nil {
		t.Fatal("invalid bootstrap config accepted")
	}
	goodPath, _, err := writeTestConfig(workDir, "http://upstream.invalid")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(goodPath)
	if err != nil {
		t.Fatalf("valid bootstrap config rejected: %v", err)
	}
	if _, ok := edge.NewConfigManager(cfg).GetPrefix("v1"); !ok {
		t.Fatal("bootstrap config is missing prefix v1")
	}
}

func TestIntegration_StartupSafety_HubConfigValidationRejectsInvalidConfig(t *testing.T) {
	if err := hub.ValidateGatewayConfig(&types.GatewayConfig{}); err == nil {
		t.Fatal("expected invalid empty gateway config to fail validation")
	}
}
