package testing

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRedeployScriptEnforcesSequentialStartupOrder(t *testing.T) {
	script := readDeployScript(t)

	hubUp := strings.Index(script, "docker compose -f docker-compose.hub.yaml")
	hubReady := strings.Index(script, "wait_for_http \"hub\" \"http://localhost:8080/readyz\"")
	analyticsUp := strings.Index(script, "docker compose -f docker-compose.analytics.yaml")
	analyticsReady := strings.Index(script, "wait_for_http \"analytics\" \"http://localhost:8091/readyz\"")
	edgeUp := strings.Index(script, "docker compose -f docker-compose.edge.yaml")
	edgeReady := strings.Index(script, "wait_for_http \"edge\" \"http://localhost:8082/readyz\"")

	if hubUp < 0 || hubReady < 0 || analyticsUp < 0 || analyticsReady < 0 || edgeUp < 0 || edgeReady < 0 {
		t.Fatalf("missing expected startup or readiness gates in redeploy script")
	}
	if !(hubUp < hubReady && hubReady < analyticsUp && analyticsUp < analyticsReady && analyticsReady < edgeUp && edgeUp < edgeReady) {
		t.Fatalf("startup order must be hub->analytics->edge with readyz gating in between")
	}
}

func TestRedeployScriptUsesReadyzForGating(t *testing.T) {
	script := readDeployScript(t)

	for _, endpoint := range []string{
		"http://localhost:8080/readyz",
		"http://localhost:8091/readyz",
		"http://localhost:8082/readyz",
	} {
		if !strings.Contains(script, endpoint) {
			t.Fatalf("expected readiness endpoint %s in redeploy script", endpoint)
		}
	}

	for _, oldEndpoint := range []string{
		"http://localhost:8080/healthz",
		"http://localhost:8091/healthz",
		"http://localhost:8082/healthz",
	} {
		if strings.Contains(script, oldEndpoint) {
			t.Fatalf("did not expect liveness endpoint %s as startup gate", oldEndpoint)
		}
	}
}

func TestHubComposeProvidesTierNATSBeforeHubStartup(t *testing.T) {
	composePath := filepath.Join("..", "deployments", "docker-compose.hub.yaml")
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read hub compose: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "nats-tier:") {
		t.Fatal("expected hub compose to define tier NATS service")
	}
	if !strings.Contains(text, "condition: service_healthy") {
		t.Fatal("expected hub compose to gate startup on dependency health")
	}
	if !strings.Contains(text, "NATS_TIER_URL=${NATS_TIER_URL:-nats://nats-tier:4222}") {
		t.Fatal("expected hub compose to default tier updates to local nats-tier service")
	}
	if !strings.Contains(text, "container_name: nats-tier") {
		t.Fatal("expected stable nats-tier container name for later edge connectivity")
	}
	if strings.Contains(text, "edge-host") {
		t.Fatal("did not expect hub compose to depend on an edge-host tier broker")
	}
	if strings.Contains(text, "demo.nats.io") {
		t.Fatal("did not expect external demo.nats.io dependency in hub compose")
	}
}

func TestEdgeEnvDoesNotUseExternalTierNATS(t *testing.T) {
	envPath := filepath.Join("..", "deployments", ".env.edge")
	data, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("read edge env: %v", err)
	}
	text := string(data)
	if strings.Contains(text, "demo.nats.io") {
		t.Fatal("did not expect external demo.nats.io tier broker in edge env")
	}
	if !strings.Contains(text, "NATS_TIER_URL=nats://nats-tier:4222") {
		t.Fatal("expected edge env to point tier updates at the internal nats-tier broker")
	}
}

func TestEdgeComposeProvidesAnalyticsHandshakeAddress(t *testing.T) {
	composePath := filepath.Join("..", "deployments", "docker-compose.edge.yaml")
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read edge compose: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "ANALYTICS_ADDR=") {
		t.Fatal("expected ANALYTICS_ADDR to be provided for edge startup handshake")
	}
}

func readDeployScript(t *testing.T) string {
	t.Helper()
	scriptPath := filepath.Join("..", "deployments", "redeploy_fresh_all_docker.sh")
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read deploy script: %v", err)
	}
	return string(data)
}
