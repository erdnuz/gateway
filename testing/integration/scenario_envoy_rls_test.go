//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/config"
	"gateway/packages/decide"
	"gateway/packages/hub"
	"gateway/packages/ledger"
	"gateway/packages/limiter"
	"gateway/packages/rls"

	rlsv3 "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
)

// A real Envoy, with its ratelimit filter pointed at Gate's decision
// service: Envoy enforces Gate's limit, adds Gate's RateLimit headers, and a
// shadow descriptor never denies.
func TestScenario_EnvoyRateLimitService(t *testing.T) {
	cfg, err := config.ParseAndCompile([]byte(`version: 2
descriptors:
  - domain: edge
    entries:
      - key: user
        limit: { requests: 3, window: 1h }
      - key: team
        limit: { requests: 1, window: 1h }
        mode: shadow
`), "envoy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	mem := ledger.NewMem()
	leases := hub.NewQuotaLeaseServer(mem, hub.NewStaticConfigManager(cfg), hub.NewInMemoryTierStore())
	client := inprocLeases{leases}
	dec := &decide.Decider{Limiter: limiter.NewLease(client, limiter.LeaseOptions{LeaseSize: 1}), Config: func() *types.GatewayConfig { return cfg }, Hub: client}

	// The decision service and an upstream, on host ports Envoy can reach.
	rlsLn, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		if err != nil {
			t.Logf("decision service error: %v", err)
		}
		return resp, err
	}))
	rlsv3.RegisterRateLimitServiceServer(srv, &rls.Server{Decider: dec})
	go func() { _ = srv.Serve(rlsLn) }()
	defer srv.Stop()
	upLn, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	up := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })}
	go func() { _ = up.Serve(upLn) }()
	defer up.Close()

	dir := t.TempDir()
	envoyCfg := fmt.Sprintf(envoyConfig, upLn.Addr().(*net.TCPAddr).Port, rlsLn.Addr().(*net.TCPAddr).Port)
	if err := os.WriteFile(filepath.Join(dir, "envoy.yaml"), []byte(envoyCfg), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	envoy, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "envoyproxy/envoy:v1.34-latest",
			ExposedPorts: []string{"10000/tcp"},
			Cmd:          []string{"envoy", "-c", "/etc/envoy/envoy.yaml", "--log-level", "warn"},
			Files:        []testcontainers.ContainerFile{{HostFilePath: filepath.Join(dir, "envoy.yaml"), ContainerFilePath: "/etc/envoy/envoy.yaml", FileMode: 0o644}},
			// Reach the host through testcontainers' tunnel (outbound from
			// this process, so host firewalls do not matter).
			HostAccessPorts: []int{upLn.Addr().(*net.TCPAddr).Port, rlsLn.Addr().(*net.TCPAddr).Port},
			WaitingFor:      wait.ForListeningPort("10000/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("envoy container unavailable: %v", err)
	}
	defer func() { _ = envoy.Terminate(context.Background()) }()
	host, _ := envoy.Host(ctx)
	port, _ := envoy.MappedPort(ctx, "10000/tcp")
	url := "http://" + net.JoinHostPort(host, port.Port()) + "/x"

	get := func(user, team string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("x-user", user)
		if team != "" {
			req.Header.Set("x-team", team)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp
	}
	// Envoy resolves its clusters asynchronously: wait until a request gets
	// through (a fresh user, so no quota under test is spent).
	ready := false
	for i := 0; i < 60 && !ready; i++ {
		ready = get(fmt.Sprintf("warm%d", i), "").StatusCode == 200
		if !ready {
			time.Sleep(250 * time.Millisecond)
		}
	}
	if !ready {
		if rc, err := envoy.Logs(ctx); err == nil {
			b, _ := io.ReadAll(rc)
			t.Logf("envoy logs:\n%s", b)
		}
		t.Fatal("Envoy never reached the upstream and the decision service")
	}
	var codes []int
	for i := 0; i < 4; i++ {
		codes = append(codes, get("alice", "").StatusCode)
	}
	if fmt.Sprint(codes) != "[200 200 200 429]" {
		t.Fatalf("alice through Envoy: %v, want [200 200 200 429]", codes)
	}
	if r := get("bob", ""); r.StatusCode != 200 || r.Header.Get("Ratelimit-Limit") != "3" {
		t.Fatalf("bob: %d, RateLimit-Limit %q (Gate's headers should pass through Envoy)", r.StatusCode, r.Header.Get("Ratelimit-Limit"))
	}
	for i := 0; i < 3; i++ { // team's limit of 1 is shadow: never denies
		if r := get("carol", "red"); r.StatusCode != 200 {
			t.Fatalf("shadow descriptor denied request %d: %d", i, r.StatusCode)
		}
	}
}

type inprocLeases struct{ srv *hub.QuotaLeaseServer }

func (c inprocLeases) RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	return c.srv.RequestQuotaLease(ctx, in)
}

func (c inprocLeases) ReturnLeases(ctx context.Context, in *types.ReturnLeasesRequest) (*types.ReturnLeasesResponse, error) {
	return c.srv.ReturnLeases(ctx, in)
}

// envoyConfig takes the upstream and rate-limit service ports on the host.
const envoyConfig = `
static_resources:
  listeners:
    - name: ingress
      address: { socket_address: { address: 0.0.0.0, port_value: 10000 } }
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: ingress
                route_config:
                  virtual_hosts:
                    - name: all
                      domains: ["*"]
                      routes:
                        - match: { prefix: "/" }
                          route:
                            cluster: upstream
                            rate_limits:
                              - actions: [{ request_headers: { header_name: x-user, descriptor_key: user } }]
                              - actions: [{ request_headers: { header_name: x-team, descriptor_key: team } }]
                http_filters:
                  - name: envoy.filters.http.ratelimit
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.ratelimit.v3.RateLimit
                      domain: edge
                      failure_mode_deny: true
                      timeout: 2s   # the default 20ms is too tight for the test's tunnel
                      rate_limit_service:
                        transport_api_version: V3
                        grpc_service: { envoy_grpc: { cluster_name: rls } }
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
    - name: upstream
      type: STRICT_DNS
      load_assignment:
        cluster_name: upstream
        endpoints: [{ lb_endpoints: [{ endpoint: { address: { socket_address: { address: host.testcontainers.internal, port_value: %d } } } }] }]
    - name: rls
      type: STRICT_DNS
      typed_extension_protocol_options:
        envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
          "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
          explicit_http_config: { http2_protocol_options: {} }
      load_assignment:
        cluster_name: rls
        endpoints: [{ lb_endpoints: [{ endpoint: { address: { socket_address: { address: host.testcontainers.internal, port_value: %d } } } }] }]
`
