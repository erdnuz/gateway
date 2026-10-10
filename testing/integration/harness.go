//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	analyticsapi "gateway/packages/analytics"
	"gateway/packages/common/types"
	"gateway/packages/config"
	"gateway/packages/edge"
	"gateway/packages/hub"
	"gateway/packages/keys"
	"gateway/packages/ledger"
	"gateway/packages/limiter"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type integrationHarness struct {
	t          *testing.T
	ctx        context.Context
	cancel     context.CancelFunc
	workingDir string
	configPath string

	redisContainer testcontainers.Container
	natsContainer  testcontainers.Container

	redisAddr string
	natsURL   string

	rdb *redis.Client

	upstreamHits atomic.Int64
	upstreamSrv  *httptest.Server
	analyticsSrv *httptest.Server
	analyticsAll []*httptest.Server
	hubHTTP      *httptest.Server
	edgeHTTP     *httptest.Server
	edgeHTTPAll  []*httptest.Server

	hubGRPCSrv      *grpc.Server
	hubGRPCListener net.Listener

	hubTierStore *hub.InMemoryTierStore
	hubControl   *hub.ControlPlane
	edgeLeases   []*limiter.Lease
	analyticsKey string
	mtlsFiles    *mtlsFiles
}

func newIntegrationHarness(t *testing.T) *integrationHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &integrationHarness{t: t, ctx: ctx, cancel: cancel, analyticsKey: types.DefaultAnalyticsKey}

	workDir, err := os.MkdirTemp("", "gate-it-*")
	if err != nil {
		t.Fatalf("mk temp dir: %v", err)
	}
	h.workingDir = workDir

	if err := h.startInfra(); err != nil {
		t.Skipf("real infra unavailable (docker/testcontainers): %v", err)
	}
	h.rdb = redis.NewClient(&redis.Options{Addr: h.redisAddr})

	h.startUpstream()
	certDir := filepath.Join(h.workingDir, "certs")
	h.mtlsFiles, err = generateMTLSFiles(certDir, "hub-server")
	if err != nil {
		t.Fatalf("generate certs: %v", err)
	}
	configPath, _, err := writeTestConfig(h.workingDir, h.upstreamSrv.URL)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}
	h.configPath = configPath

	h.hubTierStore = hub.NewInMemoryTierStore()
	h.startHubAndLeaseGRPC(configPath)
	h.startAnalytics()
	h.startEdge(configPath)
	h.startEdge(configPath)

	h.waitReady(h.hubHTTP.URL + "/healthz")
	for _, analyticsSrv := range h.analyticsAll {
		h.waitReady(analyticsSrv.URL + "/healthz")
	}
	for _, edgeSrv := range h.edgeHTTPAll {
		h.waitReady(edgeSrv.URL + "/readyz")
	}
	return h
}

func (h *integrationHarness) close() {
	h.cancel()
	if h.edgeHTTP != nil {
		h.edgeHTTP.Close()
	}
	for _, edgeSrv := range h.edgeHTTPAll {
		if edgeSrv != nil && edgeSrv != h.edgeHTTP {
			edgeSrv.Close()
		}
	}
	if h.hubHTTP != nil {
		h.hubHTTP.Close()
	}
	if h.analyticsSrv != nil {
		h.analyticsSrv.Close()
	}
	for _, analyticsSrv := range h.analyticsAll {
		if analyticsSrv != nil && analyticsSrv != h.analyticsSrv {
			analyticsSrv.Close()
		}
	}
	if h.upstreamSrv != nil {
		h.upstreamSrv.Close()
	}
	if h.hubGRPCSrv != nil {
		h.hubGRPCSrv.GracefulStop()
	}
	if h.hubGRPCListener != nil {
		_ = h.hubGRPCListener.Close()
	}
	if h.rdb != nil {
		_ = h.rdb.Close()
	}
	if h.redisContainer != nil {
		_ = h.redisContainer.Terminate(context.Background())
	}
	if h.natsContainer != nil {
		_ = h.natsContainer.Terminate(context.Background())
	}
	if h.workingDir != "" {
		_ = os.RemoveAll(h.workingDir)
	}
}

func (h *integrationHarness) startInfra() error {
	if err := h.startRedisContainer(); err != nil {
		return err
	}
	if err := h.startNATSContainer(); err != nil {
		return err
	}
	return nil
}

func (h *integrationHarness) startRedisContainer() error {
	ctx, cancel := context.WithTimeout(h.ctx, 60*time.Second)
	defer cancel()
	cont, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("start redis container: %w", err)
	}
	h.redisContainer = cont
	host, err := cont.Host(ctx)
	if err != nil {
		return err
	}
	port, err := cont.MappedPort(ctx, "6379/tcp")
	if err != nil {
		return err
	}
	h.redisAddr = net.JoinHostPort(host, port.Port())
	return nil
}

func (h *integrationHarness) startNATSContainer() error {
	ctx, cancel := context.WithTimeout(h.ctx, 90*time.Second)
	defer cancel()
	cont, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.10-alpine",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"-js"},
			WaitingFor:   wait.ForListeningPort("4222/tcp").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("start nats container: %w", err)
	}
	h.natsContainer = cont
	host, err := cont.Host(ctx)
	if err != nil {
		return err
	}
	port, err := cont.MappedPort(ctx, "4222/tcp")
	if err != nil {
		return err
	}
	h.natsURL = "nats://" + net.JoinHostPort(host, port.Port())
	return nil
}

func (h *integrationHarness) startUpstream() {
	h.upstreamSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit := h.upstreamHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"hit":    hit,
			"method": r.Method,
			"path":   r.URL.Path,
		})
	}))
}

func (h *integrationHarness) startHubAndLeaseGRPC(configPath string) {
	cfgMgr, err := hub.NewConfigManager(configPath)
	if err != nil {
		h.t.Fatalf("new hub config manager: %v", err)
	}
	h.hubControl = hub.NewControlPlane(cfgMgr, h.hubTierStore, h.rdb)
	go h.hubControl.Run(h.ctx)
	hubSrv := hub.NewHubServer(h.rdb, cfgMgr, h.hubTierStore, h.hubControl, "")
	hubSrv.SetStartupReady(true)
	h.hubHTTP = httptest.NewServer(hubSrv)

	grpcSrv, lis := h.startLeaseGRPCServer(h.mtlsFiles, cfgMgr)
	h.hubGRPCSrv = grpcSrv
	h.hubGRPCListener = lis
}

func (h *integrationHarness) startLeaseGRPCServer(files *mtlsFiles, cfgMgr *hub.ConfigManager) (*grpc.Server, net.Listener) {
	cert, err := tls.LoadX509KeyPair(files.HubCertPath, files.HubKeyPath)
	if err != nil {
		h.t.Fatalf("load hub keypair: %v", err)
	}
	caPEM, err := os.ReadFile(files.CAPath)
	if err != nil {
		h.t.Fatalf("read ca pem: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		h.t.Fatal("append ca pem")
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS13}
	grpcSrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	types.RegisterQuotaLeaseServiceServer(grpcSrv, hub.NewQuotaLeaseServer(ledger.NewRedis(h.rdb), cfgMgr, h.hubTierStore))
	if h.hubControl != nil {
		types.RegisterControlPlaneServer(grpcSrv, h.hubControl)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatalf("listen grpc: %v", err)
	}
	go func() {
		if err := grpcSrv.Serve(lis); err != nil && !strings.Contains(err.Error(), "closed") {
			h.t.Logf("grpc serve exited: %v", err)
		}
	}()
	return grpcSrv, lis
}

func (h *integrationHarness) startAnalytics() {
	api := analyticsapi.NewServer(h.rdb, h.analyticsKey, "")
	if err := api.StartNATSSubscriber(h.ctx, h.natsURL, types.DefaultRuntimePolicy().Analytics.NATSSubject, "analytics-it-primary"); err != nil {
		h.t.Fatalf("start primary analytics nats subscriber: %v", err)
	}
	api.StartBackgroundAggregator(h.ctx, 2*time.Second)
	h.analyticsSrv = httptest.NewServer(api)
	h.analyticsAll = append(h.analyticsAll, h.analyticsSrv)

	// Secondary analytics API simulates multiple analytics instances on separate hosts.
	secondaryAPI := analyticsapi.NewServer(h.rdb, h.analyticsKey, "")
	if err := secondaryAPI.StartNATSSubscriber(h.ctx, h.natsURL, types.DefaultRuntimePolicy().Analytics.NATSSubject, "analytics-it-secondary"); err != nil {
		h.t.Fatalf("start secondary analytics nats subscriber: %v", err)
	}
	secondaryAPI.StartBackgroundAggregator(h.ctx, 2*time.Second)
	secondary := httptest.NewServer(secondaryAPI)
	h.analyticsAll = append(h.analyticsAll, secondary)
}

// startEdge runs an edge the way production does: one mTLS connection to
// the hub for leases, key resolution and the config/tier stream. Edges share
// the harness Redis as their response store so cache invalidation can be
// observed across edges.
func (h *integrationHarness) startEdge(configPath string) {
	h.t.Helper()
	bootstrap, err := config.Load(configPath)
	if err != nil {
		h.t.Fatalf("load edge bootstrap config: %v", err)
	}
	tlsCfg, err := edge.ClientTLS("hub-server", h.mtlsFiles.EdgeCertPath, h.mtlsFiles.EdgeKeyPath, h.mtlsFiles.CAPath)
	if err != nil {
		h.t.Fatalf("edge tls: %v", err)
	}
	hubClient, err := edge.DialHub(h.hubGRPCListener.Addr().String(), tlsCfg)
	if err != nil {
		h.t.Fatalf("dial hub: %v", err)
	}
	go func() { <-h.ctx.Done(); hubClient.Close() }()

	policy := bootstrap.Runtime.Effective().Edge
	lease := limiter.NewLease(hubClient, limiter.LeaseOptions{LeaseSize: policy.RateLeaseSize, LowWaterPct: policy.RateLowWaterPct})
	h.edgeLeases = append(h.edgeLeases, lease)

	es := edge.NewServer(edge.Deps{
		Config:    edge.NewConfigManager(bootstrap),
		Tiers:     edge.NewTierCache(hubClient.ResolveKey),
		Limiter:   lease,
		Analytics: &redisAnalyticsSink{rdb: h.rdb, key: h.analyticsKey},
		Responses: edge.NewRedisResponses(h.rdb),
	}, edge.Options{})
	es.StartBackgroundWorkers(h.ctx)
	go es.FollowHub(h.ctx, hubClient)
	srv := httptest.NewServer(es)
	if h.edgeHTTP == nil {
		h.edgeHTTP = srv
	}
	h.edgeHTTPAll = append(h.edgeHTTPAll, srv)
}

// dialHub opens an edge-style mTLS connection to the harness hub.
func (h *integrationHarness) dialHub(serverName, caPath string) *edge.HubClient {
	h.t.Helper()
	tlsCfg, err := edge.ClientTLS(serverName, h.mtlsFiles.EdgeCertPath, h.mtlsFiles.EdgeKeyPath, caPath)
	if err != nil {
		h.t.Fatalf("edge tls: %v", err)
	}
	c, err := edge.DialHub(h.hubGRPCListener.Addr().String(), tlsCfg)
	if err != nil {
		h.t.Fatalf("dial hub: %v", err)
	}
	h.t.Cleanup(func() { c.Close() })
	return c
}

func (h *integrationHarness) waitReady(url string) {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	lastStatus := 0
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			lastStatus = resp.StatusCode
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.t.Fatalf("service never became ready: %s (last status=%d)", url, lastStatus)
}

func (h *integrationHarness) setTier(prefix, apiKey, tierID string) {
	h.t.Helper()
	// The hub stores tiers by key hash, as edges look them up.
	if err := h.hubTierStore.SetTier(h.ctx, prefix, keys.Hash(apiKey), tierID); err != nil {
		h.t.Fatalf("set tier: %v", err)
	}
}

func (h *integrationHarness) edgeRequest(method, path, apiKey string) (*http.Response, string) {
	return h.edgeRequestAt(0, method, path, apiKey)
}

func (h *integrationHarness) edgeRequestAt(index int, method, path, apiKey string) (*http.Response, string) {
	h.t.Helper()
	if index < 0 || index >= len(h.edgeHTTPAll) {
		h.t.Fatalf("edge index out of range: %d", index)
	}
	req, err := http.NewRequest(method, h.edgeHTTPAll[index].URL+path, nil)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-API-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("edge request failed: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, string(b)
}

func (h *integrationHarness) edgeCount() int {
	return len(h.edgeHTTPAll)
}

func (h *integrationHarness) analyticsEvents(limit int) ([]types.AnalyticsEntry, error) {
	return h.analyticsEventsAt(0, limit)
}

func (h *integrationHarness) analyticsEventsAt(index, limit int) ([]types.AnalyticsEntry, error) {
	if index < 0 || index >= len(h.analyticsAll) {
		return nil, fmt.Errorf("analytics index out of range: %d", index)
	}
	resp, err := http.Get(fmt.Sprintf("%s/analytics/events?limit=%d", h.analyticsAll[index].URL, limit))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d", resp.StatusCode)
	}
	var payload struct {
		Events []types.AnalyticsEntry `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Events, nil
}

func (h *integrationHarness) analyticsCount() int {
	return len(h.analyticsAll)
}

func (h *integrationHarness) triggerConfigReload() {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.hubHTTP.URL+"/config-reload", nil)
	if err != nil {
		h.t.Fatalf("new config reload request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("config reload request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		h.t.Fatalf("config reload unexpected status=%d body=%s", resp.StatusCode, string(b))
	}
}

func (h *integrationHarness) setCacheEnabled(enabled bool) {
	h.t.Helper()
	cfg, err := readConfigFile(h.configPath)
	if err != nil {
		h.t.Fatalf("read config file: %v", err)
	}
	if len(cfg.Prefixes) == 0 || len(cfg.Prefixes[0].Services) == 0 {
		h.t.Fatal("config file structure is missing service")
	}
	svc := &cfg.Prefixes[0].Services[0]
	if svc.Cache == nil {
		svc.Cache = &types.CacheConfig{}
	}
	svc.Cache.Enabled = enabled
	if svc.Cache.TTL <= 0 {
		svc.Cache.TTL = 2 * time.Second
	}
	svc.Cache.CacheKey = "$method:$path"
	if err := writeConfigFile(h.configPath, cfg); err != nil {
		h.t.Fatalf("write config file: %v", err)
	}
}

func (h *integrationHarness) setSafetyFallback(allow bool, quota uint32) {
	h.t.Helper()
	cfg, err := readConfigFile(h.configPath)
	if err != nil {
		h.t.Fatalf("read config file: %v", err)
	}
	if len(cfg.Prefixes) == 0 || len(cfg.Prefixes[0].Services) == 0 {
		h.t.Fatal("config file structure is missing service")
	}
	svc := &cfg.Prefixes[0].Services[0]
	if quota == 0 {
		quota = 1
	}
	svc.SafetyTier = &types.TierConfig{
		TierID:     "safety",
		Quota:      quota,
		GetCost:    1,
		PostCost:   1,
		PutCost:    1,
		DeleteCost: 1,
		OtherCost:  1,
	}
	svc.Failure.Hub.AllowOnRateServiceError = allow
	if err := writeConfigFile(h.configPath, cfg); err != nil {
		h.t.Fatalf("write config file: %v", err)
	}
}

func (h *integrationHarness) setLeaseClient(client limiter.LeaseClient) {
	h.setLeaseClientAt(0, client)
}

func (h *integrationHarness) setLeaseClientAt(index int, client limiter.LeaseClient) {
	h.t.Helper()
	if index < 0 || index >= len(h.edgeLeases) {
		h.t.Fatalf("edge index out of range: %d", index)
	}
	h.edgeLeases[index].SetClient(client)
}

// leaseHolding reports the tokens edge 0 holds for a key of v1/auth-api and
// when its lease expires.
func (h *integrationHarness) leaseHolding(apiKey string) (int64, time.Time) {
	return h.edgeLeases[0].Holding("v1", "auth-api", keys.Hash(apiKey))
}

// requireWindowHeadroom waits, if needed, until at least d remains in the
// current quota window (config_builder uses 1-minute windows). Edges drop
// leftover lease tokens when the window rolls, so tests that assert lease
// balances over several seconds must not straddle a boundary.
func (h *integrationHarness) requireWindowHeadroom(d time.Duration) {
	const period = time.Minute
	into := time.Duration(time.Now().UnixNano()) % period
	if left := period - into; left < d {
		time.Sleep(left + 100*time.Millisecond)
	}
}

type redisAnalyticsSink struct {
	rdb *redis.Client
	key string
}

func (s *redisAnalyticsSink) Capture(entry *types.AnalyticsEntry) {
	if entry == nil {
		return
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return
	}
	_ = s.rdb.RPush(context.Background(), s.key, b).Err()
}

func (s *redisAnalyticsSink) Close() error { return nil }
