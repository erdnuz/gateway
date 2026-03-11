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
	"gateway/packages/edge"
	"gateway/packages/hub"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
	mongoContainer testcontainers.Container

	redisAddr string
	natsURL   string
	mongoURI  string

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
	edgeRateMgr  *edge.RateManager
	edgeRateAll  []*edge.RateManager
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
	h.startAdditionalEdge(configPath)

	h.waitReady(h.hubHTTP.URL + "/readyz")
	for _, edgeSrv := range h.edgeHTTPAll {
		h.waitReady(edgeSrv.URL + "/readyz")
	}
	for _, analyticsSrv := range h.analyticsAll {
		h.waitReady(analyticsSrv.URL + "/readyz")
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
	if h.mongoContainer != nil {
		_ = h.mongoContainer.Terminate(context.Background())
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
	if err := h.startMongoContainer(); err != nil {
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

func (h *integrationHarness) startMongoContainer() error {
	ctx, cancel := context.WithTimeout(h.ctx, 90*time.Second)
	defer cancel()
	cont, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mongo:7",
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForListeningPort("27017/tcp").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("start mongo container: %w", err)
	}
	h.mongoContainer = cont
	host, err := cont.Host(ctx)
	if err != nil {
		return err
	}
	port, err := cont.MappedPort(ctx, "27017/tcp")
	if err != nil {
		return err
	}
	mongoPort, err := strconv.Atoi(port.Port())
	if err != nil {
		return err
	}
	h.mongoURI = fmt.Sprintf("mongodb://%s:%d", host, mongoPort)
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
	rateMgr := hub.NewRateManager(h.rdb, cfgMgr)
	hubSrv := hub.NewHubServerWithManagers(h.rdb, cfgMgr, h.hubTierStore, rateMgr, "", 10_000, types.DefaultHubUpdatesChannel)
	hubSrv.SetTierUpdateMessaging(h.natsURL, "tier.updates")
	hubSrv.StartBackgroundWorkers(h.ctx)
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
	types.RegisterQuotaLeaseServiceServer(grpcSrv, hub.NewQuotaLeaseServer(h.rdb, cfgMgr, h.hubTierStore))
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
	h.analyticsSrv = httptest.NewServer(api)
	h.analyticsAll = append(h.analyticsAll, h.analyticsSrv)

	// Secondary analytics API simulates multiple analytics instances on separate hosts.
	secondary := httptest.NewServer(analyticsapi.NewServer(h.rdb, h.analyticsKey, ""))
	h.analyticsAll = append(h.analyticsAll, secondary)
}

func (h *integrationHarness) startEdge(configPath string) {
	// Reuse hub CA and edge cert generated for this run while connecting to existing lease listener.
	configMgr, err := edge.NewConfigManagerWithFallback(h.hubHTTP.URL, "", nil, configPath)
	if err != nil {
		h.t.Fatalf("new edge config manager: %v", err)
	}
	configMgr.StartConfigReloadSubscriber(h.ctx, h.rdb, types.DefaultConfigReloadChannel)
	tierMgr := edge.NewTierManagerWithOptions(h.hubHTTP.URL, h.rdb, types.DefaultHubUpdatesChannel, "", nil, edge.TierManagerOptions{
		NATSURL:   h.natsURL,
		NATSSubj:  types.DefaultRuntimePolicy().Hub.TierUpdatesSubject,
		NATSQueue: "edge-tier-updates-it",
	})
	rateDefaults := configMgr.Get().Runtime.Effective().Edge
	rateMgr := edge.NewRateManagerWithOptions(h.hubHTTP.URL, h.rdb, 100, edge.RateManagerOptions{HardThresholdPct: rateDefaults.RateHardThresholdPct, LeaseSize: rateDefaults.RateLeaseSize, LowWaterPct: rateDefaults.RateLowWaterPct})
	h.edgeRateMgr = rateMgr
	h.edgeRateAll = append(h.edgeRateAll, rateMgr)

	leaseClient, err := edge.NewGRPCQuotaLeaseClient(h.hubGRPCListener.Addr().String(), "hub-server", h.mtlsFiles.EdgeCertPath, h.mtlsFiles.EdgeKeyPath, h.mtlsFiles.CAPath)
	if err != nil {
		h.t.Fatalf("new lease client: %v", err)
	}
	rateMgr.SetLeaseClient(leaseClient)

	sink := &redisAnalyticsSink{rdb: h.rdb, key: h.analyticsKey}
	es := edge.NewEdgeServer(configMgr, tierMgr, rateMgr, sink, h.rdb)
	es.StartBackgroundWorkers(h.ctx)
	h.edgeHTTP = httptest.NewServer(es)
	h.edgeHTTPAll = append(h.edgeHTTPAll, h.edgeHTTP)
}

func (h *integrationHarness) startAdditionalEdge(configPath string) {
	configMgr, err := edge.NewConfigManagerWithFallback(h.hubHTTP.URL, "", nil, configPath)
	if err != nil {
		h.t.Fatalf("new secondary edge config manager: %v", err)
	}
	configMgr.StartConfigReloadSubscriber(h.ctx, h.rdb, types.DefaultConfigReloadChannel)
	tierMgr := edge.NewTierManagerWithOptions(h.hubHTTP.URL, h.rdb, types.DefaultHubUpdatesChannel, "", nil, edge.TierManagerOptions{
		NATSURL:   h.natsURL,
		NATSSubj:  types.DefaultRuntimePolicy().Hub.TierUpdatesSubject,
		NATSQueue: "edge-tier-updates-it-secondary",
	})
	rateDefaults := configMgr.Get().Runtime.Effective().Edge
	rateMgr := edge.NewRateManagerWithOptions(h.hubHTTP.URL, h.rdb, 100, edge.RateManagerOptions{HardThresholdPct: rateDefaults.RateHardThresholdPct, LeaseSize: rateDefaults.RateLeaseSize, LowWaterPct: rateDefaults.RateLowWaterPct})
	h.edgeRateAll = append(h.edgeRateAll, rateMgr)

	leaseClient, err := edge.NewGRPCQuotaLeaseClient(h.hubGRPCListener.Addr().String(), "hub-server", h.mtlsFiles.EdgeCertPath, h.mtlsFiles.EdgeKeyPath, h.mtlsFiles.CAPath)
	if err != nil {
		h.t.Fatalf("new secondary lease client: %v", err)
	}
	rateMgr.SetLeaseClient(leaseClient)

	sink := &redisAnalyticsSink{rdb: h.rdb, key: h.analyticsKey}
	es := edge.NewEdgeServer(configMgr, tierMgr, rateMgr, sink, h.rdb)
	es.StartBackgroundWorkers(h.ctx)
	h.edgeHTTPAll = append(h.edgeHTTPAll, httptest.NewServer(es))
}

func (h *integrationHarness) waitReady(url string) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("service never became ready: %s", url)
}

func (h *integrationHarness) setTier(prefix, apiKey, tierID string) {
	h.t.Helper()
	if err := h.hubTierStore.SetTier(h.ctx, prefix, apiKey, tierID); err != nil {
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

func (h *integrationHarness) setLeaseClient(client edge.QuotaLeaseRequester) {
	h.setLeaseClientAt(0, client)
}

func (h *integrationHarness) setLeaseClientAt(index int, client edge.QuotaLeaseRequester) {
	h.t.Helper()
	if index < 0 || index >= len(h.edgeRateAll) {
		h.t.Fatalf("edge rate manager index out of range: %d", index)
	}
	h.edgeRateAll[index].SetLeaseClient(client)
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
