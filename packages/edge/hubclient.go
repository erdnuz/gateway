package edge

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"time"

	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const leaseRPCTimeout = 2 * time.Second

// HubClient is an edge's one connection to the hub. It carries quota leases,
// key resolution and the stream of config and tier changes, so the edge
// needs nothing else from the control plane.
type HubClient struct {
	conn    *grpc.ClientConn
	leases  types.QuotaLeaseServiceClient
	control types.ControlPlaneClient
}

// hubServiceConfig balances calls across every address the target
// resolves to: with HUB_GRPC_ADDR=dns:///hub:9090 and several hub replicas,
// lease RPCs spread over all of them while the watch stream stays on one.
const hubServiceConfig = `{"loadBalancingConfig": [{"round_robin": {}}]}`

// DialHub connects to a hub. A nil tlsCfg means plaintext, which only gate
// dev uses, on loopback.
func DialHub(addr string, tlsCfg *tls.Config) (*HubClient, error) {
	creds := insecure.NewCredentials()
	if tlsCfg != nil {
		creds = credentials.NewTLS(tlsCfg)
	}
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(hubServiceConfig),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}, telemetry.GRPCDialOptions()...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &HubClient{conn: conn, leases: types.NewQuotaLeaseServiceClient(conn), control: types.NewControlPlaneClient(conn)}, nil
}

// ClientTLS loads the edge's mTLS identity and the hub's CA.
func ClientTLS(serverName, certFile, keyFile, caFile string) (*tls.Config, error) {
	if serverName == "" {
		return nil, fmt.Errorf("server name is required for TLS verification")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read hub CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse hub CA PEM")
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS13}, nil
}

func (h *HubClient) RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, leaseRPCTimeout)
	defer cancel()
	start := time.Now()
	resp, err := h.leases.RequestQuotaLease(ctx, in)
	result := "granted"
	switch {
	case err != nil:
		result = "error"
	case resp.GetWaitingForCapacity():
		result = "waiting"
	}
	telemetry.LeaseRPCSeconds.WithLabelValues(result).Observe(telemetry.Since(start))
	return resp, err
}

// RequestQuotaLeases asks for many leases in one round trip.
func (h *HubClient) RequestQuotaLeases(ctx context.Context, in *types.QuotaLeaseBatchRequest) (*types.QuotaLeaseBatchResponse, error) {
	start := time.Now()
	resp, err := h.leases.RequestQuotaLeases(ctx, in)
	result := "ok"
	if err != nil {
		result = "error"
	}
	telemetry.LeaseRPCSeconds.WithLabelValues("batch_" + result).Observe(telemetry.Since(start))
	telemetry.LeaseBatchSize.Observe(float64(len(in.GetRequests())))
	return resp, err
}

// ReturnLeases gives unspent tokens back to the hub.
func (h *HubClient) ReturnLeases(ctx context.Context, in *types.ReturnLeasesRequest) (*types.ReturnLeasesResponse, error) {
	return h.leases.ReturnLeases(ctx, in)
}

// ResolveKey returns a key's tier ("" when the hub does not know the key).
func (h *HubClient) ResolveKey(ctx context.Context, prefix, apiKey string) (string, error) {
	resp, err := h.control.ResolveKey(ctx, &types.ResolveKeyRequest{Prefix: prefix, ApiKey: apiKey})
	if err != nil {
		telemetry.KeyResolutions.WithLabelValues("error").Inc()
		return "", err
	}
	telemetry.KeyResolutions.WithLabelValues("ok").Inc()
	return resp.GetTierId(), nil
}

// Watch delivers the hub's config and tier changes to handle until ctx ends,
// reconnecting with backoff. Every (re)connection starts with a config
// snapshot, so nothing missed while disconnected stays missed.
func (h *HubClient) Watch(ctx context.Context, edgeID string, handle func(*types.ControlEvent)) {
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		stream, err := h.control.Watch(ctx, &types.WatchRequest{EdgeId: edgeID})
		for err == nil {
			var ev *types.ControlEvent
			if ev, err = stream.Recv(); err == nil {
				handle(ev)
				backoff = 250 * time.Millisecond
			}
		}
		if ctx.Err() != nil {
			return
		}
		slog.Warn("hub watch interrupted; reconnecting", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (h *HubClient) Close() error { return h.conn.Close() }
