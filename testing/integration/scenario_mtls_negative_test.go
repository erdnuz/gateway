//go:build integration

package integration

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/edge"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestIntegration_MTLSHandshakeFailsWithWrongServerName(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	client, err := edge.NewGRPCQuotaLeaseClient(h.hubGRPCListener.Addr().String(), "wrong-name", h.mtlsFiles.EdgeCertPath, h.mtlsFiles.EdgeKeyPath, h.mtlsFiles.CAPath)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_, err = client.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "k1", RequestedTokens: 1})
	if err == nil {
		t.Fatal("expected TLS server-name verification error, got nil")
	}
}

func TestIntegration_MTLSHandshakeFailsWithWrongCA(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	bad, err := generateMTLSFiles(filepath.Join(h.workingDir, "bad-ca"), "hub-server")
	if err != nil {
		t.Fatalf("generate bad ca files: %v", err)
	}
	client, err := edge.NewGRPCQuotaLeaseClient(h.hubGRPCListener.Addr().String(), "hub-server", h.mtlsFiles.EdgeCertPath, h.mtlsFiles.EdgeKeyPath, bad.CAPath)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_, err = client.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "k1", RequestedTokens: 1})
	if err == nil {
		t.Fatal("expected TLS unknown authority error, got nil")
	}
}

func TestIntegration_MTLSHandshakeFailsWithoutClientCert(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	creds, err := credentials.NewClientTLSFromFile(h.mtlsFiles.CAPath, "hub-server")
	if err != nil {
		t.Fatalf("client tls from file: %v", err)
	}
	conn, err := grpc.Dial(h.hubGRPCListener.Addr().String(), grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_, err = types.NewQuotaLeaseServiceClient(conn).RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "k1", RequestedTokens: 1})
	if err == nil {
		t.Fatal("expected handshake error for missing client cert, got nil")
	}
	errText := strings.ToLower(err.Error())
	if !strings.Contains(errText, "tls") && !strings.Contains(errText, "certificate") && !strings.Contains(errText, "handshake") && !strings.Contains(errText, "broken pipe") {
		t.Fatalf("expected tls-related error, got: %v", err)
	}
}
