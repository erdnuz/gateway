//go:build integration

package integration

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gateway/packages/common/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestIntegration_MTLSHandshakeFailsWithWrongServerName(t *testing.T) {
	h := newIntegrationHarness(t)
	defer h.close()

	client := h.dialHub("wrong-name", h.mtlsFiles.CAPath)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_, err := client.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "key1", RequestedTokens: 1})
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
	client := h.dialHub("hub-server", bad.CAPath)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_, err = client.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "key1", RequestedTokens: 1})
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
	_, err = types.NewQuotaLeaseServiceClient(conn).RequestQuotaLease(ctx, &types.QuotaLeaseRequest{Prefix: "v1", ServiceId: "auth-api", ApiKey: "key1", RequestedTokens: 1})
	if err == nil {
		t.Fatal("expected handshake error for missing client cert, got nil")
	}
	errText := strings.ToLower(err.Error())
	if !strings.Contains(errText, "tls") && !strings.Contains(errText, "certificate") && !strings.Contains(errText, "handshake") &&
		// TLS 1.3 rejects the missing cert after the client's handshake
		// completes, so the client may see the closed connection instead.
		!strings.Contains(errText, "broken pipe") && !strings.Contains(errText, "connection reset") &&
		!strings.Contains(errText, "forcibly closed") { // Windows: WSAECONNRESET
		t.Fatalf("expected tls-related error, got: %v", err)
	}
}
