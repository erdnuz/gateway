package edgeapp

import (
	"strings"
	"testing"
)

func TestValidate_TLSUnlessLoopback(t *testing.T) {
	c := Config{HubGRPCAddr: "hub:9090"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "EDGE_TLS_CERT_FILE") {
		t.Fatalf("remote hub without TLS: %v", err)
	}
	c.HubGRPCAddr = "127.0.0.1:9090"
	if err := c.Validate(); err != nil {
		t.Fatalf("loopback hub needs no TLS: %v", err)
	}
	c = Config{HubGRPCAddr: "hub:9090", TLSCertFile: "c", TLSKeyFile: "k", TLSCAFile: "ca"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "HUB_GRPC_SERVER_NAME") {
		t.Fatalf("TLS without server name: %v", err)
	}
}

func TestValidate_AnalyticsNeedsNATS(t *testing.T) {
	c := Config{HubGRPCAddr: "127.0.0.1:9090", AnalyticsEnabled: true}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "NATS_URL") {
		t.Fatalf("got %v", err)
	}
}
