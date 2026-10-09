package edgeapp

import (
	"context"
	"strings"
	"testing"

	"gateway/packages/common/types"
)

type stubLeases struct{}

func (stubLeases) RequestQuotaLease(context.Context, *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	return &types.QuotaLeaseResponse{}, nil
}
func (stubLeases) Close() error { return nil }

func valid() Config {
	return Config{RedisAddr: "localhost:6379", HubAddr: "http://hub:8080", NATSTierURL: "nats://n:4222"}
}

func TestValidate_RequiresTLSForGRPCLeasing(t *testing.T) {
	err := valid().Validate()
	if err == nil || !strings.Contains(err.Error(), "HUB_GRPC_SERVER_NAME") || !strings.Contains(err.Error(), "EDGE_TLS_CERT_FILE") {
		t.Fatalf("want TLS errors, got %v", err)
	}
	c := valid()
	c.LeaseClient = stubLeases{}
	if err := c.Validate(); err != nil {
		t.Fatalf("in-process leasing needs no TLS: %v", err)
	}
}

func TestValidate_RejectsBadValues(t *testing.T) {
	c := valid()
	c.LeaseClient = stubLeases{}
	c.HubAddr = "hub:8080"
	c.AnalyticsEnabled = true
	err := c.Validate()
	for _, want := range []string{"HUB_ADDR", "NATS_ANALYTICS_URL"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}
