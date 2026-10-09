package hubapp

import (
	"strings"
	"testing"
)

const validConfig = "../../../testing/loadtest/gateway-config.loadtest.yaml"

func TestValidate_ReportsEveryMissingSetting(t *testing.T) {
	err := Config{GRPCAddr: ":9090"}.Validate()
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"HUB_AUTH_TOKEN", "CONFIG_FILE_PATH", "HUB_TLS_CERT_FILE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

func TestValidate_RejectsInvalidGatewayConfig(t *testing.T) {
	err := Config{AuthToken: "t", ConfigPath: "does-not-exist.json"}.Validate()
	if err == nil || !strings.Contains(err.Error(), "does-not-exist.json") {
		t.Fatalf("want config error naming the file, got %v", err)
	}
}

func TestValidate_PlaintextOnlyOnLoopback(t *testing.T) {
	if err := (Config{AuthToken: "t", ConfigPath: validConfig, GRPCAddr: "127.0.0.1:0"}).Validate(); err != nil {
		t.Fatalf("loopback gRPC needs no TLS: %v", err)
	}
}
