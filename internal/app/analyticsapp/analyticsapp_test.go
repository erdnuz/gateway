package analyticsapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidate_ReportsEveryMissingSetting(t *testing.T) {
	err := Config{}.Validate()
	for _, want := range []string{"CLICKHOUSE_DSN", "NATS_ANALYTICS_URL", "HUB_ADDR"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
	if err := (Config{RedisAddr: "r:6379", NATSURL: "nats://n", HubAddr: "http://hub"}).Validate(); err != nil {
		t.Fatalf("redis-backed store should be valid: %v", err)
	}
	if err := (Config{RedisAddr: "r", NATSURL: "n", HubAddr: "hub:8080"}).Validate(); err == nil {
		t.Fatal("non-URL HUB_ADDR accepted")
	}
}

func TestFetchRuntimePolicy_ErrorsOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer srv.Close()
	if _, err := fetchRuntimePolicy(context.Background(), Config{HubAddr: srv.URL}); err == nil {
		t.Fatal("expected error on 401")
	}
}
