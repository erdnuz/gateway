package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"gateway/packages/common/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestDashboardStatsEndpoints(t *testing.T) {
	// set up in-memory redis and seed a few entries
	m, _ := miniredis.Run()
	defer m.Close()
	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})

	entries := []types.RateAnalyticsEntry{
		{Prefix: "/api/v1", APIKey: "u1", Delta: 5},
		{Prefix: "/api/v1", APIKey: "u2", Delta: 7},
		{Prefix: "/api/v2", APIKey: "u1", Delta: 3},
	}
	ctx := context.Background()
	for _, e := range entries {
		b, _ := json.Marshal(e)
		rdb.RPush(ctx, "rate-analytics", b)
	}

	ds := NewDashboardServer(rdb)

	req := httptest.NewRequest("GET", "/analytics", nil)
	rw := httptest.NewRecorder()
	ds.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}

	var stats Stats
	if err := json.NewDecoder(rw.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.Count != 3 {
		t.Errorf("expected count 3, got %d", stats.Count)
	}

	// group by prefix
	req = httptest.NewRequest("GET", "/analytics?group_by=prefix", nil)
	rw = httptest.NewRecorder()
	ds.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var grouped map[string]Stats
	if err := json.NewDecoder(rw.Body).Decode(&grouped); err != nil {
		t.Fatal(err)
	}
	if len(grouped) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(grouped))
	}
}
