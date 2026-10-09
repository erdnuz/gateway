package edge

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"gateway/packages/common/types"
)

// BenchmarkEdgeRequest measures one request through the whole chain
// (route, auth, rate limit, proxy) against an in-process upstream.
func BenchmarkEdgeRequest(b *testing.B) {
	f := newFixture(b, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }, func(s *types.ServiceConfig) {
		s.Tiers[0].Quota = 1 << 31
		s.Analytics.Enabled = false
	})
	edge := httptest.NewServer(f.server)
	defer edge.Close()
	client := edge.Client()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, _ := http.NewRequest(http.MethodGet, edge.URL+"/v1/svc/x", nil)
			req.Header.Set("X-API-Key", "testkey")
			resp, err := client.Do(req)
			if err != nil {
				b.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				b.Fatalf("status %d", resp.StatusCode)
			}
		}
	})
}

func BenchmarkConfigManagerGetServiceConfig(b *testing.B) {
	cfg := &types.GatewayConfig{Prefixes: make([]types.PrefixConfig, 80)}
	for i := range cfg.Prefixes {
		p := &cfg.Prefixes[i]
		p.Prefix, p.QuotaPeriod = "p"+strconv.Itoa(i), time.Minute
		for j := 0; j < 25; j++ {
			p.Services = append(p.Services, types.ServiceConfig{ServiceID: "svc-" + strconv.Itoa(j)})
		}
	}
	cm := &ConfigManager{}
	cm.active.Store(cfg)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cm.GetServiceConfig("p37", "svc-12"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheKey(b *testing.B) {
	cm := NewCacheManager(nil, types.CacheConfig{Enabled: true, TTL: 30 * time.Second, CacheKey: "$method:$path:$query:$key:$enc"})
	req := httptest.NewRequest("GET", "http://edge.local/v1/search?b=2&a=1&z=4", nil)
	req.Header.Set("Accept-Encoding", "gzip, br")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cm.generateCacheKey(req, "benchmark-api-key")
	}
}
