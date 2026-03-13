package edge

import (
	"net/http/httptest"
	"testing"
	"time"

	"gateway/packages/common/types"
)

func BenchmarkConfigManagerGetServiceConfig(b *testing.B) {
	cfg := benchmarkGatewayConfig(80, 25)
	cm := &ConfigManager{}
	cm.active.Store(cfg)
	cm.indexes.Store(buildConfigIndexes(cfg))

	prefix := cfg.Prefixes[37].Prefix
	service := cfg.Prefixes[37].Services[12].ServiceID

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		svc, err := cm.GetServiceConfig(prefix, service)
		if err != nil || svc == nil {
			b.Fatalf("unexpected lookup failure: %v", err)
		}
	}
}

func BenchmarkCacheManagerGenerateCacheKey(b *testing.B) {
	cm := NewCacheManager(nil, types.CacheConfig{
		Enabled:  true,
		TTL:      30 * time.Second,
		CacheKey: "$method:$path:$query:$key:$enc",
	})
	req := httptest.NewRequest("GET", "http://edge.local/v1/search?b=2&a=1&z=4", nil)
	req.Header.Set("Accept-Encoding", "gzip, br")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cm.generateCacheKey(req, "benchmark-api-key")
	}
}

func benchmarkGatewayConfig(prefixCount, servicesPerPrefix int) *types.GatewayConfig {
	cfg := &types.GatewayConfig{Prefixes: make([]types.PrefixConfig, prefixCount)}
	for i := 0; i < prefixCount; i++ {
		prefix := types.PrefixConfig{
			Prefix:      "/p" + itoa(i),
			QuotaPeriod: time.Minute,
			Services:    make([]types.ServiceConfig, servicesPerPrefix),
		}
		for j := 0; j < servicesPerPrefix; j++ {
			prefix.Services[j] = types.ServiceConfig{ServiceID: "svc-" + itoa(j)}
		}
		cfg.Prefixes[i] = prefix
	}
	return cfg
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	buf := make([]byte, 0, 12)
	for v > 0 {
		buf = append(buf, byte('0'+(v%10)))
		v /= 10
	}
	for l, r := 0, len(buf)-1; l < r; l, r = l+1, r-1 {
		buf[l], buf[r] = buf[r], buf[l]
	}
	return string(buf)
}
