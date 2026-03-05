package hub

import (
	"fmt"
	"time"

	"gateway/packages/common/types"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type AnalyticsManager struct {
	client   influxdb2.Client
	writeAPI api.WriteAPI
	queryAPI api.QueryAPI
	bucket   string
}

func NewAnalyticsManager(url, token, org, bucket string) *AnalyticsManager {
	client := influxdb2.NewClient(url, token)
	// Non-blocking write API with batching enabled
	writeAPI := client.WriteAPI(org, bucket)

	return &AnalyticsManager{
		client:   client,
		writeAPI: writeAPI,
		queryAPI: client.QueryAPI(org),
		bucket:   bucket,
	}
}

// IngestEntry pushes a single entry to the internal buffer.
// The Influx client handles batching and background flushing automatically.
func (am *AnalyticsManager) IngestEntry(entry types.AnalyticsEntry) {
	p := influxdb2.NewPoint("request_logs",
		map[string]string{
			"service_prefix": entry.ServicePrefix,
			"service_id":     entry.ServiceID,
			"method":         entry.Method,
			"user_tier":      entry.UserTier,
			"response_code":  fmt.Sprintf("%d", entry.ResponseCode),
			"cache_hit":      fmt.Sprintf("%v", entry.CacheHit),
		},
		map[string]interface{}{
			"total_latency":    entry.TotalLatency,
			"upstream_latency": entry.UpstreamLatency,
			"request_size":     entry.RequestSize,
			"response_size":    entry.ResponseSize,
			"limit_used":       entry.LimitUsed,
		},
		time.Unix(entry.Timestamp, 0),
	)
	am.writeAPI.WritePoint(p)
}

func (am *AnalyticsManager) IngestBatch(entries []types.AnalyticsEntry) {
	for _, entry := range entries {
		am.IngestEntry(entry)
	}
}
