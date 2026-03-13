package analyticsapi

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	summaryKeyRequests          = "requests"
	summaryKeyLatencyTotalP90   = "latency_total_p90"
	summaryKeyLatencyAddedP90   = "latency_added_p90"
	summaryKeyVolumeRequestAvg  = "volume_request_avg"
	summaryKeyVolumeResponseAvg = "volume_response_avg"
	summaryKeyRatesCacheHit     = "rates_cache_hit"
	summaryKeyRatesUpstreamErr  = "rates_upstream_err"
	summaryKeyRatesRateLimited  = "rates_rate_limited"
)

var contractSummaryKeys = []string{
	summaryKeyRequests,
	summaryKeyLatencyTotalP90,
	summaryKeyLatencyAddedP90,
	summaryKeyVolumeRequestAvg,
	summaryKeyVolumeResponseAvg,
	summaryKeyRatesCacheHit,
	summaryKeyRatesUpstreamErr,
	summaryKeyRatesRateLimited,
}

type contractMetric struct {
	LastValue float64 `json:"last_value"`
	Trend     float64 `json:"trend"`
}

type contractLatencyPoint struct {
	SampleCount        int64   `json:"sample_count"`
	Time               string  `json:"time"`
	LatencyTotalP90    float64 `json:"latency_total_p90"`
	LatencyUpstreamP90 float64 `json:"latency_upstream_p90"`
	LatencyAddedP90    float64 `json:"latency_added_p90"`
}

type contractVolumePoint struct {
	SampleCount int64   `json:"sample_count"`
	Time        string  `json:"time"`
	RequestAvg  float64 `json:"request_avg"`
	ResponseAvg float64 `json:"response_avg"`
}

type contractRatesPoint struct {
	SampleCount int64   `json:"sample_count"`
	Time        string  `json:"time"`
	CacheHit    float64 `json:"cache_hit"`
	UpstreamErr float64 `json:"upstream_err"`
	RateLimited float64 `json:"rate_limited"`
}

type contractSeries struct {
	Latency  []contractLatencyPoint   `json:"latency"`
	Volume   []contractVolumePoint    `json:"volume"`
	Rates    []contractRatesPoint     `json:"rates"`
	Prefixes []map[string]interface{} `json:"prefixes"`
	Services []map[string]interface{} `json:"services"`
	Edges    []map[string]interface{} `json:"edges"`
	Tiers    []map[string]interface{} `json:"tiers"`
}

type contractSummaryResponse struct {
	Summary map[string]contractMetric `json:"summary"`
	Series  contractSeries            `json:"series"`
}

type analyticsFilterSpec struct {
	Prefixes        []string
	Services        []string
	EdgeIDs         []string
	Tiers           []string
	Methods         []string
	ResponseCodeMin *int64
	ResponseCodeMax *int64
	CacheHit        *bool
	UpstreamError   *bool
	LatencyMinMs    *float64
	LatencyMaxMs    *float64
	Start           time.Time
	End             time.Time
}

type analyticsSummaryQuerySpec struct {
	Interval      string
	BucketSize    time.Duration
	WindowStart   time.Time
	WindowEnd     time.Time
	PreviousStart time.Time
	PreviousEnd   time.Time
	GroupBy       string
	Filters       analyticsFilterSpec
}

func newContractSummaryResponse() contractSummaryResponse {
	resp := contractSummaryResponse{
		Summary: make(map[string]contractMetric, len(contractSummaryKeys)),
		Series: contractSeries{
			Latency:  []contractLatencyPoint{},
			Volume:   []contractVolumePoint{},
			Rates:    []contractRatesPoint{},
			Prefixes: []map[string]interface{}{},
			Services: []map[string]interface{}{},
			Edges:    []map[string]interface{}{},
			Tiers:    []map[string]interface{}{},
		},
	}
	for _, key := range contractSummaryKeys {
		resp.Summary[key] = contractMetric{}
	}
	return resp
}

func parseAnalyticsSummaryQuery(values url.Values, now time.Time) (analyticsSummaryQuerySpec, error) {
	interval := normalizeInterval(values.Get("interval"))
	bucketSize := supportedIntervals[interval]
	if bucketSize <= 0 {
		return analyticsSummaryQuerySpec{}, fmt.Errorf("unsupported interval %q", interval)
	}
	groupBy := strings.TrimSpace(values.Get("group_by"))
	if groupBy != "" && !supportedGroupBy[groupBy] {
		return analyticsSummaryQuerySpec{}, fmt.Errorf("unsupported group_by %q", groupBy)
	}

	window := intervalRangeWindow[interval]
	if window <= 0 {
		window = time.Hour
	}
	now = now.UTC()
	windowEnd := now.Truncate(bucketSize)
	windowStart := windowEnd.Add(-window)
	if start, ok := parseOptionalTime(values.Get("start")); ok {
		windowStart = start.UTC()
	}
	if end, ok := parseOptionalTime(values.Get("end")); ok {
		windowEnd = end.UTC()
	}
	if !windowEnd.After(windowStart) {
		return analyticsSummaryQuerySpec{}, fmt.Errorf("end must be after start")
	}
	span := windowEnd.Sub(windowStart)

	filters := analyticsFilterSpec{
		Prefixes: sliceQueryValues(values, "prefix"),
		Services: sliceQueryValues(values, "service"),
		EdgeIDs:  sliceQueryValues(values, "edge_id"),
		Tiers:    sliceQueryValues(values, "tier"),
		Methods:  sliceQueryValues(values, "method"),
		Start:    windowStart,
		End:      windowEnd,
	}
	if v, ok := parseOptionalInt64(values.Get("response_code_min")); ok {
		filters.ResponseCodeMin = &v
	}
	if v, ok := parseOptionalInt64(values.Get("response_code_max")); ok {
		filters.ResponseCodeMax = &v
	}
	if v, ok := parseOptionalBool(values.Get("cache_hit")); ok {
		filters.CacheHit = &v
	}
	if v, ok := parseOptionalBool(values.Get("upstream_error")); ok {
		filters.UpstreamError = &v
	}
	if v, ok := parseOptionalFloat64(values.Get("total_latency_ms_min")); ok {
		filters.LatencyMinMs = &v
	}
	if v, ok := parseOptionalFloat64(values.Get("total_latency_ms_max")); ok {
		filters.LatencyMaxMs = &v
	}

	return analyticsSummaryQuerySpec{
		Interval:      interval,
		BucketSize:    bucketSize,
		WindowStart:   windowStart,
		WindowEnd:     windowEnd,
		PreviousStart: windowStart.Add(-span),
		PreviousEnd:   windowStart,
		GroupBy:       groupBy,
		Filters:       filters,
	}, nil
}

func sliceQueryValues(values url.Values, key string) []string {
	raw := values[key]
	if len(raw) == 0 {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		for _, part := range strings.Split(item, ",") {
			clean := strings.TrimSpace(part)
			if clean != "" {
				out = append(out, clean)
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
