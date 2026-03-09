package dashboard

import (
	"encoding/json"
	"math"
	"net/http"
	"sort"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
)

// DashboardServer exposes HTTP endpoints to inspect rate analytics.
type DashboardServer struct {
	rdb *redis.Client
}

// NewDashboardServer constructs a server given a Redis client.
func NewDashboardServer(rdb *redis.Client) *DashboardServer {
	return &DashboardServer{rdb: rdb}
}

// ServeHTTP implements http.Handler to route requests.
func (ds *DashboardServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/analytics":
		ds.handleAnalytics(w, r)
	case "/health":
		ds.handleHealth(w, r)
	default:
		http.NotFound(w, r)
	}
}

// handleHealth returns a simple liveness response.
func (ds *DashboardServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// handleAnalytics reads all RateAnalyticsEntry records and returns statistics.
// Query parameters:
//
//	group_by=prefix|api_key   (optional)  - group results by this field
//	prefix=<value>            (optional)  - filter by prefix
//	api_key=<value>           (optional)  - filter by key
func (ds *DashboardServer) handleAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	groupBy := r.URL.Query().Get("group_by")
	filterPrefix := r.URL.Query().Get("prefix")
	filterKey := r.URL.Query().Get("api_key")

	// fetch all entries from redis list
	vals, err := ds.rdb.LRange(ctx, "rate-analytics", 0, -1).Result()
	if err != nil {
		http.Error(w, "redis error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	entries := make([]types.RateAnalyticsEntry, 0, len(vals))
	for _, v := range vals {
		var e types.RateAnalyticsEntry
		if err := json.Unmarshal([]byte(v), &e); err != nil {
			continue
		}
		if filterPrefix != "" && e.Prefix != filterPrefix {
			continue
		}
		if filterKey != "" && e.APIKey != filterKey {
			continue
		}
		entries = append(entries, e)
	}

	if groupBy == "prefix" || groupBy == "api_key" {
		grouped := make(map[string]Stats)
		buckets := make(map[string][]float64)
		for _, e := range entries {
			key := e.Prefix
			if groupBy == "api_key" {
				key = e.APIKey
			}
			buckets[key] = append(buckets[key], float64(e.Delta))
		}
		for k, vals := range buckets {
			grouped[k] = computeStats(vals)
		}
		writeJSON(w, grouped)
		return
	}

	// ungrouped statistics
	valsF := make([]float64, len(entries))
	for i, e := range entries {
		valsF[i] = float64(e.Delta)
	}
	stats := computeStats(valsF)
	writeJSON(w, stats)
}

// Stats holds summary statistics for a sample.
type Stats struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"stddev"`
	P10    float64 `json:"p10"`
	Median float64 `json:"median"`
	P90    float64 `json:"p90"`
}

// computeStats calculates basic metrics on a slice.
func computeStats(data []float64) Stats {
	s := Stats{Count: len(data)}
	if len(data) == 0 {
		return s
	}
	sort.Float64s(data)
	s.Min = data[0]
	s.Max = data[len(data)-1]
	total := 0.0
	for _, v := range data {
		total += v
	}
	s.Mean = total / float64(len(data))
	// variance
	var2 := 0.0
	for _, v := range data {
		d := v - s.Mean
		var2 += d * d
	}
	if len(data) > 1 {
		s.StdDev = sqrt(var2 / float64(len(data)-1))
	}
	s.P10 = percentile(data, 10)
	s.Median = percentile(data, 50)
	s.P90 = percentile(data, 90)
	return s
}

// percentile returns the p-th percentile of a sorted slice.
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := p / 100 * float64(n-1)
	low := int(rank)
	high := low + 1
	if high >= n {
		return sorted[low]
	}
	frac := rank - float64(low)
	return sorted[low]*(1-frac) + sorted[high]*frac
}

// simple square root wrapper
func sqrt(x float64) float64 {
	return float64(math.Sqrt(x))
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
