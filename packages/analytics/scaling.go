package analyticsapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"gateway/packages/common/httpx"
)

// scalingQueries are the control-plane numbers the dashboard's scaling strip
// shows. They come from Prometheus, which scrapes every edge and hub; the
// analytics event stream has no view of the hub.
var scalingQueries = map[string]string{
	"edge_rps":        `sum(rate(gate_requests_total[1m]))`,
	"hub_lease_rps":   `sum(rate(gate_hub_lease_requests_total[1m]))`,
	"waiting_rps":     `sum(rate(gate_hub_lease_requests_total{result="waiting"}[1m]))`,
	"key_lookup_rps":  `sum(rate(gate_key_resolutions_total[1m]))`,
	"edges_connected": `sum(gate_hub_edges_connected)`,
	"edges_serving":   `count(up{job="edge"} == 1)`,
	"hubs_serving":    `count(up{job="hub"} == 1)`,
}

const (
	scalingTimeout  = 2 * time.Second
	scalingCacheTTL = 5 * time.Second
)

type scalingResponse struct {
	Enabled bool               `json:"enabled"`
	Error   string             `json:"error,omitempty"`
	Values  map[string]float64 `json:"values,omitempty"`
	// HubWorkPerRequest is hub lease RPCs per gateway request.
	HubWorkPerRequest float64 `json:"hub_work_per_request"`
}

// prometheus reads instant values from a Prometheus server, caching the
// result briefly so many open dashboards cost Prometheus one query set.
type prometheus struct {
	base   string
	client *http.Client

	mu      sync.Mutex
	cached  scalingResponse
	fetched time.Time
}

// SetPrometheus enables the dashboard's scaling strip.
func (s *Server) SetPrometheus(baseURL string) {
	if baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/"); baseURL != "" {
		s.prom = &prometheus{base: baseURL, client: &http.Client{Timeout: scalingTimeout}}
	}
}

func (s *Server) handleScaling(w http.ResponseWriter, r *http.Request) {
	if s.prom == nil {
		writeJSON(w, scalingResponse{Enabled: false})
		return
	}
	resp := s.prom.snapshot(r.Context())
	if resp.Error != "" {
		httpx.Logger(r.Context()).Warn("prometheus query failed", "err", resp.Error)
	}
	writeJSON(w, resp)
}

func (p *prometheus) snapshot(ctx context.Context) scalingResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.fetched) < scalingCacheTTL {
		return p.cached
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scalingTimeout)
	defer cancel()
	resp := scalingResponse{Enabled: true, Values: map[string]float64{}}
	for name, q := range scalingQueries {
		v, err := p.query(ctx, q)
		if err != nil {
			resp = scalingResponse{Enabled: true, Error: "prometheus unreachable"}
			break
		}
		resp.Values[name] = v
	}
	if resp.Error == "" {
		resp.HubWorkPerRequest = ratio(resp.Values["hub_lease_rps"]+resp.Values["key_lookup_rps"], resp.Values["edge_rps"])
	}
	p.cached, p.fetched = resp, time.Now()
	return resp
}

// query runs an instant query and returns its single value (0 when the
// result is empty, e.g. before any traffic).
func (p *prometheus) query(ctx context.Context, q string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/api/v1/query?query="+url.QueryEscape(q), nil)
	if err != nil {
		return 0, err
	}
	res, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	var body struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return 0, err
	}
	if body.Status != "success" {
		return 0, fmt.Errorf("prometheus status %q", body.Status)
	}
	if len(body.Data.Result) == 0 {
		return 0, nil
	}
	raw, _ := body.Data.Result[0].Value[1].(string)
	v, err := strconv.ParseFloat(raw, 64)
	return finite(v), err
}
