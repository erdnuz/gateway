package analyticsapi

import (
	"net/url"
	"testing"
	"time"
)

func TestNewContractSummaryResponseContainsMandatoryKeys(t *testing.T) {
	resp := newContractSummaryResponse()
	if len(resp.Summary) != len(contractSummaryKeys) {
		t.Fatalf("expected %d summary keys, got %d", len(contractSummaryKeys), len(resp.Summary))
	}
	for _, key := range contractSummaryKeys {
		if _, ok := resp.Summary[key]; !ok {
			t.Fatalf("missing mandatory summary key %q", key)
		}
	}
	if resp.Series.Latency == nil || resp.Series.Volume == nil || resp.Series.Rates == nil || resp.Series.Prefixes == nil || resp.Series.Services == nil || resp.Series.Edges == nil {
		t.Fatal("expected all mandatory series arrays to be initialized")
	}
}

func TestParseAnalyticsSummaryQueryNormalizesWindowAndFilters(t *testing.T) {
	now := time.Date(2026, 3, 13, 12, 37, 20, 0, time.UTC)
	values := url.Values{
		"interval":             []string{"15m"},
		"group_by":             []string{"service"},
		"prefix":               []string{"/v1,/v2"},
		"service":              []string{"svc-a", "svc-b"},
		"edge_id":              []string{"edge-a,edge-b"},
		"method":               []string{"GET,POST"},
		"response_code_min":    []string{"200"},
		"response_code_max":    []string{"499"},
		"cache_hit":            []string{"true"},
		"upstream_error":       []string{"false"},
		"total_latency_ms_min": []string{"10.5"},
		"total_latency_ms_max": []string{"99.5"},
	}

	spec, err := parseAnalyticsSummaryQuery(values, now)
	if err != nil {
		t.Fatalf("parseAnalyticsSummaryQuery returned error: %v", err)
	}
	if spec.Interval != "15m" {
		t.Fatalf("expected interval 15m, got %q", spec.Interval)
	}
	if spec.GroupBy != "service" {
		t.Fatalf("expected group_by service, got %q", spec.GroupBy)
	}
	expectedEnd := time.Date(2026, 3, 13, 12, 30, 0, 0, time.UTC)
	if !spec.WindowEnd.Equal(expectedEnd) {
		t.Fatalf("expected window end %s, got %s", expectedEnd, spec.WindowEnd)
	}
	expectedStart := expectedEnd.Add(-24 * time.Hour)
	if !spec.WindowStart.Equal(expectedStart) {
		t.Fatalf("expected window start %s, got %s", expectedStart, spec.WindowStart)
	}
	if !spec.PreviousEnd.Equal(spec.WindowStart) {
		t.Fatalf("expected previous end to equal window start, got prev_end=%s window_start=%s", spec.PreviousEnd, spec.WindowStart)
	}
	if len(spec.Filters.Prefixes) != 2 || spec.Filters.Prefixes[0] != "/v1" || spec.Filters.Prefixes[1] != "/v2" {
		t.Fatalf("unexpected prefixes filter: %+v", spec.Filters.Prefixes)
	}
	if len(spec.Filters.Services) != 2 || spec.Filters.Services[0] != "svc-a" || spec.Filters.Services[1] != "svc-b" {
		t.Fatalf("unexpected services filter: %+v", spec.Filters.Services)
	}
	if len(spec.Filters.EdgeIDs) != 2 || spec.Filters.EdgeIDs[0] != "edge-a" || spec.Filters.EdgeIDs[1] != "edge-b" {
		t.Fatalf("unexpected edge_id filter: %+v", spec.Filters.EdgeIDs)
	}
	if len(spec.Filters.Methods) != 2 || spec.Filters.Methods[0] != "GET" || spec.Filters.Methods[1] != "POST" {
		t.Fatalf("unexpected methods filter: %+v", spec.Filters.Methods)
	}
	if spec.Filters.ResponseCodeMin == nil || *spec.Filters.ResponseCodeMin != 200 {
		t.Fatalf("unexpected response_code_min: %+v", spec.Filters.ResponseCodeMin)
	}
	if spec.Filters.ResponseCodeMax == nil || *spec.Filters.ResponseCodeMax != 499 {
		t.Fatalf("unexpected response_code_max: %+v", spec.Filters.ResponseCodeMax)
	}
	if spec.Filters.CacheHit == nil || !*spec.Filters.CacheHit {
		t.Fatalf("unexpected cache_hit filter: %+v", spec.Filters.CacheHit)
	}
	if spec.Filters.UpstreamError == nil || *spec.Filters.UpstreamError {
		t.Fatalf("unexpected upstream_error filter: %+v", spec.Filters.UpstreamError)
	}
	if spec.Filters.LatencyMinMs == nil || *spec.Filters.LatencyMinMs != 10.5 {
		t.Fatalf("unexpected latency min filter: %+v", spec.Filters.LatencyMinMs)
	}
	if spec.Filters.LatencyMaxMs == nil || *spec.Filters.LatencyMaxMs != 99.5 {
		t.Fatalf("unexpected latency max filter: %+v", spec.Filters.LatencyMaxMs)
	}
}

func TestParseAnalyticsSummaryQueryRejectsInvalidWindow(t *testing.T) {
	values := url.Values{
		"start": []string{"2026-03-13T12:00:00Z"},
		"end":   []string{"2026-03-13T11:00:00Z"},
	}
	if _, err := parseAnalyticsSummaryQuery(values, time.Now().UTC()); err == nil {
		t.Fatal("expected invalid window error")
	}
}
