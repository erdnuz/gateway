package analyticsapi

import (
	"testing"
	"time"

	"gateway/packages/common/types"
)

func TestTrendDelta(t *testing.T) {
	t.Run("zero_to_zero", func(t *testing.T) {
		if got := trendDelta(0, 0); got != 0 {
			t.Fatalf("expected 0, got %v", got)
		}
	})

	t.Run("zero_to_positive", func(t *testing.T) {
		if got := trendDelta(10, 0); got != 0 {
			t.Fatalf("expected 0 (no baseline), got %v", got)
		}
	})

	t.Run("standard_delta", func(t *testing.T) {
		got := trendDelta(15, 10)
		want := 0.5
		if got != want {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})
}

func TestComputeSummaryExcludesCacheHitsAndRateLimitedFromAvgUpstreamLatency(t *testing.T) {
	entries := []types.AnalyticsEntry{
		{TotalLatency: 100 * time.Millisecond, UpstreamLatency: 80 * time.Millisecond, CacheHit: false, ResponseCode: 200},
		{TotalLatency: 25 * time.Millisecond, UpstreamLatency: 0, CacheHit: true, ResponseCode: 200},
		{TotalLatency: 10 * time.Millisecond, UpstreamLatency: 0, CacheHit: false, ResponseCode: 429},
	}
	s := computeSummary(entries)
	if s.AvgUpstreamLatencyMs != 80 {
		t.Fatalf("expected upstream avg 80ms from upstream-served requests only, got %v", s.AvgUpstreamLatencyMs)
	}
}

func TestTrendDeltaWithRecentFallback(t *testing.T) {
	t.Run("uses_window_delta_when_previous_window_exists", func(t *testing.T) {
		got := trendDeltaWithRecentFallback(20, 10, true, 7, 5)
		if got != 1 {
			t.Fatalf("expected 1 from window delta, got %v", got)
		}
	})

	t.Run("falls_back_to_recent_buckets_when_previous_window_missing", func(t *testing.T) {
		got := trendDeltaWithRecentFallback(20, 0, true, 12, 8)
		want := 0.5
		if got != want {
			t.Fatalf("expected fallback trend %v, got %v", want, got)
		}
	})

	t.Run("keeps_zero_when_no_recent_bucket_pair", func(t *testing.T) {
		got := trendDeltaWithRecentFallback(20, 0, false, 12, 8)
		if got != 0 {
			t.Fatalf("expected 0 without bucket pair, got %v", got)
		}
	})
}

func TestLastTwoSampledBuckets(t *testing.T) {
	ordered := []time.Time{
		time.Unix(100, 0).UTC(),
		time.Unix(160, 0).UTC(),
		time.Unix(220, 0).UTC(),
		time.Unix(280, 0).UTC(),
	}
	buckets := map[int64]contractBucketAggregate{
		100: {SampleCount: 0},
		160: {SampleCount: 5, RatesCacheHit: 0.1},
		220: {SampleCount: 0},
		280: {SampleCount: 9, RatesCacheHit: 0.4},
	}

	latest, previous, ok := lastTwoSampledBuckets(buckets, ordered)
	if !ok {
		t.Fatal("expected sampled bucket pair")
	}
	if latest.SampleCount != 9 {
		t.Fatalf("expected latest sample_count=9, got %d", latest.SampleCount)
	}
	if previous.SampleCount != 5 {
		t.Fatalf("expected previous sample_count=5, got %d", previous.SampleCount)
	}
}
