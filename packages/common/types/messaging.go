package types

import "time"

// UsageDelta represents a change in usage for a given prefix and API key.
type UsageDelta struct {
	Prefix string `json:"prefix"`
	APIKey string `json:"api_key"`
	Delta  int64  `json:"delta"`
}

// SOTMsg (State-of-Truth) carries the authoritative total for a prefix/api key.
type SOTMsg struct {
	Prefix string `json:"prefix"`
	APIKey string `json:"api_key"`
	Total  int64  `json:"total"`
}

// RateAnalyticsEntry captures a single increment event used by the dashboard.
// The delta is the amount added to the local counter.
// Stored in Redis list "rate-analytics" by edges.
// The dashboard queries this list and computes summary statistics.
type RateAnalyticsEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Prefix    string    `json:"prefix"`
	APIKey    string    `json:"api_key"`
	Delta     int64     `json:"delta"`
}
