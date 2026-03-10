package types

import "time"

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
