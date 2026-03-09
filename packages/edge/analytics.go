package edge

import (
	"gateway/packages/common/types"
	"math/rand"
)

type AnalyticsManager struct {
	// A channel can act as a buffer so we don't block the request
	buffer chan *types.AnalyticsEntry
}

// NewAnalyticsManager creates a new analytics manager with the specified buffer size
func NewAnalyticsManager(bufferSize int) *AnalyticsManager {
	if bufferSize <= 0 {
		bufferSize = 1000 // Default buffer size
	}
	return &AnalyticsManager{
		buffer: make(chan *types.AnalyticsEntry, bufferSize),
	}
}

func (m *AnalyticsManager) ShouldSample(rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	return rand.Float64() < rate
}

func (m *AnalyticsManager) Capture(entry *types.AnalyticsEntry) {
	// Non-blocking send to the processing worker
	select {
	case m.buffer <- entry:
	default:
		// Buffer full, drop entry to protect Edge performance
	}
}
