package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"gateway/packages/common/types"
	"io"
	"math/rand"
	"net/http"
	"time"
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

// drain pulls all currently queued entries from the buffer without blocking.
func (m *AnalyticsManager) drain() []types.AnalyticsEntry {
	var batch []types.AnalyticsEntry
	for {
		select {
		case entry := <-m.buffer:
			batch = append(batch, *entry)
		default:
			return batch
		}
	}
}

// flushTo drains the buffer and POSTs the batch to the hub analytics endpoint.
func (m *AnalyticsManager) flushTo(ctx context.Context, hubAddr string, client *http.Client) {
	batch := m.drain()
	if len(batch) == 0 {
		return
	}

	b, err := json.Marshal(batch)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hubAddr+"/analytics", bytes.NewReader(b))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}

// StartFlusher periodically drains the analytics buffer and sends batches to
// the hub at hubAddr. The goroutine runs until ctx is cancelled, at which point
// it performs a final flush before exiting.
func (m *AnalyticsManager) StartFlusher(ctx context.Context, hubAddr string, interval time.Duration) {
	if hubAddr == "" || interval <= 0 {
		return
	}
	client := &http.Client{Timeout: 5 * time.Second}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.flushTo(ctx, hubAddr, client)
			case <-ctx.Done():
				// Best-effort final flush on shutdown.
				finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				m.flushTo(finalCtx, hubAddr, client)
				cancel()
				return
			}
		}
	}()
}
