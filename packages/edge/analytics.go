package edge

import (
	"context"
	"encoding/json"
	"gateway/packages/common/types"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type AnalyticsManager struct {
	// Buffer avoids blocking request path when broker is slow.
	buffer chan *types.AnalyticsEntry
	nc     *nats.Conn
	subj   string
	write  func(context.Context, *types.AnalyticsEntry) error
}

type AnalyticsSink interface {
	Capture(entry *types.AnalyticsEntry)
	Close() error
}

type NoOpAnalyticsSink struct{}

func (NoOpAnalyticsSink) Capture(_ *types.AnalyticsEntry) {}

func (NoOpAnalyticsSink) Close() error { return nil }

// NewAnalyticsManager creates a new analytics manager with the specified buffer size
func NewAnalyticsManager(bufferSize int) *AnalyticsManager {
	return NewAnalyticsManagerWithNATS(bufferSize, nats.DefaultURL, "analytics.events")
}

func NewAnalyticsManagerWithNATS(bufferSize int, natsURL, subject string) *AnalyticsManager {
	if bufferSize <= 0 {
		bufferSize = 1000 // Default buffer size
	}
	if subject == "" {
		subject = "analytics.events"
	}
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("edge analytics nats connect failed url=%s err=%v", natsURL, err)
	}
	mgr := &AnalyticsManager{
		buffer: make(chan *types.AnalyticsEntry, bufferSize),
		nc:     nc,
		subj:   subject,
	}
	mgr.write = mgr.writeNATSMessage
	return mgr
}

func (m *AnalyticsManager) Capture(entry *types.AnalyticsEntry) {
	// Non-blocking send to the processing worker
	select {
	case m.buffer <- entry:
	default:
		// Buffer full, drop entry to protect Edge performance
	}
}

func (m *AnalyticsManager) Close() error {
	if m.nc != nil {
		m.nc.Close()
	}
	return nil
}

func (m *AnalyticsManager) writeNATSMessage(ctx context.Context, entry *types.AnalyticsEntry) error {
	_ = ctx
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if m.nc == nil {
		return nil
	}
	return m.nc.Publish(m.subj, b)
}

// StartPublisher forwards buffered analytics entries to NATS as soon as they are captured.
func (m *AnalyticsManager) StartPublisher(ctx context.Context) {
	go func() {
		defer func() {
			if err := m.Close(); err != nil {
				log.Printf("edge analytics writer close failed: %v", err)
			}
		}()
		for {
			select {
			case entry := <-m.buffer:
				if entry == nil {
					continue
				}
				writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				err := m.write(writeCtx, entry)
				cancel()
				if err != nil {
					log.Printf("edge analytics nats publish failed: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
