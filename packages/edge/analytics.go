package edge

import (
	"context"
	"encoding/json"
	"gateway/packages/common/types"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type AnalyticsManager struct {
	// Buffer avoids blocking request path when broker is slow.
	buffer chan *types.AnalyticsEntry
	writer *kafka.Writer
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
	return NewAnalyticsManagerWithKafka(bufferSize, []string{"localhost:9092"}, "analytics-events")
}

func NewAnalyticsManagerWithKafka(bufferSize int, brokers []string, topic string) *AnalyticsManager {
	if bufferSize <= 0 {
		bufferSize = 1000 // Default buffer size
	}
	clean := make([]string, 0, len(brokers))
	for _, b := range brokers {
		v := strings.TrimSpace(b)
		if v != "" {
			clean = append(clean, v)
		}
	}
	if len(clean) == 0 {
		clean = []string{"localhost:9092"}
	}
	if strings.TrimSpace(topic) == "" {
		topic = "analytics-events"
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(clean...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	mgr := &AnalyticsManager{
		buffer: make(chan *types.AnalyticsEntry, bufferSize),
		writer: writer,
	}
	mgr.write = mgr.writeKafkaMessage
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
	if m.writer != nil {
		return m.writer.Close()
	}
	return nil
}

func (m *AnalyticsManager) writeKafkaMessage(ctx context.Context, entry *types.AnalyticsEntry) error {
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return m.writer.WriteMessages(ctx, kafka.Message{Value: b})
}

// StartPublisher forwards buffered analytics entries to Kafka as soon as they are captured.
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
					log.Printf("edge analytics kafka publish failed: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
