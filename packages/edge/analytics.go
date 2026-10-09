package edge

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"

	"gateway/packages/common/types"

	"github.com/nats-io/nats.go"
)

// recordAnalytics captures one entry per sampled request after it completes.
func (s *Server) recordAnalytics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := stateOf(r)
		if st.entry == nil {
			next.ServeHTTP(w, r)
			return
		}
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if !sampled(st.svc) {
			return
		}
		st.entry.TotalLatency = time.Since(st.entry.Timestamp)
		st.entry.ResponseCode = uint16(rec.status)
		if rec.status == 0 {
			st.entry.ResponseCode = 499 // the client went away before a response
		}
		st.entry.ResponseSize = uint64(rec.bytes)
		s.Analytics.Capture(st.entry)
	})
}

func sampled(svc *types.ServiceConfig) bool {
	if svc == nil || !svc.Analytics.Enabled {
		return false
	}
	rate := svc.Analytics.SamplingRate
	return rate >= 1 || (rate > 0 && rand.Float64() < rate)
}

// statusRecorder notes the status and size of a response as it streams.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *statusRecorder) WriteHeader(code int) {
	if w.status == 0 && (code >= 200 || code == http.StatusSwitchingProtocols) {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusRecorder) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *statusRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// AnalyticsSink receives analytics entries; Capture must not block.
type AnalyticsSink interface {
	Capture(entry *types.AnalyticsEntry)
	Close() error
}

// AnalyticsManager publishes entries to NATS from a bounded buffer, dropping
// entries rather than slowing requests when the bus falls behind.
type AnalyticsManager struct {
	buffer  chan *types.AnalyticsEntry
	nc      *nats.Conn
	subj    string
	timeout time.Duration
	write   func(context.Context, *types.AnalyticsEntry) error

	captured        atomic.Uint64
	dropped         atomic.Uint64
	published       atomic.Uint64
	publishFailures atomic.Uint64
}

type AnalyticsManagerStats struct {
	BufferDepth     int    `json:"buffer_depth"`
	BufferCapacity  int    `json:"buffer_capacity"`
	Captured        uint64 `json:"captured"`
	Dropped         uint64 `json:"dropped"`
	Published       uint64 `json:"published"`
	PublishFailures uint64 `json:"publish_failures"`
}

type AnalyticsManagerOptions struct {
	PublishTimeout time.Duration
}

// NewAnalyticsManager creates a manager publishing to the local NATS default.
func NewAnalyticsManager(bufferSize int) *AnalyticsManager {
	return NewAnalyticsManagerWithNATSOptions(bufferSize, nats.DefaultURL, "", AnalyticsManagerOptions{})
}

func NewAnalyticsManagerWithNATSOptions(bufferSize int, natsURL, subject string, options AnalyticsManagerOptions) *AnalyticsManager {
	def := types.DefaultRuntimePolicy()
	if bufferSize <= 0 {
		bufferSize = def.Edge.AnalyticsBufferSize
	}
	if options.PublishTimeout <= 0 {
		options.PublishTimeout = def.Edge.AnalyticsPublishTimeout
	}
	// Keep retrying: analytics may start after the edge, and requests never
	// wait on it either way.
	nc, err := nats.Connect(orDefault(natsURL, nats.DefaultURL), nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
	if err != nil {
		slog.Warn("analytics NATS connect failed; entries will be dropped", "url", natsURL, "err", err)
	}
	m := &AnalyticsManager{
		buffer:  make(chan *types.AnalyticsEntry, bufferSize),
		nc:      nc,
		subj:    orDefault(subject, def.Analytics.NATSSubject),
		timeout: options.PublishTimeout,
	}
	m.write = m.publish
	return m
}

func (m *AnalyticsManager) Capture(entry *types.AnalyticsEntry) {
	m.captured.Add(1)
	select {
	case m.buffer <- entry:
	default:
		m.dropped.Add(1) // protect request latency over completeness
	}
}

func (m *AnalyticsManager) Stats() AnalyticsManagerStats {
	return AnalyticsManagerStats{
		BufferDepth:     len(m.buffer),
		BufferCapacity:  cap(m.buffer),
		Captured:        m.captured.Load(),
		Dropped:         m.dropped.Load(),
		Published:       m.published.Load(),
		PublishFailures: m.publishFailures.Load(),
	}
}

func (m *AnalyticsManager) Close() error {
	if m.nc != nil {
		m.nc.Close()
	}
	return nil
}

func (m *AnalyticsManager) publish(_ context.Context, entry *types.AnalyticsEntry) error {
	b, err := json.Marshal(entry)
	if err != nil || m.nc == nil {
		return err
	}
	return m.nc.Publish(m.subj, b)
}

// StartPublisher forwards buffered entries to NATS until ctx is done.
func (m *AnalyticsManager) StartPublisher(ctx context.Context) {
	go func() {
		defer m.Close()
		for {
			select {
			case entry := <-m.buffer:
				writeCtx, cancel := context.WithTimeout(ctx, m.timeout)
				err := m.write(writeCtx, entry)
				cancel()
				if err != nil {
					m.publishFailures.Add(1)
					slog.Warn("analytics publish failed", "err", err)
					continue
				}
				m.published.Add(1)
			case <-ctx.Done():
				return
			}
		}
	}()
}
