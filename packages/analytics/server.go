package analyticsapi

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gateway/packages/common/config"
	"gateway/packages/common/types"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

//go:embed static/index.html static/style.css static/app.js
var staticAssets embed.FS

type Server struct {
	rdb               *redis.Client
	store             analyticsStore
	key               string
	authToken         string
	policy            types.AnalyticsRuntimePolicy
	allowTestingClear bool

	summaryMu    sync.RWMutex
	summaryCache map[string]summaryResponse
	groupedCache map[string]map[string]map[string]summary
	cacheReady   bool

	aggData map[string]*intervalAggregation

	ingestQueue   chan types.AnalyticsEntry
	ingestStarted atomic.Bool
	ingestStats   ingestionCounters
}

type ingestionCounters struct {
	received     atomic.Uint64
	enqueued     atomic.Uint64
	dropped      atomic.Uint64
	persisted    atomic.Uint64
	persistFails atomic.Uint64
	retries      atomic.Uint64
}

type ingestionMetrics struct {
	QueueDepth      int    `json:"queue_depth"`
	QueueCapacity   int    `json:"queue_capacity"`
	Received        uint64 `json:"received"`
	Enqueued        uint64 `json:"enqueued"`
	Dropped         uint64 `json:"dropped"`
	Persisted       uint64 `json:"persisted"`
	PersistFailures uint64 `json:"persist_failures"`
	PersistRetries  uint64 `json:"persist_retries"`
}

const (
	defaultAggregateTick   = 10 * time.Second
	defaultHistoryWindow   = 7 * 24 * time.Hour
	defaultRedisOpTimeout  = 250 * time.Millisecond
	defaultPercentileCap   = 2048
	defaultIngestQueueSize = 2048
	ingestMaxRetries       = 2
	ingestRetryBackoff     = 25 * time.Millisecond
)

func withRedisTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.WithTimeout(context.Background(), defaultRedisOpTimeout)
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultRedisOpTimeout)
}

var supportedIntervals = map[string]time.Duration{
	"1m":  time.Minute,
	"5m":  5 * time.Minute,
	"15m": 15 * time.Minute,
	"1h":  time.Hour,
}

var intervalRangeWindow = map[string]time.Duration{
	"1m":  time.Hour,
	"5m":  6 * time.Hour,
	"15m": 24 * time.Hour,
	"1h":  7 * 24 * time.Hour,
}

var supportedGroupBy = map[string]bool{
	"prefix":  true,
	"service": true,
	"edge_id": true,
	"tier":    true,
	"method":  true,
}

func normalizeInterval(raw string) string {
	clean := strings.TrimSpace(strings.ToLower(raw))
	if _, ok := supportedIntervals[clean]; ok {
		return clean
	}
	return "1m"
}

func NewServer(rdb *redis.Client, key, authToken string) *Server {
	return NewServerWithPolicy(rdb, key, authToken, types.AnalyticsRuntimePolicy{})
}

func NewServerWithPolicy(rdb *redis.Client, key, authToken string, policy types.AnalyticsRuntimePolicy) *Server {
	return NewServerWithStore(newRedisAnalyticsStore(rdb, key), rdb, key, authToken, policy)
}

func NewServerWithAnalyticsStore(store analyticsStore, authToken string, policy types.AnalyticsRuntimePolicy) *Server {
	return NewServerWithStore(store, nil, "", authToken, policy)
}

func NewServerWithStore(store analyticsStore, rdb *redis.Client, key, authToken string, policy types.AnalyticsRuntimePolicy) *Server {
	if key == "" {
		key = types.DefaultAnalyticsKey
	}
	effective := types.RuntimePolicy{Analytics: policy}.Effective().Analytics
	data := make(map[string]*intervalAggregation, len(supportedIntervals))
	for id, size := range supportedIntervals {
		data[id] = &intervalAggregation{
			bucketSize: size,
			buckets:    map[int64]*bucketAggregation{},
		}
	}
	return &Server{
		rdb:               rdb,
		store:             store,
		key:               key,
		authToken:         strings.TrimSpace(authToken),
		policy:            effective,
		allowTestingClear: config.Bool("ANALYTICS_ENABLE_TESTING_CLEAR", false),
		summaryCache:      map[string]summaryResponse{},
		groupedCache:      map[string]map[string]map[string]summary{},
		aggData:           data,
		ingestQueue:       make(chan types.AnalyticsEntry, defaultIngestQueueSize),
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/", "/index.html":
		s.serveFrontend(w, r)
	case "/assets/style.css", "/assets/app.js":
		s.serveFrontend(w, r)
	case "/healthz":
		s.handleHealth(w, r)
	case "/readyz":
		s.handleReady(w, r)
	case "/analytics/features":
		s.handleFeatures(w, r)
	case "/analytics/ingestion-metrics":
		if !s.authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.handleIngestionMetrics(w, r)
	case "/analytics/events":
		if !s.authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.queryEvents(w, r)
	case "/analytics/summary":
		if !s.authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.handleSummary(w, r)
	case "/analytics/clear":
		if !s.authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.handleClear(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) serveFrontend(w http.ResponseWriter, r *http.Request) {
	assets, err := fs.Sub(staticAssets, "static")
	if err != nil {
		http.Error(w, "frontend unavailable", http.StatusInternalServerError)
		return
	}

	if r.URL.Path == "/" || r.URL.Path == "/index.html" {
		b, err := fs.ReadFile(assets, "index.html")
		if err != nil {
			http.Error(w, "frontend unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(b)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/assets/") {
		http.StripPrefix("/assets/", http.FileServer(http.FS(assets))).ServeHTTP(w, r)
		return
	}

	http.NotFound(w, r)
}

func (s *Server) authorize(r *http.Request) bool {
	if s.authToken == "" {
		return true
	}
	authz := r.Header.Get("Authorization")
	const bearer = "Bearer "
	if !strings.HasPrefix(authz, bearer) {
		return false
	}
	return strings.TrimPrefix(authz, bearer) == s.authToken
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.store != nil {
		pingCtx, pingCancel := withRedisTimeout(r.Context())
		err := s.store.Ping(pingCtx)
		pingCancel()
		if err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, map[string]string{"status": "ready"})
		return
	}
	if s.rdb == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	pingCtx, pingCancel := withRedisTimeout(r.Context())
	err := s.rdb.Ping(pingCtx).Err()
	pingCancel()
	if err != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]string{"status": "ready"})
}

func (s *Server) handleFeatures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, map[string]bool{"testing_clear_enabled": s.allowTestingClear})
}

func (s *Server) handleIngestionMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, s.snapshotIngestionMetrics())
}

func (s *Server) snapshotIngestionMetrics() ingestionMetrics {
	depth := 0
	capc := 0
	if s.ingestQueue != nil {
		depth = len(s.ingestQueue)
		capc = cap(s.ingestQueue)
	}
	return ingestionMetrics{
		QueueDepth:      depth,
		QueueCapacity:   capc,
		Received:        s.ingestStats.received.Load(),
		Enqueued:        s.ingestStats.enqueued.Load(),
		Dropped:         s.ingestStats.dropped.Load(),
		Persisted:       s.ingestStats.persisted.Load(),
		PersistFailures: s.ingestStats.persistFails.Load(),
		PersistRetries:  s.ingestStats.retries.Load(),
	}
}

func (s *Server) ensureIngestionWorker(ctx context.Context) {
	if s.ingestQueue == nil {
		s.ingestQueue = make(chan types.AnalyticsEntry, defaultIngestQueueSize)
	}
	if !s.ingestStarted.CompareAndSwap(false, true) {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case entry := <-s.ingestQueue:
				s.persistAnalyticsEntry(ctx, entry)
			}
		}
	}()
}

func (s *Server) persistAnalyticsEntry(ctx context.Context, entry types.AnalyticsEntry) {
	for attempt := 0; attempt <= ingestMaxRetries; attempt++ {
		err := s.storeBatch(ctx, []types.AnalyticsEntry{entry})
		if err == nil {
			if attempt > 0 {
				s.ingestStats.retries.Add(uint64(attempt))
			}
			s.ingestStats.persisted.Add(1)
			return
		}
		if attempt == ingestMaxRetries {
			if attempt > 0 {
				s.ingestStats.retries.Add(uint64(attempt))
			}
			s.ingestStats.persistFails.Add(1)
			return
		}
		select {
		case <-ctx.Done():
			s.ingestStats.persistFails.Add(1)
			return
		case <-time.After(ingestRetryBackoff):
		}
	}
}

func (s *Server) storeBatch(ctx context.Context, batch []types.AnalyticsEntry) error {
	if s.store != nil {
		storeCtx, storeCancel := withRedisTimeout(ctx)
		defer storeCancel()
		return s.store.InsertEntries(storeCtx, batch)
	}
	storeCtx, storeCancel := withRedisTimeout(ctx)
	defer storeCancel()
	pipe := s.rdb.Pipeline()
	for _, e := range batch {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		pipe.RPush(storeCtx, s.key, b)
	}
	_, err := pipe.Exec(storeCtx)
	return err
}

// StartNATSSubscriber consumes analytics events from NATS and persists them in Redis.
func (s *Server) StartNATSSubscriber(ctx context.Context, natsURL, subject, queue string) error {
	if strings.TrimSpace(natsURL) == "" {
		natsURL = nats.DefaultURL
	}
	if strings.TrimSpace(subject) == "" {
		subject = s.policy.NATSSubject
	}
	if strings.TrimSpace(queue) == "" {
		queue = s.policy.NATSQueue
	}
	s.ensureIngestionWorker(ctx)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return err
	}
	sub, err := nc.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		s.ingestStats.received.Add(1)
		var e types.AnalyticsEntry
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			return
		}
		select {
		case s.ingestQueue <- e:
			s.ingestStats.enqueued.Add(1)
		default:
			s.ingestStats.dropped.Add(1)
		}
	})
	if err != nil {
		nc.Close()
		return err
	}
	if err := nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		nc.Close()
		return err
	}
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
		nc.Close()
	}()
	return nil
}

func (s *Server) queryEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit := parseIntWithBounds(r.URL.Query().Get("limit"), s.policy.DefaultEventsLimit, 1, s.policy.MaxEventsLimit)
	offset := parseIntWithBounds(r.URL.Query().Get("offset"), 0, 0, s.policy.MaxEventsOffset)

	entries, err := s.readEntries(ctx, limit, offset)
	if err != nil {
		http.Error(w, "analytics storage error", http.StatusInternalServerError)
		return
	}
	entries = filterEntries(entries, r)
	writeJSON(w, map[string]interface{}{"count": len(entries), "events": entries})
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	spec, err := parseAnalyticsSummaryQuery(r.URL.Query(), time.Now().UTC())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	intervalID := spec.Interval
	groupBy := spec.GroupBy
	query := r.URL.Query()

	if clickStore, ok := s.store.(*clickHouseAnalyticsStore); ok {
		if groupBy != "" {
			http.Error(w, "group_by is not supported for contract responses", http.StatusBadRequest)
			return
		}
		resp, err := clickStore.QueryContractSummary(r.Context(), spec)
		if err != nil {
			http.Error(w, "analytics storage error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, resp)
		return
	}

	if hasSummaryFilters(query) {
		limit := parseIntWithBounds(query.Get("limit"), s.policy.DefaultSummaryLimit, 1, s.policy.MaxSummaryLimit)
		offset := parseIntWithBounds(query.Get("offset"), 0, 0, s.policy.MaxEventsOffset)
		entries, err := s.readEntries(r.Context(), limit, offset)
		if err != nil {
			http.Error(w, "analytics storage error", http.StatusInternalServerError)
			return
		}
		entries = filterEntries(entries, r)

		if groupBy != "" {
			writeJSON(w, summarizeByGroup(entries, groupBy))
			return
		}

		writeJSON(w, buildFilteredSummary(entries, intervalID, spec.WindowEnd))
		return
	}

	s.summaryMu.RLock()
	ready := s.cacheReady
	snap, snapOK := s.summaryCache[intervalID]
	groupsByInterval := s.groupedCache[intervalID]
	s.summaryMu.RUnlock()

	if !ready || !snapOK {
		w.WriteHeader(http.StatusAccepted)
		writeJSON(w, map[string]interface{}{
			"status":   "processing",
			"interval": intervalID,
		})
		return
	}

	if groupBy == "" {
		writeJSON(w, snap)
		return
	}
	if groupsByInterval == nil {
		writeJSON(w, map[string]summary{})
		return
	}
	if grouped, ok := groupsByInterval[groupBy]; ok {
		writeJSON(w, grouped)
		return
	}
	writeJSON(w, map[string]summary{})
}

func (s *Server) handleClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.allowTestingClear {
		http.Error(w, "clear endpoint disabled", http.StatusForbidden)
		return
	}

	ctx := r.Context()
	var err error
	if s.store != nil {
		delCtx, delCancel := withRedisTimeout(ctx)
		err = s.store.Clear(delCtx)
		delCancel()
	} else {
		delCtx, delCancel := withRedisTimeout(ctx)
		err = s.rdb.Del(delCtx, s.key).Err()
		delCancel()
	}
	if err != nil {
		http.Error(w, "analytics storage error", http.StatusInternalServerError)
		return
	}

	s.summaryMu.Lock()
	s.summaryCache = map[string]summaryResponse{}
	s.groupedCache = map[string]map[string]map[string]summary{}
	s.cacheReady = false
	for id, size := range supportedIntervals {
		s.aggData[id] = &intervalAggregation{
			bucketSize: size,
			buckets:    map[int64]*bucketAggregation{},
		}
	}
	s.summaryMu.Unlock()

	writeJSON(w, map[string]string{"status": "cleared"})
}

// StartBackgroundAggregator keeps summary snapshots updated at a fixed cadence.
func (s *Server) StartBackgroundAggregator(ctx context.Context, cadence time.Duration) {
	if cadence <= 0 {
		cadence = defaultAggregateTick
	}
	_ = s.refreshSummaryCache(ctx)
	ticker := time.NewTicker(cadence)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = s.refreshSummaryCache(ctx)
			}
		}
	}()
}

func (s *Server) refreshSummaryCache(ctx context.Context) error {
	if err := s.reloadAggregates(ctx); err != nil {
		return err
	}

	now := time.Now().UTC()
	nextSummary := make(map[string]summaryResponse, len(supportedIntervals))
	nextGroups := make(map[string]map[string]map[string]summary, len(supportedIntervals))

	for intervalID, interval := range s.aggData {
		snap, grouped := buildSnapshot(intervalID, interval, now)
		nextSummary[intervalID] = snap
		nextGroups[intervalID] = grouped
	}

	s.summaryMu.Lock()
	s.summaryCache = nextSummary
	s.groupedCache = nextGroups
	s.cacheReady = true
	s.summaryMu.Unlock()
	return nil
}

func (s *Server) reloadAggregates(ctx context.Context) error {
	if s.store == nil {
		return nil
	}
	now := time.Now().UTC()
	entries, err := s.store.LoadRecentEntries(ctx, now.Add(-defaultHistoryWindow), now, defaultBootstrapLoadLimit)
	if err != nil {
		return err
	}
	for id, size := range supportedIntervals {
		s.aggData[id] = &intervalAggregation{
			bucketSize: size,
			buckets:    map[int64]*bucketAggregation{},
		}
	}
	for _, entry := range entries {
		s.ingestEntry(entry)
	}
	return nil
}

func (s *Server) ingestEntry(e types.AnalyticsEntry) {
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	ts := e.Timestamp.UTC()
	for _, interval := range s.aggData {
		interval.ingest(ts, e)
	}
}

type intervalAggregation struct {
	bucketSize time.Duration
	buckets    map[int64]*bucketAggregation
}

func (ia *intervalAggregation) ingest(ts time.Time, e types.AnalyticsEntry) {
	bucketStart := ts.Truncate(ia.bucketSize).Unix()
	bucket := ia.buckets[bucketStart]
	if bucket == nil {
		bucket = newBucketAggregation()
		ia.buckets[bucketStart] = bucket
	}
	bucket.add(e)
	ia.prune(ts)
}

func (ia *intervalAggregation) prune(now time.Time) {
	cutoff := now.Add(-defaultHistoryWindow).Unix()
	for k := range ia.buckets {
		if k < cutoff {
			delete(ia.buckets, k)
		}
	}
}

type bucketAggregation struct {
	total  statsAccumulator
	groups map[string]map[string]*statsAccumulator
}

func newBucketAggregation() *bucketAggregation {
	return &bucketAggregation{
		groups: map[string]map[string]*statsAccumulator{
			"prefix":  {},
			"service": {},
			"edge_id": {},
			"tier":    {},
			"method":  {},
		},
	}
}

func (b *bucketAggregation) add(e types.AnalyticsEntry) {
	b.total.add(e)
	b.addGroup("prefix", e.Prefix, e)
	b.addGroup("service", e.Service, e)
	b.addGroup("edge_id", e.EdgeID, e)
	b.addGroup("tier", e.Tier, e)
	b.addGroup("method", e.Method, e)
}

func (b *bucketAggregation) addGroup(group, key string, e types.AnalyticsEntry) {
	if key == "" {
		key = "unknown"
	}
	m := b.groups[group]
	acc := m[key]
	if acc == nil {
		acc = &statsAccumulator{}
		m[key] = acc
	}
	acc.add(e)
}

type statsAccumulator struct {
	Count              int
	TotalLatencyUsSum  int64
	UpstreamLatencySum int64
	RateLimiterUsSum   int64
	Totals             *boundedPercentiles
	RateLimiter        *boundedPercentiles
	CacheHits          int
	Successes          int
	RequestBytesTotal  uint64
	ResponseBytesTotal uint64
	ActiveTiers        map[string]struct{}
	RateLimitedCount   int
	UpstreamErrorCount int
}

type boundedPercentiles struct {
	capacity int
	count    int64
	values   []int64
	rng      uint64
}

func newBoundedPercentiles(capacity int) *boundedPercentiles {
	if capacity <= 0 {
		capacity = defaultPercentileCap
	}
	return &boundedPercentiles{capacity: capacity, values: make([]int64, 0, capacity), rng: 0x9e3779b97f4a7c15}
}

func (bp *boundedPercentiles) add(v int64) {
	if bp == nil {
		return
	}
	bp.count++
	if len(bp.values) < bp.capacity {
		bp.values = append(bp.values, v)
		return
	}
	idx := bp.nextIndex(bp.count)
	if idx >= 0 && idx < int64(bp.capacity) {
		bp.values[idx] = v
	}
}

func (bp *boundedPercentiles) merge(other *boundedPercentiles) {
	if bp == nil || other == nil {
		return
	}
	for _, v := range other.values {
		bp.add(v)
	}
	if other.count > int64(len(other.values)) {
		bp.count += other.count - int64(len(other.values))
	}
}

func (bp *boundedPercentiles) percentile(p float64) float64 {
	if bp == nil || len(bp.values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), bp.values...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	rank := p / 100 * float64(len(sorted)-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		return float64(sorted[lo])
	}
	frac := rank - float64(lo)
	return float64(sorted[lo])*(1-frac) + float64(sorted[hi])*frac
}

func (bp *boundedPercentiles) nextIndex(n int64) int64 {
	if n <= 0 {
		return -1
	}
	bp.rng += 0x9e3779b97f4a7c15
	z := bp.rng
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z = z ^ (z >> 31)
	return int64(z % uint64(n))
}

func (a *statsAccumulator) add(e types.AnalyticsEntry) {
	if a.ActiveTiers == nil {
		a.ActiveTiers = map[string]struct{}{}
	}
	if a.Totals == nil {
		a.Totals = newBoundedPercentiles(defaultPercentileCap)
	}
	if a.RateLimiter == nil {
		a.RateLimiter = newBoundedPercentiles(defaultPercentileCap)
	}
	totalUs := e.TotalLatency.Microseconds()
	upstreamUs := e.UpstreamLatency.Microseconds()
	rateLimiterUs := totalUs - upstreamUs
	if rateLimiterUs < 0 {
		rateLimiterUs = 0
	}
	a.Count++
	a.TotalLatencyUsSum += totalUs
	a.UpstreamLatencySum += upstreamUs
	a.RateLimiterUsSum += rateLimiterUs
	a.Totals.add(totalUs)
	a.RateLimiter.add(rateLimiterUs)
	if e.CacheHit {
		a.CacheHits++
	}
	if e.ResponseCode >= 200 && e.ResponseCode < 300 {
		a.Successes++
	}
	a.RequestBytesTotal += e.RequestSize
	a.ResponseBytesTotal += e.ResponseSize
	a.ActiveTiers[e.Tier] = struct{}{}
	if e.ResponseCode == http.StatusTooManyRequests {
		a.RateLimitedCount++
	}
	if e.UpstreamError {
		a.UpstreamErrorCount++
	}
}

func (a *statsAccumulator) merge(other *statsAccumulator) {
	if other == nil || other.Count == 0 {
		return
	}
	if a.ActiveTiers == nil {
		a.ActiveTiers = map[string]struct{}{}
	}
	if a.Totals == nil {
		a.Totals = newBoundedPercentiles(defaultPercentileCap)
	}
	if a.RateLimiter == nil {
		a.RateLimiter = newBoundedPercentiles(defaultPercentileCap)
	}
	a.Count += other.Count
	a.TotalLatencyUsSum += other.TotalLatencyUsSum
	a.UpstreamLatencySum += other.UpstreamLatencySum
	a.RateLimiterUsSum += other.RateLimiterUsSum
	a.Totals.merge(other.Totals)
	a.RateLimiter.merge(other.RateLimiter)
	a.CacheHits += other.CacheHits
	a.Successes += other.Successes
	a.RequestBytesTotal += other.RequestBytesTotal
	a.ResponseBytesTotal += other.ResponseBytesTotal
	for tier := range other.ActiveTiers {
		a.ActiveTiers[tier] = struct{}{}
	}
	a.RateLimitedCount += other.RateLimitedCount
	a.UpstreamErrorCount += other.UpstreamErrorCount
}

func (a *statsAccumulator) toSummary() summary {
	out := summary{Count: a.Count}
	if a.Count == 0 {
		return out
	}
	out.AvgTotalLatencyMs = microsecondsToMilliseconds(float64(a.TotalLatencyUsSum) / float64(a.Count))
	out.AvgUpstreamLatencyMs = microsecondsToMilliseconds(float64(a.UpstreamLatencySum) / float64(a.Count))
	out.AvgRateLimiterMs = microsecondsToMilliseconds(float64(a.RateLimiterUsSum) / float64(a.Count))
	out.P95RateLimiterMs = microsecondsToMilliseconds(a.RateLimiter.percentile(95))
	out.CacheHitRatePct = (float64(a.CacheHits) / float64(a.Count)) * 100
	out.SuccessRatePct = (float64(a.Successes) / float64(a.Count)) * 100
	out.AvgRequestSizeBytes = float64(a.RequestBytesTotal) / float64(a.Count)
	out.AvgResponseSizeBytes = float64(a.ResponseBytesTotal) / float64(a.Count)
	out.ActiveTiersCount = len(a.ActiveTiers)
	out.RateLimitedCount = a.RateLimitedCount
	out.UpstreamErrorCount = a.UpstreamErrorCount
	return out
}

type summaryBucket struct {
	BucketStart        time.Time `json:"bucket_start"`
	Count              int       `json:"count"`
	CacheHitCount      int       `json:"cache_hit_count"`
	RateLimitedCount   int       `json:"rate_limited_count"`
	UpstreamErrorCount int       `json:"upstream_error_count"`

	AvgTotalLatencyMs *float64 `json:"avg_total_latency_ms"`
	P50TotalLatencyMs *float64 `json:"p50_total_latency_ms"`
	P90TotalLatencyMs *float64 `json:"p90_total_latency_ms"`
	CacheHitRatePct   *float64 `json:"cache_hit_rate_pct"`
	SuccessRatePct    *float64 `json:"success_rate_pct"`
}

type summaryResponse struct {
	summary
	Interval    string          `json:"interval"`
	GeneratedAt time.Time       `json:"generated_at"`
	Series      []summaryBucket `json:"series"`
}

func buildSnapshot(intervalID string, interval *intervalAggregation, now time.Time) (summaryResponse, map[string]map[string]summary) {
	window := intervalRangeWindow[intervalID]
	if window <= 0 {
		window = time.Hour
	}
	bucketSize := interval.bucketSize
	end := now.UTC().Truncate(bucketSize)
	bucketsCount := int(window / bucketSize)
	start := end.Add(-time.Duration(bucketsCount-1) * bucketSize)

	total := &statsAccumulator{}
	groupedAcc := map[string]map[string]*statsAccumulator{
		"prefix":  {},
		"service": {},
		"edge_id": {},
		"tier":    {},
		"method":  {},
	}
	series := make([]summaryBucket, 0, bucketsCount)

	for i := 0; i < bucketsCount; i++ {
		bucketStart := start.Add(time.Duration(i) * bucketSize)
		bucket := interval.buckets[bucketStart.Unix()]
		if bucket == nil {
			series = append(series, summaryBucket{
				BucketStart:        bucketStart,
				Count:              0,
				CacheHitCount:      0,
				RateLimitedCount:   0,
				UpstreamErrorCount: 0,
			})
			continue
		}

		total.merge(&bucket.total)
		for groupBy, values := range bucket.groups {
			if groupedAcc[groupBy] == nil {
				groupedAcc[groupBy] = map[string]*statsAccumulator{}
			}
			for k, acc := range values {
				if groupedAcc[groupBy][k] == nil {
					groupedAcc[groupBy][k] = &statsAccumulator{}
				}
				groupedAcc[groupBy][k].merge(acc)
			}
		}

		bSummary := bucket.total.toSummary()
		bucketPoint := summaryBucket{
			BucketStart:        bucketStart,
			Count:              bSummary.Count,
			CacheHitCount:      bucket.total.CacheHits,
			RateLimitedCount:   bSummary.RateLimitedCount,
			UpstreamErrorCount: bSummary.UpstreamErrorCount,
		}
		if bSummary.Count > 0 {
			avg := bSummary.AvgTotalLatencyMs
			p50 := microsecondsToMilliseconds(bucket.total.Totals.percentile(50))
			p90 := microsecondsToMilliseconds(bucket.total.Totals.percentile(90))
			cacheRate := bSummary.CacheHitRatePct
			successRate := bSummary.SuccessRatePct
			bucketPoint.AvgTotalLatencyMs = &avg
			bucketPoint.P50TotalLatencyMs = &p50
			bucketPoint.P90TotalLatencyMs = &p90
			bucketPoint.CacheHitRatePct = &cacheRate
			bucketPoint.SuccessRatePct = &successRate
		}
		series = append(series, bucketPoint)
	}

	outGroups := map[string]map[string]summary{}
	for groupBy, values := range groupedAcc {
		mapped := map[string]summary{}
		for k, acc := range values {
			mapped[k] = acc.toSummary()
		}
		outGroups[groupBy] = mapped
	}

	out := summaryResponse{
		summary:     total.toSummary(),
		Interval:    intervalID,
		GeneratedAt: now.UTC(),
		Series:      series,
	}
	return out, outGroups
}

func buildFilteredSummary(entries []types.AnalyticsEntry, intervalID string, reference time.Time) summaryResponse {
	bucketSize := supportedIntervals[intervalID]
	agg := &intervalAggregation{bucketSize: bucketSize, buckets: map[int64]*bucketAggregation{}}
	for _, e := range entries {
		ts := e.Timestamp.UTC()
		if ts.IsZero() {
			continue
		}
		agg.ingest(ts, e)
	}
	snap, _ := buildSnapshot(intervalID, agg, reference.UTC())
	return snap
}

func summarizeByGroup(entries []types.AnalyticsEntry, groupBy string) map[string]summary {
	grouped := map[string]*statsAccumulator{}
	for _, e := range entries {
		k := groupKey(e, groupBy)
		if grouped[k] == nil {
			grouped[k] = &statsAccumulator{}
		}
		grouped[k].add(e)
	}
	out := map[string]summary{}
	for k, acc := range grouped {
		out[k] = acc.toSummary()
	}
	return out
}

func hasSummaryFilters(values map[string][]string) bool {
	filterKeys := []string{
		"prefix", "service", "edge_id", "tier", "method",
		"response_code_min", "response_code_max",
		"cache_hit", "upstream_error",
		"total_latency_ms_min", "total_latency_ms_max",
		"start", "end",
	}
	for _, key := range filterKeys {
		if strings.TrimSpace(firstQueryValue(values, key)) != "" {
			return true
		}
	}
	return false
}

func firstQueryValue(values map[string][]string, key string) string {
	vals, ok := values[key]
	if !ok || len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func groupKey(e types.AnalyticsEntry, groupBy string) string {
	switch groupBy {
	case "prefix":
		return e.Prefix
	case "service":
		return e.Service
	case "edge_id":
		return e.EdgeID
	case "tier":
		return e.Tier
	case "method":
		return e.Method
	default:
		return "unknown"
	}
}

func filterEntries(entries []types.AnalyticsEntry, r *http.Request) []types.AnalyticsEntry {
	prefixes := csvFilterSet(r.URL.Query().Get("prefix"))
	services := csvFilterSet(r.URL.Query().Get("service"))
	edgeIDs := csvFilterSet(r.URL.Query().Get("edge_id"))
	tiers := csvFilterSet(r.URL.Query().Get("tier"))
	methods := csvFilterSet(r.URL.Query().Get("method"))
	codeMin, hasCodeMin := parseOptionalInt64(r.URL.Query().Get("response_code_min"))
	codeMax, hasCodeMax := parseOptionalInt64(r.URL.Query().Get("response_code_max"))
	cacheHit, hasCacheHit := parseOptionalBool(r.URL.Query().Get("cache_hit"))
	upstreamErr, hasUpstreamErr := parseOptionalBool(r.URL.Query().Get("upstream_error"))
	totalMsMin, hasTotalMsMin := parseOptionalFloat64(r.URL.Query().Get("total_latency_ms_min"))
	totalMsMax, hasTotalMsMax := parseOptionalFloat64(r.URL.Query().Get("total_latency_ms_max"))
	start, hasStart := parseOptionalTime(r.URL.Query().Get("start"))
	end, hasEnd := parseOptionalTime(r.URL.Query().Get("end"))
	out := make([]types.AnalyticsEntry, 0, len(entries))
	for _, e := range entries {
		if len(prefixes) > 0 && !prefixes[e.Prefix] {
			continue
		}
		if len(services) > 0 && !services[e.Service] {
			continue
		}
		if len(edgeIDs) > 0 && !edgeIDs[e.EdgeID] {
			continue
		}
		if len(tiers) > 0 && !tiers[e.Tier] {
			continue
		}
		if len(methods) > 0 && !methods[e.Method] {
			continue
		}
		if hasCodeMin && int64(e.ResponseCode) < codeMin {
			continue
		}
		if hasCodeMax && int64(e.ResponseCode) > codeMax {
			continue
		}
		if hasCacheHit && e.CacheHit != cacheHit {
			continue
		}
		if hasUpstreamErr && e.UpstreamError != upstreamErr {
			continue
		}
		totalLatencyMs := microsecondsToMilliseconds(float64(e.TotalLatency.Microseconds()))
		if hasTotalMsMin && totalLatencyMs < totalMsMin {
			continue
		}
		if hasTotalMsMax && totalLatencyMs > totalMsMax {
			continue
		}
		ts := e.Timestamp.UTC()
		if hasStart && ts.Before(start.UTC()) {
			continue
		}
		if hasEnd && ts.After(end.UTC()) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func csvFilterSet(raw string) map[string]bool {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	set := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		value := strings.TrimSpace(part)
		if value != "" {
			set[value] = true
		}
	}
	if len(set) == 0 {
		return nil
	}
	return set
}

func parseOptionalInt64(raw string) (int64, bool) {
	if strings.TrimSpace(raw) == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func parseOptionalFloat64(raw string) (float64, bool) {
	if strings.TrimSpace(raw) == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func parseOptionalBool(raw string) (bool, bool) {
	v := strings.TrimSpace(strings.ToLower(raw))
	if v == "" {
		return false, false
	}
	if v == "true" || v == "1" || v == "yes" {
		return true, true
	}
	if v == "false" || v == "0" || v == "no" {
		return false, true
	}
	return false, false
}

func parseOptionalTime(raw string) (time.Time, bool) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return time.Time{}, false
	}
	if unixSec, err := strconv.ParseInt(v, 10, 64); err == nil {
		return time.Unix(unixSec, 0).UTC(), true
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04"}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, v); err == nil {
			return ts.UTC(), true
		}
	}
	return time.Time{}, false
}

func parseEntries(vals []string) []types.AnalyticsEntry {
	entries := make([]types.AnalyticsEntry, 0, len(vals))
	for _, raw := range vals {
		var e types.AnalyticsEntry
		if err := json.Unmarshal([]byte(raw), &e); err == nil {
			entries = append(entries, e)
		}
	}
	return entries
}

func (s *Server) readEntries(ctx context.Context, limit, offset int64) ([]types.AnalyticsEntry, error) {
	if limit <= 0 {
		return []types.AnalyticsEntry{}, nil
	}
	if s.store != nil {
		readCtx, readCancel := withRedisTimeout(ctx)
		defer readCancel()
		return s.store.ListEntries(readCtx, limit, offset)
	}
	vals, err := s.readWindow(ctx, limit, offset)
	if err != nil {
		return nil, err
	}
	return parseEntries(vals), nil
}

func (s *Server) readWindow(ctx context.Context, limit, offset int64) ([]string, error) {
	if limit <= 0 {
		return []string{}, nil
	}
	lenCtx, lenCancel := withRedisTimeout(ctx)
	length, err := s.rdb.LLen(lenCtx, s.key).Result()
	lenCancel()
	if err != nil {
		return nil, err
	}
	if length == 0 || offset >= length {
		return []string{}, nil
	}
	end := length - 1 - offset
	start := end - limit + 1
	if start < 0 {
		start = 0
	}
	rangeCtx, rangeCancel := withRedisTimeout(ctx)
	vals, err := s.rdb.LRange(rangeCtx, s.key, start, end).Result()
	rangeCancel()
	return vals, err
}

type summary struct {
	Count                int     `json:"count"`
	AvgTotalLatencyMs    float64 `json:"avg_total_latency_ms"`
	AvgUpstreamLatencyMs float64 `json:"avg_upstream_latency_ms"`
	AvgRateLimiterMs     float64 `json:"avg_rate_limiter_latency_ms"`
	P95RateLimiterMs     float64 `json:"p95_rate_limiter_latency_ms"`
	CacheHitRatePct      float64 `json:"cache_hit_rate_pct"`
	SuccessRatePct       float64 `json:"success_rate_pct"`
	AvgRequestSizeBytes  float64 `json:"avg_request_size_bytes"`
	AvgResponseSizeBytes float64 `json:"avg_response_size_bytes"`
	ActiveTiersCount     int     `json:"active_tiers_count"`
	RateLimitedCount     int     `json:"rate_limited_count"`
	UpstreamErrorCount   int     `json:"upstream_error_count"`
}

func computeSummary(entries []types.AnalyticsEntry) summary {
	acc := &statsAccumulator{}
	for _, e := range entries {
		acc.add(e)
	}
	return acc.toSummary()
}

func microsecondsToMilliseconds(us float64) float64 {
	return us / 1000.0
}

func meanMicroseconds(data []int64) float64 {
	if len(data) == 0 {
		return 0
	}
	var sum int64
	for _, v := range data {
		sum += v
	}
	return float64(sum) / float64(len(data))
}

func percentileMicroseconds(data []int64, p float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sorted := append([]int64(nil), data...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	rank := p / 100 * float64(len(sorted)-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		return float64(sorted[lo])
	}
	frac := rank - float64(lo)
	return float64(sorted[lo])*(1-frac) + float64(sorted[hi])*frac
}

func parseIntWithBounds(raw string, def, min, max int64) int64 {
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return def
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
