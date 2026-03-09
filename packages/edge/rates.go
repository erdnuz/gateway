package edge

import (
	"context"
	"log"
	"net/http"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/common/workers"

	"github.com/redis/go-redis/v9"
)

type RateManager struct {
	store            CounterStore
	maxDelta         int64
	hardThresholdPct float64
	queueWorkers     int
	submitTimeout    time.Duration
	retryMax         int
	retryBackoff     time.Duration
	sync             RateSync
	queue            *workers.BoundedQueue
}

type RateManagerOptions struct {
	QueueCapacity    int
	QueueWorkers     int
	SubmitTimeout    time.Duration
	RetryMax         int
	RetryBackoff     time.Duration
	HardThresholdPct float64
	HubAuthToken     string
	HubHTTPClient    *http.Client
	KafkaBrokers     []string
	KafkaTopic       string
	KafkaSOTTopic    string
	KafkaGroupID     string
}

func DefaultRateManagerOptions() RateManagerOptions {
	return RateManagerOptions{
		QueueCapacity:    512,
		QueueWorkers:     1,
		SubmitTimeout:    25 * time.Millisecond,
		RetryMax:         1,
		RetryBackoff:     10 * time.Millisecond,
		HardThresholdPct: 0.9,
	}
}

var _ types.Limiter = (*RateManager)(nil)

// helper message types are defined in common/types/messaging.go

func NewRateManager(hubAddr string, rdb *redis.Client, maxDelta int64, channels ...string) *RateManager {
	return NewRateManagerWithOptions(hubAddr, rdb, maxDelta, DefaultRateManagerOptions(), channels...)
}

func NewRateManagerWithOptions(hubAddr string, rdb *redis.Client, maxDelta int64, options RateManagerOptions, channels ...string) *RateManager {
	if options.QueueCapacity <= 0 {
		options.QueueCapacity = 512
	}
	if options.QueueWorkers <= 0 {
		options.QueueWorkers = 1
	}
	if options.SubmitTimeout <= 0 {
		options.SubmitTimeout = 25 * time.Millisecond
	}
	if options.RetryMax < 0 {
		options.RetryMax = 1
	}
	if options.RetryBackoff <= 0 {
		options.RetryBackoff = 10 * time.Millisecond
	}
	if options.HardThresholdPct <= 0 || options.HardThresholdPct > 1 {
		options.HardThresholdPct = 0.9
	}
	var sync RateSync = NewKafkaRateSync(rdb, options.KafkaBrokers, options.KafkaTopic, options.KafkaSOTTopic, options.KafkaGroupID)

	queue := workers.NewBoundedQueue(options.QueueCapacity)
	return &RateManager{
		store:            NewRedisCounterAdapter(rdb),
		maxDelta:         maxDelta,
		hardThresholdPct: options.HardThresholdPct,
		queueWorkers:     options.QueueWorkers,
		submitTimeout:    options.SubmitTimeout,
		retryMax:         options.RetryMax,
		retryBackoff:     options.RetryBackoff,
		sync:             sync,
		queue:            queue,
	}
}

func (rm *RateManager) StartBackgroundWorkers(ctx context.Context) {
	if rm.queue != nil {
		rm.queue.Start(ctx, rm.queueWorkers)
	}
}

func (rm *RateManager) Increment(ctx context.Context, prefix, apiKey string, limit, amount int64) (int64, error) {
	localKey := rm.getLocalKey(prefix, apiKey)
	// increment delta locally
	delta, err := rm.store.IncrBy(ctx, localKey, amount)
	if err != nil {
		return 0, err
	}

	// track unsent local increments independently from the cumulative local key.
	if amount > 0 {
		if _, err := rm.store.IncrBy(ctx, rm.getPendingKey(prefix, apiKey), amount); err != nil {
			return 0, err
		}
	}

	// read last global total (may not exist yet)
	syncKey := rm.getSyncKey(prefix, apiKey)
	lastGlobal := int64(0)
	if val, err := rm.store.Get(ctx, syncKey); err == nil {
		lastGlobal = val
	}

	projected := lastGlobal + delta

	// send updates asynchronously when threshold hit or close to limit
	if projected > int64(float64(limit)*rm.hardThresholdPct) {
		// hard threshold, publish immediately
		rm.sync.FlushPendingDelta(ctx, prefix, apiKey)
	} else if delta >= rm.maxDelta {
		// soft threshold, schedule on bounded worker queue.
		task := &flushDeltaTask{sync: rm.sync, prefix: prefix, apiKey: apiKey, retryMax: rm.retryMax, retryBackoff: rm.retryBackoff}
		if err := rm.queue.Submit(task, rm.submitTimeout); err != nil {
			log.Printf("rate flush queue submit failed prefix=%s api_key=%s err=%v", prefix, apiKey, err)
		}
	}

	return projected, nil
}

// StartSOTSubscriber listens for hub "state of truth" messages and updates
// the local sync key accordingly.  It returns immediately and spins up a
// goroutine; caller should provide a context for cancellation.
func (rm *RateManager) StartSOTSubscriber(ctx context.Context) error {
	return rm.sync.StartSOTSubscriber(ctx)
}

func rateLocalKey(p, k string) string   { return "rate-local:" + p + ":" + k }
func rateSyncKey(p, k string) string    { return "rate-sync:" + p + ":" + k }
func ratePendingKey(p, k string) string { return "rate-pending:" + p + ":" + k }
func rateSeqKey(p, k string) string     { return "rate-seq:" + p + ":" + k }

func (rm *RateManager) getLocalKey(p, k string) string { return rateLocalKey(p, k) }
func (rm *RateManager) getSyncKey(p, k string) string  { return rateSyncKey(p, k) }
func (rm *RateManager) getPendingKey(p, k string) string {
	return ratePendingKey(p, k)
}
func (rm *RateManager) getSeqKey(p, k string) string { return rateSeqKey(p, k) }

type flushDeltaTask struct {
	sync           RateSync
	prefix, apiKey string
	retryMax       int
	retryBackoff   time.Duration
}

func (t *flushDeltaTask) Execute(ctx context.Context) error {
	t.sync.FlushPendingDelta(ctx, t.prefix, t.apiKey)
	return nil
}

func (t *flushDeltaTask) RetryPolicy() *workers.RetryPolicy {
	return &workers.RetryPolicy{MaxRetries: t.retryMax, Backoff: t.retryBackoff}
}
