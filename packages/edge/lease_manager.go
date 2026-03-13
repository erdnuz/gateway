package edge

// LeaseManager is the single non-blocking background worker that consolidates
// all lease renewal I/O for a RateManager instance.
//
// Architecture (from copilot-instructions.md §1):
//   - A single goroutine drains a bounded renewal channel.
//   - Callers enqueue a renewalRequest without blocking (dropped if the channel
//     is full, which is safe — the next volume/temporal trigger will retry).
//   - WAITING keys use jitter before re-queuing to avoid thundering herds.

import (
	"context"
	"math/rand/v2"
	"time"
)

const leaseManagerQueueSize = 256

type renewalRequest struct {
	prefix    string
	serviceID string
	apiKey    string
	counter   *leaseCounter
	minimum   int64
}

// LeaseManager owns the background renewal goroutine.
type LeaseManager struct {
	rm   *RateManager
	ch   chan renewalRequest
	done chan struct{}
}

// NewLeaseManager creates and starts the background worker.
func NewLeaseManager(rm *RateManager) *LeaseManager {
	lm := &LeaseManager{
		rm:   rm,
		ch:   make(chan renewalRequest, leaseManagerQueueSize),
		done: make(chan struct{}),
	}
	return lm
}

// Start launches the single background goroutine. It runs until ctx is cancelled.
func (lm *LeaseManager) Start(ctx context.Context) {
	go lm.run(ctx)
}

// Enqueue adds a renewal request to the work channel.
// The call is non-blocking: if the channel is full the request is silently
// dropped — the next volume or temporal trigger will retry naturally.
func (lm *LeaseManager) Enqueue(prefix, serviceID, apiKey string, counter *leaseCounter, minimum int64) bool {
	select {
	case lm.ch <- renewalRequest{prefix: prefix, serviceID: serviceID, apiKey: apiKey, counter: counter, minimum: minimum}:
		return true
	default:
		// Channel full — drop and rely on next trigger.
		return false
	}
}

func (lm *LeaseManager) run(ctx context.Context) {
	defer close(lm.done)
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-lm.ch:
			if !ok {
				return
			}
			lm.handle(ctx, req)
		}
	}
}

func (lm *LeaseManager) handle(ctx context.Context, req renewalRequest) {
	defer req.counter.renewalQueued.Store(0)

	// If WAITING, apply jitter before attempting renewal.
	if leaseState(req.counter.state.Load()) == leaseStateWaiting {
		retryMs := req.counter.retryAfterMs.Load()
		if retryMs <= 0 {
			retryMs = 500
		}
		// Jitter: uniform random in [0.5×retryMs, 1.5×retryMs].
		jitter := time.Duration(retryMs/2+rand.Int64N(retryMs)) * time.Millisecond
		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			return
		}
		// Transition WAITING → IDLE so ensureLease can proceed.
		req.counter.state.CompareAndSwap(int32(leaseStateWaiting), int32(leaseStateIdle))
	}

	// Acquire RENEWING slot before calling ensureLease to deduplicate work.
	if !req.counter.state.CompareAndSwap(int32(leaseStateIdle), int32(leaseStateRenewing)) {
		return // another goroutine already in-flight
	}
	defer req.counter.state.CompareAndSwap(int32(leaseStateRenewing), int32(leaseStateIdle))
	_ = lm.rm.ensureLease(ctx, req.prefix, req.serviceID, req.apiKey, req.counter, req.minimum)
}
