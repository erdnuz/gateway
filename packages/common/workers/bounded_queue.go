package workers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrQueueFull = errors.New("bounded queue is full")

type RetryPolicy struct {
	MaxRetries int
	Backoff    time.Duration
}

type Task interface {
	Execute(ctx context.Context) error
	RetryPolicy() *RetryPolicy
}

type BoundedQueue struct {
	tasks chan Task
	once  sync.Once

	submitted uint64
	enqueued  uint64
	dropped   uint64
	dequeued  uint64
	succeeded uint64
	failed    uint64
	retries   uint64
}

type QueueSnapshot struct {
	Depth     int    `json:"depth"`
	Capacity  int    `json:"capacity"`
	Submitted uint64 `json:"submitted"`
	Enqueued  uint64 `json:"enqueued"`
	Dropped   uint64 `json:"dropped"`
	Dequeued  uint64 `json:"dequeued"`
	Succeeded uint64 `json:"succeeded"`
	Failed    uint64 `json:"failed"`
	Retries   uint64 `json:"retries"`
}

func NewBoundedQueue(capacity int) *BoundedQueue {
	if capacity <= 0 {
		capacity = 1
	}
	return &BoundedQueue{tasks: make(chan Task, capacity)}
}

func (q *BoundedQueue) Start(ctx context.Context, numWorkers int) {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	q.once.Do(func() {
		for i := 0; i < numWorkers; i++ {
			go q.worker(ctx)
		}
	})
}

func (q *BoundedQueue) Submit(task Task, timeout time.Duration) error {
	if task == nil {
		return nil
	}
	atomic.AddUint64(&q.submitted, 1)
	if timeout <= 0 {
		select {
		case q.tasks <- task:
			atomic.AddUint64(&q.enqueued, 1)
			return nil
		default:
			atomic.AddUint64(&q.dropped, 1)
			return ErrQueueFull
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case q.tasks <- task:
		atomic.AddUint64(&q.enqueued, 1)
		return nil
	case <-timer.C:
		atomic.AddUint64(&q.dropped, 1)
		return ErrQueueFull
	}
}

func (q *BoundedQueue) worker(ctx context.Context) {
	for {
		select {
		case task := <-q.tasks:
			atomic.AddUint64(&q.dequeued, 1)
			q.executeWithRetry(ctx, task)
		case <-ctx.Done():
			return
		}
	}
}

func (q *BoundedQueue) executeWithRetry(ctx context.Context, task Task) {
	policy := task.RetryPolicy()
	if policy == nil || policy.MaxRetries <= 0 {
		if err := task.Execute(ctx); err != nil {
			atomic.AddUint64(&q.failed, 1)
			return
		}
		atomic.AddUint64(&q.succeeded, 1)
		return
	}

	backoff := policy.Backoff
	if backoff <= 0 {
		backoff = 10 * time.Millisecond
	}

	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		if err := task.Execute(ctx); err == nil {
			if attempt > 0 {
				atomic.AddUint64(&q.retries, uint64(attempt))
			}
			atomic.AddUint64(&q.succeeded, 1)
			return
		}
		if attempt == policy.MaxRetries {
			if attempt > 0 {
				atomic.AddUint64(&q.retries, uint64(attempt))
			}
			atomic.AddUint64(&q.failed, 1)
			return
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			if attempt > 0 {
				atomic.AddUint64(&q.retries, uint64(attempt))
			}
			atomic.AddUint64(&q.failed, 1)
			return
		}
	}
}

func (q *BoundedQueue) Snapshot() QueueSnapshot {
	return QueueSnapshot{
		Depth:     len(q.tasks),
		Capacity:  cap(q.tasks),
		Submitted: atomic.LoadUint64(&q.submitted),
		Enqueued:  atomic.LoadUint64(&q.enqueued),
		Dropped:   atomic.LoadUint64(&q.dropped),
		Dequeued:  atomic.LoadUint64(&q.dequeued),
		Succeeded: atomic.LoadUint64(&q.succeeded),
		Failed:    atomic.LoadUint64(&q.failed),
		Retries:   atomic.LoadUint64(&q.retries),
	}
}
