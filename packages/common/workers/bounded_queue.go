package workers

import (
	"context"
	"errors"
	"sync"
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
	if timeout <= 0 {
		select {
		case q.tasks <- task:
			return nil
		default:
			return ErrQueueFull
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case q.tasks <- task:
		return nil
	case <-timer.C:
		return ErrQueueFull
	}
}

func (q *BoundedQueue) worker(ctx context.Context) {
	for {
		select {
		case task := <-q.tasks:
			q.executeWithRetry(ctx, task)
		case <-ctx.Done():
			return
		}
	}
}

func (q *BoundedQueue) executeWithRetry(ctx context.Context, task Task) {
	policy := task.RetryPolicy()
	if policy == nil || policy.MaxRetries <= 0 {
		_ = task.Execute(ctx)
		return
	}

	backoff := policy.Backoff
	if backoff <= 0 {
		backoff = 10 * time.Millisecond
	}

	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		if err := task.Execute(ctx); err == nil {
			return
		}
		if attempt == policy.MaxRetries {
			return
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
	}
}
