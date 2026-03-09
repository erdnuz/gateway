package workers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type testTask struct {
	executed *atomic.Int32
	delay    time.Duration
	retry    *RetryPolicy
}

func (t *testTask) Execute(ctx context.Context) error {
	if t.delay > 0 {
		select {
		case <-time.After(t.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t.executed.Add(1)
	return nil
}

func (t *testTask) RetryPolicy() *RetryPolicy { return t.retry }

func TestBoundedQueueBackpressure(t *testing.T) {
	q := NewBoundedQueue(1)

	counter := &atomic.Int32{}
	first := &testTask{executed: counter, delay: 100 * time.Millisecond}
	second := &testTask{executed: counter}

	if err := q.Submit(first, 0); err != nil {
		t.Fatalf("expected first submit to succeed, got %v", err)
	}
	if err := q.Submit(second, 0); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull for second submit, got %v", err)
	}
}

func TestBoundedQueueExecutesTask(t *testing.T) {
	q := NewBoundedQueue(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx, 1)

	counter := &atomic.Int32{}
	if err := q.Submit(&testTask{executed: counter}, 50*time.Millisecond); err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	deadline := time.After(200 * time.Millisecond)
	for counter.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("expected task execution")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}
