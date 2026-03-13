package workers

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type testTask struct {
	executed *atomic.Int32
	delay    time.Duration
	retry    *RetryPolicy
	failFor  int32
}

func (t *testTask) Execute(ctx context.Context) error {
	if t.delay > 0 {
		select {
		case <-time.After(t.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if t.failFor > 0 {
		remaining := atomic.AddInt32(&t.failFor, -1)
		if remaining >= 0 {
			return errors.New("forced execute failure")
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
	snap := q.Snapshot()
	if snap.Submitted != 2 || snap.Enqueued != 1 || snap.Dropped != 1 {
		t.Fatalf("unexpected queue snapshot: %+v", snap)
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

func TestBoundedQueueSnapshotRetryAndFailure(t *testing.T) {
	q := NewBoundedQueue(4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx, 1)

	counter := &atomic.Int32{}
	if err := q.Submit(&testTask{executed: counter, retry: &RetryPolicy{MaxRetries: 2, Backoff: time.Millisecond}, failFor: 1}, 20*time.Millisecond); err != nil {
		t.Fatalf("submit retry task failed: %v", err)
	}
	if err := q.Submit(&testTask{executed: counter, retry: &RetryPolicy{MaxRetries: 1, Backoff: time.Millisecond}, failFor: 5}, 20*time.Millisecond); err != nil {
		t.Fatalf("submit fail task failed: %v", err)
	}

	deadline := time.After(400 * time.Millisecond)
	for {
		snap := q.Snapshot()
		if snap.Succeeded == 1 && snap.Failed == 1 {
			if snap.Retries < 2 {
				t.Fatalf("expected retries to be tracked, snapshot=%+v", snap)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for queue results, snapshot=%+v", snap)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}
