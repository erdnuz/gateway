package ready

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	defaultInitialBackoff = 500 * time.Millisecond
	defaultMaxBackoff     = 5 * time.Second
)

// Check describes a single readiness dependency probe.
type Check struct {
	Name  string
	URL   string
	Probe func(context.Context) error
}

// ReadyWatcher continuously evaluates readiness dependencies in the background.
// It never exits the process; callers should manage process lifecycle themselves.
type ReadyWatcher struct {
	component string
	initial   time.Duration
	max       time.Duration
}

func NewReadyWatcher(component string, initialBackoff, maxBackoff time.Duration) *ReadyWatcher {
	if initialBackoff <= 0 {
		initialBackoff = defaultInitialBackoff
	}
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxBackoff
	}
	if maxBackoff < initialBackoff {
		maxBackoff = initialBackoff
	}
	return &ReadyWatcher{component: component, initial: initialBackoff, max: maxBackoff}
}

// WatchAll probes all checks repeatedly with exponential backoff while any
// dependency is unavailable. When all checks succeed, backoff resets and the
// watcher continues monitoring at the initial interval.
func (w *ReadyWatcher) WatchAll(ctx context.Context, checks []Check, onReady func(bool)) {
	if len(checks) == 0 {
		if onReady != nil {
			onReady(true)
		}
		return
	}

	backoff := w.initial
	readyState := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		allReady := true
		for _, check := range checks {
			if check.Probe == nil {
				allReady = false
				w.logPending(check.Name, check.URL, fmt.Errorf("probe is not configured"), backoff)
				break
			}

			probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := check.Probe(probeCtx)
			cancel()
			if err != nil {
				allReady = false
				w.logPending(check.Name, check.URL, err, backoff)
				break
			}
		}

		if allReady {
			if !readyState {
				log.Printf("%s readiness: all dependencies are healthy", w.component)
			}
			readyState = true
			if onReady != nil {
				onReady(true)
			}
			backoff = w.initial
		} else {
			readyState = false
			if onReady != nil {
				onReady(false)
			}
			backoff *= 2
			if backoff > w.max {
				backoff = w.max
			}
		}

		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (w *ReadyWatcher) logPending(name, url string, err error, next time.Duration) {
	if name == "" {
		name = "unknown"
	}
	if url == "" {
		url = "n/a"
	}
	log.Printf("%s readiness pending dependency=%s url=%s err=%v next_retry=%s", w.component, name, url, err, next)
}
