package edge

import (
	"errors"
	"log/slog"
	"net/url"
	"slices"
	"sync/atomic"
	"time"

	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"
)

// errCircuitOpen means every replica of a service is ejected.
var errCircuitOpen = errors.New("every upstream replica is failing")

// pool spreads a service's requests over its upstream replicas and ejects a
// replica after consecutive failures (passive health checking). Each edge
// judges replicas on its own traffic, so no health-check fan-out is needed
// as edges or replicas are added.
type pool struct {
	service   string
	upstreams []string // the config this pool was built from
	balance   string
	breaker   *types.CircuitBreakerConfig
	targets   []*target
	next      atomic.Uint64
}

type target struct {
	url       *url.URL
	inflight  atomic.Int64
	failures  atomic.Int64
	openUntil atomic.Int64 // unix nanoseconds; ejected until then
}

// poolFor returns the service's pool, rebuilding it when its config changed.
func (s *Server) poolFor(st *request) (*pool, error) {
	label, svc := st.label(), st.svc
	if v, ok := s.pools.Load(label); ok {
		p := v.(*pool)
		if slices.Equal(p.upstreams, svc.Targets()) && p.balance == svc.LoadBalancing && sameBreaker(p.breaker, svc.CircuitBreaker) {
			return p, nil
		}
	}
	p := &pool{service: label, upstreams: svc.Targets(), balance: svc.LoadBalancing, breaker: svc.CircuitBreaker}
	for _, raw := range p.upstreams {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			return nil, errors.New("invalid upstream URL")
		}
		p.targets = append(p.targets, &target{url: u})
	}
	s.pools.Store(label, p)
	return p, nil
}

func sameBreaker(a, b *types.CircuitBreakerConfig) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

// pick chooses a healthy replica, preferring one other than avoid (a retry
// should land elsewhere). It returns nil when every replica is ejected.
func (p *pool) pick(avoid *target) *target {
	now := time.Now().UnixNano()
	n := len(p.targets)
	start := int(p.next.Add(1) % uint64(n))
	var best *target
	for i := 0; i < n; i++ {
		t := p.targets[(start+i)%n]
		if t.openUntil.Load() > now || (t == avoid && n > 1) {
			continue
		}
		if p.balance != "least_requests" {
			return t
		}
		if best == nil || t.inflight.Load() < best.inflight.Load() {
			best = t
		}
	}
	if best == nil && avoid != nil && avoid.openUntil.Load() <= now {
		return avoid
	}
	return best
}

// report records an attempt's outcome for the circuit breaker.
func (p *pool) report(t *target, ok bool) {
	if p.breaker == nil {
		return
	}
	if ok {
		t.failures.Store(0)
		return
	}
	if t.failures.Add(1) >= int64(p.breaker.Failures) {
		until := time.Now().Add(p.breaker.Cooldown)
		if t.openUntil.Swap(until.UnixNano()) < time.Now().UnixNano() {
			telemetry.UpstreamEjections.WithLabelValues(p.service).Inc()
			slog.Warn("upstream ejected", "service", p.service, "upstream", t.url.Host, "for", p.breaker.Cooldown)
		}
	}
}
