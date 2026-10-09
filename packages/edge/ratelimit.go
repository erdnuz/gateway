package edge

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"gateway/packages/common/httpx"
	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"
	"gateway/packages/keys"
	"gateway/packages/limiter"
)

// rateLimit charges the request to the key's quota. While the hub is
// unreachable a service may fall back to a per-edge safety limit; otherwise
// the request is refused rather than admitted unmetered.
func (s *Server) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := stateOf(r)
		req := limiter.Request{
			Scope:   st.scope,
			Service: st.service,
			Key:     st.key,
			Limit:   int64(st.tier.Quota),
			Cost:    methodCost(r.Method, st.tier),
		}
		if p := st.prefix; p != nil {
			// Mirror the hub: a missing quota period means 1s windows.
			req.Window = max(p.QuotaPeriod, time.Second)
			req.LeaseSize = p.Leasing.Effective(s.Config.EdgePolicy(), p.QuotaPeriod).LeaseQuantum
		}
		d, err := s.Limiter.Allow(r.Context(), req)
		decide := func(result string) {
			telemetry.RateLimitDecisions.WithLabelValues(st.label(), st.tier.TierID, result).Inc()
		}
		if errors.Is(err, limiter.ErrRejected) {
			decide("rejected")
			httpx.WriteProblem(w, r, http.StatusForbidden, "key_not_permitted", "the hub does not permit this key on this service")
			return
		}
		if err != nil {
			if !errors.Is(err, limiter.ErrUnavailable) || rateErrLog.allow() {
				httpx.Logger(r.Context()).Warn("rate limit check failed", "scope", st.scope, "service", st.service, "key", keys.Short(st.key), "err", err)
			}
			safety := st.svc.SafetyTier
			if safety == nil || !st.svc.Failure.EffectiveHubPolicy().AllowOnRateServiceError {
				decide("unavailable")
				httpx.WriteProblem(w, r, http.StatusServiceUnavailable, "rate_limit_unavailable", "the rate limit service is unreachable; retry shortly")
				return
			}
			req.Limit, req.Cost = int64(safety.Quota), methodCost(r.Method, safety)
			d, _ = s.safety.Allow(r.Context(), req)
			decide(outcome("safety_", d.Allowed))
		} else {
			decide(outcome("", d.Allowed))
		}
		writeRateLimitHeaders(w, d)
		if !d.Allowed {
			httpx.WriteProblem(w, r, http.StatusTooManyRequests, "rate_limited", "quota exhausted for this window; see Retry-After")
			return
		}
		if st.entry != nil && d.Limit > 0 {
			used := d.Limit - d.Remaining
			st.entry.LimitUsed = uint64(used)
			st.entry.LimitUsedOfTotal = float64(used) / float64(d.Limit)
		}
		next.ServeHTTP(w, r)
	})
}

func outcome(prefix string, allowed bool) string {
	if allowed {
		return prefix + "allowed"
	}
	return prefix + "denied"
}

func methodCost(method string, t *types.TierConfig) int64 {
	switch method {
	case http.MethodGet:
		return int64(t.GetCost)
	case http.MethodPost:
		return int64(t.PostCost)
	case http.MethodPut:
		return int64(t.PutCost)
	case http.MethodDelete:
		return int64(t.DeleteCost)
	default:
		return int64(t.OtherCost)
	}
}

// writeRateLimitHeaders emits the IETF RateLimit header fields
// (draft-ietf-httpapi-ratelimit-headers) plus Retry-After on denial.
func writeRateLimitHeaders(w http.ResponseWriter, d limiter.Decision) {
	h := w.Header()
	h.Set("RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
	h.Set("RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
	reset := int64(1)
	if !d.ResetAt.IsZero() {
		reset = max(0, int64(math.Ceil(time.Until(d.ResetAt).Seconds())))
		h.Set("RateLimit-Reset", strconv.FormatInt(reset, 10))
	}
	if !d.Allowed {
		h.Set("Retry-After", strconv.FormatInt(max(reset, 1), 10))
	}
}

// rateErrLog throttles lease-unavailable logging during hub outages.
var rateErrLog logThrottle

type logThrottle struct{ next atomic.Int64 }

func (l *logThrottle) allow() bool {
	now := time.Now().UnixNano()
	next := l.next.Load()
	return now >= next && l.next.CompareAndSwap(next, now+int64(time.Second))
}
