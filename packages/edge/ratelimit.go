package edge

import (
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gateway/packages/common/httpx"
	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"
	"gateway/packages/keys"
	"gateway/packages/limiter"
)

// rateLimit charges the request to every limit on the service: the caller's
// tier, the owner's org budget (applied by the hub), and any additional
// limits per key, client IP or service. While the hub is unreachable a
// service may fall back to per-edge safety limits; otherwise the request is
// refused rather than admitted unmetered.
func (s *Server) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := stateOf(r)
		if n := st.svc.MaxConcurrent; n > 0 {
			release, ok := s.inflight.Acquire(st.label()+"\x00"+st.key, n)
			if !ok {
				telemetry.RateLimitDecisions.WithLabelValues(st.label(), st.tier.TierID, "concurrency_limited").Inc()
				w.Header().Set("Retry-After", "1")
				httpx.WriteProblem(w, r, http.StatusTooManyRequests, "concurrency_limited", "too many requests in flight for this caller")
				return
			}
			defer release()
		}

		checks := s.checks(r, st)
		lim := limiter.Fallback{Primary: s.Limiter, Safety: s.safety, Rule: safetyRule(st, r.Method)}
		res, err := limiter.Reserve(r.Context(), lim, checks)
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
			decide("unavailable")
			httpx.WriteProblem(w, r, http.StatusServiceUnavailable, "rate_limit_unavailable", "the rate limit service is unreachable; retry shortly")
			return
		}
		prefix := ""
		if res.Fallback {
			prefix = "safety_"
		}
		decide(outcome(prefix, res.Allowed))
		for _, i := range res.ShadowDenied {
			telemetry.ShadowDenials.WithLabelValues(st.label(), limitLabel(checks[i].LimitID)).Inc()
		}
		writeRateLimitHeaders(w, res.Binding, res.Allowed)
		if len(res.ShadowDenied) > 0 {
			ids := make([]string, len(res.ShadowDenied))
			for j, i := range res.ShadowDenied {
				ids[j] = limitLabel(checks[i].LimitID)
			}
			w.Header().Set("Gate-Shadow-Denied", strings.Join(ids, ", "))
		}
		if !res.Allowed {
			detail := "quota exhausted for this window; see Retry-After"
			if id := checks[res.BindingIndex].LimitID; id != "" {
				detail = "limit " + id + " exhausted; see Retry-After"
				telemetry.LimitDenials.WithLabelValues(st.label(), id).Inc()
			}
			httpx.WriteProblem(w, r, http.StatusTooManyRequests, "rate_limited", detail)
			return
		}
		// The hub window the caller's quota was charged to: lets clients and
		// load tests attribute requests to windows exactly.
		if win := res.Results[0].Window; win > 0 {
			w.Header().Set("Gate-Window", strconv.FormatInt(win, 10))
		}
		if st.entry != nil && res.Binding.Limit > 0 {
			used := res.Binding.Limit - res.Binding.Remaining
			st.entry.LimitUsed = uint64(max(used, 0))
			st.entry.LimitUsedOfTotal = float64(used) / float64(res.Binding.Limit)
		}

		cost := st.svc.Cost
		if cost == nil {
			next.ServeHTTP(w, r)
			return
		}
		// Post-hoc cost: the upstream reports what the request actually cost
		// (in a header, or a trailer after a streamed body); the difference
		// from the estimate is refunded or charged locally.
		st.actual.Store(-1)
		next.ServeHTTP(w, r)
		switch actual := st.actual.Load(); {
		case actual >= 0:
			res.Settle(actual)
			telemetry.CostSettled.WithLabelValues(st.label(), "reported").Inc()
		case cost.OnMissing == "refund" || st.upstreamFailed:
			res.Settle(0)
			telemetry.CostSettled.WithLabelValues(st.label(), "refunded").Inc()
		default:
			telemetry.CostSettled.WithLabelValues(st.label(), "estimated").Inc()
		}
	})
}

// checks lists the limits a request must fit, the caller's tier first.
func (s *Server) checks(r *http.Request, st *request) []limiter.Request {
	svc := st.svc
	cost := methodCost(r.Method, st.tier)
	if c := svc.Cost; c != nil {
		cost = estimate(r, c, cost)
	}
	base := limiter.Request{
		Scope:   st.scope,
		Service: st.service,
		Key:     st.key,
		Tier:    st.tier.TierID,
		Org:     st.org,
		Limit:   int64(st.tier.Quota),
		Cost:    cost,
		Algo:    st.tier.Algorithm,
		Shadow:  svc.Shadow,
	}
	if c := svc.Cost; c != nil {
		base.MaxDebt = c.MaxDebt
	}
	if p := st.prefix; p != nil {
		// Mirror the hub: a missing quota period means 1s windows.
		base.Window = max(p.QuotaPeriod, time.Second)
		base.LeaseSize = p.Leasing.Effective(s.Config.EdgePolicy(), p.QuotaPeriod).LeaseQuantum
	}
	if len(svc.Limits) == 0 {
		return []limiter.Request{base}
	}
	checks := make([]limiter.Request, 0, 1+len(svc.Limits))
	checks = append(checks, base)
	for _, l := range svc.Limits {
		c := base
		c.LimitID, c.Tier, c.Org = l.ID, "", ""
		c.Limit, c.Algo, c.Shadow = l.Quota, l.Algorithm, l.Shadow
		if l.Window > 0 {
			c.Window = l.Window
		}
		switch l.Per {
		case types.PerClientIP:
			c.Key = keys.Hash("ip:" + st.clientIP)
		case types.PerService:
			c.Key = types.ServiceSubject
		}
		checks = append(checks, c)
	}
	return checks
}

// safetyRule maps each check to its per-edge safety limit while the hub is
// unreachable: the service's when_hub_unavailable tier for the caller's
// limit, and each additional limit enforced by this edge alone.
func safetyRule(st *request, method string) func(limiter.Request) (limiter.Request, bool) {
	return func(req limiter.Request) (limiter.Request, bool) {
		safety := st.svc.SafetyTier
		if safety == nil || !st.svc.Failure.EffectiveHubPolicy().AllowOnRateServiceError {
			return req, false
		}
		req.Algo = "" // the local limiter counts fixed windows
		if req.LimitID == "" {
			req.Limit, req.Cost = int64(safety.Quota), methodCost(method, safety)
		}
		if req.Window <= 0 {
			req.Window = time.Second
		}
		return req, true
	}
}

// estimate is the cost reserved up front for a post-hoc-costed request: the
// client's estimate header when it sends one, else the configured estimate.
func estimate(r *http.Request, c *types.CostPolicy, methodCost int64) int64 {
	if c.EstimateHeader != "" {
		if v, err := strconv.ParseInt(r.Header.Get(c.EstimateHeader), 10, 64); err == nil && v > 0 {
			return v
		}
	}
	if c.Estimate > 0 {
		return c.Estimate
	}
	return methodCost
}

// captureCost records the actual cost the upstream reported, from a header
// now or from a trailer once the body has been read.
func captureCost(resp *http.Response, st *request) {
	c := st.svc.Cost
	if c == nil || c.ActualHeader == "" {
		return
	}
	if v, ok := parseCost(resp.Header.Get(c.ActualHeader)); ok {
		st.actual.Store(v)
		return
	}
	if resp.Body != nil && resp.Body != http.NoBody {
		resp.Body = &trailerCost{ReadCloser: resp.Body, resp: resp, header: c.ActualHeader, st: st}
	}
}

func parseCost(v string) (int64, bool) {
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	return n, err == nil && n >= 0
}

// trailerCost reads the actual cost from a trailer when the body ends (an
// LLM stream reports its usage last).
type trailerCost struct {
	io.ReadCloser
	resp   *http.Response
	header string
	st     *request
}

func (t *trailerCost) Read(p []byte) (int, error) {
	n, err := t.ReadCloser.Read(p)
	if err == io.EOF {
		if v, ok := parseCost(t.resp.Trailer.Get(t.header)); ok {
			t.st.actual.Store(v)
		}
	}
	return n, err
}

func limitLabel(id string) string {
	if id == "" {
		return "tier"
	}
	return id
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
// (draft-ietf-httpapi-ratelimit-headers) for the binding limit, plus
// Retry-After on denial.
func writeRateLimitHeaders(w http.ResponseWriter, d limiter.Decision, allowed bool) {
	h := w.Header()
	h.Set("RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
	h.Set("RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
	reset := int64(1)
	if !d.ResetAt.IsZero() {
		reset = max(0, int64(math.Ceil(time.Until(d.ResetAt).Seconds())))
		h.Set("RateLimit-Reset", strconv.FormatInt(reset, 10))
	}
	if !allowed {
		retry := reset
		if at := d.RetryAt(); !at.IsZero() {
			retry = int64(math.Ceil(time.Until(at).Seconds()))
		}
		h.Set("Retry-After", strconv.FormatInt(max(retry, 1), 10))
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
