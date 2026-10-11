// Package rls serves Envoy's rate-limit service API
// (envoy.service.ratelimit.v3.RateLimitService) from Gate's leased global
// quota. Point Envoy's ratelimit filter (or Istio, Contour, Emissary, Gloo:
// anything that speaks the API) at gate limiter and its descriptors are
// limited exactly, across every Envoy and every gate limiter replica.
package rls

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"gateway/packages/common/telemetry"
	"gateway/packages/decide"
	"gateway/packages/limiter"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	rlsv3 "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Server implements RateLimitService.
type Server struct {
	rlsv3.UnimplementedRateLimitServiceServer
	Decider *decide.Decider
}

// ShouldRateLimit decides every descriptor in the request. All must fit;
// when one does not, the others are refunded (Envoy's reference service
// charges them anyway).
func (s *Server) ShouldRateLimit(ctx context.Context, req *rlsv3.RateLimitRequest) (*rlsv3.RateLimitResponse, error) {
	descs := make([]decide.Descriptor, len(req.GetDescriptors()))
	for i, d := range req.GetDescriptors() {
		cost := int64(req.GetHitsAddend())
		if h := d.GetHitsAddend(); h != nil {
			cost = int64(h.GetValue())
		}
		descs[i].Cost = max(cost, 1)
		for _, e := range d.GetEntries() {
			descs[i].Entries = append(descs[i].Entries, decide.Entry{Key: e.GetKey(), Value: e.GetValue()})
		}
	}
	out, err := s.Decider.Check(ctx, req.GetDomain(), descs, 0, false)
	if errors.Is(err, limiter.ErrUnavailable) {
		// Envoy applies its own failure_mode_deny setting.
		telemetry.RateLimitDecisions.WithLabelValues("rls/"+req.GetDomain(), "", "unavailable").Inc()
		return nil, status.Error(codes.Unavailable, "rate limit ledger unavailable")
	}
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp := &rlsv3.RateLimitResponse{OverallCode: rlsv3.RateLimitResponse_OK}
	if !out.Allowed {
		resp.OverallCode = rlsv3.RateLimitResponse_OVER_LIMIT
	}
	telemetry.RateLimitDecisions.WithLabelValues("rls/"+req.GetDomain(), "", outcome(out.Allowed)).Inc()
	var binding *decide.Status
	for i := range out.Statuses {
		st := &out.Statuses[i]
		ds := &rlsv3.RateLimitResponse_DescriptorStatus{Code: rlsv3.RateLimitResponse_OK}
		if !st.Allowed {
			ds.Code = rlsv3.RateLimitResponse_OVER_LIMIT
		}
		if st.Matched {
			ds.CurrentLimit = currentLimit(st)
			ds.LimitRemaining = uint32(min(max(st.Remaining, 0), math.MaxUint32))
			ds.DurationUntilReset = durationpb.New(st.ResetIn)
			if st.ShadowDenied {
				telemetry.ShadowDenials.WithLabelValues("rls/"+req.GetDomain(), st.LimitID).Inc()
			}
			// Report the denying descriptor, else the one closest to its limit.
			switch {
			case st.Shadow:
			case binding == nil, !st.Allowed && binding.Allowed:
				binding = st
			case st.Allowed == binding.Allowed && share(st) < share(binding):
				binding = st
			}
		}
		resp.Statuses = append(resp.Statuses, ds)
	}
	if binding != nil {
		resp.ResponseHeadersToAdd = headers(binding, out)
	}
	return resp, nil
}

func outcome(allowed bool) string {
	if allowed {
		return "allowed"
	}
	return "denied"
}

func share(st *decide.Status) float64 {
	if st.Limit <= 0 {
		return 1
	}
	return float64(st.Remaining) / float64(st.Limit)
}

// headers are the IETF RateLimit fields for the binding descriptor, for
// Envoy to add to the response.
func headers(st *decide.Status, out *decide.Outcome) []*corev3.HeaderValue {
	h := []*corev3.HeaderValue{
		{Key: "RateLimit-Limit", Value: strconv.FormatInt(st.Limit, 10)},
		{Key: "RateLimit-Remaining", Value: strconv.FormatInt(max(st.Remaining, 0), 10)},
		{Key: "RateLimit-Reset", Value: strconv.FormatInt(int64(math.Ceil(st.ResetIn.Seconds())), 10)},
	}
	if !out.Allowed {
		h = append(h, &corev3.HeaderValue{Key: "Retry-After", Value: strconv.FormatInt(max(int64(math.Ceil(out.RetryAfter.Seconds())), 1), 10)})
	}
	return h
}

// currentLimit expresses a limit in Envoy's units: per second for a token
// bucket, else the window's unit when it is a whole second, minute, hour or
// day (and the closest per-unit rate otherwise).
func currentLimit(st *decide.Status) *rlsv3.RateLimitResponse_RateLimit {
	rl := &rlsv3.RateLimitResponse_RateLimit{Name: st.LimitID}
	if st.Spec.Algorithm == "token_bucket" {
		rl.RequestsPerUnit, rl.Unit = uint32(math.Ceil(st.Spec.Rate)), rlsv3.RateLimitResponse_RateLimit_SECOND
		return rl
	}
	w := max(st.Spec.Window, time.Second)
	for _, u := range []struct {
		d    time.Duration
		unit rlsv3.RateLimitResponse_RateLimit_Unit
	}{{24 * time.Hour, rlsv3.RateLimitResponse_RateLimit_DAY}, {time.Hour, rlsv3.RateLimitResponse_RateLimit_HOUR}, {time.Minute, rlsv3.RateLimitResponse_RateLimit_MINUTE}, {time.Second, rlsv3.RateLimitResponse_RateLimit_SECOND}} {
		if w >= u.d {
			rl.Unit = u.unit
			rl.RequestsPerUnit = uint32(math.Ceil(float64(st.Spec.Quota) * float64(u.d) / float64(w)))
			return rl
		}
	}
	return rl
}
