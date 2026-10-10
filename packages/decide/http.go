package decide

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"gateway/packages/common/httpx"
	"gateway/packages/limiter"
)

// HTTP serves the decision API as JSON for callers without gRPC:
//
//	POST /v1/check   {"domain", "descriptors": [{"entries": [{"key","value"}], "cost"}], "max_wait_ms", "reserve"}
//	POST /v1/settle  {"reservation", "actual"}
//
// A decision is always 200 (allowed or not); 503 means the ledger is
// unreachable. Token, when set, is required as a bearer token.
func (d *Decider) HTTP(token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/check", d.httpCheck)
	mux.HandleFunc("POST /v1/settle", d.httpSettle)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" {
			got, ok := httpx.BearerToken(r)
			if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
				httpx.WriteProblem(w, r, http.StatusUnauthorized, "unauthorized", "send the limiter token as a bearer token")
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

type checkRequest struct {
	Domain      string       `json:"domain"`
	Descriptors []Descriptor `json:"descriptors"`
	MaxWaitMs   int64        `json:"max_wait_ms"`
	Reserve     bool         `json:"reserve"`
}

type checkStatus struct {
	Limit        string `json:"limit,omitempty"`
	Matched      bool   `json:"matched"`
	Allowed      bool   `json:"allowed"`
	ShadowDenied bool   `json:"shadow_denied,omitempty"`
	LimitValue   int64  `json:"limit_value,omitempty"`
	Remaining    int64  `json:"remaining"`
	ResetMs      int64  `json:"reset_ms,omitempty"`
}

type checkResponse struct {
	Allowed      bool          `json:"allowed"`
	RetryAfterMs int64         `json:"retry_after_ms,omitempty"`
	Reservation  string        `json:"reservation,omitempty"`
	Statuses     []checkStatus `json:"statuses"`
}

const maxWaitCap = 30 * time.Second

func (d *Decider) httpCheck(w http.ResponseWriter, r *http.Request) {
	var req checkRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil || req.Domain == "" || len(req.Descriptors) == 0 {
		httpx.WriteProblem(w, r, http.StatusBadRequest, "bad_request", "body must be JSON with a domain and at least one descriptor")
		return
	}
	wait := min(time.Duration(max(req.MaxWaitMs, 0))*time.Millisecond, maxWaitCap)
	out, err := d.Check(r.Context(), req.Domain, req.Descriptors, wait, req.Reserve)
	if errors.Is(err, limiter.ErrUnavailable) {
		httpx.WriteProblem(w, r, http.StatusServiceUnavailable, "ledger_unavailable", "the rate limit ledger is unreachable; retry shortly")
		return
	}
	if err != nil {
		httpx.WriteProblem(w, r, http.StatusBadRequest, "rejected", err.Error())
		return
	}
	resp := checkResponse{Allowed: out.Allowed, Reservation: out.Reservation, Statuses: make([]checkStatus, len(out.Statuses))}
	for i, st := range out.Statuses {
		resp.Statuses[i] = checkStatus{Limit: st.LimitID, Matched: st.Matched, Allowed: st.Allowed, ShadowDenied: st.ShadowDenied,
			LimitValue: st.Limit, Remaining: max(st.Remaining, 0), ResetMs: st.ResetIn.Milliseconds()}
	}
	if !out.Allowed {
		resp.RetryAfterMs = max(out.RetryAfter.Milliseconds(), 1)
		w.Header().Set("Retry-After", strconv.FormatInt(max(int64(math.Ceil(out.RetryAfter.Seconds())), 1), 10))
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (d *Decider) httpSettle(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reservation string `json:"reservation"`
		Actual      int64  `json:"actual"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&req); err != nil || req.Reservation == "" {
		httpx.WriteProblem(w, r, http.StatusBadRequest, "bad_request", "body must be JSON with a reservation and the actual cost")
		return
	}
	switch err := d.Settle(r.Context(), req.Reservation, req.Actual); {
	case errors.Is(err, ErrBadReservation):
		httpx.WriteProblem(w, r, http.StatusBadRequest, "bad_reservation", err.Error())
	case err != nil:
		httpx.WriteProblem(w, r, http.StatusServiceUnavailable, "ledger_unavailable", "the settlement could not reach the ledger; retry with the same reservation")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
