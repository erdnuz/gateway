package decide

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gateway/packages/common/types"
	"gateway/packages/limiter"
)

// Descriptor is one thing to limit, with its cost (0 counts as 1).
type Descriptor struct {
	Entries []Entry `json:"entries"`
	Cost    int64   `json:"cost,omitempty"`
}

// Status is the decision for one descriptor.
type Status struct {
	LimitID      string        `json:"limit,omitempty"`
	Matched      bool          `json:"matched"`
	Allowed      bool          `json:"allowed"`
	Shadow       bool          `json:"shadow,omitempty"`
	ShadowDenied bool          `json:"shadow_denied,omitempty"`
	Limit        int64         `json:"limit_value,omitempty"`
	Remaining    int64         `json:"remaining"`
	ResetIn      time.Duration `json:"-"`
	Spec         types.LimitSpec
}

// Outcome is the decision for a whole request: allowed only if every
// enforced descriptor is.
type Outcome struct {
	Allowed     bool
	Statuses    []Status
	RetryAfter  time.Duration
	Reservation string // set when asked for: settle the actual cost with it
}

// Decider answers rate-limit questions from the leased global quota.
type Decider struct {
	Limiter limiter.Limiter
	Config  func() *types.GatewayConfig
	// Hub settles reservations: refunds and extra charges go straight to
	// the ledger, so any replica can settle any reservation.
	Hub    limiter.LeaseReturner
	Secret []byte // signs reservations; replicas that settle each other's must share it
	EdgeID string
}

// ErrBadReservation means a reservation token is malformed, forged or
// expired.
var ErrBadReservation = errors.New("invalid or expired reservation")

const (
	reservationTTL = 10 * time.Minute
	minRetry       = 10 * time.Millisecond
)

// Check decides a request. With maxWait > 0 a denied request waits (up to
// maxWait) for quota and tries again, so a queue consumer can simply block.
func (d *Decider) Check(ctx context.Context, domain string, descs []Descriptor, maxWait time.Duration, reserve bool) (*Outcome, error) {
	deadline := time.Now().Add(maxWait)
	for {
		out, err := d.check(ctx, domain, descs, reserve)
		if err != nil || out.Allowed || maxWait <= 0 {
			return out, err
		}
		wait := max(out.RetryAfter, minRetry)
		if time.Now().Add(wait).After(deadline) {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (d *Decider) check(ctx context.Context, domain string, descs []Descriptor, reserve bool) (*Outcome, error) {
	cfg := d.Config()
	out := &Outcome{Statuses: make([]Status, len(descs))}
	var checks []limiter.Request
	var index []int
	for i, desc := range descs {
		m, ok := Resolve(cfg, domain, desc.Entries)
		if !ok {
			out.Statuses[i] = Status{Allowed: true}
			continue
		}
		out.Statuses[i] = Status{LimitID: m.LimitID, Matched: true, Shadow: m.Spec.Shadow, Spec: m.Spec}
		checks = append(checks, limiter.Request{
			Scope: Scope, Service: domain, LimitID: m.LimitID, Key: m.Subject,
			Limit: m.Spec.Quota, Cost: max(desc.Cost, 1), Window: max(m.Spec.Window, time.Second),
			Algo: m.Spec.Algorithm, Shadow: m.Spec.Shadow,
		})
		index = append(index, i)
	}
	res, err := limiter.Reserve(ctx, d.Limiter, checks)
	if err != nil {
		return nil, err
	}
	out.Allowed = res.Allowed
	for j, r := range res.Results {
		st := &out.Statuses[index[j]]
		st.Allowed = r.Allowed || checks[j].Shadow
		st.Limit, st.Remaining = r.Limit, r.Remaining
		if !r.ResetAt.IsZero() {
			st.ResetIn = max(time.Until(r.ResetAt), 0)
		}
	}
	for _, j := range res.ShadowDenied {
		out.Statuses[index[j]].ShadowDenied = true
	}
	if !res.Allowed && res.BindingIndex >= 0 {
		b := res.Binding
		out.RetryAfter = time.Until(b.RetryAt())
		out.Statuses[index[res.BindingIndex]].Allowed = false
	}
	if reserve && res.Allowed && len(checks) > 0 {
		out.Reservation = d.sign(domain, checks, res)
	}
	return out, nil
}

// reservation is what a reservation token carries: everything needed to
// settle at the hub, with no state on the replica that issued it.
type reservation struct {
	Domain string            `json:"d"`
	Items  []reservationItem `json:"i"`
	Exp    int64             `json:"e"` // unix seconds
	Nonce  string            `json:"n"`
}

type reservationItem struct {
	Limit    string `json:"l"`
	Subject  string `json:"s"`
	Window   int64  `json:"w"`
	Estimate int64  `json:"c"`
}

func (d *Decider) sign(domain string, checks []limiter.Request, res *limiter.Reservation) string {
	r := reservation{Domain: domain, Exp: time.Now().Add(reservationTTL).Unix(), Nonce: nonce()}
	for j, c := range checks {
		if c.Shadow || !res.Results[j].Allowed {
			continue
		}
		r.Items = append(r.Items, reservationItem{Limit: c.LimitID, Subject: c.Key, Window: res.Results[j].Window, Estimate: c.Cost})
	}
	body, _ := json.Marshal(r)
	b := base64.RawURLEncoding.EncodeToString(body)
	return b + "." + base64.RawURLEncoding.EncodeToString(d.mac(b))
}

func (d *Decider) mac(b string) []byte {
	m := hmac.New(sha256.New, d.Secret)
	m.Write([]byte(b))
	return m.Sum(nil)
}

// Settle charges a reservation's actual cost: the difference from its
// estimate is credited to (or charged from) the ledger window the estimate
// came from, once, whichever replica receives it.
func (d *Decider) Settle(ctx context.Context, token string, actual int64) error {
	body, sig, ok := strings.Cut(token, ".")
	want, err := base64.RawURLEncoding.DecodeString(sig)
	if !ok || err != nil || len(d.Secret) == 0 || !hmac.Equal(want, d.mac(body)) {
		return ErrBadReservation
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return ErrBadReservation
	}
	var r reservation
	if json.Unmarshal(raw, &r) != nil || time.Now().Unix() > r.Exp {
		return ErrBadReservation
	}
	in := &types.ReturnLeasesRequest{EdgeId: d.EdgeID, Dedupe: "settle-" + r.Nonce}
	for _, it := range r.Items {
		if delta := it.Estimate - max(actual, 0); delta != 0 {
			in.Returns = append(in.Returns, &types.LeaseReturn{
				Prefix: Scope, ServiceId: r.Domain, ApiKey: it.Subject, LimitId: it.Limit, WindowId: it.Window, Tokens: delta,
			})
		}
	}
	if len(in.Returns) == 0 {
		return nil
	}
	_, err = d.Hub.ReturnLeases(ctx, in)
	return err
}

func nonce() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
