// Package ledger is the hub's source of truth for quota: how many tokens
// have been granted, for which limit, in which window.
//
// The ledger's own clock defines windows. Every grant reports the window it
// was made in and how long its tokens stay valid, relative to the ledger's
// clock, so edges never compute window boundaries themselves and clock skew
// between edges and hubs cannot let a token be spent outside its window.
//
// Keys are laid out for Redis Cluster: everything one grant touches shares a
// hash tag, so a grant is a single atomic script on a single shard.
package ledger

import (
	"context"
	"errors"
	"strings"
	"time"
)

// Algo is a rate-limit algorithm the ledger can enforce.
type Algo string

const (
	// FixedWindow admits Limit cost per epoch-aligned Window.
	FixedWindow Algo = "fixed_window"
	// SlidingWindow weights the previous window's usage by how much of it
	// still overlaps a window ending now.
	SlidingWindow Algo = "sliding_window"
	// TokenBucket refills at Rate per second up to Limit (the burst),
	// enforced as GCRA.
	TokenBucket Algo = "token_bucket"
)

// Valid reports whether a is a known algorithm ("" means FixedWindow).
func (a Algo) Valid() bool {
	switch a {
	case "", FixedWindow, SlidingWindow, TokenBucket:
		return true
	}
	return false
}

// Or returns a, or FixedWindow when a is empty.
func (a Algo) Or() Algo {
	if a == "" {
		return FixedWindow
	}
	return a
}

// Spec is a limit as the hub resolved it from config. Edges never supply
// one: they name a limit, and the hub looks it up.
type Spec struct {
	Algo   Algo
	Limit  int64         // cost per window, or the bucket's burst
	Window time.Duration // fixed and sliding windows
	Rate   float64       // token bucket: cost per second
}

// Period is the spec's natural reset period: the window, or the time to
// refill a full bucket.
func (s Spec) Period() time.Duration {
	if s.Algo.Or() == TokenBucket {
		if s.Rate <= 0 {
			return time.Second
		}
		return time.Duration(float64(s.Limit) / s.Rate * float64(time.Second))
	}
	return max(s.Window, time.Second)
}

// Ref names one quota: a subject (a key, an IP hash, "*" for a whole
// service) under one limit of one service. Parent, when set, names an
// enclosing quota (an organisation) that every grant must also fit.
type Ref struct {
	Scope   string
	Service string
	Limit   string // "" is the caller's tier limit
	Subject string
	Parent  string
}

// Key is the ledger key for r under algo. The hash tag is the subject, or
// the parent when there is one, so a nested grant stays on one shard.
func (r Ref) Key(algo Algo) string {
	tagged := r.Subject
	if r.Parent != "" {
		tagged = "org:" + r.Parent
	}
	limit := r.Limit
	if limit == "" {
		limit = "tier"
	}
	var b strings.Builder
	b.WriteString("gate:{")
	b.WriteString(r.Scope)
	b.WriteByte(':')
	b.WriteString(r.Service)
	b.WriteByte(':')
	b.WriteString(tagged)
	b.WriteString("}:")
	b.WriteString(limit)
	if r.Parent != "" {
		b.WriteByte(':')
		b.WriteString(r.Subject)
	}
	b.WriteByte(':')
	b.WriteString(string(algo.Or()))
	return b.String()
}

// ParentKey is the ledger key of r's parent quota under algo.
func (r Ref) ParentKey(algo Algo) string {
	return "gate:{" + r.Scope + ":" + r.Service + ":org:" + r.Parent + "}:org:" + string(algo.Or())
}

// GrantArgs is what an edge asks for.
type GrantArgs struct {
	Requested int64
	// EdgeID identifies the asking edge for endgame splitting; empty
	// disables it.
	EdgeID string
	// Horizon is how long an edge should be able to serve from one grant;
	// with ObservedRate it sizes adaptive grants. Zero disables adaptive
	// sizing and grants Requested.
	Horizon      time.Duration
	ObservedRate float64 // cost per second this edge spends on the quota
	MinGrant     int64
	MaxGrant     int64
	// Parent, when non-nil, is the enclosing quota's spec (Ref.Parent).
	Parent *Spec
	// Regional, when set, makes the quota this region's allocation of a
	// global one: AllocBase plus whatever peers lent it this window (see
	// Lend and Credit). Fixed windows only.
	Regional  bool
	AllocBase int64
}

// Grant is the ledger's answer.
type Grant struct {
	Granted   int64
	Remaining int64 // quota left after this grant (the tighter of key and parent)
	// WindowID identifies the window the tokens belong to (0 for a token
	// bucket, which has none).
	WindowID int64
	// ExpiresIn is how long the granted tokens remain valid, measured from
	// when the ledger made the grant.
	ExpiresIn time.Duration
	// ResetIn is how long until the quota is whole again: the window end,
	// or the time to refill the bucket.
	ResetIn time.Duration
	// RetryAfter is set when nothing was granted: when capacity returns.
	RetryAfter  time.Duration
	ActiveEdges int64
	// ParentWindowID identifies the parent quota's window, for returns.
	ParentWindowID int64
}

// Return gives unspent tokens (or, when negative, unpaid debt) back to the
// window they were granted in.
type Return struct {
	Ref            Ref
	Spec           Spec
	Parent         *Spec
	WindowID       int64
	ParentWindowID int64
	Tokens         int64
}

// Ledger grants and takes back quota.
type Ledger interface {
	Grant(ctx context.Context, ref Ref, spec Spec, args GrantArgs) (Grant, error)
	// Return credits tokens back to their window. dedupe identifies the
	// return so a retried call is applied once; it is ignored when empty.
	// Tokens for a window that has already ended are discarded.
	Return(ctx context.Context, dedupe string, returns []Return) error
}

// BatchItem is one grant in a batch.
type BatchItem struct {
	Ref  Ref
	Spec Spec
	Args GrantArgs
}

// BatchResult is the outcome of one BatchItem.
type BatchResult struct {
	Grant Grant
	Err   error
}

// GrantAll runs items in one round trip when l supports batching, and one
// by one otherwise.
func GrantAll(ctx context.Context, l Ledger, items []BatchItem) []BatchResult {
	if b, ok := l.(interface {
		GrantBatch(context.Context, []BatchItem) []BatchResult
	}); ok {
		return b.GrantBatch(ctx, items)
	}
	out := make([]BatchResult, len(items))
	for i, it := range items {
		out[i].Grant, out[i].Err = l.Grant(ctx, it.Ref, it.Spec, it.Args)
	}
	return out
}

// ErrBadSpec means a spec cannot be enforced.
var ErrBadSpec = errors.New("ledger: invalid limit spec")

func (s Spec) validate() error {
	if !s.Algo.Valid() || s.Limit <= 0 {
		return ErrBadSpec
	}
	if s.Algo.Or() == TokenBucket && s.Rate <= 0 {
		return ErrBadSpec
	}
	return nil
}

// target sizes a grant: the request, or for adaptive leasing the edge's
// observed demand over the horizon, within [MinGrant, MaxGrant].
func (a GrantArgs) target() int64 {
	want := a.Requested
	if a.Horizon > 0 && a.ObservedRate > 0 {
		want = int64(a.ObservedRate*a.Horizon.Seconds() + 0.999)
	}
	if a.MinGrant > 0 {
		want = max(want, a.MinGrant)
	}
	if a.MaxGrant > 0 {
		want = min(want, a.MaxGrant)
	}
	return max(want, 1)
}

// waitingRetry is the retry hint for an exhausted window: a tenth of the
// period, at least 500 ms, and never past the reset.
func waitingRetry(period, resetIn time.Duration) time.Duration {
	retry := max(period/10, 500*time.Millisecond)
	if resetIn > 0 {
		retry = min(retry, resetIn)
	}
	return retry
}
