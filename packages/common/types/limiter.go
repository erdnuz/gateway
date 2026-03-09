package types

import "context"

// Limiter is the minimal edge-facing contract for rate accounting.
type Limiter interface {
	Increment(ctx context.Context, prefix, key string, limit, amount int64) (int64, error)
}

// AuthoritativeStore is the minimal hub-facing contract for source-of-truth accounting.
type AuthoritativeStore interface {
	Increment(ctx context.Context, prefix, key string, amount int64) (int64, error)
}
