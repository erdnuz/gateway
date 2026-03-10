package hub

import (
	"context"
	"gateway/packages/common/types"
)

type ConfigStore interface {
	Get() *types.GatewayConfig
}

type TierStore interface {
	GetTier(ctx context.Context, prefix, apiKey string) (string, error)
	SetTier(ctx context.Context, prefix, apiKey, tierID string) error
	DeleteTier(ctx context.Context, prefix, apiKey string) error
}

type RateLimiter interface {
	Increment(ctx context.Context, prefix, key string, delta int64) (int64, error)
}
