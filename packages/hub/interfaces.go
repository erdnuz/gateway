package hub

import (
	"context"
	"gateway/packages/common/types"
)

type ConfigStore interface {
	Get() *types.GatewayConfig
}

// ConfigRegistry exposes typed, read-only slices of GatewayConfig so modules
// do not need to read raw config blobs.
type ConfigRegistry interface {
	Snapshot() *types.GatewayConfig
	HubPolicy() types.HubRuntimePolicy
	EdgePolicy() types.EdgeRuntimePolicy
	AnalyticsPolicy() types.AnalyticsRuntimePolicy
	Prefixes() []types.PrefixConfig
	FindPrefix(prefix string) (*types.PrefixConfig, bool)
	FindService(prefix, service string) (*types.PrefixConfig, *types.ServiceConfig, bool)
}

type TierStore interface {
	GetTier(ctx context.Context, prefix, apiKey string) (string, error)
	SetTier(ctx context.Context, prefix, apiKey, tierID string) error
	DeleteTier(ctx context.Context, prefix, apiKey string) error
}

// OwnerStore maps a key to its owner, whose keys share an org budget.
type OwnerStore interface {
	GetOwner(ctx context.Context, prefix, apiKey string) (string, error)
	SetOwner(ctx context.Context, prefix, apiKey, owner string) error
}
