package hub

import (
	"context"
	"fmt"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
)

var leaseGrantLua = redis.NewScript(`
local key = KEYS[1]
local quota = tonumber(ARGV[1])
local requested = tonumber(ARGV[2])
local expiry = tonumber(ARGV[3])

local used = tonumber(redis.call("GET", key) or "0")
local remaining = quota - used
if remaining <= 0 then
  return {0, 0}
end

local grant = requested
if grant > remaining then
  grant = remaining
end

local new_used = used + grant
redis.call("SET", key, new_used, "EX", expiry)
return {grant, quota - new_used}
`)

type QuotaLeaseServer struct {
	types.UnimplementedQuotaLeaseServiceServer

	rdb       *redis.Client
	cfgStore  ConfigStore
	tierStore TierStore
}

func NewQuotaLeaseServer(rdb *redis.Client, cfgStore ConfigStore, tierStore TierStore) *QuotaLeaseServer {
	return &QuotaLeaseServer{rdb: rdb, cfgStore: cfgStore, tierStore: tierStore}
}

func (s *QuotaLeaseServer) RequestQuotaLease(ctx context.Context, req *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	cfg := s.cfgStore.Get()
	prefixCfg, svcCfg, err := lookupServiceConfig(cfg, req.Prefix, req.ServiceId)
	if err != nil {
		return nil, err
	}

	tierID, err := s.tierStore.GetTier(ctx, req.Prefix, req.ApiKey)
	if err != nil {
		return nil, fmt.Errorf("tier lookup failed: %w", err)
	}
	tierCfg, ok := findTier(svcCfg, tierID)
	if !ok {
		return nil, fmt.Errorf("tier %q not found for service", tierID)
	}

	periodSeconds := int64(prefixCfg.QuotaPeriod / time.Second)
	if periodSeconds <= 0 {
		periodSeconds = 1
	}
	windowID := time.Now().Unix() / periodSeconds
	leaseKey := fmt.Sprintf("lease-used:%s:%s:%s:%d", req.Prefix, req.ServiceId, req.ApiKey, windowID)

	result, err := leaseGrantLua.Run(ctx, s.rdb, []string{leaseKey}, int64(tierCfg.Quota), req.RequestedTokens, periodSeconds*2).Int64Slice()
	if err != nil {
		return nil, err
	}
	if len(result) < 2 {
		return nil, fmt.Errorf("invalid lease grant result")
	}

	return &types.QuotaLeaseResponse{
		GrantedTokens:   result[0],
		LeaseTtlSeconds: periodSeconds,
		RemainingGlobal: result[1],
	}, nil
}

func lookupServiceConfig(cfg *types.GatewayConfig, prefixID, serviceID string) (*types.PrefixConfig, *types.ServiceConfig, error) {
	if cfg == nil {
		return nil, nil, fmt.Errorf("config not loaded")
	}
	for i := range cfg.Prefixes {
		prefix := &cfg.Prefixes[i]
		if prefix.Prefix != prefixID {
			continue
		}
		for j := range prefix.Services {
			svc := &prefix.Services[j]
			if svc.ServiceID == serviceID {
				return prefix, svc, nil
			}
		}
		return nil, nil, fmt.Errorf("service %q not found in prefix %q", serviceID, prefixID)
	}
	return nil, nil, fmt.Errorf("prefix %q not found", prefixID)
}

func findTier(svcCfg *types.ServiceConfig, tierID string) (*types.TierConfig, bool) {
	if svcCfg == nil {
		return nil, false
	}
	for i := range svcCfg.Tiers {
		if svcCfg.Tiers[i].TierID == tierID {
			return &svcCfg.Tiers[i], true
		}
	}
	if svcCfg.SafetyTier != nil && svcCfg.SafetyTier.TierID == tierID {
		return svcCfg.SafetyTier, true
	}
	return nil, false
}
