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
local max_grant_pct = tonumber(ARGV[4])

local used = tonumber(redis.call("GET", key) or "0")
local remaining = quota - used
if remaining <= 0 then
  return {0, 0}
end

local grant = requested
if grant > remaining then
  grant = remaining
end

-- Banker cap for massive requests: only apply the percentage cap when a
-- caller asks for more than the full quota in one shot.
if requested > quota and max_grant_pct and max_grant_pct > 0 and max_grant_pct < 100 then
	local cap = math.floor((quota * max_grant_pct) / 100)
	if cap < 1 then
		cap = 1
	end
	if grant > cap then
		grant = cap
	end
end

local new_used = used + grant
redis.call("SET", key, new_used, "EX", expiry)
return {grant, quota - new_used}
`)

const bankerMassiveRequestCapPct = int64(25)

const leaseRedisOpTimeout = 250 * time.Millisecond

type QuotaLeaseServer struct {
	types.UnimplementedQuotaLeaseServiceServer

	rdb       *redis.Client
	cfgStore  ConfigRegistry
	tierStore TierStore
}

func NewQuotaLeaseServer(rdb *redis.Client, cfgStore ConfigRegistry, tierStore TierStore) *QuotaLeaseServer {
	return &QuotaLeaseServer{rdb: rdb, cfgStore: cfgStore, tierStore: tierStore}
}

func (s *QuotaLeaseServer) RequestQuotaLease(ctx context.Context, req *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	prefixCfg, svcCfg, ok := s.cfgStore.FindService(req.Prefix, req.ServiceId)
	if !ok {
		return nil, fmt.Errorf("service %q not found in prefix %q", req.ServiceId, req.Prefix)
	}

	tierCtx, tierCancel := context.WithTimeout(ctx, leaseRedisOpTimeout)
	tierID, err := s.tierStore.GetTier(tierCtx, req.Prefix, req.ApiKey)
	tierCancel()
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

	leaseCtx, leaseCancel := context.WithTimeout(ctx, leaseRedisOpTimeout)
	result, err := leaseGrantLua.Run(leaseCtx, s.rdb, []string{leaseKey}, int64(tierCfg.Quota), req.RequestedTokens, periodSeconds*2, bankerMassiveRequestCapPct).Int64Slice()
	leaseCancel()
	if err != nil {
		return nil, err
	}
	if len(result) < 2 {
		return nil, fmt.Errorf("invalid lease grant result")
	}

	// Partial-grant or zero-grant case. When available == 0, return the WAITING
	// signal per the Hub "Banker" Logic spec (§4): the Edge must back off and
	// schedule a retry after retry_after_ms using jitter.
	if result[0] == 0 {
		retryAfterMs := periodSeconds * 1000 / 10 // 10 % of the quota period, min 500 ms
		if retryAfterMs < 500 {
			retryAfterMs = 500
		}
		return &types.QuotaLeaseResponse{
			GrantedTokens:      0,
			LeaseTtlSeconds:    periodSeconds,
			RemainingGlobal:    result[1],
			WaitingForCapacity: true,
			RetryAfterMs:       retryAfterMs,
		}, nil
	}

	return &types.QuotaLeaseResponse{
		GrantedTokens:   result[0],
		LeaseTtlSeconds: periodSeconds,
		RemainingGlobal: result[1],
	}, nil
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
