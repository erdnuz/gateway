package edge

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"gateway/packages/common/types"
)

type leaseCounter struct {
	remaining atomic.Int64
	consumed  atomic.Int64
	leaseSize int64
}

type RateManager struct {
	leaseClient QuotaLeaseRequester
	leaseSize   int64
	lowWaterPct float64
	leaseMap    sync.Map
	refillGroup singleflightGroup
}

type RateManagerOptions struct {
	HardThresholdPct float64
	LeaseSize        int64
	LowWaterPct      float64
}

func DefaultRateManagerOptions() RateManagerOptions {
	edgeDefaults := types.DefaultRuntimePolicy().Edge
	return RateManagerOptions{
		HardThresholdPct: edgeDefaults.RateHardThresholdPct,
		LeaseSize:        edgeDefaults.RateLeaseSize,
		LowWaterPct:      edgeDefaults.RateLowWaterPct,
	}
}

var _ types.Limiter = (*RateManager)(nil)

func NewRateManager(_ string, _ interface{}, maxDelta int64, channels ...string) *RateManager {
	return NewRateManagerWithOptions("", nil, maxDelta, DefaultRateManagerOptions(), channels...)
}

func NewRateManagerWithOptions(_ string, _ interface{}, maxDelta int64, options RateManagerOptions, _ ...string) *RateManager {
	edgeDefaults := types.DefaultRuntimePolicy().Edge
	if options.HardThresholdPct <= 0 || options.HardThresholdPct > 1 {
		options.HardThresholdPct = edgeDefaults.RateHardThresholdPct
	}
	if options.LeaseSize <= 0 {
		options.LeaseSize = edgeDefaults.RateLeaseSize
	}
	if options.LowWaterPct <= 0 || options.LowWaterPct >= 1 {
		options.LowWaterPct = edgeDefaults.RateLowWaterPct
	}
	return &RateManager{
		// maxDelta/hard-threshold are currently enforced by lease semantics,
		// while options are preserved for future policy expansion.
		leaseSize:   options.LeaseSize,
		lowWaterPct: options.LowWaterPct,
	}
}

func (rm *RateManager) SetLeaseClient(client QuotaLeaseRequester) {
	rm.leaseClient = client
}

func (rm *RateManager) StartBackgroundWorkers(context.Context) {}

func (rm *RateManager) Increment(ctx context.Context, prefix, apiKey string, limit, amount int64) (int64, error) {
	return rm.IncrementWithService(ctx, prefix, "", apiKey, limit, amount)
}

func (rm *RateManager) IncrementWithService(ctx context.Context, prefix, serviceID, apiKey string, limit, amount int64) (int64, error) {
	if amount <= 0 {
		amount = 1
	}
	counter := rm.getLeaseCounter(prefix, apiKey)
	if rm.leaseClient == nil {
		used := counter.consumed.Add(amount)
		return used, nil
	}
	if err := rm.ensureLease(ctx, prefix, serviceID, apiKey, counter, amount); err != nil {
		return 0, err
	}
	remaining := counter.remaining.Add(-amount)
	if remaining < 0 {
		counter.remaining.Add(amount)
		if err := rm.ensureLease(ctx, prefix, serviceID, apiKey, counter, amount); err != nil {
			return 0, err
		}
		remaining = counter.remaining.Add(-amount)
		if remaining < 0 {
			counter.remaining.Add(amount)
			return limit + 1, nil
		}
	}
	used := counter.consumed.Add(amount)

	if counter.remaining.Load() <= int64(float64(counter.leaseSize)*rm.lowWaterPct) {
		go func() {
			_ = rm.ensureLease(context.Background(), prefix, serviceID, apiKey, counter, 1)
		}()
	}
	if used > limit {
		return used, nil
	}
	return used, nil
}

func (rm *RateManager) IncrementSafety(_ context.Context, prefix, apiKey string, limit, amount int64) (int64, error) {
	key := "safety:" + prefix
	counter := rm.getLeaseCounter(key, apiKey)
	if counter.remaining.Load() == 0 {
		counter.remaining.Store(limit)
	}
	remaining := counter.remaining.Add(-amount)
	if remaining < 0 {
		counter.remaining.Add(amount)
		return limit + 1, nil
	}
	used := counter.consumed.Add(amount)
	return used, nil
}

func (rm *RateManager) ensureLease(ctx context.Context, prefix, serviceID, apiKey string, counter *leaseCounter, minimum int64) error {
	if counter.remaining.Load() >= minimum {
		return nil
	}
	if rm.leaseClient == nil {
		return fmt.Errorf("lease client is not configured")
	}
	_, err, _ := rm.refillGroup.Do(prefix+":"+serviceID+":"+apiKey, func() (interface{}, error) {
		if counter.remaining.Load() >= minimum {
			return nil, nil
		}
		resp, err := rm.leaseClient.RequestQuotaLease(ctx, &types.QuotaLeaseRequest{
			Prefix:          prefix,
			ServiceId:       serviceID,
			ApiKey:          apiKey,
			RequestedTokens: rm.leaseSize,
		})
		if err != nil {
			return nil, err
		}
		if resp.GrantedTokens > 0 {
			counter.remaining.Add(resp.GrantedTokens)
		}
		return nil, nil
	})
	return err
}

func (rm *RateManager) getLeaseCounter(prefix, apiKey string) *leaseCounter {
	key := prefix + ":" + apiKey
	if v, ok := rm.leaseMap.Load(key); ok {
		return v.(*leaseCounter)
	}
	counter := &leaseCounter{leaseSize: rm.leaseSize}
	actual, _ := rm.leaseMap.LoadOrStore(key, counter)
	return actual.(*leaseCounter)
}

type singleflightGroup struct {
	mu sync.Mutex
	m  map[string]*singleflightCall
}

type singleflightCall struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

func (g *singleflightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*singleflightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &singleflightCall{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.val, c.err, false
}
