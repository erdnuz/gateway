package types

import "time"

// GatewayConfig is the root object held by the Source of Truth.
type GatewayConfig struct {
	Prefixes []PrefixConfig `bson:"prefixes" json:"prefixes"`
	Runtime  RuntimePolicy  `bson:"runtime" json:"runtime,omitempty"`
	// Descriptors are limits served by the decision service (gate limiter)
	// to Envoy and other callers, keyed by descriptor like Envoy's
	// rate-limit service.
	Descriptors []DescriptorDomain `json:"descriptors,omitempty"`
}

// DescriptorDomain is one Envoy rate-limit domain.
type DescriptorDomain struct {
	Domain  string            `json:"domain"`
	Entries []DescriptorEntry `json:"entries"`
}

// DescriptorEntry matches one descriptor entry: a key, and a value unless
// every value gets its own limit. Entries nest to match longer descriptors.
type DescriptorEntry struct {
	Key     string            `json:"key"`
	Value   string            `json:"value,omitempty"`
	Limit   *LimitSpec        `json:"limit,omitempty"`
	Entries []DescriptorEntry `json:"entries,omitempty"`
}

func (cfg *GatewayConfig) Validate() error {
	if cfg == nil {
		return errConfig("config is nil")
	}
	for i := range cfg.Prefixes {
		if err := cfg.Prefixes[i].Validate(); err != nil {
			return errConfig("prefixes[%d]: %v", i, err)
		}
	}
	if err := cfg.Runtime.Validate(); err != nil {
		return errConfig("runtime: %v", err)
	}
	return nil
}

// LeasingConfig holds per-prefix lease parameters. Zero-value fields fall back
// to the global EdgeRuntimePolicy defaults via LeasingConfig.Effective().
type LeasingConfig struct {
	// LeaseQuantum is the number of tokens requested per lease grant.
	// Zero → falls back to EdgeRuntimePolicy.RateLeaseSize.
	LeaseQuantum int64 `bson:"lease_quantum" json:"lease_quantum,omitempty"`
	// LowWaterPct is the percentage of LeaseQuantum remaining that triggers an
	// async renewal (0–100). Zero → falls back to EdgeRuntimePolicy.RateLowWaterPct.
	LowWaterPct float64 `bson:"low_water_pct" json:"low_water_pct,omitempty"`
	// LeaseTTL is the Redis key TTL for the local token bucket.
	// Zero → computed from the prefix QuotaPeriod; falls back to 60s.
	LeaseTTL time.Duration `bson:"lease_ttl" json:"lease_ttl,omitempty"`
	// RenewalBuffer is the look-ahead window before expiry at which the temporal
	// trigger fires. Zero → 2s.
	RenewalBuffer time.Duration `bson:"renewal_buffer" json:"renewal_buffer,omitempty"`
}

// Effective merges per-prefix overrides with global EdgeRuntimePolicy defaults.
// quotaPeriod is the prefix quota period, used to derive a sensible LeaseTTL
// when the field is zero.
func (lc LeasingConfig) Effective(global EdgeRuntimePolicy, quotaPeriod time.Duration) LeasingConfig {
	if lc.LeaseQuantum <= 0 {
		lc.LeaseQuantum = global.RateLeaseSize
	}
	if lc.LowWaterPct <= 0 {
		lc.LowWaterPct = global.RateLowWaterPct
	}
	if lc.LeaseTTL <= 0 {
		if quotaPeriod > 0 {
			lc.LeaseTTL = quotaPeriod
		} else {
			lc.LeaseTTL = 60 * time.Second // 1-minute safe default
		}
	}
	if lc.RenewalBuffer <= 0 {
		lc.RenewalBuffer = 2 * time.Second
	}
	return lc
}

// PrefixConfig groups services under a common path (e.g., /v1).
type PrefixConfig struct {
	Prefix      string          `bson:"prefix" json:"prefix"`
	QuotaPeriod time.Duration   `bson:"quota_period" json:"quota_period"` // Global reset window
	Leasing     LeasingConfig   `bson:"leasing" json:"leasing,omitempty"`
	Services    []ServiceConfig `bson:"services" json:"services"`
}

func (p *PrefixConfig) Validate() error {
	if p == nil {
		return errConfig("prefix is nil")
	}
	if p.Leasing.LeaseQuantum < 0 {
		return errConfig("leasing.lease_quantum must be >= 0")
	}
	if p.Leasing.LowWaterPct < 0 {
		return errConfig("leasing.low_water_pct must be >= 0")
	}
	if p.Leasing.LeaseTTL < 0 {
		return errConfig("leasing.lease_ttl must be >= 0")
	}
	if p.Leasing.RenewalBuffer < 0 {
		return errConfig("leasing.renewal_buffer must be >= 0")
	}
	for i := range p.Services {
		if err := p.Services[i].Validate(); err != nil {
			return errConfig("services[%d]: %v", i, err)
		}
	}
	return nil
}

// ServiceConfig defines the specific behavior for an individual upstream service.
type ServiceConfig struct {
	ServiceID  string       `bson:"_id" json:"service_id" validate:"required,kebab-case"`
	TargetURL  string       `bson:"target_url" json:"target_url"`
	Tiers      []TierConfig `bson:"tiers" json:"tiers"`
	SafetyTier *TierConfig  `bson:"safety_tier" json:"safety_tier,omitempty"`

	// Middleware Policy Blocks
	Transform TransformConfig `bson:"transform" json:"transform"`
	CORS      *CORSConfig     `bson:"cors" json:"cors,omitempty"`
	Analytics AnalyticsConfig `bson:"analytics" json:"analytics"`
	Cache     *CacheConfig    `bson:"cache" json:"cache,omitempty"`

	// Reliability Strategy
	Failure FailureConfig `bson:"failure" json:"failure"`

	// Upstreams, when set, replaces TargetURL with a load-balanced pool.
	Upstreams      []string              `json:"upstreams,omitempty"`
	LoadBalancing  string                `json:"load_balancing,omitempty"` // round_robin (default) or least_requests
	CircuitBreaker *CircuitBreakerConfig `json:"circuit_breaker,omitempty"`

	Auth          AuthConfig    `json:"auth,omitempty"`
	MaxConcurrent int           `json:"max_concurrent,omitempty"` // per caller, per edge; 0 = unlimited
	Methods       []string      `json:"methods,omitempty"`        // allowed methods; empty = any
	Host          string        `json:"host,omitempty"`           // required Host; empty = any
	Timeout       time.Duration `json:"timeout,omitempty"`        // whole upstream exchange, retries included
	MaxBodyBytes  int64         `json:"max_body_bytes,omitempty"` // request body limit; 0 = edge default

	// Limits are enforced on top of the caller's tier; a request must fit
	// every one of them.
	Limits []LimitSpec `json:"limits,omitempty"`
	// Org, when set, is a budget shared by every key of one owner.
	Org *OrgLimit `json:"org,omitempty"`
	// Shadow evaluates the tier limit without enforcing it.
	Shadow bool `json:"shadow,omitempty"`
	// Cost, when set, charges the actual cost reported by the upstream
	// instead of a fixed cost per method.
	Cost *CostPolicy `json:"cost,omitempty"`
	// Lease tunes how the hub sizes grants for this service.
	Lease LeasePolicy `json:"lease,omitempty"`
	// Global, when set, makes the tier quotas global across regions: each
	// region enforces its allocation and borrows from the others.
	Global *GlobalLimit `json:"global,omitempty"`
}

// GlobalLimit splits a service's quotas between regions.
type GlobalLimit struct {
	// Floors are each region's guaranteed share (0..1, summing to at most
	// 1); the rest of each key's quota belongs to its home region.
	Floors map[string]float64 `json:"floors"`
}

// LimitSpec is one additional limit on a service.
type LimitSpec struct {
	ID        string        `json:"id"`
	Per       string        `json:"per"` // key (default), client_ip or service
	Algorithm string        `json:"algorithm,omitempty"`
	Quota     int64         `json:"quota"` // per window, or the bucket's burst
	Window    time.Duration `json:"window,omitempty"`
	Rate      float64       `json:"rate,omitempty"` // token bucket: per second
	Shadow    bool          `json:"shadow,omitempty"`
}

// Limit subjects.
const (
	PerKey      = "key"
	PerClientIP = "client_ip"
	PerService  = "service"
)

// ServiceSubject is the subject of a service-wide limit.
const ServiceSubject = "_service"

// OrgLimit is a budget shared by every key with the same owner.
type OrgLimit struct {
	Quota  int64         `json:"quota"`
	Window time.Duration `json:"window"`
	Claim  string        `json:"claim,omitempty"` // JWT claim naming the org
}

// CostPolicy charges what a request actually cost, as the upstream reports
// it, after the response: the edge reserves an estimate up front and settles
// the difference.
type CostPolicy struct {
	Estimate       int64  `json:"estimate,omitempty"`
	EstimateHeader string `json:"estimate_header,omitempty"`
	ActualHeader   string `json:"actual_header"`
	OnMissing      string `json:"on_missing,omitempty"` // estimate (default) or refund
	MaxDebt        int64  `json:"max_debt,omitempty"`
}

// LeasePolicy tunes grant sizing. Adaptive (the default) sizes each grant to
// what the asking edge spends over Horizon; fixed grants the lease size.
type LeasePolicy struct {
	Mode    string        `json:"mode,omitempty"` // adaptive (default) or fixed
	Horizon time.Duration `json:"horizon,omitempty"`
	Min     int64         `json:"min,omitempty"`
	Max     int64         `json:"max,omitempty"`
}

// Default adaptive leasing.
const (
	DefaultLeaseHorizon = 2 * time.Second
	DefaultLeaseMin     = 1
)

// Adaptive reports whether grants follow demand.
func (p LeasePolicy) Adaptive() bool { return p.Mode != "fixed" }

// EffectiveHorizon is the horizon with its default.
func (p LeasePolicy) EffectiveHorizon() time.Duration {
	if p.Horizon > 0 {
		return p.Horizon
	}
	return DefaultLeaseHorizon
}

// CircuitBreakerConfig ejects an upstream target after consecutive failures.
type CircuitBreakerConfig struct {
	Failures int           `json:"failures"`
	Cooldown time.Duration `json:"cooldown"`
}

// AuthConfig selects how callers are identified.
type AuthConfig struct {
	Type string     `json:"type,omitempty"` // api_key (default), jwt or none
	JWT  *JWTConfig `json:"jwt,omitempty"`
}

// JWTConfig verifies bearer JWTs against a JWKS.
type JWTConfig struct {
	JWKSURL   string `json:"jwks_url"`
	Issuer    string `json:"issuer,omitempty"`
	Audience  string `json:"audience,omitempty"`
	TierClaim string `json:"tier_claim,omitempty"`
}

// Targets returns the service's upstream base URLs.
func (s *ServiceConfig) Targets() []string {
	if len(s.Upstreams) > 0 {
		return s.Upstreams
	}
	return []string{s.TargetURL}
}

// AuthType returns the effective auth type.
func (s *ServiceConfig) AuthType() string {
	if s.Auth.Type == "" {
		return "api_key"
	}
	return s.Auth.Type
}

func (s *ServiceConfig) Validate() error {
	if s == nil {
		return errConfig("service is nil")
	}
	if err := s.Analytics.Validate(); err != nil {
		return errConfig("analytics: %v", err)
	}
	if s.SafetyTier != nil {
		if s.SafetyTier.TierID == "" {
			return errConfig("safety_tier.tier_id is required")
		}
		if s.SafetyTier.Quota == 0 {
			return errConfig("safety_tier.quota must be > 0")
		}
	}
	return nil
}
