// Package config defines Gate's user-facing configuration file (version 2):
// YAML or JSON, human-readable durations, documented defaults, and
// validation errors that point at the offending line.
//
// A File compiles to the internal runtime model (types.GatewayConfig) that the
// hub serves to edges; Compile and FromInternal convert between the two.
package config

// Version is the only config format version this package reads.
const Version = 2

// File is the root of a gateway config file.
type File struct {
	Version  int       `yaml:"version" desc:"Config format version. Must be 2."`
	Services []Service `yaml:"services" desc:"Upstream services exposed through the gateway."`
	Runtime  Runtime   `yaml:"runtime,omitempty" desc:"Process tuning. Every field is optional; defaults suit most deployments."`
}

// Service is one upstream exposed under a path prefix.
type Service struct {
	Name     string `yaml:"name" desc:"Unique service name, used in analytics and errors."`
	Match    Match  `yaml:"match" desc:"Which requests this service handles."`
	Upstream string `yaml:"upstream,omitempty" desc:"Base URL of the upstream, e.g. http://orders:8080. A path here is prepended to forwarded paths. Use upstreams for several replicas."`

	Upstreams      []string        `yaml:"upstreams,omitempty" desc:"Replicas of the upstream, load balanced. They must share one base path. Use instead of upstream."`
	LoadBalancing  string          `yaml:"load_balancing,omitempty" desc:"How requests spread over upstreams. Default round_robin." enum:"round_robin,least_requests"`
	CircuitBreaker *CircuitBreaker `yaml:"circuit_breaker,omitempty" desc:"Stop sending to an upstream after consecutive failures. Default with several upstreams: 5 failures, 10s cooldown."`
	Timeout        Duration        `yaml:"timeout,omitempty" desc:"Limit for the whole upstream exchange, retries included. Default runtime.edge.upstream_timeout per attempt."`
	MaxRequestBody int64           `yaml:"max_request_body,omitempty" desc:"Largest request body in bytes. Default 10485760."`

	Auth *Auth `yaml:"auth,omitempty" desc:"How callers are identified. Default: an API key in X-API-Key."`

	StripPathPrefix *bool    `yaml:"strip_path_prefix,omitempty" desc:"Remove match.path_prefix before forwarding. Default true."`
	Headers         *Headers `yaml:"headers,omitempty" desc:"Header changes on the way to the upstream and back."`

	Limits    Limits     `yaml:"limits" desc:"API keys, tiers and rate limits. Every request must carry a key with a tier."`
	Cache     *Cache     `yaml:"cache,omitempty" desc:"Response caching for GET requests."`
	Retries   *Retries   `yaml:"retries,omitempty" desc:"Retry policy for failed upstream attempts."`
	CORS      *CORS      `yaml:"cors,omitempty" desc:"Cross-origin resource sharing."`
	Analytics *Analytics `yaml:"analytics,omitempty" desc:"Request event capture. Default: every request."`

	OnUpstreamError *UpstreamError `yaml:"on_upstream_error,omitempty" desc:"Response when the upstream fails after retries. Default: a 502 problem (504 on timeout)."`
}

// Match selects requests for a service.
type Match struct {
	PathPrefix string   `yaml:"path_prefix" desc:"Two path segments, /<group>/<service>, e.g. /v1/orders. Services sharing the first segment share a quota window."`
	Methods    []string `yaml:"methods,omitempty" desc:"Allowed methods; others get 405. Default: any."`
	Host       string   `yaml:"host,omitempty" desc:"Required Host header (without port). Default: any."`
}

// CircuitBreaker ejects failing upstreams.
type CircuitBreaker struct {
	Failures int      `yaml:"failures" desc:"Consecutive failures (connection errors or 5xx) that eject an upstream."`
	Cooldown Duration `yaml:"cooldown" desc:"How long an ejected upstream rests before it is tried again."`
}

// Auth selects caller identification.
type Auth struct {
	Type string `yaml:"type" desc:"api_key (X-API-Key, tiers assigned on the hub), jwt (bearer token, tier from a claim) or none (callers limited by client IP)." enum:"api_key,jwt,none"`
	JWT  *JWT   `yaml:"jwt,omitempty" desc:"JWT verification, for type jwt."`
}

// JWT verifies bearer tokens.
type JWT struct {
	JWKSURL   string `yaml:"jwks_url" desc:"URL of the issuer's JSON Web Key Set."`
	Issuer    string `yaml:"issuer,omitempty" desc:"Required iss claim."`
	Audience  string `yaml:"audience,omitempty" desc:"Required aud claim."`
	TierClaim string `yaml:"tier_claim,omitempty" desc:"Claim naming the caller's tier. Without it (or when absent) limits.default_tier applies."`
}

// Headers adds or removes headers.
type Headers struct {
	Add  map[string]string `yaml:"add,omitempty" desc:"Headers set on requests to the upstream."`
	Hide []string          `yaml:"hide,omitempty" desc:"Headers removed from requests and responses."`
}

// Limits configures API-key authentication and quotas.
type Limits struct {
	Window             Duration        `yaml:"window" desc:"Quota window, e.g. 1m or 1h. Quotas reset when the window rolls over."`
	Tiers              map[string]Tier `yaml:"tiers" desc:"Tiers by name. Each API key is assigned one tier."`
	DefaultTier        string          `yaml:"default_tier,omitempty" desc:"Tier used for a key whose tier cannot be looked up while the hub is unreachable (after any cached tier)."`
	WhenHubUnavailable *Fallback       `yaml:"when_hub_unavailable,omitempty" desc:"Local limit applied while the hub cannot grant quota. Omit to reject with 503 instead."`
	LeaseSize          int64           `yaml:"lease_size,omitempty" desc:"Tokens an edge asks the hub for per lease (the first lease for a key, or every lease with leasing.mode fixed). Default runtime.edge.lease_size."`
	MaxConcurrent      int             `yaml:"max_concurrent,omitempty" desc:"Requests one caller may have in flight on one edge. Default unlimited."`

	Algorithm string       `yaml:"algorithm,omitempty" desc:"Default algorithm for the tiers. fixed_window (default) resets every window; sliding_window weights the previous window by its overlap, smoothing the burst at a reset; token_bucket refills continuously at a rate up to a burst." enum:"fixed_window,sliding_window,token_bucket"`
	Mode      string       `yaml:"mode,omitempty" desc:"enforce (default), or shadow: evaluate the tier limits and report what they would deny (metrics and a Gate-Shadow-Denied header) without denying." enum:"enforce,shadow"`
	Leasing   *Leasing     `yaml:"leasing,omitempty" desc:"How the hub sizes the grants it leases to edges. Default adaptive: each grant covers what the asking edge spends in 2s."`
	Org       *OrgLimit    `yaml:"org,omitempty" desc:"A budget shared by every key with the same owner (API keys) or the same org claim (JWT), on top of each key's tier."`
	Also      []ExtraLimit `yaml:"also,omitempty" desc:"Further limits every request must also fit: per key, per client IP, or across the whole service."`
	Cost      *Cost        `yaml:"cost,omitempty" desc:"Charge what each request actually cost, as the upstream reports it after the response (e.g. LLM tokens), instead of a fixed cost per method."`
}

// Tier is a quota level.
type Tier struct {
	Requests  uint32           `yaml:"requests,omitempty" desc:"Budget per window, in cost units. Required except for token_bucket."`
	Cost      map[string]uint8 `yaml:"cost,omitempty" desc:"Cost per request by method (GET, POST, PUT, DELETE, or * for others). Default 1."`
	Algorithm string           `yaml:"algorithm,omitempty" desc:"Overrides limits.algorithm for this tier." enum:"fixed_window,sliding_window,token_bucket"`
	Rate      Rate             `yaml:"rate,omitempty" desc:"token_bucket: refill rate, e.g. 20/s or 1200/m."`
	Burst     uint32           `yaml:"burst,omitempty" desc:"token_bucket: the most cost admitted at once (the bucket's size)."`
}

// Leasing tunes grant sizing.
type Leasing struct {
	Mode    string   `yaml:"mode,omitempty" desc:"adaptive (default) sizes each grant to the asking edge's demand; fixed always grants lease_size." enum:"adaptive,fixed"`
	Horizon Duration `yaml:"horizon,omitempty" desc:"adaptive: how long one grant should last the asking edge. Longer means fewer hub calls; shorter means less quota held per edge. Default 2s."`
	Min     int64    `yaml:"min,omitempty" desc:"adaptive: smallest grant. Default 1."`
	Max     int64    `yaml:"max,omitempty" desc:"adaptive: largest grant. Default unbounded (a quarter of the quota at most when one ask exceeds it)."`
}

// OrgLimit is a budget shared by one owner's keys.
type OrgLimit struct {
	Requests uint32   `yaml:"requests" desc:"Budget per window for all of one owner's keys together, in cost units."`
	Window   Duration `yaml:"window,omitempty" desc:"Window of the org budget, e.g. 1h or 720h. Default limits.window."`
	Claim    string   `yaml:"claim,omitempty" desc:"jwt auth: the claim naming the caller's org. API-key callers' org is their key's owner."`
}

// ExtraLimit is a limit applied in addition to the caller's tier.
type ExtraLimit struct {
	Name      string   `yaml:"name" desc:"Names the limit in metrics, errors and the ledger. Lowercase letters, digits and dashes."`
	Per       string   `yaml:"per,omitempty" desc:"What the limit counts: key (default; each caller), client_ip (each client address), or service (all traffic together, e.g. to protect the upstream)." enum:"key,client_ip,service"`
	Algorithm string   `yaml:"algorithm,omitempty" desc:"Default fixed_window." enum:"fixed_window,sliding_window,token_bucket"`
	Requests  uint32   `yaml:"requests,omitempty" desc:"Budget per window. Required except for token_bucket."`
	Window    Duration `yaml:"window,omitempty" desc:"Default limits.window."`
	Rate      Rate     `yaml:"rate,omitempty" desc:"token_bucket: refill rate, e.g. 2000/s."`
	Burst     uint32   `yaml:"burst,omitempty" desc:"token_bucket: the bucket's size."`
	Mode      string   `yaml:"mode,omitempty" desc:"enforce (default) or shadow." enum:"enforce,shadow"`
}

// Cost charges post-hoc costs.
type Cost struct {
	ActualHeader   string `yaml:"actual_header" desc:"Response header (or trailer, for streamed bodies) carrying the request's actual cost, e.g. X-Usage-Total-Tokens."`
	Estimate       int64  `yaml:"estimate,omitempty" desc:"Cost reserved before the request is forwarded. Default the tier's cost for the method."`
	EstimateHeader string `yaml:"estimate_header,omitempty" desc:"Request header a client may use to state its own estimate, e.g. X-Max-Tokens."`
	OnMissing      string `yaml:"on_missing,omitempty" desc:"When the upstream reports no cost: estimate (default; the reservation stands) or refund. A failed upstream is always refunded." enum:"estimate,refund"`
	MaxDebt        int64  `yaml:"max_debt,omitempty" desc:"How far actual costs may overdraw one edge's share of the quota before it waits for the next grant. Default the estimate."`
}

// Fallback is the degraded-mode limit used without the hub.
type Fallback struct {
	Allow uint32 `yaml:"allow" desc:"Requests each edge admits per key and window while the hub is unreachable."`
}

// Cache configures response caching.
type Cache struct {
	TTL Duration `yaml:"ttl" desc:"How long a response is served from cache."`
	Key string   `yaml:"key,omitempty" desc:"Cache key template from {method} {path} {query} {key} {encoding}. Default {method}:{path}:{query}:{key}."`
}

// Retries configures upstream retries.
type Retries struct {
	Attempts          int      `yaml:"attempts" desc:"Total attempts including the first (1 = no retry)."`
	Statuses          []int    `yaml:"statuses,omitempty" desc:"Upstream status codes that trigger a retry, e.g. [502, 503]. Connection errors always do."`
	Backoff           Duration `yaml:"backoff,omitempty" desc:"Pause between attempts. Default 50ms."`
	PerAttemptTimeout Duration `yaml:"per_attempt_timeout,omitempty" desc:"Timeout for each attempt. Default: the overall upstream timeout."`
	NonIdempotent     bool     `yaml:"non_idempotent,omitempty" desc:"Also retry POST and PATCH. Default false."`
}

// CORS configures cross-origin access.
type CORS struct {
	Origins []string `yaml:"origins" desc:"Allowed origins, or [\"*\"]."`
	Methods []string `yaml:"methods,omitempty" desc:"Allowed methods. Default GET, POST, PUT, DELETE, OPTIONS."`
	Headers []string `yaml:"headers,omitempty" desc:"Allowed request headers. Default Authorization, Content-Type, X-API-Key."`
	MaxAge  Duration `yaml:"max_age,omitempty" desc:"How long browsers may cache a preflight. Default 5m."`
}

// Analytics configures request event capture.
type Analytics struct {
	Enabled    *bool    `yaml:"enabled,omitempty" desc:"Capture events for this service. Default true."`
	SampleRate *float64 `yaml:"sample_rate,omitempty" desc:"Fraction of requests captured, 0 to 1. Default 1."`
}

// UpstreamError configures the response after upstream failure.
type UpstreamError struct {
	Mode    string            `yaml:"mode" desc:"fail-closed (502, or 504 on timeout) or fail-open (serve the fallback below)." enum:"fail-closed,fail-open"`
	Status  int               `yaml:"status,omitempty" desc:"Fallback status code. Default 503."`
	Body    string            `yaml:"body,omitempty" desc:"Fallback body."`
	Headers map[string]string `yaml:"headers,omitempty" desc:"Fallback headers."`
}

// Runtime is process tuning shared by all components.
type Runtime struct {
	Hub       HubRuntime       `yaml:"hub,omitempty" desc:"Hub tuning."`
	Edge      EdgeRuntime      `yaml:"edge,omitempty" desc:"Edge tuning."`
	Analytics AnalyticsRuntime `yaml:"analytics,omitempty" desc:"Analytics tuning."`
}

// HubRuntime tunes the hub.
type HubRuntime struct {
	APIKeyPattern   string   `yaml:"api_key_pattern,omitempty" desc:"Regular expression every API key must match. Default ^[A-Za-z0-9_]{3,128}$."`
	CORSHeaders     []string `yaml:"cors_headers,omitempty" desc:"Headers allowed on cross-origin hub API calls."`
	CORSMethods     []string `yaml:"cors_methods,omitempty" desc:"Methods allowed on cross-origin hub API calls."`
	CORSMaxAge      Duration `yaml:"cors_max_age,omitempty" desc:"Preflight cache time for the hub API. Default 5m."`
	ReadTimeout     Duration `yaml:"read_timeout,omitempty" desc:"HTTP read timeout. Default 5s."`
	WriteTimeout    Duration `yaml:"write_timeout,omitempty" desc:"HTTP write timeout. Default 10s."`
	ShutdownTimeout Duration `yaml:"shutdown_timeout,omitempty" desc:"Graceful shutdown limit. Default 15s."`
}

// EdgeRuntime tunes edges.
type EdgeRuntime struct {
	LeaseSize               int64    `yaml:"lease_size,omitempty" desc:"Default tokens per lease. Default 100."`
	LeaseLowWaterPct        float64  `yaml:"lease_low_water_pct,omitempty" desc:"Renew a lease when this percentage is left. Default 20."`
	AnalyticsBufferSize     int      `yaml:"analytics_buffer_size,omitempty" desc:"Events buffered before dropping. Default 1000."`
	AnalyticsPublishTimeout Duration `yaml:"analytics_publish_timeout,omitempty" desc:"Default 2s."`
	CacheMaxObjectBytes     int64    `yaml:"cache_max_object_bytes,omitempty" desc:"Largest cacheable response. Default 1048576."`
	UpstreamTimeout         Duration `yaml:"upstream_timeout,omitempty" desc:"Overall upstream request timeout. Default 30s."`
	UpstreamMaxIdleConns    int      `yaml:"upstream_max_idle_conns,omitempty" desc:"Idle keep-alive connections per upstream. Default 100."`
	UpstreamIdleConnTimeout Duration `yaml:"upstream_idle_conn_timeout,omitempty" desc:"Default 90s."`
	ReadTimeout             Duration `yaml:"read_timeout,omitempty" desc:"Client read timeout. Default 15s."`
	WriteTimeout            Duration `yaml:"write_timeout,omitempty" desc:"Client write timeout. Default 15s."`
	IdleTimeout             Duration `yaml:"idle_timeout,omitempty" desc:"Client keep-alive timeout. Default 60s."`
}

// AnalyticsRuntime tunes the analytics service.
type AnalyticsRuntime struct {
	NATSSubject         string   `yaml:"nats_subject,omitempty" desc:"Subject edges publish events on. Default analytics.events."`
	NATSQueue           string   `yaml:"nats_queue,omitempty" desc:"Queue group shared by analytics replicas. Default analytics-subscribers."`
	DefaultEventsLimit  int64    `yaml:"default_events_limit,omitempty" desc:"Default 100."`
	MaxEventsLimit      int64    `yaml:"max_events_limit,omitempty" desc:"Default 5000."`
	MaxEventsOffset     int64    `yaml:"max_events_offset,omitempty" desc:"Default 1000000."`
	DefaultSummaryLimit int64    `yaml:"default_summary_limit,omitempty" desc:"Default 1000."`
	MaxSummaryLimit     int64    `yaml:"max_summary_limit,omitempty" desc:"Default 10000."`
	ReadTimeout         Duration `yaml:"read_timeout,omitempty" desc:"Default 10s."`
	WriteTimeout        Duration `yaml:"write_timeout,omitempty" desc:"Default 10s."`
	IdleTimeout         Duration `yaml:"idle_timeout,omitempty" desc:"Default 30s."`
	ConfigFetchTimeout  Duration `yaml:"config_fetch_timeout,omitempty" desc:"Default 5s."`
	ShutdownTimeout     Duration `yaml:"shutdown_timeout,omitempty" desc:"Default 5s."`
}
