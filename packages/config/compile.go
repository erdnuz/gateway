package config

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"gateway/packages/common/types"
)

const (
	defaultCacheKey = "{method}:{path}:{query}:{key}"
	// safetyTierID names the internal tier behind when_hub_unavailable. The
	// leading underscore keeps it out of the user's tier namespace.
	safetyTierID = "_hub_unavailable"
)

var defaultCORSMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}

// The runtime's cache-key template uses $tokens; {encoding} is $enc there.
var toInternalCacheKey = strings.NewReplacer(
	"{method}", "$method", "{path}", "$path", "{query}", "$query", "{key}", "$key", "{encoding}", "$enc")

// Compile converts a validated File into the runtime model the hub serves to
// edges. Services sharing a path group become one prefix.
func Compile(f *File) *types.GatewayConfig {
	out := &types.GatewayConfig{Runtime: compileRuntime(f.Runtime)}
	for _, d := range f.Descriptors {
		out.Descriptors = append(out.Descriptors, types.DescriptorDomain{Domain: d.Domain, Entries: compileEntries(d.Entries)})
	}
	byGroup := map[string]int{}
	for _, s := range f.Services {
		g := s.group()
		i, ok := byGroup[g]
		if !ok {
			i = len(out.Prefixes)
			byGroup[g] = i
			out.Prefixes = append(out.Prefixes, types.PrefixConfig{
				Prefix:      g,
				QuotaPeriod: s.Limits.Window.D(),
				Leasing:     types.LeasingConfig{LeaseQuantum: s.Limits.LeaseSize},
			})
		}
		out.Prefixes[i].Services = append(out.Prefixes[i].Services, compileService(s))
	}
	return out
}

func compileEntries(es []DescriptorEntry) []types.DescriptorEntry {
	var out []types.DescriptorEntry
	for _, e := range es {
		c := types.DescriptorEntry{Key: e.Key, Value: e.Value, Entries: compileEntries(e.Entries)}
		if l := e.Limit; l != nil {
			spec := &types.LimitSpec{ID: e.Key, Algorithm: internalAlgo(l.Algorithm), Quota: int64(l.Requests), Window: l.Window.D(), Shadow: e.Mode == "shadow"}
			if spec.Algorithm == "token_bucket" {
				spec.Quota, spec.Rate = int64(l.Burst), l.Rate.PerSecond()
			}
			c.Limit = spec
		}
		out = append(out, c)
	}
	return out
}

func entriesFromInternal(es []types.DescriptorEntry) []DescriptorEntry {
	var out []DescriptorEntry
	for _, e := range es {
		c := DescriptorEntry{Key: e.Key, Value: e.Value, Entries: entriesFromInternal(e.Entries)}
		if l := e.Limit; l != nil {
			c.Limit = &DescriptorLimit{Algorithm: l.Algorithm, Requests: uint32(l.Quota), Window: Duration(l.Window)}
			if l.Algorithm == "token_bucket" {
				c.Limit.Requests, c.Limit.Burst, c.Limit.Rate = 0, uint32(l.Quota), Rate(l.Rate)
			}
			if l.Shadow {
				c.Mode = "shadow"
			}
		}
		out = append(out, c)
	}
	return out
}

func compileService(s Service) types.ServiceConfig {
	svc := types.ServiceConfig{
		ServiceID:     s.Name,
		TargetURL:     s.Upstream,
		Transform:     types.TransformConfig{StripPrefix: s.StripPathPrefix == nil || *s.StripPathPrefix, AddHeaders: map[string]string{}},
		Analytics:     types.AnalyticsConfig{Enabled: true, Mode: types.AnalyticsModeHeavy, SamplingRate: 1},
		LoadBalancing: s.LoadBalancing,
		MaxConcurrent: s.Limits.MaxConcurrent,
		Methods:       s.Match.Methods,
		Host:          s.Match.Host,
		Timeout:       s.Timeout.D(),
		MaxBodyBytes:  s.MaxRequestBody,
	}
	if len(s.Upstreams) > 0 {
		svc.Upstreams = s.Upstreams
		svc.TargetURL = s.Upstreams[0]
	}
	switch cb := s.CircuitBreaker; {
	case cb != nil:
		svc.CircuitBreaker = &types.CircuitBreakerConfig{Failures: cb.Failures, Cooldown: cb.Cooldown.D()}
	case len(s.Upstreams) > 1:
		svc.CircuitBreaker = &types.CircuitBreakerConfig{Failures: 5, Cooldown: 10 * time.Second}
	}
	if a := s.Auth; a != nil {
		svc.Auth.Type = a.Type
		if j := a.JWT; j != nil {
			svc.Auth.JWT = &types.JWTConfig{JWKSURL: j.JWKSURL, Issuer: j.Issuer, Audience: j.Audience, TierClaim: j.TierClaim}
		}
	}

	for _, name := range sortedTierNames(s.Limits.Tiers) {
		svc.Tiers = append(svc.Tiers, compileTier(name, s.Limits.Tiers[name], s.Limits.Algorithm))
	}
	compileLimits(&svc, s.Limits)
	hubPolicy := types.HubFailurePolicy{TierLookupStrategy: "fail-closed"}
	if s.Limits.DefaultTier != "" {
		hubPolicy.TierLookupStrategy = "stale-or-default"
		hubPolicy.DefaultTier = s.Limits.DefaultTier
	}
	if fb := s.Limits.WhenHubUnavailable; fb != nil {
		safety := compileTier(safetyTierID, Tier{Requests: fb.Allow}, "")
		svc.SafetyTier = &safety
		hubPolicy.AllowOnRateServiceError = true
	}
	svc.Failure.Hub = hubPolicy

	if h := s.Headers; h != nil {
		for k, v := range h.Add {
			svc.Transform.AddHeaders[k] = v
		}
		svc.Transform.HideHeaders = append([]string(nil), h.Hide...)
	}
	if c := s.Cache; c != nil {
		key := c.Key
		if key == "" {
			key = defaultCacheKey
		}
		svc.Cache = &types.CacheConfig{Enabled: true, TTL: c.TTL.D(), CacheKey: toInternalCacheKey.Replace(key)}
	}
	if c := s.CORS; c != nil {
		methods := c.Methods
		if len(methods) == 0 {
			methods = defaultCORSMethods
		}
		maxAge := c.MaxAge.D()
		if maxAge == 0 {
			maxAge = 5 * time.Minute
		}
		svc.CORS = &types.CORSConfig{AllowedOrigins: c.Origins, AllowedMethods: methods, AllowedHeaders: c.Headers, MaxAge: maxAge}
	}
	if a := s.Analytics; a != nil {
		if a.Enabled != nil {
			svc.Analytics.Enabled = *a.Enabled
		}
		if a.SampleRate != nil {
			svc.Analytics.SamplingRate = *a.SampleRate
		}
	}
	up := types.UpstreamFailurePolicy{Mode: "fail-closed"}
	if r := s.Retries; r != nil {
		up.MaxRetries = r.Attempts - 1
		up.RetryOnStatuses = r.Statuses
		up.RetryBackoff = r.Backoff.D()
		up.AttemptTimeout = r.PerAttemptTimeout.D()
		up.RetryNonIdempotentMethods = r.NonIdempotent
	}
	if e := s.OnUpstreamError; e != nil {
		up.Mode = e.Mode
		up.FallbackStatusCode = e.Status
		up.FallbackBody = e.Body
		up.FallbackHeaders = e.Headers
		svc.Failure.FailOpen = e.Mode == "fail-open"
	}
	svc.Failure.Upstream = up
	return svc
}

// compileLimits carries the v3 limit settings: algorithms, shadow mode,
// leasing, the org budget, additional limits and post-hoc cost.
func compileLimits(svc *types.ServiceConfig, l Limits) {
	svc.Shadow = l.Mode == "shadow"
	if p := l.Leasing; p != nil {
		svc.Lease = types.LeasePolicy{Mode: p.Mode, Horizon: p.Horizon.D(), Min: p.Min, Max: p.Max}
	}
	if o := l.Org; o != nil {
		svc.Org = &types.OrgLimit{Quota: int64(o.Requests), Window: o.Window.D(), Claim: o.Claim}
	}
	for _, a := range l.Also {
		spec := types.LimitSpec{
			ID:        a.Name,
			Per:       orDefault(a.Per, types.PerKey),
			Algorithm: internalAlgo(a.Algorithm),
			Quota:     int64(a.Requests),
			Window:    a.Window.D(),
			Shadow:    a.Mode == "shadow",
		}
		if spec.Algorithm == "token_bucket" {
			spec.Quota, spec.Rate = int64(a.Burst), a.Rate.PerSecond()
		}
		svc.Limits = append(svc.Limits, spec)
	}
	if c := l.Cost; c != nil {
		svc.Cost = &types.CostPolicy{Estimate: c.Estimate, EstimateHeader: c.EstimateHeader, ActualHeader: c.ActualHeader, OnMissing: c.OnMissing, MaxDebt: c.MaxDebt}
	}
}

// internalAlgo stores the default algorithm as "", so configs that do not
// use the others are unchanged on the wire.
func internalAlgo(a string) string {
	if a == "fixed_window" {
		return ""
	}
	return a
}

func compileTier(name string, t Tier, defaultAlgo string) types.TierConfig {
	cost := func(method string) uint8 {
		if c, ok := t.Cost[method]; ok {
			return c
		}
		if c, ok := t.Cost["*"]; ok {
			return c
		}
		return 1
	}
	tc := types.TierConfig{
		TierID:     name,
		Quota:      t.Requests,
		GetCost:    cost(http.MethodGet),
		PostCost:   cost(http.MethodPost),
		PutCost:    cost(http.MethodPut),
		DeleteCost: cost(http.MethodDelete),
		OtherCost:  cost("*"),
		Algorithm:  internalAlgo(orDefault(t.Algorithm, defaultAlgo)),
	}
	if tc.Algorithm == "token_bucket" {
		tc.Quota, tc.Rate = t.Burst, t.Rate.PerSecond()
	}
	return tc
}

func compileRuntime(r Runtime) types.RuntimePolicy {
	rp := types.RuntimePolicy{
		Hub: types.HubRuntimePolicy{
			APIKeyPattern:       r.Hub.APIKeyPattern,
			CORSAllowedHeaders:  r.Hub.CORSHeaders,
			CORSAllowedMethods:  r.Hub.CORSMethods,
			CORSPreflightMaxAge: r.Hub.CORSMaxAge.D(),
			HTTPReadTimeout:     r.Hub.ReadTimeout.D(),
			HTTPWriteTimeout:    r.Hub.WriteTimeout.D(),
			HTTPShutdownTimeout: r.Hub.ShutdownTimeout.D(),
		},
		Edge: types.EdgeRuntimePolicy{
			RateLeaseSize:           r.Edge.LeaseSize,
			RateLowWaterPct:         r.Edge.LeaseLowWaterPct,
			AnalyticsBufferSize:     r.Edge.AnalyticsBufferSize,
			AnalyticsPublishTimeout: r.Edge.AnalyticsPublishTimeout.D(),
			CacheMaxObjectBytes:     r.Edge.CacheMaxObjectBytes,
			UpstreamClientTimeout:   r.Edge.UpstreamTimeout.D(),
			UpstreamMaxIdleConns:    r.Edge.UpstreamMaxIdleConns,
			UpstreamIdleConnTimeout: r.Edge.UpstreamIdleConnTimeout.D(),
			HTTPReadTimeout:         r.Edge.ReadTimeout.D(),
			HTTPWriteTimeout:        r.Edge.WriteTimeout.D(),
			HTTPIdleTimeout:         r.Edge.IdleTimeout.D(),
		},
		Analytics: types.AnalyticsRuntimePolicy{
			NATSSubject:         r.Analytics.NATSSubject,
			NATSQueue:           r.Analytics.NATSQueue,
			DefaultEventsLimit:  r.Analytics.DefaultEventsLimit,
			MaxEventsLimit:      r.Analytics.MaxEventsLimit,
			MaxEventsOffset:     r.Analytics.MaxEventsOffset,
			DefaultSummaryLimit: r.Analytics.DefaultSummaryLimit,
			MaxSummaryLimit:     r.Analytics.MaxSummaryLimit,
			ReadTimeout:         r.Analytics.ReadTimeout.D(),
			WriteTimeout:        r.Analytics.WriteTimeout.D(),
			IdleTimeout:         r.Analytics.IdleTimeout.D(),
			ConfigFetchTimeout:  r.Analytics.ConfigFetchTimeout.D(),
			ShutdownTimeout:     r.Analytics.ShutdownTimeout.D(),
		},
	}
	return rp.Effective()
}

func sortedTierNames(m map[string]Tier) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// FromInternal converts a runtime model (for example a parsed version 1 file)
// to a version 2 File, omitting everything that equals a default.
func FromInternal(cfg *types.GatewayConfig) *File {
	f := &File{Version: Version, Runtime: runtimeFromInternal(cfg.Runtime)}
	for _, d := range cfg.Descriptors {
		f.Descriptors = append(f.Descriptors, DescriptorDomain{Domain: d.Domain, Entries: entriesFromInternal(d.Entries)})
	}
	for _, p := range cfg.Prefixes {
		group := strings.Trim(p.Prefix, "/")
		for _, svc := range p.Services {
			f.Services = append(f.Services, serviceFromInternal(group, p, svc))
		}
	}
	return f
}

func serviceFromInternal(group string, p types.PrefixConfig, svc types.ServiceConfig) Service {
	s := Service{
		Name:           svc.ServiceID,
		Match:          Match{PathPrefix: "/" + group + "/" + svc.ServiceID, Methods: svc.Methods, Host: svc.Host},
		Upstream:       svc.TargetURL,
		LoadBalancing:  svc.LoadBalancing,
		Timeout:        Duration(svc.Timeout),
		MaxRequestBody: svc.MaxBodyBytes,
		Limits:         Limits{Window: Duration(p.QuotaPeriod), LeaseSize: p.Leasing.LeaseQuantum, Tiers: map[string]Tier{}, MaxConcurrent: svc.MaxConcurrent},
	}
	if len(svc.Upstreams) > 0 {
		s.Upstream, s.Upstreams = "", svc.Upstreams
	}
	if cb := svc.CircuitBreaker; cb != nil && !(len(svc.Upstreams) > 1 && cb.Failures == 5 && cb.Cooldown == 10*time.Second) {
		s.CircuitBreaker = &CircuitBreaker{Failures: cb.Failures, Cooldown: Duration(cb.Cooldown)}
	}
	if svc.Auth.Type != "" && svc.Auth.Type != "api_key" {
		s.Auth = &Auth{Type: svc.Auth.Type}
		if j := svc.Auth.JWT; j != nil {
			s.Auth.JWT = &JWT{JWKSURL: j.JWKSURL, Issuer: j.Issuer, Audience: j.Audience, TierClaim: j.TierClaim}
		}
	}
	if !svc.Transform.StripPrefix {
		f := false
		s.StripPathPrefix = &f
	}
	if len(svc.Transform.AddHeaders) > 0 || len(svc.Transform.HideHeaders) > 0 {
		s.Headers = &Headers{Add: svc.Transform.AddHeaders, Hide: svc.Transform.HideHeaders}
	}
	for _, t := range svc.Tiers {
		s.Limits.Tiers[t.TierID] = tierFromInternal(t)
	}
	limitsFromInternal(&s.Limits, svc)
	hp := svc.Failure.EffectiveHubPolicy()
	if hp.TierLookupStrategy != "fail-closed" && hp.DefaultTier != "" {
		s.Limits.DefaultTier = hp.DefaultTier
	}
	if hp.AllowOnRateServiceError && svc.SafetyTier != nil {
		s.Limits.WhenHubUnavailable = &Fallback{Allow: svc.SafetyTier.Quota}
	}
	if c := svc.Cache; c != nil && c.Enabled {
		key := strings.NewReplacer("$method", "{method}", "$path", "{path}", "$query", "{query}", "$key", "{key}", "$enc", "{encoding}",
			"$METHOD", "{method}", "$PATH", "{path}", "$QUERY", "{query}", "$KEY", "{key}", "$ENC", "{encoding}").Replace(c.CacheKey)
		if key == defaultCacheKey {
			key = ""
		}
		s.Cache = &Cache{TTL: Duration(c.TTL), Key: key}
	}
	if c := svc.CORS; c != nil {
		s.CORS = &CORS{Origins: c.AllowedOrigins, Headers: c.AllowedHeaders}
		if !equalStrings(c.AllowedMethods, defaultCORSMethods) {
			s.CORS.Methods = c.AllowedMethods
		}
		if c.MaxAge != 5*time.Minute {
			s.CORS.MaxAge = Duration(c.MaxAge)
		}
	}
	if !svc.Analytics.Enabled {
		f := false
		s.Analytics = &Analytics{Enabled: &f}
	} else if svc.Analytics.SamplingRate != 1 {
		r := svc.Analytics.SamplingRate
		s.Analytics = &Analytics{SampleRate: &r}
	}
	up := svc.Failure.Upstream
	if up.MaxRetries > 0 {
		s.Retries = &Retries{
			Attempts: up.MaxRetries + 1, Statuses: up.RetryOnStatuses, Backoff: Duration(up.RetryBackoff),
			PerAttemptTimeout: Duration(up.AttemptTimeout), NonIdempotent: up.RetryNonIdempotentMethods,
		}
	}
	if eff := svc.Failure.EffectiveUpstreamPolicy(); eff.Mode == "fail-open" {
		s.OnUpstreamError = &UpstreamError{Mode: "fail-open", Status: up.FallbackStatusCode, Body: up.FallbackBody, Headers: up.FallbackHeaders}
	}
	return s
}

func limitsFromInternal(l *Limits, svc types.ServiceConfig) {
	if svc.Shadow {
		l.Mode = "shadow"
	}
	if p := svc.Lease; p != (types.LeasePolicy{}) {
		l.Leasing = &Leasing{Mode: p.Mode, Horizon: Duration(p.Horizon), Min: p.Min, Max: p.Max}
	}
	if o := svc.Org; o != nil {
		l.Org = &OrgLimit{Requests: uint32(o.Quota), Window: Duration(o.Window), Claim: o.Claim}
	}
	for _, spec := range svc.Limits {
		a := ExtraLimit{Name: spec.ID, Algorithm: spec.Algorithm, Requests: uint32(spec.Quota), Window: Duration(spec.Window)}
		if spec.Per != types.PerKey {
			a.Per = spec.Per
		}
		if spec.Algorithm == "token_bucket" {
			a.Requests, a.Burst, a.Rate = 0, uint32(spec.Quota), Rate(spec.Rate)
		}
		if spec.Shadow {
			a.Mode = "shadow"
		}
		l.Also = append(l.Also, a)
	}
	if c := svc.Cost; c != nil {
		l.Cost = &Cost{Estimate: c.Estimate, EstimateHeader: c.EstimateHeader, ActualHeader: c.ActualHeader, OnMissing: c.OnMissing, MaxDebt: c.MaxDebt}
	}
}

func tierFromInternal(t types.TierConfig) Tier {
	out := Tier{Requests: t.Quota, Algorithm: t.Algorithm}
	if t.Algorithm == "token_bucket" {
		out.Requests, out.Burst, out.Rate = 0, t.Quota, Rate(t.Rate)
	}
	add := func(m string, c uint8) {
		if c > 1 {
			if out.Cost == nil {
				out.Cost = map[string]uint8{}
			}
			out.Cost[m] = c
		}
	}
	add("GET", t.GetCost)
	add("POST", t.PostCost)
	add("PUT", t.PutCost)
	add("DELETE", t.DeleteCost)
	add("*", t.OtherCost)
	return out
}

// runtimeFromInternal keeps only settings that differ from the defaults.
func runtimeFromInternal(rp types.RuntimePolicy) Runtime {
	d := types.DefaultRuntimePolicy()
	rp = rp.Effective()
	str := func(v, def string) string {
		if v == def {
			return ""
		}
		return v
	}
	dur := func(v, def time.Duration) Duration {
		if v == def {
			return 0
		}
		return Duration(v)
	}
	num := func(v, def int64) int64 {
		if v == def {
			return 0
		}
		return v
	}
	strs := func(v, def []string) []string {
		if equalStrings(v, def) {
			return nil
		}
		return v
	}
	var r Runtime
	r.Hub = HubRuntime{
		APIKeyPattern:   str(rp.Hub.APIKeyPattern, d.Hub.APIKeyPattern),
		CORSHeaders:     strs(rp.Hub.CORSAllowedHeaders, d.Hub.CORSAllowedHeaders),
		CORSMethods:     strs(rp.Hub.CORSAllowedMethods, d.Hub.CORSAllowedMethods),
		CORSMaxAge:      dur(rp.Hub.CORSPreflightMaxAge, d.Hub.CORSPreflightMaxAge),
		ReadTimeout:     dur(rp.Hub.HTTPReadTimeout, d.Hub.HTTPReadTimeout),
		WriteTimeout:    dur(rp.Hub.HTTPWriteTimeout, d.Hub.HTTPWriteTimeout),
		ShutdownTimeout: dur(rp.Hub.HTTPShutdownTimeout, d.Hub.HTTPShutdownTimeout),
	}
	r.Edge = EdgeRuntime{
		LeaseSize:               num(rp.Edge.RateLeaseSize, d.Edge.RateLeaseSize),
		AnalyticsBufferSize:     int(num(int64(rp.Edge.AnalyticsBufferSize), int64(d.Edge.AnalyticsBufferSize))),
		AnalyticsPublishTimeout: dur(rp.Edge.AnalyticsPublishTimeout, d.Edge.AnalyticsPublishTimeout),
		CacheMaxObjectBytes:     num(rp.Edge.CacheMaxObjectBytes, d.Edge.CacheMaxObjectBytes),
		UpstreamTimeout:         dur(rp.Edge.UpstreamClientTimeout, d.Edge.UpstreamClientTimeout),
		UpstreamMaxIdleConns:    int(num(int64(rp.Edge.UpstreamMaxIdleConns), int64(d.Edge.UpstreamMaxIdleConns))),
		UpstreamIdleConnTimeout: dur(rp.Edge.UpstreamIdleConnTimeout, d.Edge.UpstreamIdleConnTimeout),
		ReadTimeout:             dur(rp.Edge.HTTPReadTimeout, d.Edge.HTTPReadTimeout),
		WriteTimeout:            dur(rp.Edge.HTTPWriteTimeout, d.Edge.HTTPWriteTimeout),
		IdleTimeout:             dur(rp.Edge.HTTPIdleTimeout, d.Edge.HTTPIdleTimeout),
	}
	if rp.Edge.RateLowWaterPct != d.Edge.RateLowWaterPct {
		r.Edge.LeaseLowWaterPct = rp.Edge.RateLowWaterPct
	}
	r.Analytics = AnalyticsRuntime{
		NATSSubject:         str(rp.Analytics.NATSSubject, d.Analytics.NATSSubject),
		NATSQueue:           str(rp.Analytics.NATSQueue, d.Analytics.NATSQueue),
		DefaultEventsLimit:  num(rp.Analytics.DefaultEventsLimit, d.Analytics.DefaultEventsLimit),
		MaxEventsLimit:      num(rp.Analytics.MaxEventsLimit, d.Analytics.MaxEventsLimit),
		MaxEventsOffset:     num(rp.Analytics.MaxEventsOffset, d.Analytics.MaxEventsOffset),
		DefaultSummaryLimit: num(rp.Analytics.DefaultSummaryLimit, d.Analytics.DefaultSummaryLimit),
		MaxSummaryLimit:     num(rp.Analytics.MaxSummaryLimit, d.Analytics.MaxSummaryLimit),
		ReadTimeout:         dur(rp.Analytics.ReadTimeout, d.Analytics.ReadTimeout),
		WriteTimeout:        dur(rp.Analytics.WriteTimeout, d.Analytics.WriteTimeout),
		IdleTimeout:         dur(rp.Analytics.IdleTimeout, d.Analytics.IdleTimeout),
		ConfigFetchTimeout:  dur(rp.Analytics.ConfigFetchTimeout, d.Analytics.ConfigFetchTimeout),
		ShutdownTimeout:     dur(rp.Analytics.ShutdownTimeout, d.Analytics.ShutdownTimeout),
	}
	return r
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
