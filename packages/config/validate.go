package config

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	nameRe        = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	segmentRe     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	tierRe        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
	cacheTokenRe  = regexp.MustCompile(`\{([a-z]+)\}`)
	cacheTokens   = map[string]bool{"method": true, "path": true, "query": true, "key": true, "encoding": true}
	costMethods   = map[string]bool{"GET": true, "POST": true, "PUT": true, "DELETE": true, "*": true}
	upstreamModes = map[string]bool{"fail-closed": true, "fail-open": true}
	balancers     = map[string]bool{"": true, "round_robin": true, "least_requests": true}
	authTypes     = map[string]bool{"api_key": true, "jwt": true, "none": true}
	httpMethods   = map[string]bool{"GET": true, "HEAD": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "OPTIONS": true}
)

// group returns the first path segment of a service's path prefix.
func (s Service) group() string {
	parts := strings.Split(strings.Trim(s.Match.PathPrefix, "/"), "/")
	return parts[0]
}

func (p *parser) validate(f *File) {
	if f.Version != Version {
		p.at("version", "must be %d (got %d)", Version, f.Version)
	}
	if len(f.Services) == 0 && len(f.Descriptors) == 0 {
		p.at("services", "at least one service (or a descriptors domain for gate limiter) is required")
	}
	p.validateDescriptors(f.Descriptors)

	names := map[string]int{}
	prefixes := map[string]int{}
	type groupInfo struct {
		first  int
		window Duration
		lease  int64
	}
	groups := map[string]groupInfo{}

	for i, s := range f.Services {
		path := fmt.Sprintf("services[%d]", i)
		switch {
		case s.Name == "":
			p.at(path+".name", "is required")
		case !nameRe.MatchString(s.Name):
			p.at(path+".name", "%q must be lowercase letters, digits and dashes", s.Name)
		default:
			if j, dup := names[s.Name]; dup {
				p.at(path+".name", "%q is already used by services[%d]", s.Name, j)
			}
			names[s.Name] = i
		}

		pp := s.Match.PathPrefix
		segs := strings.Split(strings.Trim(pp, "/"), "/")
		switch {
		case pp == "":
			p.at(path+".match.path_prefix", "is required, e.g. /v1/%s", orDefault(s.Name, "orders"))
		case !strings.HasPrefix(pp, "/") || len(segs) != 2 || !segmentRe.MatchString(segs[0]) || !segmentRe.MatchString(segs[1]):
			p.at(path+".match.path_prefix", "%q must be two path segments, /<group>/<service>", pp)
		case s.Name != "" && segs[1] != s.Name:
			p.at(path+".match.path_prefix", "%q must end with the service name (/%s/%s)", pp, segs[0], s.Name)
		default:
			if j, dup := prefixes[pp]; dup {
				p.at(path+".match.path_prefix", "%q is already used by services[%d]", pp, j)
			}
			prefixes[pp] = i
		}

		p.validateUpstreams(path, s)
		p.validateAuth(path, s)
		for j, m := range s.Match.Methods {
			if !httpMethods[m] {
				p.at(fmt.Sprintf("%s.match.methods[%d]", path, j), "%q is not an HTTP method (use upper case, e.g. GET)", m)
			}
		}
		if s.Limits.MaxConcurrent < 0 {
			p.at(path+".limits.max_concurrent", "must not be negative")
		}
		if s.MaxRequestBody < 0 {
			p.at(path+".max_request_body", "must not be negative")
		}

		p.validateLimits(path+".limits", s.Limits)
		if pp != "" && len(segs) == 2 && s.Limits.Window > 0 {
			g := s.group()
			if info, seen := groups[g]; !seen {
				groups[g] = groupInfo{first: i, window: s.Limits.Window, lease: s.Limits.LeaseSize}
			} else {
				// The runtime keeps one quota window and lease size per group.
				if info.window != s.Limits.Window {
					p.at(path+".limits.window", "%s differs from services[%d] (%s); services under /%s must share a window", s.Limits.Window, info.first, info.window, g)
				}
				if info.lease != s.Limits.LeaseSize {
					p.at(path+".limits.lease_size", "%d differs from services[%d] (%d); services under /%s must share a lease size", s.Limits.LeaseSize, info.first, info.lease, g)
				}
			}
		}

		if c := s.Cache; c != nil {
			if c.TTL <= 0 {
				p.at(path+".cache.ttl", "is required, e.g. 30s")
			}
			for _, m := range cacheTokenRe.FindAllStringSubmatch(c.Key, -1) {
				if !cacheTokens[m[1]] {
					p.at(path+".cache.key", "unknown token {%s} (use {method} {path} {query} {key} {encoding})", m[1])
				}
			}
		}
		if r := s.Retries; r != nil {
			if r.Attempts < 1 {
				p.at(path+".retries.attempts", "must be at least 1 (1 = no retry)")
			}
			for j, code := range r.Statuses {
				if code < 100 || code > 599 {
					p.at(fmt.Sprintf("%s.retries.statuses[%d]", path, j), "%d is not an HTTP status code", code)
				}
			}
		}
		if c := s.CORS; c != nil && len(c.Origins) == 0 {
			p.at(path+".cors.origins", "list at least one origin, or [\"*\"]")
		}
		if a := s.Analytics; a != nil && a.SampleRate != nil && (*a.SampleRate < 0 || *a.SampleRate > 1) {
			p.at(path+".analytics.sample_rate", "must be between 0 and 1")
		}
		if e := s.OnUpstreamError; e != nil {
			if !upstreamModes[e.Mode] {
				p.at(path+".on_upstream_error.mode", "must be fail-closed or fail-open (got %q)", e.Mode)
			}
			if e.Status != 0 && (e.Status < 100 || e.Status > 599) {
				p.at(path+".on_upstream_error.status", "%d is not an HTTP status code", e.Status)
			}
		}
	}

	if pat := f.Runtime.Hub.APIKeyPattern; pat != "" {
		if _, err := regexp.Compile(pat); err != nil {
			p.at("runtime.hub.api_key_pattern", "invalid regular expression: %v", err)
		}
	}
	if v := f.Runtime.Edge.LeaseLowWaterPct; v < 0 || v > 100 {
		p.at("runtime.edge.lease_low_water_pct", "must be between 0 and 100")
	}
}

func (p *parser) validateUpstreams(path string, s Service) {
	switch {
	case s.Upstream == "" && len(s.Upstreams) == 0:
		p.at(path+".upstream", "is required, e.g. http://%s:8080 (or upstreams: [...] for replicas)", orDefault(s.Name, "orders"))
		return
	case s.Upstream != "" && len(s.Upstreams) > 0:
		p.at(path+".upstreams", "set upstream or upstreams, not both")
		return
	}
	check := func(field, raw string) *url.URL {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			p.at(field, "%q must be an http(s) URL with a host", raw)
			return nil
		}
		return u
	}
	if s.Upstream != "" {
		check(path+".upstream", s.Upstream)
	}
	basePath := ""
	for i, raw := range s.Upstreams {
		u := check(fmt.Sprintf("%s.upstreams[%d]", path, i), raw)
		if u == nil {
			continue
		}
		if i == 0 {
			basePath = strings.TrimSuffix(u.Path, "/")
		} else if strings.TrimSuffix(u.Path, "/") != basePath {
			p.at(fmt.Sprintf("%s.upstreams[%d]", path, i), "must have the same path as upstreams[0] (%q) so retries can move between replicas", basePath)
		}
	}
	if !balancers[s.LoadBalancing] {
		p.at(path+".load_balancing", "must be round_robin or least_requests (got %q)", s.LoadBalancing)
	}
	if cb := s.CircuitBreaker; cb != nil {
		if cb.Failures < 1 {
			p.at(path+".circuit_breaker.failures", "must be at least 1")
		}
		if cb.Cooldown <= 0 {
			p.at(path+".circuit_breaker.cooldown", "is required, e.g. 10s")
		}
	}
}

func (p *parser) validateAuth(path string, s Service) {
	a := s.Auth
	if a == nil {
		return
	}
	if !authTypes[a.Type] {
		p.at(path+".auth.type", "must be api_key, jwt or none (got %q)", a.Type)
		return
	}
	hasDefault := s.Limits.DefaultTier != "" || len(s.Limits.Tiers) == 1
	switch a.Type {
	case "jwt":
		if a.JWT == nil || a.JWT.JWKSURL == "" {
			p.at(path+".auth.jwt.jwks_url", "is required for jwt auth")
		} else if u, err := url.Parse(a.JWT.JWKSURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			p.at(path+".auth.jwt.jwks_url", "%q must be an http(s) URL", a.JWT.JWKSURL)
		}
		if (a.JWT == nil || a.JWT.TierClaim == "") && !hasDefault {
			p.at(path+".limits.default_tier", "is required for jwt auth without auth.jwt.tier_claim")
		}
	case "none":
		if !hasDefault {
			p.at(path+".limits.default_tier", "is required for auth type none (callers are limited by client IP on that tier)")
		}
	}
	if a.Type != "jwt" && a.JWT != nil {
		p.at(path+".auth.jwt", "only applies to auth type jwt")
	}
}

func (p *parser) validateLimits(path string, l Limits) {
	if l.Window <= 0 {
		p.at(path+".window", "is required, e.g. 1m")
	}
	if len(l.Tiers) == 0 {
		p.at(path+".tiers", "define at least one tier, e.g. free: {requests: 100}")
	}
	names := make([]string, 0, len(l.Tiers))
	for name := range l.Tiers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t := l.Tiers[name]
		tp := path + ".tiers." + name
		if !tierRe.MatchString(name) {
			p.at(tp, "tier name %q must be letters, digits, dashes or underscores", name)
		}
		p.validateBudget(tp, orDefault(t.Algorithm, l.Algorithm), t.Requests, t.Rate, t.Burst)
		if t.Algorithm != "" && !algorithms[t.Algorithm] {
			p.at(tp+".algorithm", "must be fixed_window, sliding_window or token_bucket (got %q)", t.Algorithm)
		}
		for m, c := range t.Cost {
			if !costMethods[m] {
				p.at(tp+".cost."+m, "unknown method %q (use GET, POST, PUT, DELETE or *)", m)
			}
			if c == 0 {
				p.at(tp+".cost."+m, "must be at least 1")
			}
		}
	}
	if l.DefaultTier != "" {
		if _, ok := l.Tiers[l.DefaultTier]; !ok {
			p.at(path+".default_tier", "%q is not one of the tiers (%s)", l.DefaultTier, strings.Join(names, ", "))
		}
	}
	if fb := l.WhenHubUnavailable; fb != nil && fb.Allow == 0 {
		p.at(path+".when_hub_unavailable.allow", "must be greater than 0 (omit when_hub_unavailable to reject with 503)")
	}
	if l.LeaseSize < 0 {
		p.at(path+".lease_size", "must not be negative")
	}
	p.validateV3Limits(path, l, names)
}

var (
	algorithms = map[string]bool{"fixed_window": true, "sliding_window": true, "token_bucket": true}
	modes      = map[string]bool{"": true, "enforce": true, "shadow": true}
	subjects   = map[string]bool{"": true, "key": true, "client_ip": true, "service": true}
)

// validateBudget checks a tier's or limit's size for its algorithm: a token
// bucket has a rate and a burst; the windowed algorithms have requests.
func (p *parser) validateBudget(path, algo string, requests uint32, rate Rate, burst uint32) {
	if algo == "token_bucket" {
		if rate <= 0 {
			p.at(path+".rate", "is required for token_bucket, e.g. 20/s")
		}
		if burst == 0 {
			p.at(path+".burst", "is required for token_bucket (the most admitted at once)")
		}
		if requests != 0 {
			p.at(path+".requests", "does not apply to token_bucket; use rate and burst")
		}
		return
	}
	if requests == 0 {
		p.at(path+".requests", "must be greater than 0")
	}
	if rate != 0 || burst != 0 {
		p.at(path, "rate and burst apply only to algorithm token_bucket")
	}
}

func (p *parser) validateV3Limits(path string, l Limits, tiers []string) {
	if l.Algorithm != "" && !algorithms[l.Algorithm] {
		p.at(path+".algorithm", "must be fixed_window, sliding_window or token_bucket (got %q)", l.Algorithm)
	}
	if !modes[l.Mode] {
		p.at(path+".mode", "must be enforce or shadow (got %q)", l.Mode)
	}
	if lp := l.Leasing; lp != nil {
		if lp.Mode != "" && lp.Mode != "adaptive" && lp.Mode != "fixed" {
			p.at(path+".leasing.mode", "must be adaptive or fixed (got %q)", lp.Mode)
		}
		if lp.Min < 0 || lp.Max < 0 {
			p.at(path+".leasing", "min and max must not be negative")
		}
		if lp.Max > 0 && lp.Min > lp.Max {
			p.at(path+".leasing.min", "%d is more than max (%d)", lp.Min, lp.Max)
		}
	}
	if o := l.Org; o != nil {
		if o.Requests == 0 {
			p.at(path+".org.requests", "must be greater than 0")
		}
		if o.Window != 0 && o.Window < Duration(time.Second) {
			p.at(path+".org.window", "must be at least 1s")
		}
	}
	seen := map[string]int{}
	for _, t := range tiers {
		seen[t] = -1
	}
	for i, a := range l.Also {
		ap := fmt.Sprintf("%s.also[%d]", path, i)
		if !nameRe.MatchString(a.Name) {
			p.at(ap+".name", "%q must be lowercase letters, digits and dashes", a.Name)
		} else if j, dup := seen[a.Name]; dup {
			if j >= 0 {
				p.at(ap+".name", "%q is already used by also[%d]", a.Name, j)
			} else {
				p.at(ap+".name", "%q is already the name of a tier", a.Name)
			}
		}
		seen[a.Name] = i
		if !subjects[a.Per] {
			p.at(ap+".per", "must be key, client_ip or service (got %q)", a.Per)
		}
		if a.Algorithm != "" && !algorithms[a.Algorithm] {
			p.at(ap+".algorithm", "must be fixed_window, sliding_window or token_bucket (got %q)", a.Algorithm)
		}
		if !modes[a.Mode] {
			p.at(ap+".mode", "must be enforce or shadow (got %q)", a.Mode)
		}
		if a.Window != 0 && a.Window < Duration(time.Second) {
			p.at(ap+".window", "must be at least 1s")
		}
		p.validateBudget(ap, a.Algorithm, a.Requests, a.Rate, a.Burst)
	}
	if g := l.Global; g != nil {
		var sum float64
		for name, f := range g.Floors {
			if !nameRe.MatchString(name) {
				p.at(path+".global.floors."+name, "region name %q must be lowercase letters, digits and dashes", name)
			}
			if f < 0 || f > 1 {
				p.at(path+".global.floors."+name, "must be between 0 and 1")
			}
			sum += f
		}
		if len(g.Floors) == 0 {
			p.at(path+".global.floors", "list every region, e.g. {eu-west: 0.3, us-east: 0.3}")
		}
		if sum > 1 {
			p.at(path+".global.floors", "shares sum to %.2f; they must sum to at most 1", sum)
		}
		for _, name := range tiers {
			if a := orDefault(l.Tiers[name].Algorithm, l.Algorithm); a != "" && a != "fixed_window" {
				p.at(path+".tiers."+name+".algorithm", "global quotas support fixed_window only")
			}
		}
		if l.Org != nil {
			p.at(path+".org", "an org budget cannot be combined with global (multi-region) quotas")
		}
	}
	if c := l.Cost; c != nil {
		if strings.TrimSpace(c.ActualHeader) == "" {
			p.at(path+".cost.actual_header", "is required: the response header (or trailer) carrying the actual cost")
		}
		if c.OnMissing != "" && c.OnMissing != "estimate" && c.OnMissing != "refund" {
			p.at(path+".cost.on_missing", "must be estimate or refund (got %q)", c.OnMissing)
		}
		if c.Estimate < 0 || c.MaxDebt < 0 {
			p.at(path+".cost", "estimate and max_debt must not be negative")
		}
	}
}

func (p *parser) validateDescriptors(ds []DescriptorDomain) {
	domains := map[string]int{}
	for i, d := range ds {
		path := fmt.Sprintf("descriptors[%d]", i)
		if d.Domain == "" {
			p.at(path+".domain", "is required")
		} else if j, dup := domains[d.Domain]; dup {
			p.at(path+".domain", "%q is already used by descriptors[%d]", d.Domain, j)
		}
		domains[d.Domain] = i
		if len(d.Entries) == 0 {
			p.at(path+".entries", "list at least one entry, e.g. {key: remote_address, limit: {requests: 100, window: 1s}}")
		}
		p.validateEntries(path+".entries", d.Entries)
	}
}

func (p *parser) validateEntries(path string, es []DescriptorEntry) {
	seen := map[string]int{}
	for i, e := range es {
		ep := fmt.Sprintf("%s[%d]", path, i)
		switch {
		case e.Key == "":
			p.at(ep+".key", "is required")
		case strings.ContainsAny(e.Key+e.Value, "/="):
			p.at(ep, "key and value must not contain / or =")
		default:
			if j, dup := seen[e.Key+"="+e.Value]; dup {
				p.at(ep, "duplicates %s[%d]", path, j)
			}
			seen[e.Key+"="+e.Value] = i
		}
		if !modes[e.Mode] {
			p.at(ep+".mode", "must be enforce or shadow (got %q)", e.Mode)
		}
		if l := e.Limit; l != nil {
			if l.Algorithm != "" && !algorithms[l.Algorithm] {
				p.at(ep+".limit.algorithm", "must be fixed_window, sliding_window or token_bucket (got %q)", l.Algorithm)
			}
			p.validateBudget(ep+".limit", l.Algorithm, l.Requests, l.Rate, l.Burst)
			if l.Algorithm != "token_bucket" && l.Window < Duration(time.Second) {
				p.at(ep+".limit.window", "is required and at least 1s, e.g. 1m")
			}
		} else if len(e.Entries) == 0 {
			p.at(ep, "has neither a limit nor nested entries")
		}
		p.validateEntries(ep+".entries", e.Entries)
	}
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
