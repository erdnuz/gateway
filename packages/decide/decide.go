// Package decide is Gate's rate limiter as a service: callers that are not
// the Gate gateway (Envoy, Istio, queue consumers, batch jobs) describe a
// request with descriptors and get a decision from the same leased, global
// quota the gateway uses.
//
// Descriptors follow Envoy's rate-limit service: a domain, and a list of
// key/value entries matched against a tree of configured entries. An entry
// with a value matches only that value; an entry without one gives every
// value its own limit.
package decide

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"gateway/packages/common/types"
)

// Scope is the ledger scope of descriptor limits: lease requests for them
// name it as their prefix and the domain as their service.
const Scope = "_rls"

// Entry is one key/value pair of a descriptor.
type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Match is a descriptor resolved against config.
type Match struct {
	// LimitID names the matched entry by its path, e.g.
	// "generic_key=checkout/user_id"; the hub resolves the spec from it.
	LimitID string
	// Subject identifies the counter: one per combination of values of the
	// entries without a configured value.
	Subject string
	Spec    types.LimitSpec
}

// Resolve matches a descriptor against a domain's entries. It reports false
// when no configured entry matches the whole descriptor or the matching
// entry has no limit (the descriptor is then not limited).
func Resolve(cfg *types.GatewayConfig, domain string, entries []Entry) (Match, bool) {
	d := findDomain(cfg, domain)
	if d == nil || len(entries) == 0 {
		return Match{}, false
	}
	level := d.Entries
	var node *types.DescriptorEntry
	var path, values []string
	for _, e := range entries {
		node = nil
		for i := range level {
			if level[i].Key == e.Key && level[i].Value == e.Value {
				node = &level[i] // an exact value wins over a wildcard
				break
			}
		}
		if node == nil {
			for i := range level {
				if level[i].Key == e.Key && level[i].Value == "" {
					node = &level[i]
					values = append(values, e.Value)
					break
				}
			}
		}
		if node == nil {
			return Match{}, false
		}
		path = append(path, segment(node))
		level = node.Entries
	}
	if node.Limit == nil {
		return Match{}, false
	}
	return Match{LimitID: strings.Join(path, "/"), Subject: subject(values), Spec: *node.Limit}, true
}

// Find returns the limit configured at a LimitID path (for the hub, which
// never trusts a caller's quota).
func Find(cfg *types.GatewayConfig, domain, limitID string) (*types.LimitSpec, bool) {
	d := findDomain(cfg, domain)
	if d == nil || limitID == "" {
		return nil, false
	}
	level := d.Entries
	var node *types.DescriptorEntry
	for _, seg := range strings.Split(limitID, "/") {
		key, value, _ := strings.Cut(seg, "=")
		node = nil
		for i := range level {
			if level[i].Key == key && level[i].Value == value {
				node = &level[i]
				break
			}
		}
		if node == nil {
			return nil, false
		}
		level = node.Entries
	}
	if node.Limit == nil {
		return nil, false
	}
	return node.Limit, true
}

func findDomain(cfg *types.GatewayConfig, domain string) *types.DescriptorDomain {
	if cfg == nil {
		return nil
	}
	for i := range cfg.Descriptors {
		if cfg.Descriptors[i].Domain == domain {
			return &cfg.Descriptors[i]
		}
	}
	return nil
}

func segment(e *types.DescriptorEntry) string {
	if e.Value == "" {
		return e.Key
	}
	return e.Key + "=" + e.Value
}

// subject hashes the variable values, so arbitrary caller-supplied strings
// never reach ledger keys.
func subject(values []string) string {
	if len(values) == 0 {
		return "_all"
	}
	sum := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return "v" + hex.EncodeToString(sum[:16])
}
