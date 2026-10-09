// Package keys defines Gate's API key format and how keys are identified.
//
// A key is shown to its owner once ("gk_<id>_<secret>"). Everything after
// the edge (hub, Redis, leases, logs) sees only its SHA-256 hash, so a leak
// of any of those reveals no usable key. Keys chosen by an operator through
// the hub's /tiers API are hashed the same way.
package keys

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const prefix = "gk_"

// Hash identifies a key everywhere past the edge.
func Hash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// Short is a log-safe fingerprint of a hashed key.
func Short(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
}

// New issues a key: a public ID (shown in listings and audit logs) and the
// full secret key, which is returned to the caller exactly once.
func New() (id, secret string) {
	id = randomHex(6)
	return id, prefix + id + "_" + randomHex(20)
}

// Rotate keeps a key's ID and issues a new secret for it.
func Rotate(id string) string { return prefix + id + "_" + randomHex(20) }

// ID returns the public ID embedded in an issued key, or "".
func ID(raw string) string {
	rest, ok := strings.CutPrefix(raw, prefix)
	if !ok {
		return ""
	}
	id, _, ok := strings.Cut(rest, "_")
	if !ok {
		return ""
	}
	return id
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
