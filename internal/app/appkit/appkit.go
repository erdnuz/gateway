// Package appkit holds small helpers shared by the service entrypoints.
package appkit

import (
	"net"
	"strings"
)

// ListenAddr accepts "8080", ":8080" or "host:8080" and returns a
// net.Listen address. An empty value stays empty.
func ListenAddr(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || strings.Contains(v, ":") {
		return v
	}
	return ":" + v
}

// IsLoopback reports whether a host:port address names only the local
// machine ("localhost", 127.0.0.0/8 or ::1). An empty host, which listens
// on every interface, is not loopback.
func IsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// SplitCSV splits a comma-separated list, dropping blanks.
func SplitCSV(value string) []string {
	var out []string
	for _, p := range strings.Split(value, ",") {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}
