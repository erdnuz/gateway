// Package appkit holds small helpers shared by the service entrypoints.
package appkit

import "strings"

// ListenAddr accepts "8080", ":8080" or "host:8080" and returns a
// net.Listen address. An empty value stays empty.
func ListenAddr(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || strings.Contains(v, ":") {
		return v
	}
	return ":" + v
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
