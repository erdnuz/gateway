package devstack

import (
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// EchoHandler is a demo upstream that describes the request it received, so
// the effect of the gateway (stripped prefix, forwarded headers, removed API
// key) is visible in the response.
func EchoHandler(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		headers := map[string]string{}
		names := make([]string, 0, len(r.Header))
		for k := range r.Header {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			headers[k] = strings.Join(r.Header.Values(k), ", ")
		}
		resp := map[string]any{
			"upstream": name,
			"method":   r.Method,
			"path":     r.URL.Path,
			"query":    r.URL.RawQuery,
			"headers":  headers,
			"time":     time.Now().UTC().Format(time.RFC3339Nano),
		}
		if len(body) > 0 {
			resp["body"] = string(body)
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(resp)
	})
}
