// Package httpx holds the HTTP conventions every Gate service shares: RFC 9457
// problem responses, request IDs, and log-safe API key fingerprints.
package httpx

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
)

// Problem is an RFC 9457 problem details body. Code is a stable,
// machine-readable identifier (e.g. "rate_limited") clients can switch on.
type Problem struct {
	Type      string `json:"type,omitempty"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail,omitempty"`
	Code      string `json:"code"`
	RequestID string `json:"request_id,omitempty"`
}

// WriteProblem writes an application/problem+json response.
func WriteProblem(w http.ResponseWriter, r *http.Request, status int, code, detail string) {
	p := Problem{Title: http.StatusText(status), Status: status, Detail: detail, Code: code}
	if r != nil {
		p.RequestID = RequestID(r.Context())
	}
	h := w.Header()
	h.Set("Content-Type", "application/problem+json")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Del("Content-Length")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}

// Error writes a problem whose code is derived from the status; use
// WriteProblem when clients need a more specific code.
func Error(w http.ResponseWriter, r *http.Request, status int, detail string) {
	code, ok := statusCodes[status]
	if !ok {
		code = strings.ReplaceAll(strings.ToLower(http.StatusText(status)), " ", "_")
	}
	WriteProblem(w, r, status, code, detail)
}

var statusCodes = map[int]string{
	http.StatusBadRequest:          "bad_request",
	http.StatusInternalServerError: "internal",
	http.StatusServiceUnavailable:  "unavailable",
}

// WriteJSON writes v as a JSON response.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// HeaderRequestID carries the request ID between clients, Gate and upstreams.
const HeaderRequestID = "X-Request-Id"

type requestIDKey struct{}

// WithRequestID accepts a sane inbound X-Request-Id or generates one, echoes
// it on the response, and stores it in the request context.
func WithRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderRequestID)
		if !validRequestID(id) {
			id = newRequestID()
			r.Header.Set(HeaderRequestID, id)
		}
		w.Header().Set(HeaderRequestID, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}

// RequestID returns the request ID stored by WithRequestID, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// Logger returns slog.Default with the request ID attached.
func Logger(ctx context.Context) *slog.Logger {
	if id := RequestID(ctx); id != "" {
		return slog.With("request_id", id)
	}
	return slog.Default()
}

func validRequestID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if c <= ' ' || c > '~' {
			return false
		}
	}
	return true
}

func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// RedactKey returns a log-safe fingerprint of an API key.
func RedactKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return "sha256:" + hex.EncodeToString(sum[:6])
}

// BearerToken returns the token of an "Authorization: Bearer" header.
func BearerToken(r *http.Request) (string, bool) {
	return strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
}
