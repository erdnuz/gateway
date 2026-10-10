package edge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gateway/packages/common/httpx"
	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"
)

// newProxy builds the reverse proxy shared by every service. ReverseProxy
// handles hop-by-hop headers, streaming, trailers and protocol upgrades;
// Gate adds routing (rewrite), retries (retryTransport) and the service's
// failure policy (proxyError).
func (s *Server) newProxy(transport http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite:        rewrite,
		Transport:      retryTransport{base: transport},
		ModifyResponse: modifyResponse,
		ErrorHandler:   proxyError,
		ErrorLog:       slog.NewLogLogger(slog.Default().Handler(), slog.LevelWarn),
		// Without a pool ReverseProxy allocates a 32 KiB copy buffer per
		// response, which dominates the edge's garbage at high request rates.
		BufferPool: &copyBuffers,
	}
}

var copyBuffers bufferPool

type bufferPool struct{ pool sync.Pool }

func (b *bufferPool) Get() []byte {
	if buf, ok := b.pool.Get().(*[]byte); ok {
		return *buf
	}
	return make([]byte, 32<<10)
}

func (b *bufferPool) Put(buf []byte) { b.pool.Put(&buf) }

// forward is the end of the chain: it picks an upstream replica, bounds the
// request body and time, and proxies.
func (s *Server) forward(w http.ResponseWriter, r *http.Request) {
	st := stateOf(r)
	pool, err := s.poolFor(st)
	if err != nil {
		httpx.WriteProblem(w, r, http.StatusBadGateway, "bad_upstream", "the service's upstream URL is invalid")
		return
	}
	st.pool, st.target = pool, pool.pick(nil)
	if st.target == nil {
		proxyError(w, r, errCircuitOpen)
		return
	}
	if d := st.svc.Timeout; d > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		r = r.WithContext(ctx)
	}
	limit := s.opts.MaxRequestBodyBytes
	if st.svc.MaxBodyBytes > 0 {
		limit = st.svc.MaxBodyBytes
	}
	hasBody := r.Body != nil && r.Body != http.NoBody
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	// Retries resend the body, so buffer it (bounded) when one is possible.
	if p := st.svc.Failure.Upstream; hasBody && p.MaxRetries > 0 && retryable(r.Method, p) {
		buf, err := io.ReadAll(r.Body)
		if err != nil {
			bodyError(w, r, err)
			return
		}
		r.ContentLength = int64(len(buf))
		r.Body = io.NopCloser(bytes.NewReader(buf))
		r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf)), nil }
	}
	if st.entry != nil && r.ContentLength > 0 {
		st.entry.RequestSize = uint64(r.ContentLength)
	}
	s.proxy.ServeHTTP(w, r)
}

func rewrite(pr *httputil.ProxyRequest) {
	st := stateOf(pr.In)
	pr.Out.URL.Path = upstreamPath(pr.In.URL.Path, st.svc.Transform.StripPrefix)
	pr.Out.URL.RawPath = ""
	pr.SetURL(st.target.url)
	pr.Out.Header["X-Forwarded-For"] = pr.In.Header["X-Forwarded-For"] // append, don't replace
	pr.SetXForwarded()

	h := pr.Out.Header
	h.Del("X-API-Key") // the gateway credential is for the gateway
	for k := range h {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), "X-Gate-") {
			delete(h, k)
		}
	}
	for k, v := range st.svc.Transform.AddHeaders {
		h.Set(k, v)
	}
	for _, k := range st.svc.Transform.HideHeaders {
		h.Del(k)
	}
}

// upstreamPath drops the /{group}/{service} prefix when the service strips it.
func upstreamPath(path string, strip bool) string {
	if !strip {
		return path
	}
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 3)
	if len(parts) < 3 {
		return "/"
	}
	return "/" + parts[2]
}

func modifyResponse(resp *http.Response) error {
	st := stateOf(resp.Request)
	captureCost(resp, st)
	for _, k := range st.svc.Transform.HideHeaders {
		resp.Header.Del(k)
	}
	resp.Header.Del(httpx.HeaderRequestID) // the edge already sent its own
	return nil
}

// proxyError applies the service's upstream failure policy once retries are
// exhausted.
func proxyError(w http.ResponseWriter, r *http.Request, err error) {
	st := stateOf(r)
	st.upstreamFailed = true
	if st.entry != nil {
		st.entry.UpstreamError = true
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		bodyError(w, r, err)
		return
	}
	if errors.Is(r.Context().Err(), context.Canceled) {
		return // the client went away; nobody is listening
	}
	httpx.Logger(r.Context()).Warn("upstream request failed", "scope", st.scope, "service", st.service, "err", err)
	p := st.svc.Failure.EffectiveUpstreamPolicy()
	if p.Mode == "fail-open" {
		h := w.Header()
		h.Set("X-Upstream-Error", "true")
		for k, v := range p.FallbackHeaders {
			h.Set(k, v)
		}
		w.WriteHeader(p.FallbackStatusCode)
		_, _ = io.WriteString(w, p.FallbackBody)
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		httpx.WriteProblem(w, r, http.StatusGatewayTimeout, "upstream_timeout", "the upstream service did not respond in time")
		return
	}
	if errors.Is(err, errCircuitOpen) {
		w.Header().Set("Retry-After", "1")
		httpx.WriteProblem(w, r, http.StatusServiceUnavailable, "upstream_circuit_open", "every replica of the upstream is failing; retry shortly")
		return
	}
	httpx.WriteProblem(w, r, http.StatusBadGateway, "upstream_unavailable", "the upstream service failed")
}

func bodyError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		httpx.WriteProblem(w, r, http.StatusRequestEntityTooLarge, "body_too_large", fmt.Sprintf("request bodies are limited to %d bytes", tooLarge.Limit))
		return
	}
	httpx.WriteProblem(w, r, http.StatusBadRequest, "bad_body", "the request body could not be read")
}

// retryTransport sends each attempt to a replica of the service's pool,
// retries as the service's policy allows (on another replica when there is
// one), feeds outcomes to the circuit breaker, and records upstream latency.
type retryTransport struct{ base http.RoundTripper }

// errRetryableStatus marks a final response whose status the policy treats as
// a failure.
var errRetryableStatus = errors.New("upstream returned a retryable status")

func (t retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	st := stateOf(req)
	p := st.svc.Failure.EffectiveUpstreamPolicy()
	attempts := 1
	if retryable(req.Method, p) && (req.Body == nil || req.GetBody != nil) {
		attempts += p.MaxRetries
	}
	start := time.Now()
	defer func() {
		telemetry.UpstreamSeconds.WithLabelValues(st.label()).Observe(telemetry.Since(start))
		if st.entry != nil {
			st.entry.UpstreamLatency = time.Since(start)
		}
	}()
	target := st.target
	for i := 0; ; i++ {
		if i > 0 {
			if target = st.pool.pick(target); target == nil {
				return nil, errCircuitOpen
			}
			st.target = target
		}
		attempt, cancel, err := newAttempt(req, i, p.AttemptTimeout)
		if err != nil {
			return nil, err
		}
		attempt.URL.Scheme, attempt.URL.Host = target.url.Scheme, target.url.Host
		used := target
		used.inflight.Add(1)
		done := func() { used.inflight.Add(-1); cancel() }
		resp, err := t.base.RoundTrip(attempt)
		st.pool.report(target, err == nil && resp.StatusCode < 500)
		if err == nil && !retryStatus(resp.StatusCode, p) {
			if resp.StatusCode == http.StatusSwitchingProtocols { // upgraded bodies must stay writable
				target.inflight.Add(-1)
			} else {
				resp.Body = &onClose{ReadCloser: resp.Body, done: done}
			}
			return resp, nil
		}
		if err == nil {
			resp.Body.Close()
			err = fmt.Errorf("%w: %d", errRetryableStatus, resp.StatusCode)
		}
		done()
		if i+1 >= attempts {
			return nil, err
		}
		if !sleepCtx(req.Context(), p.RetryBackoff) {
			return nil, req.Context().Err()
		}
	}
}

// newAttempt prepares attempt i: retries get a fresh body, and each attempt
// may have its own deadline (which must outlive RoundTrip, since it also
// governs reading the body).
func newAttempt(req *http.Request, i int, timeout time.Duration) (*http.Request, context.CancelFunc, error) {
	attempt := req
	if i > 0 {
		attempt = req.Clone(req.Context()) // also copies the URL, which retries re-point
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, nil, err
			}
			attempt.Body = body
		}
	}
	if timeout <= 0 {
		return attempt, func() {}, nil
	}
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	return attempt.WithContext(ctx), cancel, nil
}

func retryStatus(code int, p types.UpstreamFailurePolicy) bool {
	for _, c := range p.RetryOnStatuses {
		if c == code {
			return true
		}
	}
	return false
}

func retryable(method string, p types.UpstreamFailurePolicy) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	}
	return p.RetryNonIdempotentMethods
}

// onClose runs done once, when the response body is closed: the attempt's
// deadline must outlive RoundTrip (it governs reading the body) and the
// replica counts as busy until then.
type onClose struct {
	io.ReadCloser
	done   func()
	closed atomic.Bool
}

func (c *onClose) Close() error {
	err := c.ReadCloser.Close()
	if c.closed.CompareAndSwap(false, true) {
		c.done()
	}
	return err
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
