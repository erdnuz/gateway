package edge

import (
	"net"
	"net/http"
	"time"
)

const defaultHubHTTPTimeout = 5 * time.Second

func newHubHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = defaultHubHTTPTimeout
	}
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   timeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          100,
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}

// NewHubHTTPClient builds a shared timeout-configured HTTP client for edge->hub calls.
func NewHubHTTPClient(timeout time.Duration) *http.Client {
	return newHubHTTPClient(timeout)
}
