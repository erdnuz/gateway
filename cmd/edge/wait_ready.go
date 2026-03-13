package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

type waitUntilReadyOptions struct {
	HubBaseURL       string
	AnalyticsBaseURL string
	AnalyticsEnabled bool
	MaxAttempts      int
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	RequestTimeout   time.Duration
}

func waitUntilReady(ctx context.Context, options waitUntilReadyOptions) error {
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 30
	}
	if options.InitialBackoff <= 0 {
		options.InitialBackoff = 500 * time.Millisecond
	}
	if options.MaxBackoff <= 0 {
		options.MaxBackoff = 5 * time.Second
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = 1 * time.Second
	}
	if err := waitForHealthz(ctx, "hub", options.HubBaseURL, options.MaxAttempts, options.InitialBackoff, options.MaxBackoff, options.RequestTimeout); err != nil {
		return err
	}
	if options.AnalyticsEnabled {
		if err := waitForHealthz(ctx, "analytics", options.AnalyticsBaseURL, options.MaxAttempts, options.InitialBackoff, options.MaxBackoff, options.RequestTimeout); err != nil {
			return err
		}
	}
	return nil
}

func waitForHealthz(ctx context.Context, name, baseURL string, maxAttempts int, initialBackoff, maxBackoff, requestTimeout time.Duration) error {
	endpoint := strings.TrimSuffix(strings.TrimSpace(baseURL), "/") + "/healthz"
	if strings.TrimSpace(baseURL) == "" {
		return fmt.Errorf("%s base URL is required for startup handshake", name)
	}
	backoff := initialBackoff
	start := time.Now()
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s handshake canceled: %w", name, ctx.Err())
		default:
		}

		err := probeHealthz(ctx, endpoint, requestTimeout)
		if err == nil {
			return nil
		}

		elapsed := time.Since(start)
		log.Printf("edge startup wait target=%s url=%s attempt=%d elapsed=%s next_delay=%s err=%v", name, endpoint, attempt, elapsed.Truncate(time.Millisecond), backoff, err)
		if attempt == maxAttempts {
			return fmt.Errorf("%s startup handshake failed after %d attempts: %w", name, maxAttempts, err)
		}

		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("%s handshake canceled: %w", name, ctx.Err())
		case <-t.C:
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return fmt.Errorf("%s startup handshake exhausted without success", name)
}

func probeHealthz(ctx context.Context, endpoint string, timeout time.Duration) error {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status=%d", resp.StatusCode)
	}
	return nil
}
