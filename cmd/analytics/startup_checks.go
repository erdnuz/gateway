package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	analyticsapi "gateway/packages/analytics"
	"gateway/packages/common/config"
	"gateway/packages/common/types"

	"github.com/nats-io/nats.go"
)

type analyticsStartupChecks struct {
	clickHouseDSN   string
	clickHouseTable string
	natsURL         string
	natsSubject     string
	natsQueue       string
	hubAddr         string
	hubToken        string
	hubHTTPTimeout  time.Duration
}

func newAnalyticsStartupChecks(policy types.AnalyticsRuntimePolicy) (*analyticsStartupChecks, error) {
	dsn, err := config.Required("CLICKHOUSE_DSN")
	if err != nil {
		return nil, err
	}
	natsURL := strings.TrimSpace(config.String("NATS_ANALYTICS_URL", config.String("NATS_URL", "")))
	hubAddr := strings.TrimSpace(config.String("HUB_ADDR", ""))
	return &analyticsStartupChecks{
		clickHouseDSN:   strings.TrimSpace(dsn),
		clickHouseTable: strings.TrimSpace(config.String("ANALYTICS_CLICKHOUSE_TABLE", "analytics_events")),
		natsURL:         natsURL,
		natsSubject:     strings.TrimSpace(config.String("NATS_ANALYTICS_SUBJECT", policy.NATSSubject)),
		natsQueue:       strings.TrimSpace(config.String("NATS_ANALYTICS_QUEUE", policy.NATSQueue)),
		hubAddr:         hubAddr,
		hubToken:        strings.TrimSpace(config.String("HUB_AUTH_TOKEN", "")),
		hubHTTPTimeout:  policy.ConfigFetchTimeout,
	}, nil
}

func (a *analyticsStartupChecks) ValidateConfig(_ context.Context) error {
	if a.clickHouseDSN == "" {
		return fmt.Errorf("CLICKHOUSE_DSN is required")
	}
	if a.natsURL == "" {
		return fmt.Errorf("NATS_ANALYTICS_URL or NATS_URL is required")
	}
	if a.natsSubject == "" {
		return fmt.Errorf("NATS_ANALYTICS_SUBJECT is required")
	}
	if a.hubAddr == "" {
		return fmt.Errorf("HUB_ADDR is required")
	}
	parsed, err := url.ParseRequestURI(a.hubAddr)
	if err != nil {
		return fmt.Errorf("HUB_ADDR must be a valid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("HUB_ADDR scheme must be http or https")
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return fmt.Errorf("HUB_ADDR host is required")
	}
	if a.hubHTTPTimeout <= 0 {
		a.hubHTTPTimeout = 2 * time.Second
	}
	return nil
}

func (a *analyticsStartupChecks) CheckDependencies(ctx context.Context) error {
	store, err := analyticsapi.NewClickHouseAnalyticsStore(a.clickHouseDSN, a.clickHouseTable)
	if err != nil {
		return fmt.Errorf("clickhouse store init failed: %w", err)
	}
	defer store.Close()
	if err := store.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse ping failed: %w", err)
	}

	nc, err := nats.Connect(a.natsURL)
	if err != nil {
		return fmt.Errorf("nats connect failed at %s: %w", a.natsURL, err)
	}
	defer nc.Close()

	canaryInbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(canaryInbox)
	if err != nil {
		return fmt.Errorf("nats canary subscribe failed: %w", err)
	}
	defer sub.Unsubscribe()
	if err := nc.Publish(canaryInbox, []byte("analytics-startup-canary")); err != nil {
		return fmt.Errorf("nats canary publish failed: %w", err)
	}
	if err := nc.Flush(); err != nil {
		return fmt.Errorf("nats flush failed: %w", err)
	}
	if err := nc.LastError(); err != nil {
		return fmt.Errorf("nats connectivity check failed: %w", err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		return fmt.Errorf("nats canary message receive failed: %w", err)
	}

	hubHealthURL := strings.TrimSuffix(a.hubAddr, "/") + "/healthz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hubHealthURL, nil)
	if err != nil {
		return fmt.Errorf("hub health request build failed: %w", err)
	}
	if a.hubToken != "" {
		req.Header.Set("Authorization", "Bearer "+a.hubToken)
	}
	client := &http.Client{Timeout: a.hubHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("hub health probe failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("hub health probe returned status=%d", resp.StatusCode)
	}
	var payload map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&payload); err == nil {
		if status, ok := payload["status"]; ok && strings.ToLower(strings.TrimSpace(status)) != "ok" {
			return fmt.Errorf("hub health probe returned non-ok status=%q", status)
		}
	}
	return nil
}
