// Package devstack runs hub, edge and analytics in one process with embedded
// Redis (miniredis) and NATS, so the whole gateway runs with no external
// services. State lives in memory and is lost on exit.
package devstack

import (
	"bytes"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"gateway/internal/app/analyticsapp"
	"gateway/internal/app/edgeapp"
	"gateway/internal/app/hubapp"
	"gateway/packages/common/types"
	"gateway/packages/hub"

	"github.com/alicebob/miniredis/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

//go:embed default-config.yaml
var defaultConfig []byte

// Options configures a dev stack. Zero values pick the defaults shown by
// `gate dev -h`.
type Options struct {
	ConfigPath    string            // empty: built-in config (v1/echo → echo upstream)
	EdgeAddr      string            // gateway
	HubAddr       string            // hub HTTP API
	AnalyticsAddr string            // analytics API + dashboard; empty disables analytics
	EchoAddr      string            // built-in echo upstream; empty disables it
	Keys          map[string]string // API key → tier; nil seeds one key per tier
	Out           io.Writer         // banner output
	OnReady       func(Endpoints)   // called once everything serves traffic
}

// Endpoints describes a running dev stack.
type Endpoints struct {
	Edge, Hub, Analytics string // base URLs; Analytics is empty when disabled
	HubToken             string
	Keys                 map[string]string
}

// Run starts the stack, prints how to use it, and blocks until ctx ends.
func Run(ctx context.Context, opts Options) error {
	if opts.Out == nil {
		opts.Out = os.Stdout
	}
	cfgPath, cleanup, err := resolveConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	defer cleanup()
	cfgManager, err := hub.NewConfigManager(cfgPath)
	if err != nil {
		return fmt.Errorf("gateway config: %w", err)
	}

	mr := miniredis.NewMiniRedis()
	if err := mr.StartAddr("127.0.0.1:0"); err != nil {
		return fmt.Errorf("embedded redis: %w", err)
	}
	defer mr.Close()

	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: natsserver.RANDOM_PORT, NoLog: true, NoSigs: true})
	if err != nil {
		return fmt.Errorf("embedded nats: %w", err)
	}
	go ns.Start()
	defer ns.Shutdown()
	if !ns.ReadyForConnections(5 * time.Second) {
		return errors.New("embedded nats did not start")
	}

	var shutdowns []func(context.Context) error
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := len(shutdowns) - 1; i >= 0; i-- {
			_ = shutdowns[i](sctx)
		}
	}()

	if opts.EchoAddr != "" {
		ln, err := net.Listen("tcp", opts.EchoAddr)
		if err != nil {
			return fmt.Errorf("echo upstream listen %s: %w", opts.EchoAddr, err)
		}
		srv := &http.Server{Handler: EchoHandler("gate-dev-echo"), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.Serve(ln) }()
		shutdowns = append(shutdowns, srv.Shutdown)
	}

	token := randomToken()
	h, err := hubapp.Start(ctx, hubapp.Config{
		HTTPAddr:            opts.HubAddr,
		ConfigPath:          cfgPath,
		AuthToken:           token,
		RedisAddr:           mr.Addr(),
		NATSURL:             ns.ClientURL(),
		ConfigReloadSubject: types.DefaultConfigReloadSubject,
	})
	if err != nil {
		return fmt.Errorf("hub: %w", err)
	}
	shutdowns = append(shutdowns, h.Shutdown)
	hubURL := "http://" + h.Addr()

	if err := waitReady(ctx, hubURL+"/readyz", 10*time.Second); err != nil {
		return fmt.Errorf("hub: %w", err)
	}

	e, err := edgeapp.Start(ctx, edgeapp.Config{
		HTTPAddr:            opts.EdgeAddr,
		RedisAddr:           mr.Addr(),
		EdgeID:              "dev-edge",
		HubAddr:             hubURL,
		HubToken:            token,
		LeaseClient:         inProcessLeases{h.Leases},
		NATSTierURL:         ns.ClientURL(),
		NATSAnalyticsURL:    ns.ClientURL(),
		AnalyticsEnabled:    opts.AnalyticsAddr != "",
		ConfigReloadSubject: types.DefaultConfigReloadSubject,
	})
	if err != nil {
		return fmt.Errorf("edge: %w", err)
	}
	shutdowns = append(shutdowns, e.Shutdown)

	analyticsURL := ""
	if opts.AnalyticsAddr != "" {
		a, err := analyticsapp.Start(ctx, analyticsapp.Config{
			HTTPAddr:  opts.AnalyticsAddr,
			HubAddr:   hubURL,
			HubToken:  token,
			RedisAddr: mr.Addr(),
			NATSURL:   ns.ClientURL(),
		})
		if err != nil {
			return fmt.Errorf("analytics: %w", err)
		}
		shutdowns = append(shutdowns, a.Shutdown)
		analyticsURL = "http://" + a.Addr()
	}

	edgeURL := "http://" + e.Addr()
	if err := waitReady(ctx, edgeURL+"/readyz", 10*time.Second); err != nil {
		return fmt.Errorf("edge: %w", err)
	}

	keys := opts.Keys
	if keys == nil {
		keys = defaultKeys(cfgManager.Snapshot())
	}
	prefix, service := firstRoute(cfgManager.Snapshot())
	if err := seedKeys(ctx, hubURL, token, prefix, keys); err != nil {
		return err
	}

	printBanner(opts, edgeURL, hubURL, analyticsURL, token, prefix, service, keys)
	if opts.OnReady != nil {
		opts.OnReady(Endpoints{Edge: edgeURL, Hub: hubURL, Analytics: analyticsURL, HubToken: token, Keys: keys})
	}
	<-ctx.Done()
	fmt.Fprintln(opts.Out, "\nstopping gate dev")
	return nil
}

// inProcessLeases lets the edge lease quota straight from the hub's lease
// service without gRPC or TLS.
type inProcessLeases struct{ srv *hub.QuotaLeaseServer }

func (l inProcessLeases) RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	return l.srv.RequestQuotaLease(ctx, in)
}

func (inProcessLeases) Close() error { return nil }

func resolveConfig(path string) (string, func(), error) {
	if path != "" {
		return path, func() {}, nil
	}
	dir, err := os.MkdirTemp("", "gate-dev-*")
	if err != nil {
		return "", nil, err
	}
	p := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(p, defaultConfig, 0o600); err != nil {
		os.RemoveAll(dir)
		return "", nil, err
	}
	return p, func() { os.RemoveAll(dir) }, nil
}

var nonAlnum = regexp.MustCompile(`[^A-Za-z0-9]`)

// defaultKeys issues one demo key per tier of the first prefix, named after
// the tier (e.g. "freekey", "prokey").
func defaultKeys(cfg *types.GatewayConfig) map[string]string {
	keys := map[string]string{}
	if cfg == nil || len(cfg.Prefixes) == 0 {
		return keys
	}
	for _, svc := range cfg.Prefixes[0].Services {
		for _, t := range svc.Tiers {
			keys[strings.ToLower(nonAlnum.ReplaceAllString(t.TierID, ""))+"key"] = t.TierID
		}
	}
	return keys
}

func firstRoute(cfg *types.GatewayConfig) (prefix, service string) {
	if cfg == nil || len(cfg.Prefixes) == 0 || len(cfg.Prefixes[0].Services) == 0 {
		return "", ""
	}
	return cfg.Prefixes[0].Prefix, cfg.Prefixes[0].Services[0].ServiceID
}

func seedKeys(ctx context.Context, hubURL, token, prefix string, keys map[string]string) error {
	for key, tier := range keys {
		body, _ := json.Marshal(map[string]string{"tier_id": tier})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/tiers/%s/%s", hubURL, prefix, key), bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("seed key %s: %w", key, err)
		}
		msg, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("seed key %s (tier %s): hub returned %d: %s", key, tier, resp.StatusCode, strings.TrimSpace(string(msg)))
		}
	}
	return nil
}

func waitReady(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not ready after %s (%s)", timeout, url)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func printBanner(opts Options, edgeURL, hubURL, analyticsURL, token, prefix, service string, keys map[string]string) {
	w := opts.Out
	fmt.Fprintln(w, "\ngate dev is running — embedded Redis and NATS, all state in memory")
	fmt.Fprintf(w, "  gateway     %s\n", edgeURL)
	fmt.Fprintf(w, "  hub API     %s   (Authorization: Bearer %s)\n", hubURL, token)
	if analyticsURL != "" {
		fmt.Fprintf(w, "  analytics   %s   (dashboard)\n", analyticsURL)
	}
	if opts.EchoAddr != "" {
		fmt.Fprintf(w, "  upstream    http://%s   (built-in echo)\n", opts.EchoAddr)
	}
	names := slices.Sorted(maps.Keys(keys))
	fmt.Fprintln(w, "\nAPI keys:")
	for _, key := range names {
		fmt.Fprintf(w, "  %-12s tier %s\n", key, keys[key])
	}
	if prefix != "" && len(names) > 0 {
		fmt.Fprintf(w, "\nTry it:\n  curl -i -H \"X-API-Key: %s\" %s/%s/%s/hello\n", names[0], edgeURL, prefix, service)
	}
	fmt.Fprintln(w, "\nPress Ctrl+C to stop.")
}

func randomToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "dev-" + hex.EncodeToString(b[:])
}
