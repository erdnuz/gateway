// Command gate is the single binary for every Gate component.
//
//	gate dev                 whole gateway in one process, no external services
//	gate hub|edge|analytics  run one component (configured by environment)
//	gate echo                demo upstream that echoes requests
//	gate config ...          validate, migrate or describe config files
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gateway/internal/app/analyticsapp"
	"gateway/internal/app/devstack"
	"gateway/internal/app/edgeapp"
	"gateway/internal/app/hubapp"
	"gateway/packages/common/telemetry"
	"gateway/packages/config"
	"gateway/packages/hub"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `gate — distributed API gateway with leased rate limiting

Usage:
  gate <command> [flags]

Commands:
  dev                 run hub, edge and analytics in one process (no external services)
  hub                 run the control plane        (configured by CONFIG_FILE_PATH, HUB_*, REDIS_ADDR, NATS_* env)
  edge                run the gateway data plane   (configured by EDGE_*, HUB_*, REDIS_ADDR, NATS_* env)
  analytics           run analytics + dashboard    (configured by CLICKHOUSE_DSN, HUB_ADDR, NATS_* env)
  echo                run a demo upstream that echoes each request as JSON
  config ...          validate, migrate (v1 to v2) or print the schema of config files
  version             print the version

Run "gate <command> -h" for command flags.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "gate:", strings.ReplaceAll(err.Error(), "\n", "\n  "))
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd, rest := args[0], args[1:]
	setupLogging(cmd)
	shutdownTracing, err := telemetry.SetupTracing(ctx, cmd)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx)
	}()
	switch cmd {
	case "dev":
		return runDev(ctx, rest)
	case "hub":
		return noFlags("hub", rest, func() error { return hubapp.Run(ctx, hubapp.FromEnv()) })
	case "edge":
		return noFlags("edge", rest, func() error { return edgeapp.Run(ctx, edgeapp.FromEnv()) })
	case "analytics":
		return noFlags("analytics", rest, func() error { return analyticsapp.Run(ctx, analyticsapp.FromEnv()) })
	case "echo":
		return runEcho(ctx, rest)
	case "config":
		return runConfig(rest)
	case "version", "--version":
		fmt.Println("gate", version)
		return nil
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q (run \"gate help\")", cmd)
	}
}

// setupLogging routes slog (and the log package) to stderr: JSON for the
// services, text for gate dev. LOG_FORMAT and LOG_LEVEL override.
func setupLogging(cmd string) {
	format := os.Getenv("LOG_FORMAT")
	if format == "" {
		format = "json"
		if cmd == "dev" {
			format = "text"
		}
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))) // empty: info
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewJSONHandler(os.Stderr, opts)
	if format == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h).With("service", cmd))
}

func noFlags(name string, args []string, fn func() error) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: gate %s\n\nConfigured entirely by environment variables; see docs/configuration.md.\n", name)
	}
	if err := fs.Parse(args); err != nil {
		return ignoreHelp(err)
	}
	return fn()
}

func runDev(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dev", flag.ContinueOnError)
	opts := devstack.Options{}
	fs.StringVar(&opts.ConfigPath, "config", "", "gateway config file (default: built-in v1/echo example)")
	fs.StringVar(&opts.EdgeAddr, "addr", "127.0.0.1:8080", "gateway listen address")
	fs.StringVar(&opts.HubAddr, "hub-addr", "127.0.0.1:8081", "hub API listen address")
	fs.StringVar(&opts.AnalyticsAddr, "analytics-addr", "127.0.0.1:8091", `analytics listen address ("" disables analytics)`)
	fs.StringVar(&opts.EchoAddr, "echo-addr", "127.0.0.1:9000", `built-in echo upstream address ("" disables it)`)
	fs.StringVar(&opts.MetricsAddr, "metrics-addr", "127.0.0.1:9100", `Prometheus /metrics address ("" disables it)`)
	keys := fs.String("keys", "", `API keys to issue as key=tier,key=tier (default: one per tier, e.g. "freekey")`)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), "Usage: gate dev [flags]\n\nRuns hub, edge and analytics in one process with embedded Redis and NATS.\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return ignoreHelp(err)
	}
	if *keys != "" {
		opts.Keys = map[string]string{}
		for _, pair := range strings.Split(*keys, ",") {
			k, t, ok := strings.Cut(strings.TrimSpace(pair), "=")
			if !ok || k == "" || t == "" {
				return fmt.Errorf("-keys: %q is not key=tier", pair)
			}
			opts.Keys[k] = t
		}
	}
	return devstack.Run(ctx, opts)
}

func runEcho(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("echo", flag.ContinueOnError)
	addr := fs.String("addr", ":9000", "listen address")
	name := fs.String("name", "echo", "name reported in responses")
	if err := fs.Parse(args); err != nil {
		return ignoreHelp(err)
	}
	srv := &http.Server{Addr: *addr, Handler: devstack.EchoHandler(*name), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	fmt.Printf("echo upstream listening on %s\n", *addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

const configUsage = `usage:
  gate config validate <file>   check a config file and list every problem
  gate config migrate <file>    convert a version 1 JSON config to version 2 YAML (stdout)
  gate config schema            print the JSON Schema for config files`

func runConfig(args []string) error {
	if len(args) == 0 {
		return errors.New(configUsage)
	}
	switch {
	case args[0] == "validate" && len(args) == 2:
		cfg, err := config.Load(args[1])
		if err != nil {
			return err
		}
		if err := hub.ValidateGatewayConfig(cfg); err != nil {
			return fmt.Errorf("%s: %w", args[1], err)
		}
		n := 0
		for _, p := range cfg.Prefixes {
			n += len(p.Services)
		}
		fmt.Printf("%s is valid (%d services)\n", args[1], n)
		return nil
	case args[0] == "migrate" && len(args) == 2:
		data, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		f, migrateErr := config.MigrateV1(data)
		if f == nil {
			return migrateErr
		}
		out, err := config.Marshal(f)
		if err != nil {
			return err
		}
		os.Stdout.Write(out)
		return migrateErr
	case args[0] == "schema" && len(args) == 1:
		out, err := config.JSONSchema()
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		return nil
	default:
		return errors.New(configUsage)
	}
}

func ignoreHelp(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}
