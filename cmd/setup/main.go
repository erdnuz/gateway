package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"gateway/packages/common/types"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	envFilePath    = "config/env.local"
	configFilePath = "config/policies.json"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	service := askChoice(reader, "Select service", []string{"hub", "edge", "analytics"})

	if err := os.MkdirAll(filepath.Dir(envFilePath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create config dir: %v\n", err)
		os.Exit(1)
	}

	env := defaultEnv(service)
	if err := writeEnvFile(envFilePath, env); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write env file: %v\n", err)
		os.Exit(1)
	}
	exportScriptPath := filepath.Join("config", fmt.Sprintf("export.%s.sh", service))
	if err := writeExportScript(exportScriptPath, env); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write export script: %v\n", err)
		os.Exit(1)
	}

	cfg := defaultGatewayConfig()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "generated config validation failed: %v\n", err)
		os.Exit(1)
	}
	if err := writeGatewayConfig(configFilePath, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write config file: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("[EXPORTING] generated environment and config files")
	fmt.Printf("Service selected: %s\n", service)
	fmt.Printf("Wrote env file: %s\n", envFilePath)
	fmt.Printf("Wrote export script: %s\n", exportScriptPath)
	fmt.Printf("Wrote config file: %s\n", configFilePath)

	if err := validateSetup(service); err != nil {
		fmt.Fprintf(os.Stderr, "setup validation failed: %v\n", err)
		os.Exit(1)
	}
	if err := deployAndVerify(service, env); err != nil {
		fmt.Fprintf(os.Stderr, "setup deployment failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Setup completed successfully.")
	fmt.Printf("To load env in your current shell: source %s\n", exportScriptPath)
}

func defaultEnv(service string) map[string]string {
	// One shared env file for all services. PORT is set based on selected service.
	port := map[string]string{
		"hub":       "8080",
		"edge":      ":8082",
		"analytics": ":8091",
	}[service]
	if port == "" {
		port = "8080"
	}

	return map[string]string{
		"GATE_SERVICE": service,

		"PORT":       port,
		"REDIS_ADDR": "localhost:6379",
		"NATS_URL":   "nats://localhost:4222",

		"CONFIG_FILE_PATH":           configFilePath,
		"EDGE_BOOTSTRAP_CONFIG_FILE": configFilePath,

		"HUB_AUTH_TOKEN":               "change-me-hub-token",
		"HUB_ENFORCE_REDIS_NOEVICTION": "true",
		"HUB_UPDATES_CHANNEL":          types.DefaultHubUpdatesChannel,
		"HUB_CONFIG_RELOAD_CHANNEL":    types.DefaultConfigReloadChannel,
		"HUB_GRPC_PORT":                "9090",
		"HUB_QUEUE_WORKERS":            "2",
		"HUB_QUEUE_SUBMIT_TIMEOUT":     "25ms",
		"HUB_QUEUE_RETRY_MAX":          "1",
		"HUB_QUEUE_RETRY_BACKOFF":      "10ms",
		"MAX_DELTA":                    "10000",

		"HUB_ADDR":                     "http://localhost:8080",
		"HUB_GRPC_ADDR":                "localhost:9090",
		"HUB_GRPC_SERVER_NAME":         "hub-server",
		"EDGE_MAX_DELTA":               "100",
		"EDGE_LEASE_SIZE":              "100",
		"EDGE_LEASE_LOW_WATER_PERCENT": "20",
		"EDGE_CONFIG_REFRESH_SECONDS":  "0",
		"EDGE_CONFIG_RELOAD_CHANNEL":   types.DefaultConfigReloadChannel,
		"RATE_HARD_THRESHOLD_PERCENT":  "90",
		"HUB_HTTP_TIMEOUT_SECONDS":     "5",

		"ANALYTICS_ENABLED":      "true",
		"GATE_ANALYTICS_ENABLED": "true",
		"ANALYTICS_BUFFER_SIZE":  "1000",
		"ANALYTICS_REDIS_KEY":    types.DefaultAnalyticsKey,
		"ANALYTICS_API_TOKEN":    "change-me-analytics-token",

		"NATS_TIER_UPDATES_SUBJECT": "tier.updates",
		"NATS_EDGE_QUEUE":           "edge-tier-updates",
		"NATS_ANALYTICS_SUBJECT":    "analytics.events",
		"NATS_ANALYTICS_QUEUE":      "analytics-subscribers",

		"HUB_TLS_CERT_FILE":  "./deployments/certs/hub/tls.crt",
		"HUB_TLS_KEY_FILE":   "./deployments/certs/hub/tls.key",
		"HUB_TLS_CA_FILE":    "./deployments/certs/ca/ca.crt",
		"EDGE_TLS_CERT_FILE": "./deployments/certs/edge/tls.crt",
		"EDGE_TLS_KEY_FILE":  "./deployments/certs/edge/tls.key",
		"EDGE_TLS_CA_FILE":   "./deployments/certs/ca/ca.crt",
	}
}

func defaultGatewayConfig() *types.GatewayConfig {
	return &types.GatewayConfig{
		UpdatedAt: time.Now().UTC(),
		Prefixes: []types.PrefixConfig{
			{
				Prefix:      "v1",
				QuotaPeriod: time.Minute,
				Services: []types.ServiceConfig{
					{
						ServiceID: "auth-api",
						TargetURL: "http://localhost:9000",
						Tiers: []types.TierConfig{
							{
								TierID:     "free",
								Quota:      20,
								GetCost:    1,
								PostCost:   1,
								PutCost:    1,
								DeleteCost: 1,
								OtherCost:  1,
							},
						},
						Transform: types.TransformConfig{StripPrefix: true},
						Analytics: types.AnalyticsConfig{Enabled: true, Mode: types.AnalyticsModeHeavy, SamplingRate: 1.0},
						Cache:     &types.CacheConfig{Enabled: true, TTL: 2 * time.Second, CacheKey: "$method:$path"},
						Failure: types.FailureConfig{
							FailOpen: false,
							Hub: types.HubFailurePolicy{
								TierLookupStrategy:      "default-tier",
								DefaultTier:             "free",
								AllowOnRateServiceError: false,
								StaleTierMaxAge:         5 * time.Minute,
							},
							Upstream: types.UpstreamFailurePolicy{Mode: "fail-closed"},
						},
					},
				},
			},
		},
	}
}

func askChoice(reader *bufio.Reader, prompt string, options []string) string {
	for {
		fmt.Printf("%s (%s): ", prompt, strings.Join(options, "/"))
		line := strings.ToLower(strings.TrimSpace(readLine(reader)))
		for _, opt := range options {
			if line == opt {
				return opt
			}
		}
		fmt.Printf("choose one of: %s\n", strings.Join(options, ", "))
	}
}

func readLine(reader *bufio.Reader) string {
	line, err := reader.ReadString('\n')
	if err != nil {
		return strings.TrimSpace(line)
	}
	return strings.TrimRight(line, "\r\n")
}

func writeEnvFile(path string, env map[string]string) error {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, k := range keys {
		if _, err := fmt.Fprintf(f, "%s=%s\n", k, strings.ReplaceAll(env[k], "\n", "")); err != nil {
			return err
		}
	}
	return nil
}

func writeGatewayConfig(path string, cfg *types.GatewayConfig) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(cfg)
}

func writeExportScript(path string, env map[string]string) error {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.WriteString("#!/usr/bin/env bash\n"); err != nil {
		return err
	}
	for _, k := range keys {
		v := strings.ReplaceAll(env[k], "\n", "")
		if _, err := fmt.Fprintf(f, "export %s=%q\n", k, v); err != nil {
			return err
		}
	}
	return os.Chmod(path, 0o755)
}

func validateSetup(service string) error {
	fmt.Println("[VALIDATING] generated env file")
	if _, err := os.Stat(envFilePath); err != nil {
		return fmt.Errorf("env file missing: %w", err)
	}
	fmt.Println("[VALIDATING] generated config file")
	b, err := os.ReadFile(configFilePath)
	if err != nil {
		return fmt.Errorf("config file read failed: %w", err)
	}
	var cfg types.GatewayConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("config json decode failed: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}
	fmt.Println("[VALIDATING] docker compose availability")
	if _, err := composeCommand(filepath.Join("deployments", "docker-compose.yaml"), []string{"nats"}); err != nil {
		return err
	}
	if service == "hub" || service == "edge" {
		if err := validateCertAssets(service); err != nil {
			return err
		}
	}
	return nil
}

func validateCertAssets(service string) error {
	required := []string{
		"deployments/certs/ca/ca.crt",
		"deployments/certs/hub/tls.crt",
		"deployments/certs/hub/tls.key",
	}
	if service == "edge" {
		required = append(required, "deployments/certs/edge/tls.crt", "deployments/certs/edge/tls.key")
	}
	for _, p := range required {
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("required TLS asset missing: %s (%v)", p, err)
		}
	}
	return nil
}

func deployAndVerify(service string, env map[string]string) error {
	chain := dependencyChainForService(service)
	if len(chain) == 0 {
		return fmt.Errorf("unknown service: %s", service)
	}
	for _, s := range chain {
		fmt.Printf("[DEPLOYING] %s\n", s)
		if err := composeUp(composeServicesForService(s), env); err != nil {
			return fmt.Errorf("%s deploy failed: %w", s, err)
		}
		fmt.Printf("[VERIFYING] %s\n", s)
		if err := verifyService(s); err != nil {
			return fmt.Errorf("%s verification failed: %w", s, err)
		}
	}
	if service == "edge" || service == "analytics" {
		fmt.Printf("[VERIFYING] %s -> hub handshake\n", service)
		if err := verifyHubHandshake(service); err != nil {
			return err
		}
	}
	return nil
}

func dependencyChainForService(service string) []string {
	switch service {
	case "hub":
		return []string{"hub"}
	case "edge":
		return []string{"hub", "edge"}
	case "analytics":
		return []string{"hub", "analytics"}
	default:
		return nil
	}
}

func composeServicesForService(service string) []string {
	switch service {
	case "hub":
		return []string{"hub-redis", "nats", "hub-server"}
	case "edge":
		return []string{"edge-redis", "nats", "hub-server", "edge-server"}
	case "analytics":
		return []string{"analytics-redis", "nats", "hub-server", "analytics"}
	default:
		return nil
	}
}

func composeUp(services []string, env map[string]string) error {
	if len(services) == 0 {
		return fmt.Errorf("no compose services provided")
	}
	composeFile := filepath.Join("deployments", "docker-compose.yaml")
	cmd, err := composeDeployCommand(composeFile, services)
	if err != nil {
		return err
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = mergedEnv(env)
	return cmd.Run()
}

func composeDeployCommand(composeFile string, services []string) (*exec.Cmd, error) {
	if _, err := exec.LookPath("docker"); err == nil {
		args := []string{"compose", "-f", composeFile, "up", "-d", "--build", "--force-recreate", "--remove-orphans"}
		args = append(args, services...)
		return exec.Command("docker", args...), nil
	}
	if _, err := exec.LookPath("docker-compose"); err == nil {
		args := []string{"-f", composeFile, "up", "-d", "--build", "--force-recreate", "--remove-orphans"}
		args = append(args, services...)
		return exec.Command("docker-compose", args...), nil
	}
	return nil, fmt.Errorf("docker compose is not installed")
}

func composeCommand(composeFile string, services []string) (*exec.Cmd, error) {
	if _, err := exec.LookPath("docker"); err == nil {
		args := []string{"compose", "-f", composeFile, "up", "-d"}
		args = append(args, services...)
		return exec.Command("docker", args...), nil
	}
	if _, err := exec.LookPath("docker-compose"); err == nil {
		args := []string{"-f", composeFile, "up", "-d"}
		args = append(args, services...)
		return exec.Command("docker-compose", args...), nil
	}
	return nil, fmt.Errorf("docker compose is not installed")
}

func mergedEnv(overrides map[string]string) []string {
	envMap := map[string]string{}
	for _, entry := range os.Environ() {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}
	for k, v := range overrides {
		envMap[k] = v
	}
	out := make([]string, 0, len(envMap))
	for k, v := range envMap {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

func verifyService(service string) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var ok bool
		switch service {
		case "hub":
			ok = checkHTTPReady("http://localhost:8080/readyz")
		case "edge":
			ok = checkHTTPReady("http://localhost:8082/readyz")
		case "analytics":
			ok = checkHTTPReady("http://localhost:8091/readyz")
		}
		if ok {
			return nil
		}
		time.Sleep(750 * time.Millisecond)
	}
	return fmt.Errorf("readiness timeout")
}

func verifyHubHandshake(service string) error {
	if !checkHTTPReady("http://localhost:8080/readyz") {
		return fmt.Errorf("hub is not reachable")
	}
	if service == "edge" {
		conn, err := net.DialTimeout("tcp", "localhost:9090", 2*time.Second)
		if err != nil {
			return fmt.Errorf("hub grpc handshake failed: %w", err)
		}
		_ = conn.Close()
	}
	return nil
}

func checkHTTPReady(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
