package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gateway/packages/common/types"
	"gateway/packages/hub"
)

const preCommitHookContent = `#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

say() {
  printf "%b\n" "$1"
}

step() {
  say "${BLUE}[pre-commit]${NC} $1"
}

pass() {
  say "${GREEN}[ok]${NC} $1"
}

fail() {
  say "${RED}[error]${NC} $1" >&2
}

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

step "Running go fmt on all packages"
if ! fmt_output="$(go fmt ./... 2>&1)"; then
  fail "go fmt failed"
  say "${RED}${fmt_output}${NC}" >&2
  exit 1
fi
if [[ -n "$fmt_output" ]]; then
  say "$fmt_output"
fi
if [[ -n "$(git diff --name-only -- '*.go')" ]]; then
  fail "go fmt changed Go files. Review changes and stage them before committing."
	git --no-pager diff --name-only -- '*.go' || true
  exit 1
fi
pass "go fmt passed"

step "Running go vet on all packages"
if ! vet_output="$(go vet ./... 2>&1)"; then
  fail "go vet failed"
  say "${RED}${vet_output}${NC}" >&2
  exit 1
fi
pass "go vet passed"

step "Running unit test suite (short mode)"
if ! test_output="$(go test -short ./... 2>&1)"; then
  fail "go test -short failed"
  say "${RED}${test_output}${NC}" >&2
  exit 1
fi
pass "go test -short passed"

step "Validating GatewayConfig against schema"
if ! cfg_output="$(go run ./packages/common/cmd/install-hooks validate-config 2>&1)"; then
  fail "GatewayConfig schema validation failed"
  say "${RED}${cfg_output}${NC}" >&2
  exit 1
fi
pass "$cfg_output"

say "${GREEN}[pre-commit] All checks passed.${NC}"
`

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		if err := installHook(); err != nil {
			exitErr(err)
		}
		fmt.Println("pre-commit hook installed successfully")
		return
	}

	switch args[0] {
	case "install":
		if err := installHook(); err != nil {
			exitErr(err)
		}
		fmt.Println("pre-commit hook installed successfully")
	case "validate-config":
		configPath := ""
		if len(args) > 1 {
			configPath = args[1]
		}
		resolved, err := resolveGatewayConfigPath(configPath)
		if err != nil {
			exitErr(err)
		}
		if err := validateGatewayConfigFile(resolved); err != nil {
			exitErr(err)
		}
		fmt.Printf("GatewayConfig schema validation passed (%s)\n", resolved)
	default:
		exitErr(fmt.Errorf("unknown subcommand %q (supported: install, validate-config)", args[0]))
	}
}

func installHook() error {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return err
	}

	hooksDir := filepath.Join(repoRoot, ".git", "hooks")
	if err := os.MkdirAll(hooksDir, 0o755); err != nil {
		return fmt.Errorf("failed to ensure hooks dir: %w", err)
	}

	hookPath := filepath.Join(hooksDir, "pre-commit")
	if err := os.WriteFile(hookPath, []byte(preCommitHookContent), 0o755); err != nil {
		return fmt.Errorf("failed to write pre-commit hook: %w", err)
	}
	if err := os.Chmod(hookPath, 0o755); err != nil {
		return fmt.Errorf("failed to set execute permission on pre-commit hook: %w", err)
	}

	return nil
}

func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get working directory: %w", err)
	}

	cur := wd
	for {
		gitDir := filepath.Join(cur, ".git")
		if info, err := os.Stat(gitDir); err == nil && info.IsDir() {
			return cur, nil
		}
		next := filepath.Dir(cur)
		if next == cur {
			break
		}
		cur = next
	}

	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	cmd.Dir = wd
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err == nil {
		root := strings.TrimSpace(out.String())
		if root != "" {
			return root, nil
		}
	}

	return "", errors.New("unable to locate repository root (.git)")
}

func resolveGatewayConfigPath(provided string) (string, error) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return "", err
	}

	if strings.TrimSpace(provided) != "" {
		p := provided
		if !filepath.IsAbs(p) {
			p = filepath.Join(repoRoot, p)
		}
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("provided config path does not exist: %s", p)
		}
		return p, nil
	}

	candidates := []string{
		"GatewayConfig.json",
		"deployments/hub.gateway-config.json",
		"cmd/config.json",
	}
	for _, rel := range candidates {
		p := filepath.Join(repoRoot, rel)
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	return "", fmt.Errorf("no GatewayConfig file found; looked for %s", strings.Join(candidates, ", "))
}

func validateGatewayConfigFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var cfg types.GatewayConfig
	if err := dec.Decode(&cfg); err != nil {
		return fmt.Errorf("schema decode failed for %s: %w", path, err)
	}

	if err := hub.ValidateGatewayConfig(&cfg); err != nil {
		return fmt.Errorf("schema validation failed for %s: %w", path, err)
	}
	return nil
}

func exitErr(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
