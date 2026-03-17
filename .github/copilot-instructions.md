# Project Guidelines

## Code Style
- Keep service boundaries strict: code in `packages/edge` must not import `packages/hub` (and vice versa). Share cross-service types/utilities via `packages/common`.
- Use existing config helpers (`packages/common/config`) for env parsing and required values instead of ad-hoc `os.Getenv` logic.
- Follow existing startup wiring patterns in `cmd/*/main.go`: validate config/dependencies first, then initialize managers, then expose readiness.

## Architecture
- This repository has three services with separate entrypoints: `cmd/hub/main.go`, `cmd/analytics/main.go`, and `cmd/edge/main.go`.
- Core contract for quota leasing is proto-first at `api/proto/quota_lease.proto` with generated types in `packages/common/types`.
- Startup and deployment order is mandatory: Hub -> Analytics -> Edge. Use `/readyz` for gating, `/healthz` for liveness.
- For architecture details, see `docs/architecture.md` and `docs/deployments.md`.

## Build and Test
- Start implementations with tests before writing code. Write unit tests for core logic and integration tests for end-to-end scenarios.
- Full suite: `go test ./...`
- Integration tests: `go test -tags=integration ./testing/integration/...` (requires Docker/testcontainers dependencies).
- Race checks when touching concurrency/async code: `go test -race ./...`
- Run focused tests for changed service/package first, then run the full suite before finishing.

## Conventions
- Keep startup safety behavior intact: changes affecting startup must preserve checks in `cmd/*/startup_checks.go` and readiness behavior.
- Preserve auth and mTLS assumptions:
  - Hub requires `HUB_AUTH_TOKEN`.
  - Edge/Hub gRPC lease path depends on TLS settings (`EDGE_TLS_*`, `HUB_TLS_*`, `HUB_GRPC_SERVER_NAME`).
- Prefer existing integration harness utilities in `testing/integration` for scenario tests instead of bespoke setup.
- Do not use destructive deployment scripts unless explicitly requested (for example `deployments/redeploy_fresh_all_docker.sh`).

## TODO and Planning
- Use TODO.md for tracking larger tasks and initiatives, not ad-hoc comments in code. Link to relevant code/files in the TODO item.
- For proposed changes that affect architecture, startup, or conventions, include a design doc in `docs/design/` and link to it in the TODO item.
- For non-trivial PRs, include a summary of the change, motivation, and any relevant context in the PR description to help reviewers understand the impact.
- Always update documentation if the change affects usage, configuration, or architecture. Link to relevant docs in the PR description.

## Key References
- `README.md`
- `docs/configuration.md`
- `docs/edge.md`
- `docs/hub.md`
- `docs/analytics.md`
- `testing/integration/harness.go`