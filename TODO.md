# Runtime Config Updates (Post-Startup)

## Goal

Enable safe runtime config updates after infrastructure is running, with validation, edge notification, and consistency-preserving cache handling.

## Phase 1: Hub API Contract + Validation

- Add dry-run endpoint to validate and analyze config changes before apply.
- Support apply from file-based reload and API payload reload.
- Keep atomic config swap semantics.
- Keep current auth model (`HUB_AUTH_TOKEN`) for admin endpoints.
- Files:
- [packages/hub/server.go](packages/hub/server.go)
- [packages/hub/config.go](packages/hub/config.go)
- [packages/hub/interfaces.go](packages/hub/interfaces.go)

## Phase 2: Diff Analysis + Invalidation Mapping

- Build config diff analysis to classify what changed.
- Map change categories to invalidation actions.
- Initial selected invalidation scope:
  - Response cache invalidation only.
- Lease/rate state behavior:
  - Do not clear active lease state; let current lease expire naturally.
- Removal semantics:
  - Mark-and-expire behavior for removed prefix/service cache surfaces.
- Files:
  - [packages/hub](packages/hub)
  - [packages/common/types](packages/common/types)

## Phase 3: Edge Notification + Execution

- Publish reload event from Hub with epoch + invalidation payload.
- Keep eventual consistency via pub/sub plus existing polling fallback.
- Execute scoped response-cache invalidation on edge.
- Ensure idempotency for duplicate/stale notifications.
- Files:
  - [packages/edge/config.go](packages/edge/config.go)
  - [packages/edge/cache.go](packages/edge/cache.go)
  - [packages/edge/server.go](packages/edge/server.go)

## Phase 4: Tests + Docs

- Unit tests for dry-run, apply, diff mapping, and notification payload behavior.
- Integration tests for runtime convergence and scoped invalidation.
- Docs update for new runtime update API and behavior.
- Files:
  - [packages/hub/server_test.go](packages/hub/server_test.go)
  - [testing/integration/scenario_config_reload_test.go](testing/integration/scenario_config_reload_test.go)
  - [docs/hub.md](docs/hub.md)
  - [docs/configuration.md](docs/configuration.md)

Status: completed.

## Implementation Checklist

- [x] Add Hub dry-run endpoint.
- [x] Add Hub apply-from-payload path.
- [x] Add config diff analyzer.
- [x] Add reload event payload type with epoch + invalidations.
- [x] Add edge subscriber payload handling.
- [x] Add edge response-cache invalidation executor.
- [x] Add unit tests for new Hub endpoints.
- [x] Add integration test coverage for reload/invalidation convergence.
