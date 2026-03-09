# TODO: Current Backlog

Last updated: 2026-03-09

## Done In Current Architecture

- [x] Service split is `edge`, `hub`, `analytics` (no standalone `dashboard` service).
- [x] Cross-plane sync is Kafka-backed for rate deltas, SOT totals, tier updates, and analytics events.
- [x] Hub enforces bearer auth for all control-plane routes except `/health`.
- [x] Hub validates gateway config structure and policy semantics before loading it into memory.
- [x] Edge supports hub outage strategies: `fail-closed`, `default-tier`, `stale-or-default`.
- [x] Edge proxy path supports retry policy and `fail-open` fallback responses.
- [x] Analytics service provides `/analytics/events` and `/analytics/summary` read APIs plus an embedded frontend.
- [x] A lite deployment path is available via `deployments/docker-compose.lite.yaml` with analytics functionality disabled.

## P0: Security and Correctness

- [ ] Add request throttling or per-token rate limits to analytics read endpoints.
	- Why: analytics endpoints are authenticated but still vulnerable to high-frequency polling.
	- Files: `packages/analytics/server.go`, `cmd/analytics/main.go`

- [ ] Add explicit secret rotation and token management runbook.
	- Why: `HUB_AUTH_TOKEN` and `ANALYTICS_API_TOKEN` are required in production.
	- Files: `README.md`, `deployments/docker-compose.yaml`

- [ ] Add end-to-end test for tier update propagation over Kafka.
	- Why: edge cache invalidation correctness depends on `tier-updates` consumption.
	- Files: `packages/hub/server_test.go`, `packages/edge/tiers_async_test.go`

## P1: Resilience and Observability

- [ ] Add queue metrics for bounded workers (submitted, dropped, retries, failures).
	- Why: queue pressure is currently log-driven and hard to alert on.
	- Files: `packages/common/workers/bounded_queue.go`, `packages/edge/rates.go`, `packages/hub/server.go`

- [ ] Add outage matrix tests for hub-tier fallback strategies.
	- Why: `default-tier` and `stale-or-default` behavior should remain stable under refactors.
	- Files: `packages/edge/server_test.go`, `packages/edge/tiers.go`

- [ ] Add failure-path tests for upstream retry policies and fallback response headers.
	- Why: policy behavior (`retry_on_statuses`, `fallback_headers`) is central to runtime safety.
	- Files: `packages/edge/server_test.go`, `packages/common/types/policies.go`

## P2: Performance and Cost

- [ ] Add benchmarks for edge hot path (GET cached, GET uncached, POST proxied).
	- Why: recent streaming/cache changes should be measured before tuning defaults.
	- Files: new benchmark tests under `packages/edge/*_test.go`

- [ ] Add analytics retention/trim policy (max list length or time-based trimming).
	- Why: analytics Redis list growth is currently unbounded.
	- Files: `packages/analytics/server.go`, `cmd/analytics/main.go`

- [ ] Document and validate `memory` tier store mode for lightweight deployments.
	- Why: this mode is useful in non-persistent environments and should be clearly operationalized.
	- Files: `README.md`, `cmd/hub/main.go`

## P3: Developer Experience

- [ ] Add `make` targets or scripts for local run/test/lint.
	- Why: standard commands reduce onboarding friction and CI drift.
	- Files: `README.md`, `testing/`, optional `Makefile`

- [ ] Add architecture diagram for edge-hub-analytics Kafka topology.
	- Why: messaging flows are now central and should be visible to contributors.
	- Files: `README.md`

- [ ] Add smoke test script that validates all three services and auth contracts.
	- Why: catches wiring regressions quickly after config changes.
	- Files: `testing/simulate_requests.sh`, `testing/simulate_requests_extensive.sh`