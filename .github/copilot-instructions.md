## Distributed Readiness Handshake Plan

### Goals
- Expose dual probes on all services:
	- `/healthz`: liveness, returns `200` once process is running.
	- `/readyz`: readiness, returns `200` only when service dependencies are satisfied.
- Keep services alive in pending mode when dependencies are missing.
- Never exit on readiness probe failures; continuously retry and log missing dependencies.

### Dependency Contract
- Hub (root): `/readyz` is healthy only when local Redis and local NATS are healthy.
- Analytics (middle): `/readyz` is healthy only when Hub `/readyz` is healthy and local ClickHouse + NATS are healthy.
- Edge (leaf): `/readyz` is healthy only when Hub `/readyz` and Analytics `/readyz` are both healthy.

### Startup Contract
- Hub HTTP starts immediately so `/healthz`, `/readyz`, and `/config` are available even if NATS/Redis are not yet ready.
- Analytics and Edge start HTTP and remain pending (`/readyz=503`) until watchers confirm dependencies.
- Edge blocks all client proxy traffic until upstream readiness is achieved.

### ReadyWatcher Contract
- Use shared `ReadyWatcher` with exponential backoff.
- Initial backoff: `500ms`.
- Maximum backoff: `5s`.
- Run continuously in background with dependency-specific logging.

### Rollout Phases
1. Build shared watcher utility in `packages/common/ready`.
2. Wire Hub local dependency watcher and non-blocking startup.
3. Wire Analytics watcher against Hub `/readyz` + local deps.
4. Wire Edge watcher against Hub and Analytics `/readyz` + client traffic gate.
5. Validate with fast full suite (`go test ./... -count=1`).
