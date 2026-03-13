# Gate Demo: Redeploy to Live Simulation

This demo runs the full local stack from a clean Docker reset to live traffic generation.

## What This Demo Does

1. Removes all existing Docker containers and images.
2. Starts a mock upstream service on port `9000`.
3. Redeploys services in strict dependency order:
   - Hub (`/readyz`)
   - Analytics (`/readyz`)
   - Edge (`/readyz`)
4. Runs a live traffic simulator that continuously sends requests through Edge.

## Prerequisites

- Docker and Docker Compose installed.
- Bash shell.
- Local ports available:
  - `8080` (Hub)
  - `8082` (Edge)
  - `8091` (Analytics)
  - `9000` (Mock upstream)
- You are in the repository root.

## Important Safety Note

`deployments/redeploy_fresh_all_docker.sh` is destructive on your Docker host.

It will remove:
- all containers
- all images
- Docker build cache and unused networks/volumes

Use it only on a development machine.

## Step 1: Full Redeploy

From the repository root:

```bash
cd deployments
CONFIRM=YES START_SIMULATION=no ./redeploy_fresh_all_docker.sh
```

Why `START_SIMULATION=no` here:
- It keeps redeploy and simulation as two explicit demo steps.
- You can verify readiness first, then start traffic on demand.

Expected milestones in output:
- `hub is ready (http://localhost:8080/readyz)`
- `analytics is ready (http://localhost:8091/readyz)`
- `edge is ready (http://localhost:8082/readyz)`
- `Redeploy complete.`

## Step 2: Start Live Traffic Simulation

From the repository root:

```bash
bash ./testing/simulate_live_traffic.sh
```

What the simulator does:
- Verifies `healthz` on Hub/Edge/Analytics.
- Pulls Hub config and validates demo prefix/service.
- Seeds two API keys into tiers.
- Sends mixed traffic patterns (cache-like, quota pressure, mixed methods).
- Prints a running line like:

```text
iter=42 status=200 approx_cache_patterns=11 rate_limited=4
```

Stop simulation with `Ctrl+C`.

## Optional: One-Command Demo

If you want redeploy + simulation in one command:

```bash
cd deployments
CONFIRM=YES ./redeploy_fresh_all_docker.sh
```

This runs simulation automatically after successful redeploy.

## Quick Verification Endpoints

```bash
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8091/healthz
curl -fsS http://localhost:8082/healthz

curl -fsS http://localhost:8080/readyz
curl -fsS http://localhost:8091/readyz
curl -fsS http://localhost:8082/readyz
```

## Troubleshooting

- `timed out waiting for ... /readyz`:
  - Inspect logs: `docker logs <container-name>`
  - Confirm dependent service is up first (Hub -> Analytics -> Edge).
- Simulator exits with config errors:
  - Ensure Hub config includes default `SIM_PREFIX` and `SIM_SERVICE` values.
  - Or override:

```bash
SIM_PREFIX=<prefix> SIM_SERVICE=<service> bash ./testing/simulate_live_traffic.sh
```

- Need slower or faster traffic:

```bash
SIM_MIN_SLEEP_MS=300 SIM_MAX_SLEEP_MS=1800 bash ./testing/simulate_live_traffic.sh
```
