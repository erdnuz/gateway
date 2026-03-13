#!/usr/bin/env bash
set -euo pipefail

# WARNING:
# This script removes ALL Docker containers and ALL Docker images on the host.
# Use only on a development machine where this is acceptable.

UPSTREAM_CONTAINER_NAME="${UPSTREAM_CONTAINER_NAME:-gate-mock-upstream}"
UPSTREAM_HOST_PORT="${UPSTREAM_HOST_PORT:-9000}"
UPSTREAM_CONTAINER_PORT="${UPSTREAM_CONTAINER_PORT:-5678}"
UPSTREAM_TEXT="${UPSTREAM_TEXT:-gate upstream ok}"
START_SIMULATION="${START_SIMULATION:-yes}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but not found" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

wait_for_http() {
  local name="$1"
  local url="$2"
  local attempts="${3:-30}"
  local delay_seconds="${4:-2}"
  local i

  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "${name} is ready"
      return 0
    fi
    sleep "${delay_seconds}"
  done

  echo "timed out waiting for ${name} at ${url}" >&2
  return 1
}

start_upstream() {
  echo "Starting upstream mock service..."

  if docker ps --format '{{.Names}}' | grep -Fxq "${UPSTREAM_CONTAINER_NAME}"; then
    echo "Upstream container ${UPSTREAM_CONTAINER_NAME} is already running"
  else
    if docker ps -a --format '{{.Names}}' | grep -Fxq "${UPSTREAM_CONTAINER_NAME}"; then
      echo "Removing stale upstream container ${UPSTREAM_CONTAINER_NAME}"
      docker rm -f "${UPSTREAM_CONTAINER_NAME}" >/dev/null
    fi

    docker run -d \
      --name "${UPSTREAM_CONTAINER_NAME}" \
      -p "${UPSTREAM_HOST_PORT}:${UPSTREAM_CONTAINER_PORT}" \
      hashicorp/http-echo:1.0.0 \
      -listen=":${UPSTREAM_CONTAINER_PORT}" \
      -text="${UPSTREAM_TEXT}" >/dev/null
  fi

  wait_for_http "mock upstream" "http://localhost:${UPSTREAM_HOST_PORT}/healthcheck"
}

start_simulation() {
  if [ "${START_SIMULATION}" != "yes" ]; then
    echo "Skipping simulation startup because START_SIMULATION=${START_SIMULATION}"
    return 0
  fi

  echo "Starting live traffic simulation..."
  exec bash "${SCRIPT_DIR}/../testing/simulate_live_traffic.sh"
}

if [ "${CONFIRM:-}" != "YES" ]; then
  cat <<'EOF'
This will:
1) Stop and remove ALL Docker containers
2) Remove ALL Docker images
3) Start the mock upstream service on port 9000
4) Redeploy hub, edge, and analytics compose stacks
5) Wait for health endpoints and start the live simulator

Re-run with: CONFIRM=YES ./redeploy_fresh_all_docker.sh
Optional: START_SIMULATION=no CONFIRM=YES ./redeploy_fresh_all_docker.sh
EOF
  exit 1
fi

echo "[1/7] Stopping/removing all containers..."
ALL_CONTAINERS="$(docker ps -aq || true)"
if [ -n "${ALL_CONTAINERS}" ]; then
  # shellcheck disable=SC2086
  docker rm -f ${ALL_CONTAINERS}
else
  echo "No containers found."
fi

echo "[2/7] Removing all images..."
ALL_IMAGES="$(docker images -aq | sort -u || true)"
if [ -n "${ALL_IMAGES}" ]; then
  # shellcheck disable=SC2086
  docker rmi -f ${ALL_IMAGES}
else
  echo "No images found."
fi

echo "[3/7] Pruning leftover build cache/networks/volumes..."
docker system prune -af --volumes

echo "[4/7] Starting upstream dependency..."
start_upstream

echo "[5/7] Redeploying compose stacks..."
docker compose -f docker-compose.hub.yaml --env-file .env.hub up -d --build --force-recreate
docker compose -f docker-compose.edge.yaml --env-file .env.edge up -d --build --force-recreate
docker compose -f docker-compose.analytics.yaml --env-file .env.analytics up -d --build --force-recreate

echo "[6/7] Waiting for service health..."
wait_for_http "hub" "http://localhost:8080/healthz"
wait_for_http "edge" "http://localhost:8082/healthz"
wait_for_http "analytics" "http://localhost:8091/healthz"

echo "[7/7] Final container status:"
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

echo "Redeploy complete."
start_simulation
