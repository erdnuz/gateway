#!/usr/bin/env bash
# Hub scaling: grants per second as hub replicas and Redis Cluster shards
# grow together (1+1, 2+2, 3+3), every container capped at 1 CPU.
#
#   testing/loadtest/hubscale.sh [label]
#
# Writes testing/loadtest/results/<label>/report.md. SIZES="1 3" for a subset.
set -uo pipefail
export MSYS_NO_PATHCONV=1

LABEL="${1:-hubscale}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"
abs() { (cd "$1" && (pwd -W 2>/dev/null || pwd)); }

CERT_DIR="${ROOT}/testing/loadtest/.certs"
if [ ! -f "${CERT_DIR}/edge/tls.crt" ]; then
  bash deploy/gen-dev-certs.sh "${CERT_DIR}" hub-server,localhost
fi
export LT_CERT_DIR="$(abs "${CERT_DIR}")"

COMPOSE=(docker compose -f testing/loadtest/compose.hubscale.yaml)
RESULTS="${ROOT}/testing/loadtest/results/${LABEL}"
rm -rf "${RESULTS}" && mkdir -p "${RESULTS}"
{
  echo "# Hub scaling: ${LABEL}"
  echo
  echo "- commit: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (dirty)')"
  echo "- date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- host: $(docker info --format '{{.NCPU}} cpu, {{.MemTotal}} bytes mem')"
  echo "- each hub and Redis shard capped at 1 CPU; loadgen 2 CPU, 64 callers, ${BATCH:-64} leases per RPC, 100k keys"
  echo
  echo "| topology | hubs | grants/s | RPCs/s | p50 ms | p99 ms | errors |"
  echo "|---|---|---|---|---|---|---|"
} >"${RESULTS}/report.md"

"${COMPOSE[@]}" --profile tools build || exit 1
for n in ${SIZES:-1 2 3}; do
  echo "=== ${n} hubs, ${n} shards ==="
  "${COMPOSE[@]}" --profile tools down -v --remove-orphans >/dev/null 2>&1
  export SHARDS="${n}"
  export REDIS_ADDRS="$(echo r1:6379,r2:6379,r3:6379 | cut -d, -f1-"${n}")"
  "${COMPOSE[@]}" up -d --wait --scale hub-server="${n}" hub-server || { "${COMPOSE[@]}" logs --tail 40; continue; }
  "${COMPOSE[@]}" run --rm -T loadgen -scenario hubscale -out "/results/${LABEL}" -hubscale-label "${n} hub(s) + ${n} shard(s)" -hubscale-batch "${BATCH:-64}"
done
"${COMPOSE[@]}" --profile tools down -v --remove-orphans >/dev/null 2>&1
echo "results: ${RESULTS}"
