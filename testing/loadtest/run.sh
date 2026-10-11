#!/usr/bin/env bash
# End-to-end rate-limiter load test.
#
#   testing/loadtest/run.sh [label]
#
# Brings up hub (+ Redis) + 2 edges + upstream via
# testing/loadtest/compose.yaml, runs every loadgen scenario, and
# writes testing/loadtest/results/<label>/{report.md,*.json}.
# KEEP=1 leaves the stack running afterwards.
# ONLY="throughput hotkey" runs a subset (setup always runs).
set -uo pipefail
export MSYS_NO_PATHCONV=1

LABEL="${1:-run}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

abs() { (cd "$1" && (pwd -W 2>/dev/null || pwd)); }

CERT_DIR="${ROOT}/testing/loadtest/.certs"
if [ ! -f "${CERT_DIR}/edge/tls.crt" ]; then
  bash deploy/gen-dev-certs.sh "${CERT_DIR}" hub-server,localhost
fi
export LT_CERT_DIR="$(abs "${CERT_DIR}")"

COMPOSE=(docker compose -f testing/loadtest/compose.yaml)
RESULTS="${ROOT}/testing/loadtest/results/${LABEL}"
rm -rf "${RESULTS}"
mkdir -p "${RESULTS}"

log() { printf '\n=== %s ===\n' "$*"; }

log "build + start stack"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1
"${COMPOSE[@]}" --profile tools build || exit 1
"${COMPOSE[@]}" up -d --wait || { "${COMPOSE[@]}" logs --tail 50; exit 1; }

{
  echo "# Gate rate-limiter e2e: ${LABEL}"
  echo
  echo "- commit: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (dirty)')"
  echo "- date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- topology: hub(2 cpu) + hub Redis + 2 edges(2 cpu each, in-memory caches) + 2 gate limiter replicas(1 cpu each) + upstream; loadgen 4 cpu"
  echo "- host: $(docker info --format '{{.NCPU}} cpu, {{.MemTotal}} bytes mem, {{.OperatingSystem}}')"
} >"${RESULTS}/report.md"

PASS=()
FAIL=()
want() { [ -z "${ONLY:-}" ] || [[ " ${ONLY} " == *" $1 "* ]]; }

lg() {
  local scenario="$1"; shift
  case "${scenario}" in setup) ;; outage-warm|outage-check) want outage || return 0 ;; drain-warm) want drain || return 0 ;; *) want "${scenario}" || return 0 ;; esac
  log "scenario ${scenario}"
  "${COMPOSE[@]}" run --rm -T loadgen -scenario "${scenario}" -out "/results/${LABEL}" "$@"
  local rc=$?
  case "${scenario}" in setup|outage-warm) [ "${rc}" = 0 ] || FAIL+=("${scenario}(error ${rc})"); return ;; esac
  case "${rc}" in
    0) PASS+=("${scenario}") ;;
    2) FAIL+=("${scenario}") ;;
    *) FAIL+=("${scenario}(error ${rc})") ;;
  esac
}

# hub_calls counts commands on the hub's Redis: the control plane's whole
# share of the work, which should grow with leases, not requests.
hub_calls() {
  "${COMPOSE[@]}" exec -T hub-redis redis-cli INFO commandstats \
    | tr -d '\r' | sed -n 's/^cmdstat_[^:]*:calls=\([0-9]*\).*/\1/p' | awk '{s+=$1} END {print s+0}'
}

lg setup
lg windows
lg carryover
lg isolation
lg bypass
lg propagation

if want throughput; then
before="$(hub_calls)"
( for _ in $(seq 1 12); do
    docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null
    sleep 2
  done ) >"${RESULTS}/docker-stats.txt" &
STATS_PID=$!
lg throughput
after="$(hub_calls)"
wait "${STATS_PID}" 2>/dev/null
reqs="$(sed -n 's/.*"total_requests": \([0-9]*\).*/\1/p' "${RESULTS}/throughput.json" | head -1)"
{
  echo
  echo "### control-plane cost (throughput scenario)"
  echo
  echo "- hub Redis commands: $((after - before)) for ${reqs:-0} requests = **$(awk -v a=$((after - before)) -v r="${reqs:-1}" 'BEGIN{printf "%.2f", a/(r>0?r:1)}') per request**"
  echo "- peak CPU per container (docker stats samples):"
  echo
  awk '{gsub("%","",$2); if ($2+0 > max[$1]) max[$1]=$2+0} END {for (k in max) printf "  - %s: %.0f%%\n", k, max[k]}' "${RESULTS}/docker-stats.txt" | sort
} | tee -a "${RESULTS}/report.md"
fi

lg hotkey
lg endgame
lg algorithms
lg multilimit
lg llm
lg manykeys
lg rls

# Lease return on drain: both edges lease, edge 1 shuts down gracefully
# (returning its tokens), edge 2 then uses the rest of the window.
if want drain; then
  "${COMPOSE[@]}" run --rm -T loadgen -scenario drain-warm -out "/results/${LABEL}" || FAIL+=("drain-warm")
  log "stopping edge-1 (graceful)"
  "${COMPOSE[@]}" stop -t 10 edge-1
  lg drain -edges http://edge-1:8080,http://edge-2:8080
  log "starting edge-1"
  "${COMPOSE[@]}" start edge-1
  "${COMPOSE[@]}" up -d --wait >/dev/null 2>&1
fi

if want outage; then
  lg outage-warm
  log "stopping hub"
  "${COMPOSE[@]}" stop hub-server
  lg outage
  log "starting hub"
  "${COMPOSE[@]}" start hub-server
  "${COMPOSE[@]}" up -d --wait >/dev/null 2>&1
  lg outage-check
fi

{
  echo
  echo "## Summary"
  echo
  echo "- passed: ${PASS[*]:-none}"
  echo "- failed: ${FAIL[*]:-none}"
} | tee -a "${RESULTS}/report.md"

"${COMPOSE[@]}" logs --no-color >"${RESULTS}/compose.log" 2>&1
if [ "${KEEP:-0}" != "1" ]; then
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1
fi
echo "results: ${RESULTS}"
