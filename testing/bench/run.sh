#!/usr/bin/env bash
# Comparative benchmark of four rate limiters on one host.
#
#   testing/bench/run.sh [label]
#
# For each system (gate, counter, envoy-ref, envoy-gate) and data-plane size
# (1, 2, 4 replicas of 1 CPU each) it runs the loadgen "bench" scenario:
#   accuracy    one key at 2x its 500/s quota for 10 s: admitted per second
#   latency     2000 rps open loop over 1000 keys (quota never binding) for
#               20 s: p50/p99/p99.9, errors
#   backend     Redis commands per request during the latency phase
# Results: testing/bench/results/<label>/{report.md,results.csv,*.json}.
# SYSTEMS="gate envoy-gate" and SCALES="1 2" run a subset.
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
export BENCH_CERT_DIR="$(abs "${CERT_DIR}")"

SYSTEMS="${SYSTEMS:-gate counter envoy-ref envoy-gate}"
SCALES="${SCALES:-1 2 4}"
RESULTS="${ROOT}/testing/bench/results/${LABEL}"
rm -rf "${RESULTS}" && mkdir -p "${RESULTS}"
COMPOSE=(docker compose -f testing/bench/compose.yaml)

redis_calls() {
  "${COMPOSE[@]}" exec -T redis redis-cli INFO commandstats \
    | tr -d '\r' | sed -n 's/^cmdstat_[^:]*:calls=\([0-9]*\).*/\1/p' | awk '{s+=$1} END {print s+0}'
}

{
  echo "# Rate-limiter benchmark: ${LABEL}"
  echo
  echo "- commit: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (dirty)')"
  echo "- date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- host: $(docker info --format '{{.NCPU}} cpu, {{.MemTotal}} bytes mem, {{.OperatingSystem}}')"
  echo "- every data-plane replica, rate-limit service, hub and Redis is capped at 1 CPU; the load generator at 2"
  echo "- accuracy: one key offered 2x its 500/s quota for 10 s (most / mean admitted per full second)"
  echo "- latency: 2000 rps open loop over 1000 keys for 20 s; Redis commands per request over both phases"
  echo
  echo "| system | replicas | admitted per s (max / mean) | quota | rps | p50 ms | p99 ms | p99.9 ms | errors | Redis cmds / req |"
  echo "|---|---|---|---|---|---|---|---|---|---|"
} >"${RESULTS}/report.md"
echo "system,replicas,accuracy_max_per_s,accuracy_mean_per_s,quota_per_s,rps,p50_ms,p99_ms,p999_ms,errors,redis_cmds_per_req" >"${RESULTS}/results.csv"

"${COMPOSE[@]}" --profile tools --profile gate build || exit 1

for sys in ${SYSTEMS}; do
  for n in ${SCALES}; do
    echo "=== ${sys} x${n} ==="
    "${COMPOSE[@]}" --profile '*' down -v --remove-orphans >/dev/null 2>&1
    case "${sys}" in
      gate)       export EDGE_LIMITER=lease;         profile=gate;       svc=edge;  target=edge:8080;  path=/b/bench/ping ;;
      counter)    export EDGE_LIMITER=redis-counter; profile=counter;    svc=edge;  target=edge:8080;  path=/b/bench/ping ;;
      envoy-ref)  export ENVOY_CONFIG=envoy-ref.yaml;  profile=envoy-ref;  svc=envoy; target=envoy:8080; path=/ping ;;
      envoy-gate) export ENVOY_CONFIG=envoy-gate.yaml; profile=envoy-gate; svc=envoy; target=envoy:8080; path=/ping ;;
    esac
    "${COMPOSE[@]}" --profile "${profile}" up -d --wait --scale "${svc}=${n}" || { "${COMPOSE[@]}" --profile "${profile}" logs --tail 40; continue; }
    sleep 3
    "${COMPOSE[@]}" run --rm -T loadgen -scenario bench-setup -hub http://hub-server:8080 -prefix b >/dev/null || true
    before="$(redis_calls)"
    "${COMPOSE[@]}" run --rm -T loadgen -scenario bench -bench-target "${target}" -bench-path "${path}" \
      -bench-label "${sys}-x${n}" -out "/results/${LABEL}" -bench-calls-before "${before}"
    after="$(redis_calls)"
    # The loadgen wrote a CSV row with a placeholder for Redis commands.
    reqs="$(cat "${RESULTS}/${sys}-x${n}.requests" 2>/dev/null || echo 1)"
    per="$(awk -v a="$((after - before))" -v r="${reqs}" 'BEGIN{printf "%.3f", a/(r>0?r:1)}')"
    sed -i "s/REDIS_PER_REQ_${sys}-x${n}/${per}/" "${RESULTS}/results.csv" "${RESULTS}/report.md"
  done
done
"${COMPOSE[@]}" --profile '*' down -v --remove-orphans >/dev/null 2>&1
echo "results: ${RESULTS}"
