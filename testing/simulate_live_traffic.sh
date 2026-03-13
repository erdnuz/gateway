#!/usr/bin/env bash
set -euo pipefail

HUB_URL="${HUB_URL:-http://localhost:8080}"
EDGE_URL="${EDGE_URL:-http://localhost:8082}"
ANALYTICS_URL="${ANALYTICS_URL:-http://localhost:8091}"
HUB_TOKEN="${HUB_AUTH_TOKEN:-gate-dev-token}"

SIM_PREFIX="${SIM_PREFIX:-v1}"
SIM_SERVICE="${SIM_SERVICE:-auth-api}"
SIM_MIN_SLEEP_MS="${SIM_MIN_SLEEP_MS:-120}"
SIM_MAX_SLEEP_MS="${SIM_MAX_SLEEP_MS:-1400}"

# Hub policy regex allows only alphanumeric API keys.
FREE_KEY="${FREE_KEY:-livesimfreekey}"
PRO_KEY="${PRO_KEY:-livesimprokey}"

if [ "${SIM_MAX_SLEEP_MS}" -lt "${SIM_MIN_SLEEP_MS}" ]; then
  echo "SIM_MAX_SLEEP_MS must be >= SIM_MIN_SLEEP_MS" >&2
  exit 1
fi

for cmd in curl awk sed grep; do
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "required command missing: ${cmd}" >&2
    exit 1
  fi
done

cleanup() {
  echo
  echo "stopping live simulation"
  exit 0
}
trap cleanup INT TERM

rand_between() {
  local min="$1"
  local max="$2"
  local span=$((max - min + 1))
  echo $((min + RANDOM % span))
}

ms_to_seconds() {
  local ms="$1"
  awk "BEGIN { printf \"%.3f\", ${ms}/1000 }"
}

consult_config() {
  local cfg
  cfg="$(curl -fsS -H "Authorization: Bearer ${HUB_TOKEN}" "${HUB_URL}/config")"

  if ! printf '%s' "${cfg}" | grep -q '"prefix"[[:space:]]*:[[:space:]]*"'"${SIM_PREFIX}"'"'; then
    echo "prefix ${SIM_PREFIX} not found in hub config" >&2
    exit 1
  fi
  if ! printf '%s' "${cfg}" | grep -q '"service_id"[[:space:]]*:[[:space:]]*"'"${SIM_SERVICE}"'"'; then
    echo "service ${SIM_SERVICE} not found in hub config" >&2
    exit 1
  fi

  FREE_TIER=""
  PRO_TIER=""
  if printf '%s' "${cfg}" | grep -q '"tier_id"[[:space:]]*:[[:space:]]*"free"'; then
    FREE_TIER="free"
  fi
  if printf '%s' "${cfg}" | grep -q '"tier_id"[[:space:]]*:[[:space:]]*"pro"'; then
    PRO_TIER="pro"
  fi

  if [ -z "${FREE_TIER}" ] || [ -z "${PRO_TIER}" ]; then
    mapfile -t VALID_TIERS < <(printf '%s' "${cfg}" | grep -o '"tier_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed -E 's/.*"([^"]+)"$/\1/' | awk '!seen[$0]++')
    if [ "${#VALID_TIERS[@]}" -eq 0 ]; then
      echo "no tiers found in hub config" >&2
      exit 1
    fi
    if [ -z "${FREE_TIER}" ]; then
      FREE_TIER="${VALID_TIERS[0]}"
    fi
    if [ -z "${PRO_TIER}" ]; then
      if [ "${#VALID_TIERS[@]}" -gt 1 ]; then
        PRO_TIER="${VALID_TIERS[1]}"
      else
        PRO_TIER="${VALID_TIERS[0]}"
      fi
    fi
  fi

  echo "config consulted: prefix=${SIM_PREFIX}, service=${SIM_SERVICE}, free_tier=${FREE_TIER}, pro_tier=${PRO_TIER}"
}

seed_api_keys() {
  echo "seeding api keys with valid tiers"

  curl -fsS -X PUT "${HUB_URL}/tiers/${SIM_PREFIX}/${FREE_KEY}" \
    -H "Authorization: Bearer ${HUB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"tier_id\":\"${FREE_TIER}\"}" >/dev/null

  curl -fsS -X PUT "${HUB_URL}/tiers/${SIM_PREFIX}/${PRO_KEY}" \
    -H "Authorization: Bearer ${HUB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"tier_id\":\"${PRO_TIER}\"}" >/dev/null

  echo "seeded: ${FREE_KEY} -> ${FREE_TIER}, ${PRO_KEY} -> ${PRO_TIER}"
}

health_check() {
  curl -fsS "${HUB_URL}/healthz" >/dev/null
  curl -fsS "${EDGE_URL}/healthz" >/dev/null
  curl -fsS "${ANALYTICS_URL}/healthz" >/dev/null
  echo "health check passed for hub/edge/analytics"
}

send_request() {
  local key="$1"
  local method="$2"
  local path="$3"
  local body="${4:-}"
  local url="${EDGE_URL}/${SIM_PREFIX}/${SIM_SERVICE}/${path}?r=$RANDOM&t=$(date +%s%N)"

  if [ -n "${body}" ]; then
    curl -s -o /dev/null -w "%{http_code}" -X "${method}" \
      -H "X-API-Key: ${key}" \
      -H "Content-Type: application/json" \
      --data-binary "${body}" \
      "${url}"
  else
    curl -s -o /dev/null -w "%{http_code}" -X "${method}" \
      -H "X-API-Key: ${key}" \
      "${url}"
  fi
}

main_loop() {
  local iter=0
  local hits=0
  local limited=0

  echo "starting live non-deterministic simulation (ctrl-c to stop)"

  while true; do
    iter=$((iter + 1))
    local mode=$((RANDOM % 100))
    local status=""

    if [ "${mode}" -lt 35 ]; then
      # Cache-hit pattern: same GET path twice quickly on pro key.
      local hot_path="hot-cache-$((RANDOM % 3))"
      local first
      local second
      first="$(send_request "${PRO_KEY}" "GET" "${hot_path}")"
      sleep "$(ms_to_seconds "$(rand_between 80 180)")"
      second="$(send_request "${PRO_KEY}" "GET" "${hot_path}")"
      status="${second}"
      if [ "${first}" = "200" ] && [ "${second}" = "200" ]; then
        hits=$((hits + 1))
      fi
    elif [ "${mode}" -lt 75 ]; then
      # Quota-pressure pattern: mostly free-key requests to induce some 429.
      local paths=("burst-a" "burst-b" "burst-c" "limits")
      local method="GET"
      if [ $((RANDOM % 100)) -lt 25 ]; then
        method="POST"
      fi
      local picked_path="${paths[$((RANDOM % ${#paths[@]}))]}"
      if [ "${method}" = "POST" ]; then
        status="$(send_request "${FREE_KEY}" "POST" "${picked_path}" "{\"load\":$RANDOM}")"
      else
        status="$(send_request "${FREE_KEY}" "GET" "${picked_path}")"
      fi
    else
      # Mixed background traffic.
      local key="${FREE_KEY}"
      if [ $((RANDOM % 100)) -lt 45 ]; then
        key="${PRO_KEY}"
      fi
      local method="GET"
      if [ $((RANDOM % 100)) -lt 30 ]; then
        method="PUT"
      fi
      local path="mixed-$((RANDOM % 8))"
      if [ "${method}" = "PUT" ]; then
        status="$(send_request "${key}" "PUT" "${path}" "{\"n\":$RANDOM}")"
      else
        status="$(send_request "${key}" "GET" "${path}")"
      fi
    fi

    if [ "${status}" = "429" ]; then
      limited=$((limited + 1))
    fi

    printf 'iter=%d status=%s approx_cache_patterns=%d rate_limited=%d\n' "${iter}" "${status}" "${hits}" "${limited}"

    local sleep_ms
    sleep_ms="$(rand_between "${SIM_MIN_SLEEP_MS}" "${SIM_MAX_SLEEP_MS}")"
    sleep "$(ms_to_seconds "${sleep_ms}")"
  done
}

health_check
consult_config
seed_api_keys
main_loop
