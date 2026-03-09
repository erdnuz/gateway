#!/usr/bin/env bash
set -euo pipefail

HUB_URL="${HUB_URL:-http://localhost:8080}"
EDGE_URL="${EDGE_URL:-http://localhost:8082}"
ANALYTICS_URL="${ANALYTICS_URL:-http://localhost:8091}"
HUB_TOKEN="${HUB_AUTH_TOKEN:-dev-hub-token}"
ANALYTICS_TOKEN="${ANALYTICS_API_TOKEN:-dev-analytics-token}"
ANALYTICS_KEY="${ANALYTICS_REDIS_KEY:-rate-analytics}"
ANALYTICS_REDIS_CONTAINER="${ANALYTICS_REDIS_CONTAINER:-analytics-redis}"

SIM_TOTAL_REQUESTS="${SIM_TOTAL_REQUESTS:-120}"
SIM_MIN_SLEEP_MS="${SIM_MIN_SLEEP_MS:-150}"
SIM_MAX_SLEEP_MS="${SIM_MAX_SLEEP_MS:-900}"
SIM_RESULT_FILE="${SIM_RESULT_FILE:-/tmp/gate_random_results.csv}"
SIM_CLEAR_ANALYTICS="${SIM_CLEAR_ANALYTICS:-true}"
SIM_SEED="${SIM_SEED:-}"

SIM_API_KEYS="${SIM_API_KEYS:-test-owner-key-a,test-owner-key-b,test-owner-key-c,test-owner-key-d}"
SIM_PREFIXES="${SIM_PREFIXES:-v1}"
SIM_SERVICES="${SIM_SERVICES:-auth-api}"
SIM_PATHS="${SIM_PATHS:-ping,status,echo,profile,usage,items,limits,healthcheck}"
SIM_METHODS="${SIM_METHODS:-GET,POST,PUT,DELETE,PATCH}"
SIM_BODY_SIZES="${SIM_BODY_SIZES:-0,32,128,512,1024,4096,8192,16384}"
SIM_TIERS="${SIM_TIERS:-free,pro,enterprise}"

ANALYTICS_INGEST_WAIT_SECONDS="${ANALYTICS_INGEST_WAIT_SECONDS:-2}"
ANALYTICS_INGEST_MAX_WAIT_SECONDS="${ANALYTICS_INGEST_MAX_WAIT_SECONDS:-40}"

if [ -n "${SIM_SEED}" ]; then
  RANDOM="${SIM_SEED}"
fi

if [ "${SIM_MAX_SLEEP_MS}" -lt "${SIM_MIN_SLEEP_MS}" ]; then
  echo "SIM_MAX_SLEEP_MS must be >= SIM_MIN_SLEEP_MS" >&2
  exit 1
fi

split_csv() {
  local raw="$1"
  local -n target_ref="$2"
  IFS=',' read -r -a target_ref <<<"${raw}"
}

pick_random() {
  local -n arr_ref="$1"
  local size="${#arr_ref[@]}"
  local idx=$((RANDOM % size))
  printf '%s' "${arr_ref[$idx]}"
}

wait_for_analytics_data() {
  local elapsed=0
  while [ "${elapsed}" -lt "${ANALYTICS_INGEST_MAX_WAIT_SECONDS}" ]; do
    summary="$(curl -fsS -H "Authorization: Bearer ${ANALYTICS_TOKEN}" "${ANALYTICS_URL}/analytics/summary?limit=1000" || true)"
    count="$(printf '%s' "${summary}" | sed -n 's/.*"count"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)"
    if [ -n "${count}" ] && [ "${count}" -gt 0 ]; then
      return 0
    fi
    sleep "${ANALYTICS_INGEST_WAIT_SECONDS}"
    elapsed=$((elapsed + ANALYTICS_INGEST_WAIT_SECONDS))
  done
  return 1
}

create_payload() {
  local n="$1"
  if [ "${n}" -le 0 ]; then
    printf '{}'
    return
  fi
  local blob
  blob="$(head -c "${n}" < /dev/zero | tr '\0' 'x')"
  printf '{"blob":"%s"}' "${blob}"
}

ms_to_seconds() {
  local ms="$1"
  awk "BEGIN {printf \"%.3f\", ${ms}/1000}"
}

split_csv "${SIM_API_KEYS}" API_KEYS
split_csv "${SIM_PREFIXES}" PREFIXES
split_csv "${SIM_SERVICES}" SERVICES
split_csv "${SIM_PATHS}" PATHS
split_csv "${SIM_METHODS}" METHODS
split_csv "${SIM_BODY_SIZES}" BODY_SIZES
split_csv "${SIM_TIERS}" TIERS

if [ "${#API_KEYS[@]}" -eq 0 ] || [ "${#METHODS[@]}" -eq 0 ] || [ "${#PATHS[@]}" -eq 0 ]; then
  echo "Invalid configuration: one or more CSV lists are empty" >&2
  exit 1
fi

printf '\n[1/7] Checking health endpoints...\n'
curl -fsS "${HUB_URL}/health" >/dev/null
curl -fsS "${ANALYTICS_URL}/health" >/dev/null
printf 'Hub and analytics are healthy.\n'

printf '\n[2/7] Seeding random tiers for API keys...\n'
for key in "${API_KEYS[@]}"; do
  tier="$(pick_random TIERS)"
  prefix="$(pick_random PREFIXES)"
  curl -fsS -X PUT "${HUB_URL}/tiers/${prefix}/${key}" \
    -H "Authorization: Bearer ${HUB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"tier_id\":\"${tier}\"}" >/dev/null
  printf 'seeded key=%s prefix=%s tier=%s\n' "${key}" "${prefix}" "${tier}"
done

printf '\n[3/7] Optional analytics reset...\n'
if [ "${SIM_CLEAR_ANALYTICS}" = "true" ] && docker ps --format '{{.Names}}' | grep -qx "${ANALYTICS_REDIS_CONTAINER}"; then
  docker exec "${ANALYTICS_REDIS_CONTAINER}" redis-cli DEL "${ANALYTICS_KEY}" >/dev/null
  printf 'cleared analytics key=%s in %s\n' "${ANALYTICS_KEY}" "${ANALYTICS_REDIS_CONTAINER}"
else
  printf 'analytics reset skipped.\n'
fi

printf '\n[4/7] Running randomized simulation (%s requests)...\n' "${SIM_TOTAL_REQUESTS}"
printf 'req,api_key,prefix,service,path,method,body_bytes,sleep_ms,status,time_total_s,size_upload,size_download\n' >"${SIM_RESULT_FILE}"

for i in $(seq 1 "${SIM_TOTAL_REQUESTS}"); do
  api_key="$(pick_random API_KEYS)"
  prefix="$(pick_random PREFIXES)"
  service="$(pick_random SERVICES)"
  path="$(pick_random PATHS)"
  method="$(pick_random METHODS)"
  body_size="$(pick_random BODY_SIZES)"

  range=$((SIM_MAX_SLEEP_MS - SIM_MIN_SLEEP_MS + 1))
  sleep_ms=$((SIM_MIN_SLEEP_MS + (RANDOM % range)))

  url="${EDGE_URL}/${prefix}/${service}/${path}?request_id=${i}&seed=${RANDOM}&size=${body_size}"

  if [ "${method}" = "GET" ] || [ "${method}" = "DELETE" ]; then
    metrics="$(curl -s -o /tmp/gate_random_response.txt \
      -w '%{http_code},%{time_total},%{size_upload},%{size_download}' \
      -X "${method}" \
      -H "X-API-Key: ${api_key}" \
      "${url}")"
  else
    payload="$(create_payload "${body_size}")"
    metrics="$(curl -s -o /tmp/gate_random_response.txt \
      -w '%{http_code},%{time_total},%{size_upload},%{size_download}' \
      -X "${method}" \
      -H "X-API-Key: ${api_key}" \
      -H 'Content-Type: application/json' \
      --data-binary "${payload}" \
      "${url}")"
  fi

  status="$(printf '%s' "${metrics}" | cut -d',' -f1)"
  total_s="$(printf '%s' "${metrics}" | cut -d',' -f2)"
  size_upload="$(printf '%s' "${metrics}" | cut -d',' -f3)"
  size_download="$(printf '%s' "${metrics}" | cut -d',' -f4)"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${i}" "${api_key}" "${prefix}" "${service}" "${path}" "${method}" "${body_size}" "${sleep_ms}" "${status}" "${total_s}" "${size_upload}" "${size_download}" \
    >>"${SIM_RESULT_FILE}"

  printf 'request=%s key=%s method=%s path=%s body=%s status=%s sleep=%sms\n' \
    "${i}" "${api_key}" "${method}" "${path}" "${body_size}" "${status}" "${sleep_ms}"

  sleep "$(ms_to_seconds "${sleep_ms}")"
done

printf '\n[5/7] Status distribution:\n'
awk -F',' 'NR>1 {c[$9]++} END {for (k in c) printf "status %s -> %d\n", k, c[k]}' "${SIM_RESULT_FILE}" | sort

printf '\nBy method and status:\n'
awk -F',' 'NR>1 {k=$6":"$9; c[k]++} END {for (k in c) printf "%s -> %d\n", k, c[k]}' "${SIM_RESULT_FILE}" | sort

printf '\n[6/7] Waiting for analytics ingestion...\n'
if ! wait_for_analytics_data; then
  printf 'Timed out waiting for analytics events after %ss.\n' "${ANALYTICS_INGEST_MAX_WAIT_SECONDS}"
fi

printf '\n[7/7] Analytics summary snapshots:\n'
printf '\nOverall summary:\n'
curl -fsS -H "Authorization: Bearer ${ANALYTICS_TOKEN}" \
  "${ANALYTICS_URL}/analytics/summary?limit=2000"
printf '\n\nBy service:\n'
curl -fsS -H "Authorization: Bearer ${ANALYTICS_TOKEN}" \
  "${ANALYTICS_URL}/analytics/summary?group_by=service&limit=2000"
printf '\n\nBy method:\n'
curl -fsS -H "Authorization: Bearer ${ANALYTICS_TOKEN}" \
  "${ANALYTICS_URL}/analytics/summary?group_by=method&limit=2000"
printf '\n\nRecent events:\n'
curl -fsS -H "Authorization: Bearer ${ANALYTICS_TOKEN}" \
  "${ANALYTICS_URL}/analytics/events?limit=10"
printf '\n\nDetailed results written to %s\n' "${SIM_RESULT_FILE}"
