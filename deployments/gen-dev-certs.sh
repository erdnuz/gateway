#!/usr/bin/env bash
# Generates a throwaway CA plus hub (server) and edge (client) certificates for
# local development and load testing. Private keys never belong in git; the
# output directory is gitignored.
#
# Usage: deployments/gen-dev-certs.sh [out_dir] [hub_dns_names]
#   out_dir        default: deployments/certs
#   hub_dns_names  comma-separated SANs for the hub cert, default: hub-server,localhost
set -euo pipefail

OUT_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs}"
HUB_SANS="${2:-hub-server,localhost}"
DAYS="${CERT_DAYS:-825}"

# Git Bash on Windows ships a native openssl that cannot resolve POSIX temp
# paths; generate inside a container there for identical results everywhere.
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    if [ -z "${GEN_CERTS_IN_CONTAINER:-}" ]; then
      mkdir -p "${OUT_DIR}"
      out_win="$(cd "${OUT_DIR}" && pwd -W)"
      script_win="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -W)"
      MSYS_NO_PATHCONV=1 exec docker run --rm \
        -e GEN_CERTS_IN_CONTAINER=1 -e CERT_DAYS="${DAYS}" \
        -v "${out_win}:/out" -v "${script_win}:/scripts:ro" \
        alpine:3.20 sh -c "apk add --no-cache -q bash openssl >/dev/null && bash /scripts/gen-dev-certs.sh /out '${HUB_SANS}'"
    fi
    ;;
esac

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}/ca" "${OUT_DIR}/hub" "${OUT_DIR}/edge"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

san_list=""
IFS=',' read -ra names <<<"${HUB_SANS}"
for n in "${names[@]}"; do
  san_list="${san_list:+${san_list},}DNS:${n}"
done

# CA. The CA key stays in the work dir and is discarded: certs can be
# regenerated at any time, so nothing can mint new identities later.
openssl req -x509 -newkey rsa:2048 -nodes -days "${DAYS}" \
  -subj "/CN=gate-dev-ca" \
  -keyout "${WORK}/ca.key" -out "${OUT_DIR}/ca/ca.crt" 2>/dev/null

issue() {
  local name="$1" cn="$2" eku="$3" san="$4"
  cat >"${WORK}/${name}.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=${eku}
${san:+subjectAltName=${san}}
EOF
  openssl req -newkey rsa:2048 -nodes -subj "/CN=${cn}" \
    -keyout "${OUT_DIR}/${name}/tls.key" -out "${WORK}/${name}.csr"
  openssl x509 -req -in "${WORK}/${name}.csr" -CA "${OUT_DIR}/ca/ca.crt" -CAkey "${WORK}/ca.key" \
    -CAcreateserial -days "${DAYS}" -extfile "${WORK}/${name}.ext" \
    -out "${OUT_DIR}/${name}/tls.crt"
  # Readable by the non-root service users in containers; dev-only material.
  chmod 644 "${OUT_DIR}/${name}/tls.key"
}

issue hub hub-server serverAuth "${san_list}"
issue edge gate-edge clientAuth ""

echo "wrote dev certs to ${OUT_DIR} (hub SANs: ${HUB_SANS})"
