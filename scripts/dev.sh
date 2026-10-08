#!/usr/bin/env bash
# Task runner behind the Makefile; also usable directly where make is not
# installed:  bash scripts/dev.sh <task>
set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

STACK=(docker compose -f deploy/compose.yaml)
GO_IMAGE="golang:1.26"

# go runs the Go toolchain locally when present, otherwise in a container
# with persistent module and build caches.
go_() {
  if command -v go >/dev/null 2>&1; then
    go "$@"
    return
  fi
  local src
  src="$(pwd -W 2>/dev/null || pwd)"
  docker run --rm -v "${src}:/src" -w /src \
    -v gate-gomod:/go/pkg/mod -v gate-gobuild:/root/.cache/go-build \
    ${GO_DOCKER_ARGS:-} "${GO_IMAGE}" go "$@"
}

urls() {
  cat <<EOF

Gate is running:
  gateway     http://localhost:8080   try: curl -H "X-API-Key: demopro1" http://localhost:8080/v1/echo/hello
  hub API     http://localhost:8081   (Authorization: Bearer ${GATE_HUB_TOKEN:-gate-local-token})
  analytics   http://localhost:8091   (dashboard)
EOF
}

# stack_up builds images, waits for the gateway first (usable as soon as
# possible), then for the optional profiles.
stack_up() {
  local profiles=()
  for p in "$@"; do profiles+=(--profile "$p"); done
  echo "building images and starting the gateway..."
  "${STACK[@]}" "${profiles[@]}" build --quiet
  "${STACK[@]}" up -d --wait --quiet-pull edge
  echo "gateway ready on http://localhost:8080; starting $*..."
  "${STACK[@]}" "${profiles[@]}" up -d --wait --quiet-pull
}

task="${1:-help}"
shift || true
case "${task}" in
  up)
    stack_up analytics
    urls
    echo "  keys: run 'make demo' to seed demo keys and traffic"
    ;;
  demo)
    stack_up analytics demo
    urls
    echo
    echo "Demo traffic is running (keys demopro1, demopro2, demofree1, demofree2, demonoisy)."
    echo "Watch it:  ${STACK[*]} logs -f traffic"
    ;;
  down)
    "${STACK[@]}" --profile analytics --profile demo down
    ;;
  clean)
    "${STACK[@]}" --profile analytics --profile demo down -v --remove-orphans
    ;;
  logs)
    "${STACK[@]}" --profile analytics --profile demo logs -f "$@"
    ;;
  dev)
    # Whole gateway in one container, no other services.
    docker build -q -f deploy/Dockerfile --target gate -t gate:local . >/dev/null
    exec docker run --rm -it -p 8080:8080 -p 8081:8081 -p 8091:8091 gate:local dev \
      -addr 0.0.0.0:8080 -hub-addr 0.0.0.0:8081 -analytics-addr 0.0.0.0:8091 "$@"
    ;;
  build)
    go_ build -o bin/gate ./cmd/gate
    ;;
  test)
    go_ vet ./...
    go_ test -race -count=1 ./...
    ;;
  test-integration)
    GO_DOCKER_ARGS="-v /var/run/docker.sock:/var/run/docker.sock -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal -e TESTCONTAINERS_RYUK_DISABLED=true --add-host host.docker.internal:host-gateway" \
      go_ test -tags integration -count=1 -timeout 25m ./testing/integration/...
    ;;
  loadtest)
    bash testing/loadtest/run.sh "${1:-run}"
    ;;
  lint)
    unformatted="$(go_ run golang.org/x/tools/cmd/goimports@v0.31.0 -l . 2>/dev/null | grep -v '^testing/loadtest/results' || true)"
    if [ -n "${unformatted}" ]; then
      echo "not goimports-formatted:"; echo "${unformatted}"; exit 1
    fi
    go_ vet ./...
    ;;
  certs)
    bash deploy/gen-dev-certs.sh deploy/certs hub,localhost
    ;;
  help|*)
    cat <<'EOF'
Usage: make <task>   (or: bash scripts/dev.sh <task>)

  up                start hub, edge, analytics and an echo upstream (Docker)
  demo              up + seed demo keys + continuous demo traffic
  down | clean      stop the stack (clean also deletes its volumes)
  logs [service]    follow logs
  dev               whole gateway in one container, no other services
  build             build bin/gate
  test              go vet + unit tests with the race detector
  test-integration  integration suite (testcontainers)
  loadtest [label]  end-to-end rate-limiter load test
  lint              formatting and vet
  certs             generate dev mTLS certs into deploy/certs
EOF
    ;;
esac
