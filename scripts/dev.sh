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
  grafana     http://localhost:${GATE_GRAFANA_PORT:-3000}   (Gate overview: rps, 429s per tier, hub work per request)
  prometheus  http://localhost:9090
  jaeger      http://localhost:16686  (traces: start with GATE_OTLP=http://jaeger:4317)
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
    stack_up analytics observability
    urls
    echo "  keys: run 'make demo' to seed demo keys and traffic"
    ;;
  demo)
    stack_up analytics observability demo
    urls
    echo
    echo "Demo traffic is running (keys demopro1, demopro2, demofree1, demofree2, demonoisy)."
    echo "Watch it:  ${STACK[*]} logs -f traffic"
    ;;
  down)
    "${STACK[@]}" --profile analytics --profile observability --profile demo down
    ;;
  clean)
    "${STACK[@]}" --profile analytics --profile observability --profile demo down -v --remove-orphans
    ;;
  logs)
    "${STACK[@]}" --profile analytics --profile observability --profile demo logs -f "$@"
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
  sim)
    # Deterministic simulation: generated traffic and faults on a virtual
    # clock against the real limiter and hub. ARGS=N sets the scenario count.
    go_ test -count=1 -timeout 60m ./testing/sim/ -run Simulation -v -sim.seeds="${1:-1000}"
    ;;
  loadtest)
    bash testing/loadtest/run.sh "${1:-run}"
    ;;
  lint)
    # golangci-lint (config: .golangci.yml) also checks gofmt.
    # Built with the module's Go version: prebuilt images lag Go releases.
    GO_DOCKER_ARGS="-v gate-lintcache:/root/.cache/golangci-lint" \
      go_ run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...
    ;;
  certs)
    bash deploy/gen-dev-certs.sh deploy/certs hub,localhost
    ;;
  proto)
    # Regenerates packages/common/types from api/proto in a pinned toolchain.
    docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/src -w /src "${GO_IMAGE}" sh -c '
      apt-get update -qq >/dev/null && apt-get install -y -qq protobuf-compiler >/dev/null &&
      GOBIN=/usr/local/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10 &&
      GOBIN=/usr/local/bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1 &&
      protoc --go_out=. --go_opt=module=gateway --go-grpc_out=. --go-grpc_opt=module=gateway api/proto/*.proto'
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
  sim [N]           N simulated scenarios (default 1000): traffic, clock skew and faults on a virtual clock
  loadtest [label]  end-to-end rate-limiter load test
  lint              golangci-lint (formatting, vet and more)
  certs             generate dev mTLS certs into deploy/certs
  proto             regenerate gRPC code from api/proto
EOF
    ;;
esac
