# One build stage, two images:
#   default target  gate        `gate hub|edge|analytics|echo|dev`
#   --target tools  gate-tools  load generator and minimal upstream for tests
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
ARG VERSION=dev
# Cache mounts instead of `go mod download`: only modules the binaries import
# are fetched (not test-only ones like testcontainers), and rebuilds reuse them.
RUN --mount=type=cache,target=/go/pkg/mod     --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/gate ./cmd/gate \
 && CGO_ENABLED=0 go build -trimpath -o /out/loadgen ./testing/loadtest/cmd/loadgen \
 && CGO_ENABLED=0 go build -trimpath -o /out/upstream ./testing/loadtest/cmd/upstream

FROM alpine:3.20 AS tools
COPY --from=build /out/loadgen /out/upstream /usr/local/bin/

FROM alpine:3.20 AS gate
RUN apk add --no-cache ca-certificates tzdata && adduser -D -H -u 10001 gate
COPY --from=build /out/gate /usr/local/bin/gate
USER gate
ENTRYPOINT ["gate"]
CMD ["help"]
