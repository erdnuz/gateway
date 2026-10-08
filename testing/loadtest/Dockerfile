FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/loadgen ./testing/loadtest/cmd/loadgen \
 && CGO_ENABLED=0 go build -o /out/upstream ./testing/loadtest/cmd/upstream

FROM alpine:3.20
COPY --from=builder /out/ /usr/local/bin/
