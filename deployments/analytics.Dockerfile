FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /analytics-api ./cmd/analytics

FROM alpine:3.20
RUN adduser -D -g '' app
USER app
WORKDIR /home/app
COPY --from=builder /analytics-api /usr/local/bin/analytics-api
EXPOSE 8091
ENTRYPOINT ["/usr/local/bin/analytics-api"]
