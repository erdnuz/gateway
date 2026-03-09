# --- BUILD STAGE ---
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /dashboard-app ./cmd/dashboard/main.go

# --- FINAL STAGE ---
FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /
COPY --from=builder /dashboard-app /dashboard-app
COPY --from=builder /app/cmd/config.json /cmd/config.json
EXPOSE 8090
CMD ["/dashboard-app"]
