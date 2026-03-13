# --- BUILD STAGE ---
FROM golang:1.26-alpine AS builder

# git may be needed for any modules
RUN apk add --no-cache git

WORKDIR /app

# cache dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /edge-app ./cmd/edge

# --- FINAL STAGE ---
FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /

COPY --from=builder /edge-app /edge-app
COPY --from=builder /app/cmd/config.json /cmd/config.json

EXPOSE 8080

CMD ["/edge-app"]
