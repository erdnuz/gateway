# --- BUILD STAGE ---
FROM golang:1.26-alpine AS builder

# git is often needed for private modules or specific dependencies
RUN apk add --no-cache git

WORKDIR /app

# 1. Cache dependencies (Best practice for speed)
COPY go.mod go.sum ./
RUN go mod download

# 2. Copy source and build
COPY . .
# We build a static binary to ensure it runs perfectly on the slim alpine final image
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /hub-app ./cmd/hub/main.go

# --- FINAL STAGE ---
FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata

# Create the same directory structure your Compose environment variable expects
WORKDIR /

# 3. Copy only what is needed from builder
COPY --from=builder /hub-app /hub-app
COPY --from=builder /app/cmd/config.json /cmd/config.json

EXPOSE 8080

# Run the binary
CMD ["/hub-app"]