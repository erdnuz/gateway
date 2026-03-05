FROM golang:1.24-alpine AS builder

# Install git as some Go modules require it to download
RUN apk add --no-cache git

WORKDIR /app

# 1. Copy over the module files
COPY go.mod go.sum ./

# 2. Download dependencies (this uses the cached layers if mod files haven't changed)
RUN go mod download

# 3. Copy the rest of the source code
COPY . .

# 4. Final check/sync of dependencies inside the container
RUN go mod tidy

# 5. Build the binary
RUN go build -o /hub-app ./cmd/hub/main.go

FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /root/
COPY --from=builder /hub-app .
EXPOSE 8080
CMD ["./hub-app"]