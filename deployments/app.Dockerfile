# Build Stage
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o gate-binary cmd/gate-app/main.go

# Final Stage
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/gate-binary .
EXPOSE 8080
CMD ["./gate-binary"]