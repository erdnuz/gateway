# Hub Service

Hub is the control-plane authority.

## Main Entry

- cmd/hub/main.go

## Responsibilities

- Load, validate, and serve gateway config.
- Manage tier assignments per prefix and API key.
- Provide authoritative quota/lease coordination.
- Publish tier update events to edge instances.

## APIs

Common endpoints include:

- /health
- /config
- /rate/{prefix}/{api_key}
- /tiers/{prefix}/{api_key}

Most endpoints require Authorization: Bearer <HUB_AUTH_TOKEN>.

## Config Validation

Hub validates policy at startup, including:

- prefix/service/tier structure
- duplicate detection
- target URL validity
- policy and retry range checks

## Messaging and State

- Stores authoritative state in Redis.
- Publishes tier update invalidation events via NATS.
- Coordinates with edge lease clients over mTLS gRPC where configured.

## Key Hub Packages

- packages/hub/server.go
- packages/hub/config.go
- packages/hub/tiers.go
- packages/hub/rates.go
- packages/hub/quota_lease_server.go
