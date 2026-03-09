# Testing Guide

This repository includes a comprehensive testing framework covering edge logic,
hub logic, and now a lightweight dashboard service.  Use this document as a
reference when writing or running tests.

## Structure

- `packages/edge` – unit tests for the edge gateway (`server_test.go`, plus
  async/rate tests).
- `packages/hub` – unit tests for the hub server (`server_test.go`) and rate
  update/listener tests.
- `packages/dashboard` – tests for the dashboard statistics computation and
  HTTP handler.
- `testutils` – helpers, mocks, and shared test data generators.

Each package contains its own `_test.go` files.  The `testutils` helpers
provide assertion functions (e.g. `AssertEqual`, `AssertNoError`) plus mock
implementations of various managers.

## Running Tests

```bash
# run all packages
go test ./...

# single package
go test ./packages/edge

go test -run TestEdgeRateManager_SOTSubscription ./packages/edge
```

## Rate Manager Tests

- Synchronous logic is covered by `packages/edge/rates.go` + existing tests.
- Asynchronous pub/sub behaviour is validated in `rates_async_test.go` files.
  These spin up an in-memory Redis (`miniredis`) and ensure:
  1. Edges publish `UsageDelta` messages when thresholds are reached.
  2. Hub consumes the deltas and emits `SOTMsg` messages.
  3. Edge subscriber updates its local sync key on SoT.

## Dashboard Service

A lightweight HTTP dashboard is now available to examine rate‑limiter analytics.

- Run with the rest of the stack (`docker-compose up --build`); it listens on
  port **8090** by default.
- Endpoint `/analytics` returns JSON statistics computed from the
  `rate-analytics` Redis list populated by edges. Query parameters support
  grouping (`group_by=prefix|api_key`) and filters (`prefix=`, `api_key=`).

Example curl:

```bash
curl "http://localhost:8090/analytics?group_by=prefix"
```

This service is covered by the `dashboard` Go package and has unit tests that
validate the computations.

## Test Coverage Goals

- **Edge Server**: >90% coverage
- **Cache Manager**: >85% coverage
- **Rate Manager**: >90% coverage
- **Tier Manager**: >85% coverage
- **Config Manager**: >90% coverage
- **Hub Server**: >80% coverage
- **Dashboard**: basic coverage via `dashboard_test.go`

## Debugging Tests

### Verbose Output

```bash
go test -v -run TestName ./packages/edge
```

### Print Statements

```go
t.Logf("Debug info: %v", value)
```

### Interactive Debugging

```bash
dlv test ./packages/edge
```

### Test Timeout

```bash
go test -timeout 10s ./...
```

## CI/CD Integration

Add `go test ./...` to your pipeline.  Ensure the in-memory redis dependency is
installed (only required for the async tests).
