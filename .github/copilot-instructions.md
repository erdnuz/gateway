# Deployment Robustness & Validation Standards

## 1. Sequential Startup & Dependency Handshake

All services must implement a `HealthCheck` interface that is executed **before** the main server loop starts. Startup must strictly follow this order:

1. **Hub:** Must boot first. It must perform a self-validation of `.env` and `GatewayConfig.json`. It must establish a successful connection to its internal database and NATS before declaring itself `READY`.
2. **Analytics:** Must boot after the Hub. It must perform its own dependency check (Database/NATS) and verify connectivity to the Hub's health endpoint.
3. **Edge:** Must boot last. It must verify local configuration and perform a **3-way handshake**:
* Verify connectivity to the local NATS broker.
* Ping the Hub’s `/health` endpoint.
* (If enabled) Ping the Analytics API’s `/health` endpoint.


4. **Fail-Fast Mechanism:** If any dependency fails a health check during startup, the process **must** log a descriptive, actionable error (e.g., "Failed to connect to NATS at [URL]: Connection Refused") and exit with `os.Exit(1)`.

## 2. Configuration Validation

* **Schema Enforcement:** Use a library to validate `GatewayConfig` against a JSON Schema on boot. If required fields are missing, the process must terminate immediately.
* **Connectivity Tests:** Startup code must include a "Canary Call"—a lightweight request to verify that the service can actually communicate with its declared dependencies (e.g., a simple `SELECT 1` for DBs, or a `PUB/SUB` test for NATS).

## 3. Deployment Flow

* **Orchestration:** Orchestrators (or scripts) must use wait-for-it or native readiness probes to ensure each service in the chain is confirmed "Healthy" before moving to the next.





