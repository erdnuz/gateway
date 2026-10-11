# Observability

`make up` starts Prometheus, Grafana and Jaeger next to the gateway (the
`observability` compose profile).

| | URL |
| --- | --- |
| Grafana, "Gate overview" dashboard | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |

## The dashboard

The top row exists to show the architecture working: as traffic and edges
grow, the control plane's share of the work stays flat.

- **Edges serving / edges connected to hubs.** Prometheus finds edges by DNS,
  so `docker compose -f deploy/compose.yaml up -d --scale edge=3` (after
  dropping the edge's fixed port mapping) shows three edges here, each
  holding one stream to the hub.
- **Requests vs hub lease RPCs** and **hub work per request.** Lease RPCs
  per gateway request stay near `1 / lease size` however many edges serve
  the traffic.
- **Decisions by tier** and **share rate limited, by tier.** The demo traffic
  (`make demo`) runs some keys over quota, so 429s show up within seconds.
- **Hub lease answers.** `waiting` means a key's global quota for the window
  is exhausted. Edges stop asking until a jittered retry time.

## Metrics

Every component serves Prometheus metrics on `METRICS_ADDR` (`:9100`; `gate
dev` serves one endpoint for all components on `127.0.0.1:9100`). Labels are
bounded by config (service, tier). API keys are never labels.

| Metric | Component | Meaning |
| --- | --- | --- |
| `gate_requests_total{service,code}` | edge | requests by status |
| `gate_request_duration_seconds{service}` | edge | request latency at the edge |
| `gate_ratelimit_decisions_total{service,tier,result}` | edge | `allowed`, `denied`, `safety_allowed`, `safety_denied`, `unavailable`, `rejected` |
| `gate_lease_rpc_seconds{result}` | edge | lease RPC latency: `granted`, `waiting`, `error` |
| `gate_edge_lease_batch_size` | edge | lease requests per batched RPC (batching keeps hub calls proportional to edges, not keys) |
| `gate_ratelimit_shadow_denials_total{service,limit}` | edge, limiter | requests a shadow limit would have denied (they were admitted) |
| `gate_ratelimit_limit_denials_total{service,limit}` | edge | denials by an additional limit (per client IP, per service) rather than the tier |
| `gate_ratelimit_cost_settlements_total{service,how}` | edge | post-hoc costs: `reported`, `estimated` (none reported), `refunded` |
| `gate_key_resolutions_total{result}` | edge | tier-cache misses resolved through the hub |
| `gate_upstream_duration_seconds{service}` | edge | upstream round trips, retries included |
| `gate_cache_lookups_total{service,result}` | edge | response cache `hit` / `miss` |
| `gate_control_events_total{kind}` | edge | config and tier changes applied |
| `gate_hub_lease_requests_total{result}` | hub | `granted`, `waiting`, `rejected`, `error` |
| `gate_hub_tokens_granted_total` | hub | tokens handed to edges |
| `gate_hub_tokens_returned_total` | hub | unspent tokens edges gave back on drain or idle eviction |
| `gate_hub_debt_settled_total` | hub | post-hoc cost debt settled by draining edges |
| `gate_hub_lease_active_edges` | hub | edges sharing a quota at grant time (above 1, the endgame split applies) |
| `gate_hub_region_transfers_total{peer,result}` | hub | borrow attempts: `credited`, `refused`, `unreachable`, `late`, `lost` |
| `gate_hub_region_tokens_borrowed_total`, `gate_hub_region_tokens_lent_total` | hub | global-quota allocation moved between regions |
| `gate_hub_edges_connected` | hub | open control-plane streams |
| `gate_hub_control_events_total{kind}` | hub | changes delivered to this hub's edges |
| `gate_hub_watchers_dropped_total` | hub | edges disconnected for falling behind |
| `gate_analytics_events_total{result}` | analytics | events `queued`, `dropped`, `invalid` |

## Tracing

Tracing is off until an OTLP endpoint is set, and then it uses the standard
OpenTelemetry variables:

```bash
GATE_OTLP=http://jaeger:4317 make up      # compose: every service exports to Jaeger
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 OTEL_EXPORTER_OTLP_INSECURE=true gate dev
```

A traced request shows the edge's handling, its upstream call (with W3C
`traceparent` forwarded to the upstream), and any lease or key-lookup gRPC
call to the hub, with the hub's side of it. With tracing off, no
instrumentation is installed at all.

## Logs

Logs are structured (`log/slog`): JSON for the services, text for `gate
dev`. Set `LOG_FORMAT=json|text` and `LOG_LEVEL=debug|info|warn|error` to
override. Request-scoped lines carry `request_id`, which matches the
`X-Request-Id` header clients and upstreams see, and the `request_id` field
of every error response. API keys appear only as `sha256:` fingerprints.

## Analytics retention

ClickHouse keeps `ANALYTICS_RETENTION_DAYS` (30) of events. The table is
partitioned by day, so expired days are dropped whole, and it is ordered by
`(prefix, service, timestamp)`, so per-service queries read only that
service's data.
