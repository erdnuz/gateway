# Gate Architecture

Gate is a distributed API governance platform with three services:

- Edge: data plane for request handling, caching, enforcement, and analytics emission.
- Hub: control plane for configuration, tier assignment, and authoritative quota leasing.
- Analytics: observability plane for ingesting edge events and serving summaries and charts.

## Service Responsibilities

## Edge
- Receives client traffic at paths shaped as /{prefix}/{service}/...
- Applies middleware pipeline (analytics capture, CORS, auth, cache, rate limit, proxy)
- Resolves tier policy via cached data with hub-backed fallback behavior
- Emits analytics entries to NATS, including edge_id

## Hub
- Loads and validates gateway config JSON
- Serves effective control-plane config to edge and analytics
- Handles tier CRUD and rate lease coordination
- Publishes tier updates for edge cache invalidation

## Analytics
- Subscribes to analytics events via NATS
- Persists events in ClickHouse
- Serves summary and series endpoints and static dashboard assets
- Applies filter pushdown and computes trends from current vs previous windows

## Data and Messaging Paths

- Client -> Edge -> Upstream -> Client
- Edge -> NATS analytics subject -> Analytics -> ClickHouse
- Hub -> NATS tier updates subject -> Edge
- Edge/Analytics -> Hub config endpoint for effective policy

## High-Level Topology

```mermaid
flowchart TD
    subgraph Hub [Hub - Control Plane]
        H_S[(Redis)]
    end

    subgraph Edge [Edge - Data Plane]
        E_S[(Redis)]
    end

    subgraph Analytics [Analytics - Observability Plane]
        A_S[(ClickHouse)]
    end

    C[Client] -->|HTTP| E_Proxy[Edge Proxy]
    E_Proxy -->|HTTP| U[Upstream]
    U -->|HTTP| E_Proxy

    E_Proxy -.->|HTTP: getConfig| H_API[Hub API]
    A_API[Analytics API] -.->|HTTP: getConfig | H_API
    
    NATS_Tier[(NATS: Tier Updates)]
    H_API -->|NATS: publishTierUpdate| NATS_Tier
    NATS_Tier -->|Subscribe: onTierUpdate| E_Proxy

    NATS_Events[(NATS: Analytics Events)]
    E_Proxy -->|NATS: emitAnalytics| NATS_Events
    NATS_Events -->|Consume: ingest| A_API
    A_API -->|Native: write| A_S

    H_API -->|Native: setLease/getTier| H_S
    E_Proxy -->|Native: getCache/checkQuota| E_S
```

## Code Map

- cmd/edge/main.go
- cmd/hub/main.go
- cmd/analytics/main.go
- packages/edge
- packages/hub
- packages/analytics
- packages/common
