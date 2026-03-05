# Backbone Facade: Distributed API Governance Layer

A high-performance, language-agnostic API Gateway Facade built in Golang. This system provides centralized Rate Limiting, Dynamic Response Caching, and User Tiering for microservices deployed on AWS EKS.

## Overview

In a distributed environment, managing API traffic, security, and cost across multiple services (Python, Java, Node) is a complex challenge. Backbone Facade abstracts these concerns into a unified infrastructure layer.

* **Performance:** Sub-2ms latency overhead using Go and Redis.
* **Intelligence:** Injects user-tier metadata (Free, Pro, Enterprise) directly into request headers.
* **Control:** A real-time dashboard for live configuration updates and traffic analytics.

---

## System Architecture

The system is built on the Sidecar/Reverse Proxy pattern, ensuring that downstream services do not need to implement custom throttling or caching logic.



* **The Backbone (Go):** Stateless proxy nodes that intercept traffic.
* **State Layer (Redis):** Distributed locking and high-speed counters for rate limiting.
* **Persistence (SQL):** Storage for service configurations and user-tier mappings.
* **Control Plane (Next.js):** A unified UI for managing limits and viewing performance metrics.

---

## Key Features

### 1. Multi-Tiered Rate Limiting
Implements the Token Bucket algorithm. Developers define custom limits per user-tier via the dashboard.
* **Logic:** Config -> Tier -> Token Bucket -> Redis Counter.



### 2. Intelligent Facade (Header Enrichment)
Downstream services receive enriched requests. 
* **Mechanism:** The facade attaches an `X-User-Context` header (Base64 JSON) containing Tier and Quota information.

### 3. Distributed Response Caching
Avoids redundant data transfers by caching expensive API responses in Redis based on user-defined TTL (Time-To-Live).

---

## Configuration Schema

Services are configured via MongoDB using a hierarchical structure:

```json
{
  "service_id": "payment-processor",
  "tiers": {
    "FREE":[
        {"limit": 100, "window": "1h" }, 
        {"limit": 10, "window": "1m" }
    ],
    "PRO":[
        {"limit": 100, "window": "1h" }, 
        {"limit": 10, "window": "1m" }
    ],
  },
  "caching": {
    "enabled": true,
    "ttl_per_tier": {
      "FREE": "300s",
      "PRO": "60s"
    }
  },
}
```

## Analytics and Observability

The integrated dashboard provides real-time visibility into the following metrics:

* **Throughput:** Requests Per Second (RPS) per service.

* **Efficiency:** Cache Hit vs. Miss ratios.

* **Reliability:** Distribution of 429 (Too Many Requests) vs 5xx (Server Error) status codes.

* **Latency:** P50, P90, and P99 latency heatmaps.

## Getting Started
### Prerequisites

* Docker and Docker Compose

* Kubernetes Cluster (Minikube or AWS EKS)

* Go >= 1.21

### Local Development

1. Clone the repository.

2. Execute ```docker-compose up -d``` to initialize infrastructure.

3. Access the Facade on port 8080 and the Management Dashboard on port 3000.

### Deployment (AWS and K8s)
* This project includes Terraform scripts and Helm charts for cloud deployment.

* Provision EKS infrastructure using Terraform.

* Deploy the stack using the provided Helm chart: ```helm install backbone ./deployments/k8s/chart```.