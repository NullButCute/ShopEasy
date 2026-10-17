<div align="center">

# ShopEasy

**An event-driven e-commerce backend built on .NET 8 microservices**

Reliable order processing across independently deployable services: a saga with compensation,
transactional messaging and an idempotent API, designed to stay correct when parts of the system fail.

[![CI/CD](https://github.com/NullButCute/ShopEasy/actions/workflows/ci.yml/badge.svg)](https://github.com/NullButCute/ShopEasy/actions/workflows/ci.yml)
![.NET 8](https://img.shields.io/badge/.NET-8.0-512BD4?logo=dotnet&logoColor=white)
![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-FF6600?logo=rabbitmq&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-ready-326CE5?logo=kubernetes&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

[Quick start](#getting-started) ·
[Architecture](#architecture) ·
[Order API](#order-api) ·
[Operations](#developer-portal--monitoring) ·
[Kubernetes](#kubernetes-deployment) ·
[Roadmap](#production-hardening-roadmap--known-limitations)

</div>

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [The Services](#the-services)
4. [Technology Stack](#technology-stack)
5. [Patterns & Mechanisms Explained](#patterns--mechanisms-explained)
6. [Getting Started](#getting-started)
7. [Configuration & Secrets](#configuration--secrets)
8. [How to Use It](#how-to-use-it)
9. [Order API](#order-api)
10. [Inventory Stock Model](#inventory-stock-model)
11. [Developer Portal & Monitoring](#developer-portal--monitoring)
12. [Testing](#testing)
13. [CI/CD](#cicd)
14. [Horizontal Scaling](#horizontal-scaling)
15. [Kubernetes Deployment](#kubernetes-deployment)
16. [Project Structure](#project-structure)
17. [Troubleshooting](#troubleshooting)
18. [Production Hardening Roadmap & Known Limitations](#production-hardening-roadmap--known-limitations)

---

## Overview

ShopEasy is the backend of an online store, split into six independently deployable services. It is built around one job and does it carefully: turn a customer's **Place order** into exactly one stock reservation, exactly one charge and one confirmed order, even when a service crashes, a message is delivered twice, the database restarts or the client retries the request.

There is no distributed-transaction coordinator and no shared database. An **orchestrated saga** drives the order through Inventory and Payment, **transactional outbox and inbox** messaging keeps every state change and the message announcing it atomic, **row-level locks** keep stock correct, and **idempotency** at every edge makes repeated delivery harmless. Every failure path has a defined compensation (release the stock, refund the payment, cancel the order) that is covered by tests.

| Capability | What it means here |
|---|---|
| **Correct order processing** | A saga coordinates Order, Inventory and Payment. A declined payment releases the stock, a late payment is refunded, and an order that stops making progress is recovered by a timeout service instead of hanging. |
| **Safe under retries and duplicates** | Transactional outbox and inbox, idempotent consumers and an `Idempotency-Key` on the public order API: delivering or sending something twice has the effect of once. |
| **No overselling** | Stock is reserved under row-level locks and tracked in a ledger (reserve, then commit or release), so concurrent orders on any instance cannot take the same unit twice. |
| **Resilient** | Database and network outages are ridden out with retry policies and bounded waits. Rate limits, circuit breakers, health probes and dead-letter tooling cover the rest. |
| **Secure by default** | OpenID Connect through a Backend-for-Frontend gateway (tokens never reach the browser), JWT validation in every service, no secrets in the repository, unprivileged read-only containers and default-deny network policies. |
| **Observable** | One correlation id across HTTP and messages, structured logs, distributed traces, Prometheus metrics, Grafana dashboards, and built-in saga, outbox and dead-letter monitors. |
| **Deployable** | Docker Compose for development and Kubernetes manifests with pinned images. GitHub Actions builds, tests against real PostgreSQL and RabbitMQ, validates the deployment files, starts the stack and publishes the images. |

**Scope.** ShopEasy ships the APIs and an operations portal, not a customer-facing storefront, and payments go through a simulated provider. The [known limitations](#production-hardening-roadmap--known-limitations) are listed at the end of this document.

---

## Architecture

### System overview

```mermaid
flowchart LR
    client(["<b>Browser /<br/>API client</b>"])

    subgraph edge["Edge"]
        direction TB
        gateway["<b>API Gateway</b><br/>YARP reverse proxy + BFF<br/>session cookie · rate limiting<br/>routing · developer portal"]
        identity["<b>Identity</b><br/>OpenID Connect / OAuth 2.0<br/>Duende IdentityServer"]
    end

    subgraph services["Business services"]
        direction TB
        order["<b>Order</b><br/>saga orchestrator · outbox<br/>idempotent API · read model"]
        inventory["<b>Inventory</b><br/>stock · reservations<br/>REST and gRPC"]
        payment["<b>Payment</b><br/>charges · refunds"]
        product["<b>Product</b><br/>catalog · v1 and v2 API"]
    end

    subgraph backbone["Shared infrastructure"]
        direction TB
        rabbit[("<b>RabbitMQ</b><br/>commands and events")]
        postgres[("<b>PostgreSQL 16</b><br/>one database per service")]
        redis[("<b>Redis</b><br/>product and stock cache")]
    end

    obs["<b>Observability</b><br/>Seq · Jaeger · Prometheus · Grafana"]

    client -->|HTTPS| gateway
    gateway -. "sign-in" .-> identity
    gateway -->|"user's token"| services
    order -->|"HTTP: prices"| product
    order -->|"gRPC: stock check"| inventory
    services --> backbone
    services -.-> obs
```

- **Edge.** The gateway is the only entry point. It keeps the user's session in a cookie, holds the access token server-side and attaches it to every proxied call, and enforces rate limits per route. Identity issues the tokens (authorization-code flow); each service validates them itself.
- **Business services.** Every service owns its data and exposes only its API. No service reads another's tables.
- **Shared infrastructure.** One PostgreSQL server with a separate database per service, RabbitMQ for commands and events, and Redis as a best-effort cache (the services keep working when it is down).

### How the services talk to each other

| Interaction | Mechanism | Why |
|---|---|---|
| Client to gateway | HTTPS, session cookie | The browser never holds a token ([pattern 1](#1-api-gateway-yarp--bff)) |
| Gateway to services | HTTP through YARP, user's access token attached | One public URL; each service authorises the request on its own ([pattern 2](#2-authentication--authorization-oauth2--oidc--jwt)). A service that goes silent is cut off after 30 s with `504`, and a GET that hit a broken connection is repeated ([pattern 26](#26-resilient-calls-between-services)) |
| Order to Product | HTTP with a deadline, budgeted retries and a circuit breaker | Prices and names always come from the catalog, never from the request; the order is refused (`503`) if the catalog cannot answer |
| Order to Inventory | gRPC for the stock check, with a deadline, retries after connection failures and a circuit breaker | Fast typed batch call; the answer is cached in Redis, and the cached value answers when Inventory cannot ([patterns 11, 12 and 26](#11-eventual-vs-strong-consistency-hybrid-inventory-check)) |
| Order, Inventory and Payment among themselves | Commands and events over RabbitMQ (MassTransit) | Loose coupling; at-least-once delivery made safe by the outbox, the inbox and idempotent consumers ([patterns 5 to 8](#5-outbox-pattern)) |

### Life of an order

```mermaid
sequenceDiagram
    autonumber
    actor C as Customer
    participant G as Gateway
    participant O as Order
    participant P as Product
    participant I as Inventory
    participant Y as Payment
    participant MQ as RabbitMQ

    C->>G: POST /api/orders (session cookie, Idempotency-Key)
    G->>O: forward with the user's access token
    O->>P: authoritative names and prices
    O->>I: stock check (cached, gRPC on a miss)
    Note over O: one transaction: order + outbox event<br/>+ read-model row + idempotency key
    O-->>C: 201 Created (order is Pending)

    rect rgb(240, 245, 255)
    Note over O,MQ: asynchronous from here, driven by the saga
    O->>MQ: OrderConfirmedEvent (outbox delivery)
    MQ->>O: saga starts
    O->>MQ: ReserveInventoryCommand
    MQ->>I: reserve stock (row locks)
    I->>MQ: InventoryReservedEvent
    MQ->>O: saga waits for payment
    O->>MQ: ProcessPaymentCommand
    MQ->>Y: charge
    Y->>MQ: PaymentSucceededEvent
    MQ->>O: saga is completing
    O->>MQ: CommitInventoryCommand
    MQ->>I: commit the reservation
    I->>MQ: InventoryCommittedEvent
    MQ->>O: saga Completed, order Confirmed
    end
```

Everything above the dashed region happens inside the request: the request is validated, every product is priced by the Product service, stock is checked against the cache (gRPC on a miss), and the order, its outbox event, its read-model row and its idempotency key are saved in **one transaction** before the customer receives `201`. Everything below it is asynchronous, so Inventory or Payment being slow or briefly down delays an order but never loses it. The details are in [Order API](#order-api) and [patterns 4 to 9](#4-saga-pattern-orchestration-based).

### The order saga

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Started: order placed
    Started --> ReservingInventory: ReserveInventoryCommand sent
    ReservingInventory --> ProcessingPayment: InventoryReserved
    ProcessingPayment --> Completing: PaymentSucceeded
    Completing --> Completed: InventoryCommitted

    ReservingInventory --> Cancelled: InventoryReservationFailed
    ProcessingPayment --> ReleasingInventory: PaymentFailed, customer cancel or timeout
    ReleasingInventory --> Cancelled: InventoryReleased
    Started --> Cancelled: customer cancel
    Completing --> Failed: InventoryCommitFailed (payment refunded)

    Completed --> RefundingPayment: cancel of a paid order
    Completing --> RefundingPayment: cancel asked while committing, once InventoryCommitted
    RefundingPayment --> RestockingInventory: PaymentRefunded
    RestockingInventory --> Cancelled: InventoryRestocked

    Completed --> [*]
    Cancelled --> [*]
    Failed --> [*]
```

The saga state is saved before the next command is published, every handler acts only in the state its event is valid for (a duplicate or late event can never move an order backwards), and a timeout service compensates sagas that stop making progress. The order the customer sees is `Pending` until the saga reaches `Completed` (then `Confirmed`) or ends `Cancelled`. The full compensation matrix is in [pattern 4](#4-saga-pattern-orchestration-based).

### Where the data lives

| Store | Holds |
|---|---|
| **Order** database | Orders and their lines, the outbox, saga state, the `OrderSummaries` read model, idempotency keys |
| **Inventory** database | Stock levels, the stock ledger (reserved / committed / released per order), processed-message records, the MassTransit inbox and outbox |
| **Payment** database | Payments and refunds, processed-message records, the MassTransit inbox and outbox |
| **Product** database | The product catalog |
| **Identity** database | Users, signing keys, the Data Protection key ring and issued grants, shared by every Identity replica |
| **Redis** | Product and stock cache (a cache only: nothing depends on it for correctness) |
| **RabbitMQ** | Durable queues per consumer; messages that keep failing land in `*_error` queues, which the operations portal lists and can requeue |

### Cross-cutting qualities

| Quality | How it is achieved |
|---|---|
| **Consistency** | Saga with compensation, transactional outbox and inbox, row-level locking, idempotent consumers and an idempotent order API ([patterns 4 to 8](#4-saga-pattern-orchestration-based), [25](#25-idempotent-order-placement)) |
| **Reliability** | Patient retries for outages, bounded database waits, deadlines, budgeted retries and circuit breakers on calls between services, dead-letter queues with requeue, liveness and readiness probes ([13](#13-resilience-polly), [24](#24-surviving-database-and-network-outages), [26](#26-resilient-calls-between-services)) |
| **Security** | OIDC with a BFF gateway, JWT validation per service, secrets only in `.env` or Kubernetes Secrets, non-root read-only containers, network policies ([Configuration & Secrets](#configuration--secrets)) |
| **Scalability** | Order is stateless and scales horizontally (`--scale order=N`, or an autoscaler from 2 to 10 pods on Kubernetes); outbox rows are claimed with `SELECT … FOR UPDATE SKIP LOCKED`; Identity keeps its state in PostgreSQL ([Horizontal Scaling](#horizontal-scaling)) |
| **Observability** | Correlation id on every log line, trace and message; Seq, Jaeger, Prometheus and Grafana; saga, outbox and dead-letter monitors ([Developer Portal & Monitoring](#developer-portal--monitoring)) |
| **Delivery** | CI gates every change; images are published to GHCR and pinned by commit; deployment files are validated against schema and security rules ([CI/CD](#cicd)) |

### Deployment

| | Docker Compose | Kubernetes |
|---|---|---|
| **Use** | Local development and a single host | A cluster |
| **What runs** | The six services plus PostgreSQL, RabbitMQ, Redis, Seq, Jaeger, Prometheus, Grafana, pgAdmin and Redis Insight | The six services plus PostgreSQL, RabbitMQ and Redis |
| **Hardening** | Read-only root file system, no capabilities, non-root user, health-gated start order | The same, plus pinned `sha-<commit>` images, default-deny network policies and an autoscaler for Order |
| **Data stores** | Containers with a volume | Single-replica Deployments: use a managed service or an operator for production data |

---

## The Services

| Service | Role | Owns | Interfaces | Port (Compose) |
|---|---|---|---|---|
| **Gateway** | Single entry point: YARP reverse proxy and Backend-for-Frontend. Authentication, rate limiting, routing, operations portal. | Session cookie | Public HTTP | 5000 |
| **Identity** | OAuth 2.0 / OpenID Connect provider (Duende IdentityServer). Issues JWTs. | Users, keys, grants | OIDC endpoints | 5001 |
| **Product** | Product catalog. API-versioned (v1 and v2), Redis-cached. | Catalog | REST | 5101 |
| **Order** | Order placement and orchestration: validation, server-side pricing, the saga, the outbox, the CQRS read model and the idempotent order API. Horizontally scalable. | Orders, saga, outbox, read model, idempotency keys | REST, RabbitMQ | behind the gateway (scaled) |
| **Inventory** | Stock levels, reservation, commit and release under row-level locks. | Stock and ledger | REST, gRPC, RabbitMQ | 5103 (REST), 8090 (gRPC, internal) |
| **Payment** | Payment processing and refunds (simulated provider). | Payments | REST, RabbitMQ | 5104 |

**Infrastructure containers (Compose):** PostgreSQL, RabbitMQ, Redis, Seq, Jaeger, Prometheus, Grafana, pgAdmin and Redis Insight.

---

## Technology Stack

| Area | Technology |
|---|---|
| **Runtime** | .NET 8, C# 12, ASP.NET Core Web API |
| **Data** | PostgreSQL 16 (Entity Framework Core 8 with Npgsql), Redis 7 |
| **Messaging** | RabbitMQ 3.13 through MassTransit, with transactional inbox and outbox |
| **Communication** | YARP reverse proxy, gRPC (`inventory.proto`), HTTP with Polly resilience (retries, circuit breakers, timeouts) |
| **Identity and security** | Duende IdentityServer (OAuth 2.0 / OIDC), JWT bearer authentication, Backend-for-Frontend pattern |
| **Observability** | Serilog and Seq, OpenTelemetry and Jaeger, prometheus-net, Prometheus and Grafana |
| **Testing** | xUnit, FluentAssertions, NSubstitute; tests that run against real PostgreSQL and RabbitMQ |
| **Delivery** | Docker and Docker Compose, Kubernetes (kustomize), GitHub Actions, GitHub Container Registry |

---

## Patterns & Mechanisms Explained

Each mechanism below is implemented and covered by tests. The numbers are referenced from the rest of this document.

### 1. API Gateway (YARP + BFF)

A single entry point (`localhost:5000`) that routes requests to internal services. It also implements the **Backend-for-Frontend** pattern: the browser holds only a secure session cookie, while the gateway holds the actual JWT and attaches it to downstream calls. This keeps tokens out of the browser entirely.

**Why it matters:** clients talk to one URL, never to internal services directly. Cross-cutting concerns (auth, rate limiting, CORS) live in one place.

### 2. Authentication & Authorization (OAuth2 / OIDC / JWT)

Identity issues JWTs via the OpenID Connect authorization-code flow. The gateway exchanges the code for tokens and stores them server-side. Services validate JWTs independently. Service-to-service calls (Order → Inventory) use the **Client Credentials** grant — a separate machine-to-machine token with no user involved.

Every service decides for itself whether a call is allowed, and three things are checked, in this order:

| Check | Question | Failure |
|---|---|---|
| **Audience** (`aud`) | Is this token meant for this API? Identity has one API resource per API: `product-api`, `order-api`, `inventory-api`, `payment-api`. A valid token issued for another API is useless here. | `401` |
| **Scope** (`scope`) | What was the client allowed to do with the token? For example `orders.write`. | `403` |
| **Role** (`role`) | What may the user do? Administration needs the `admin` role **and** the scope: a role without the scope (a client that was never given administration rights) is refused, and so is a scope without the role. | `403` |

What each client may ask for is fixed in Identity (`IdentityServerConfig`), from the lists in `ShopEasy.Shared/Security/ApiContract.cs`:

| Client | Scopes |
|---|---|
| Web gateway, for the signed-in user | `products.read`, `products.write`, `orders.read`, `orders.write`, `inventory.read`, `inventory.write`, `payment.read` |
| Mobile app (public client, PKCE) | `products.read`, `orders.read`, `orders.write`, `inventory.read`, `payment.read` — no administration scopes |
| Order service, for itself (client credentials) | `inventory.read` only |

What each endpoint needs (the policies are in `ShopEasy.Shared/Security/AccessPolicies.cs`):

| API | Endpoint | Needs |
|---|---|---|
| Product | `GET /api/v1/products…`, `GET /api/v2/products…` | nothing: the public catalog |
| | `POST`, `PUT`, `DELETE /api/v1/products…` | `products.write` and admin |
| Order | `GET /api/orders…`, `POST /api/orders`, `DELETE /api/orders/{id}` | `orders.read` for reading, `orders.write` for placing and cancelling; the order list and details are always the caller's own, cancelling is allowed to the owner or an admin |
| | `/api/outbox/*` (outbox, sagas, dead letters) | `orders.read` and admin; requeueing dead letters also needs `orders.write` |
| Inventory | `GET /api/inventory`, `GET /api/inventory/{id}` | `inventory.read` |
| | `GET /api/inventory/{id}/transactions` | `inventory.read` and admin |
| | `POST /api/inventory/{id}/adjust` | `inventory.write` and admin |
| | gRPC stock check (port 8090) | `inventory.read` on a token a service got for itself, see below |
| Payment | `GET /api/payments/order/{id}` | `payment.read`; a user sees only their own payment, an admin any |
| | `GET /api/payments` | `payment.read` and admin |

**Closed unless opened.** An endpoint with no attribute requires a signed-in caller (the fallback policy), so a new endpoint is never public by accident; health checks and the metrics endpoint are marked anonymous on purpose. `Security/EndpointAccessTests` calls every endpoint of the four APIs with seven kinds of caller (nobody, customer, admin, a client without API scopes, an admin without API scopes, a token for another API, the Order service's token) against the real controllers, and fails when an endpoint is missing from its table.

**Service identity.** The Order service asks Identity for its own token (`order-service` client, client credentials). It names Inventory as its audience and carries `inventory.read`, the only scope the client may request: Identity answers `invalid_scope` to anything else, and the other APIs refuse the token for its audience. Inventory's gRPC endpoint accepts only such a token, one that names a client and no user: a customer's or an administrator's token is refused with `403` even when it carries the scope. The Order service caches the token for its 15-minute life and renews it a minute early. While Identity is unreachable it keeps using the old token until it is about to expire (and then falls back to the cached stock levels, [pattern 26](#26-resilient-calls-between-services)); if Inventory refuses the token it is dropped, a new one is fetched on the next call and an error is logged.

**Session renewal.** The access token the gateway forwards lives one hour; the session lasts up to 30 days. The gateway renews the token with the session's refresh token when less than a minute of it is left. Requests of one page share a single exchange. If Identity cannot be reached the session and its old token are kept and the next request tries again; if Identity no longer accepts the refresh token (revoked, user disabled, idle for 15 days) the session ends and the next request is a login.

**Deploying behind HTTPS.** Tokens are validated against Identity's discovery document and signing keys, fetched from `IdentityServer:Authority`. Inside a private network that is the plain-HTTP internal address, which is why `IdentityServer:RequireHttpsMetadata` is off by default: whoever can alter that traffic could hand the services their own signing key, so that network has to be trusted (network policies, a mesh). With an `https://` authority switch it on and the services refuse anything else. For a deployment reached over HTTPS, also set (all are in `.env.example`, the Compose file and the ConfigMap):

```
Security__CookieSecurePolicy=Always       # Secure flag on the session and login cookies (gateway, Identity)
ForwardedHeaders__Enabled=true            # believe the X-Forwarded-* headers of the ingress in front of them,
ForwardedHeaders__KnownNetworks=10.0.0.0/8   # but only from these networks / addresses (KnownProxies)
```

Without the forwarded headers a service behind TLS termination sees plain HTTP: it would build `http://` login redirects and rate-limit the proxy instead of the client. Turning the headers on without listing the proxy stops the service at start-up, because believing everybody would let any client claim any address and scheme.

**Upgrading.** Tokens issued before this model carry the old audience (`shopeasy-api`) and are refused by the services: deploy Identity first, then the services and the gateway, and expect users to sign in again.

### 3. Database per Service

Each service owns its own PostgreSQL database (`shopeasy_orders`, `shopeasy_inventory`, etc). No service reads another service's tables directly. This is the foundation of microservice independence — a schema change in one service can never break another.

### 4. Saga Pattern (Orchestration-based)

A "Place Order" is a **distributed transaction** across Order, Inventory, and Payment. Since there's no shared database transaction, we use a **saga** — a state machine that coordinates the steps and compensates (undoes) them on failure.

**Happy path:**
```
OrderConfirmed → ReserveInventory → ProcessPayment → CommitInventory → InventoryCommitted → Completed
```

**Saga states:** `Started → ReservingInventory → ProcessingPayment → Completing → Completed`
(or the compensation branch: `→ ReleasingInventory → Cancelled/Failed`;
or, for a paid order that is cancelled: `→ RefundingPayment → RestockingInventory → Cancelled`)

The saga only becomes `Completed` (and the order `Confirmed`) when the Inventory service has **confirmed the stock commit** with an `InventoryCommittedEvent`. While it waits it is in `Completing`; if no answer arrives the timeout service re-sends the commit (safe, because commit is idempotent).

Each handler only acts when the saga is in the state its event is valid for. A duplicate or late event is ignored (or compensated), never allowed to move an order backwards. The saga state is always saved **before** the next command is published, so a fast reply can never arrive while the saga still shows the old state.

**Compensation matrix — every failure has a defined recovery:**

| Failure point | Inventory | Payment | Recovery action |
|---|---|---|---|
| Stock check fails | not reserved | not charged | Cancel order |
| Payment declined | reserved | not charged | Release inventory → Cancel |
| Timeout waiting for payment | reserved | not charged | Release inventory → Cancel |
| Late payment after cancel | released | charged | **Refund** → stay Cancelled |
| Commit fails transiently after payment (DB hiccup) | reserved | charged | Message retried, then re-sent by the timeout service (commit is idempotent) |
| Commit impossible after payment (reservation missing or already released) | not committed | charged | **Refund** → Failed / order Cancelled |
| Duplicate "payment succeeded" event | unchanged | charged | Ignored — never refunds an order that is completing or completed |
| Customer cancels before payment | reserved or nothing | not charged | Release inventory → Cancelled (see [Cancelling an order](#cancelling-an-order)) |
| Customer cancels while payment is in flight | reserved | may still be charged | Release inventory; a payment that succeeds is **refunded** → Cancelled |
| Customer cancels a completed (paid) order | committed | charged | **Refund** → **restock** → Cancelled (`RefundingPayment`, `RestockingInventory`) |
| Customer cancels while the stock commit is in flight | committing | charged | The request is kept; when the commit lands the order is refunded, restocked and Cancelled, never shown as Confirmed |
| A refund or restock command (or its answer) is lost | committed | charged or refunded | Re-sent by the timeout service every two minutes; never given up on or cancelled on a guess, logged as critical after five tries |
| Customer cancels a shipped order | shipped | charged | Rejected with `409` (there is no shipping or return flow yet) |

### 5. Outbox Pattern

When an order is saved, the event that starts the saga is written to an **outbox table in the same database transaction** as the order itself. A background service then publishes outbox entries to RabbitMQ.

**Why:** it guarantees that "order saved" and "event published" are atomic. If RabbitMQ is down when the order is placed, the event still gets delivered once the broker recovers — no lost orders, no phantom events.

The delivery worker only publishes while the message bus is **ready** (pattern 23), so a broker outage or a start-up race is never counted as a failed delivery attempt. If publishing genuinely fails, the retry count is stored; after 5 failed attempts an entry is parked as a dead letter (visible in the Outbox Monitor) and no longer blocks the entries behind it. The Payment and Inventory services have their own transactional outbox (pattern 22).

### 6. Distributed Outbox Locking

When the Order service runs as **multiple instances**, they'd all try to publish the same outbox messages. We use PostgreSQL's `SELECT ... FOR UPDATE SKIP LOCKED` so each message is claimed by exactly one instance; others skip locked rows. The database row lock *is* the distributed mutex — no external coordination service (ZooKeeper, etcd) needed.

### 7. Idempotent Consumers

Every message consumer must be safe to run twice, because RabbitMQ guarantees only *at-least-once* delivery. How each one does it:

- **Saga handlers** check the saga's current state (see pattern 4).
- **Inventory reserve** records a `ProcessedEvents` row per order, checked while the stock rows are locked.
- **Inventory commit / release** read the order's own ledger rows (`StockTransactions` for that order: reserved / committed / released) under the row locks, so a duplicate is detected exactly and only re-sends its result event. See [Inventory Stock Model](#inventory-stock-model).
- **Payment** keeps a `ProcessedEvents` row per order.
- **Payment and Inventory** additionally use MassTransit's **inbox** (pattern 22): a redelivered copy of the *same message* is recognised by its message id and not consumed again. The business-level checks above remain the safety net for *different* messages about the same order (for example the saga re-sending a command after a timeout).

### 8. Zombie Message Guard

A subtle distributed-systems bug: a payment command sent *before* a saga timed out can arrive *after* the saga was already cancelled. Without a guard, this "zombie" message would complete a cancelled order. Each consumer re-reads the saga state inside a transaction (with the saga row locked, see pattern 21) and, if the saga has already given up on the order, issues a compensating action (refund / release) instead of proceeding. Duplicates of events for orders that are completing or completed are ignored instead (a duplicate must never refund a paid, finished order).

### 9. Saga Timeout Handling

A background `SagaTimeoutService` scans every 30 seconds for sagas stuck in a non-terminal state longer than 2 minutes, and applies the correct compensation based on which state they're stuck in. This prevents orders from hanging forever if a downstream service dies permanently.

### 10. CQRS (Command Query Responsibility Segregation)

Reads and writes use **different models**:
- **Write side** — normalized `Orders` + `OrderItems` tables, optimized for consistency.
- **Read side** — a denormalized `OrderSummaries` table, pre-computed (status name, formatted amount, item summary, payment status), optimized for fast display with no joins.

The read-model row is written in the same transaction as the order when it is placed, and kept current by the saga's events. Same database, separate tables (a separate read database is a one-config-change upgrade for later).

### 11. Eventual vs Strong Consistency (Hybrid Inventory Check)

The stock check before accepting an order uses a **layered hybrid** approach:
- **Layer 1** — Redis cache (~1ms, eventual, 30s TTL)
- **Layer 2** — gRPC to Inventory on cache miss (~150ms, strong)
- **Layer 3** — stale cache fallback (5min TTL) if Inventory is down, too slow or its circuit is open, so orders still flow ([pattern 26](#26-resilient-calls-between-services))
- **Layer 4** — reject only if no data at all
- **Layer 5** — the saga's `ReserveInventory` does the authoritative check inside the Inventory service's database transaction, with the stock rows locked (`SELECT … FOR UPDATE`), so concurrent reservations — on any Inventory instance — are serialized per product and cannot oversell. See [Inventory Stock Model](#inventory-stock-model).

This decouples order acceptance from the Inventory service being online, while the saga still prevents overselling. This is the industry-standard approach for high-volume commodity e-commerce.

### 12. gRPC Internal Communication

Order → Inventory stock checks use gRPC instead of REST: a binary, strongly-typed contract (`inventory.proto`) with a single batch call for multiple products. ~3× faster than JSON/HTTP and the contract is compile-time enforced. REST stays available on a separate port for external clients. Every call carries a deadline and is protected by a retry and a circuit breaker, see [pattern 26](#26-resilient-calls-between-services).

### 13. Resilience (Polly)

HTTP clients use Polly pipelines: exponential-backoff retries and circuit breakers, with the retries limited by a shared retry budget ([pattern 26](#26-resilient-calls-between-services)). MassTransit consumers retry too, with a different policy for infrastructure outages than for everything else — see [pattern 24](#24-surviving-database-and-network-outages).

### 14. Dead Letter Queues

Messages that fail repeatedly (3 quick retries for an ordinary failure, about 7 minutes of patient retries for a database or network outage — [pattern 24](#24-surviving-database-and-network-outages)) land in `*_error` queues. A `DeadLetterMonitor` background service in the Order service checks them every 30 seconds and logs a `DEAD LETTER ALERT` for every queue that holds messages. The Dead Letter Monitor page and `GET /api/outbox/deadletters` (admin) list them, and `POST /api/outbox/deadletters/{queue}/requeue` (admin) moves the messages back to the queue they came from once the cause is fixed — see [Working with dead letters](#working-with-dead-letters).

This talks **AMQP** — the protocol and credentials the services already use for messaging — and does **not** need the RabbitMQ management plugin (HTTP, port 15672). That UI stays available as a tool for humans, but nothing in the application depends on it.

### 15. Rate Limiting

The gateway enforces tiered limits and returns `429` with `Retry-After` when one is exceeded:

| Policy | Applies to | Limit |
|---|---|---|
| `anonymous` | public routes (products, login, health) | 30/min per client IP |
| `authenticated` | everything behind login, **including reading and cancelling orders** | 100/min per user (admin 500) |
| `orders` | **order placement only** (`POST /api/orders`) | 10/min per user |

Users are identified by the `sub` claim and admins by the `role` claim of their login session, so the per-user and admin limits depend on the gateway copying those claims (it does; see [Troubleshooting](#troubleshooting) if `/account/me` shows no user id after an upgrade). The Order service has its own 10/min limit on placement as a second line of defence.

### 16. Redis Caching

Products (5-min TTL) and inventory (30-sec TTL) are cached, with `X-Cache: HIT/MISS` headers. Caches are invalidated on writes. Connection uses `abortConnect=false` so the app degrades gracefully if Redis is briefly unavailable.

### 17. API Versioning

Product service exposes `v1` and `v2` simultaneously (`/api/v1/products`, `/api/v2/products`). v2 adds formatted price, currency, and audit fields. Swagger documents both.

### 18. Health Checks

Every service exposes `/health`, `/health/ready`, and `/health/live`. Used by Docker healthchecks and Kubernetes liveness/readiness probes.

### 19. Observability — the three pillars

- **Logs** — Serilog writes structured logs to the console and to Seq. Every line names its service and carries the **correlation id** of the request it belongs to: `[12:00:01 INF] [ShopEasy.PaymentService] [a1b2c3d4] Payment … succeeded`. Outside a request or message the id shows `-`. Where Seq is not available, set `Seq__ServerUrl` to an empty value (see [Configuration](#service-configuration-keys)); logs still go to the console.
- **Correlation across messages** — the id does not stop at the first RabbitMQ message. The gateway generates one when the client sends none (and returns it in the `X-Correlation-Id` response header), passes it to the service behind it, and from there it follows the order: it is stored on the order's outbox entry and saga, stamped as a header on every command and event, and restored in each consumer — also across the Payment/Inventory outboxes and the background workers (outbox delivery, orchestrator, timeout service), which have no HTTP request of their own. Every consumer also logs the message's `OrderId`. See [Following one order across services](#following-one-order-across-services).
- **Traces** — OpenTelemetry exports spans to Jaeger: HTTP requests and calls, **one span per RabbitMQ message** sent or consumed (MassTransit; linked across services), and **one span per database command** (Npgsql, with the SQL in the `db.statement` tag), so a request fans out across services, queues and databases on one timeline.
- **Metrics** — custom Prometheus counters (`orders_placed_total`, `outbox_pending_messages`, `sagas_active`, `saga_duration_seconds`, `inventory_cache_hits_total`) visualized in Grafana. The **ShopEasy Overview** dashboard has panels for order outcomes, orders refused at the door, payments, confirmed sales and the age of the oldest active order.
  - **Orders:** `shopeasy_orders_placed_total{status="accepted"}` counts orders accepted at the door. `shopeasy_order_outcomes_total{outcome,reason}` counts how they ended: `confirmed`; `cancelled`, with the reason (`stock_unavailable`, `payment_declined`, `customer`, `admin`, `timeout`, `release_not_confirmed`, `inventory_commit_failed`); or `failed`, when the orchestrator gave up on an order and a person has to look at it. `shopeasy_orders_rejected_total{reason}` counts the requests refused before anything was saved (`invalid_request`, `unknown_product`, `catalog_unavailable`, `insufficient_stock`, `inventory_unreachable`). `shopeasy_orders_confirmed_amount_total` adds up the amounts of the confirmed orders in the shop's currency. An order cancelled after payment was counted as `confirmed` when it was completed and is counted as `cancelled` too (its refund shows in `shopeasy_payments_total{result="refunded"}`); the confirmed counters are not corrected.
  - **Payments:** `shopeasy_payments_total{result}` and `shopeasy_payments_amount_total{result}` count what the payment provider answered (`succeeded`, `declined`) and what was given back (`refunded`), with the amounts. A command delivered twice is counted once.
  - **Stuck orders:** `shopeasy_sagas_active` is the number of orders still being processed and `shopeasy_saga_oldest_active_age_seconds` the age of the oldest of them (0 when there is none). The timeout service reads both from the database every 30 seconds, so every Order instance reports the same value (use `max()` across instances). An age that keeps rising means an order is stuck.
  - **Alert rules:** `prometheus-rules.yml` raises `OrderStuck`, `OrderFailed`, `StockMayHaveLeaked`, `OutboxBacklog` and `PaymentsDeclinedHigh`; firing alerts are listed at http://localhost:9090/alerts. Sending them anywhere needs an Alertmanager, which is not part of this stack. `prometheus-rules.test.yml` checks each rule against made-up series (`promtool test rules prometheus-rules.test.yml`), and CI runs it.

### 20. Server-Side Pricing & Request Validation

A client must never decide what it pays. `POST /api/orders` only accepts **what to buy** (`productId` + `quantity`); the product **name and unit price are always looked up from the Product service** and any `unitPrice` / `productName` sent by the client are ignored. The request is also validated (non-empty, size limits, quantity range, duplicate products merged) before anything is written.

**Design decision — what if the Product service is down?** The order is **rejected with HTTP 503** (`Retry-After: 5`), not accepted with a cached or client-supplied price. A stale or guessed price ends up on a payment, which is far more expensive to fix than asking the caller to retry. The lookup uses tight timeouts, 2 retries and a circuit breaker so a failing Product service fails fast instead of hanging customers. See the [Order API](#order-api) for the exact rules and responses.

### 21. Saga Row Locking

Several actors change a saga: the event consumers, the orchestrator that starts it, the timeout service and customer cancellation. Every transition therefore runs in a database transaction and loads the saga with `SELECT … FOR UPDATE` (`ISagaLocking`, `Sagas/SagaLocking.cs`), so a payment result racing the timeout, a cancel racing the orchestrator, or two Order instances starting the same saga are serialized: the second actor waits and then sees the first one's outcome. Commands are published only after the transaction commits.

### 22. Transactional Inbox/Outbox (Payment & Inventory)

A consumer that changes its database *and* publishes a result event has a classic gap: a crash between the commit and the publish loses the event, a failure after the publish leaves an event for a change that was rolled back. Payment and Inventory close it with MassTransit's **Entity Framework inbox/outbox** (`AddEntityFrameworkOutbox` + `UseEntityFrameworkOutbox` on every consumer endpoint):

1. MassTransit opens **one database transaction per message** (isolation `READ COMMITTED`, which the Inventory row locks need).
2. The consumer's changes — the payment row, the stock changes and ledger — an **inbox** row for the message, and the **events the consumer publishes** (stored in the `OutboxMessage` table instead of being sent) are all written in that transaction.
3. After the commit MassTransit sends the stored events to RabbitMQ and removes them. If the process dies or the broker is unreachable at that moment, the command was never acknowledged, so RabbitMQ redelivers it; the inbox shows it was already consumed, the consumer is **not** run again, and only the delivery of the stored events is completed.

Result: a payment or stock change is never recorded without its event, and no event is sent for work that was rolled back. This holds when the service is killed between the commit and the delivery, and when RabbitMQ is stopped at that moment: after the restart exactly one event arrives and the payment or stock change is not repeated. The all-or-nothing behaviour is covered by tests against a real PostgreSQL (see *Testing*).

The consumers call `db.Database.BeginOrJoinAsync()` (`Data/UnitOfWork.cs`): inside the outbox they **join** MassTransit's transaction; without it (unit tests) they open and own one. Tables: `InboxState`, `OutboxMessage` and `OutboxState` in the Payment and Inventory databases; old rows are cleaned up by MassTransit (duplicate-detection window: 30 minutes). The Order service keeps its own hand-written outbox (pattern 5) for the event that starts the saga.

### 23. Broker Readiness (no management API)

The Order service's outbox worker asks `IBrokerReadiness` before it publishes. The implementation reads **MassTransit's own bus health check** (`masstransit-bus`): healthy only when the bus is started, the RabbitMQ connection is up and every receive endpoint has declared its queue and bindings. The last part matters at start-up — an event published to an exchange that has no queue bound yet is silently dropped by the broker. While the bus is not ready, entries simply stay pending (one warning when the state changes, not one per cycle) and no retry is spent. This replaces the previous design, which polled the RabbitMQ management HTTP API (`:15672`) with Basic auth.

### 24. Surviving Database and Network Outages

Two kinds of failure reach a message consumer, and they need opposite reactions. The policy lives in `ShopEasy.Shared/Resilience` and is the same in the Order, Inventory and Payment services.

| Failure | Examples | What happens |
|---|---|---|
| **Transient** — the infrastructure is unavailable for a while | PostgreSQL restarting or failing over, a dropped connection, a command or connection timeout, a lock timeout, a deadlock, a session ended by the server, a connection pool that is used up | Retried patiently: 12 retries with pauses doubling from 1 s to 60 s (about 7 minutes in total, ±20 % jitter so the replicas do not retry in step). The message stays unacknowledged meanwhile, so that consumer slot takes nothing new from the queue (back-pressure), and a pod that dies meanwhile has the message redelivered. If the database is back inside the window the message is simply processed: no dead letter, nothing to requeue by hand. |
| **Anything else** | a bug, a business rule that cannot be satisfied, a unique-key conflict with the twin message | 3 quick retries (1–10 s) cover races, then the message goes to the `*_error` queue (pattern 14). `InvalidOperationException` and `ArgumentNullException` are not retried at all. |

Which of the two it is comes from `TransientFaults`, which looks through the whole exception chain. That matters: EF Core reports a database failure as an `InvalidOperationException` ("An exception has been raised that is likely due to a transient failure") and that type used to be on the "never retry" list, so before this a database outage was **not retried at all** — the message went to the error queue on the first failure. A failure-injection test (below) reproduced that against the previous policy.

Database connections are bounded as well (`PostgresConnection`, all five services). Without limits a stuck lock or a half-dead connection holds a request for as long as it lasts, and everything queues up behind it until the service itself is unresponsive. Values written in the connection string win; `0` switches a limit off. The settings are `Resilience:Postgres:<Setting>` (see [Where the settings live](#where-the-resilience-settings-live) below):

| Setting | Default | Effect |
|---|---|---|
| `LockTimeoutSeconds` | 10 | A `SELECT … FOR UPDATE` (stock, saga, payment rows) gives up after this long instead of queueing behind a lock holder that may be stuck. Counts as transient, so the consumer retries. |
| `IdleInTransactionTimeoutSeconds` | 60 | The server ends a session that sits inside an open transaction doing nothing. That is what releases the row locks of a pod that crashed or hung mid-transaction. |
| `CommandTimeoutSeconds` | 30 | A command running longer is cancelled on the server and fails. It also applies to the migrations at start-up: raise it for a migration that legitimately takes longer. |
| `ConnectTimeoutSeconds` | 15 | Waiting for a connection: opening a new one, or a free one from the pool. |
| `MaxPoolSize` | 20 | Connections one process may open. When they are used up callers wait `ConnectTimeoutSeconds` and then fail (and are retried), instead of the database refusing everybody. |
| `TcpKeepAliveSeconds` | 30 | Finds out that the other end vanished (failover, a dropped network path) in seconds instead of the operating system's default of hours. |

**Sizing the pool:** `replicas × MaxPoolSize` must stay below the server's `max_connections` minus a reserve for administrators (the Order service at its HPA maximum is the one to count). `LockTimeoutSeconds` and `IdleInTransactionTimeoutSeconds` travel to the server as connection start-up options; PgBouncer in transaction mode rejects those, so behind it set both to `0` and use `ALTER ROLE … SET lock_timeout = '10s'` instead.

The consumer schedule is configurable as `Resilience:Consumers:<Setting>`: `TransientRetryLimit` (12; `0` turns the patient retries off), `TransientMinIntervalSeconds` (1), `TransientMaxIntervalSeconds` (60), `JitterRatio` (0.2), and for everything else `RetryLimit` (3), `MinIntervalSeconds` (1), `MaxIntervalSeconds` (10), `IntervalDeltaSeconds` (2). Nonsensical values stop the service at start-up rather than at the next outage.

#### Where the Resilience Settings Live

Every setting has a default in the code, and the same defaults are written out where an operator looks for them:

| Where | What is there | How to change one |
|---|---|---|
| Each service's `appsettings.json` | `Resilience:Postgres` (all five services with a database), `Resilience:Consumers` (Order, Inventory, Payment) and, for calls between services, `Resilience:Inventory` and `Resilience:RetryBudget` (Order) and `Resilience:Gateway` and `Resilience:RetryBudget` (gateway), with the default values | Edit the file. It is baked into the image, so this needs a rebuild: use it for a value that should be the same everywhere. |
| `docker-compose.yml` | `Resilience__Postgres__*`, `Resilience__Consumers__*` and the call settings (`Resilience__Inventory__*`, `Resilience__Gateway__*`, `Resilience__RetryBudget__*`) as environment variables on the services that use them, each as `${RESILIENCE_…:-default}` | Put the variable in `.env` (the commented examples in `.env.example`, e.g. `RESILIENCE_PG_MAX_POOL_SIZE=30`) and run `docker compose up -d`. |
| Kubernetes `k8s/configmaps/configmap.yaml` | The same keys in the `shopeasy-config` ConfigMap, which every Deployment loads | Edit the ConfigMap, apply it, then `kubectl -n shopeasy rollout restart deployment` (environment variables are read at pod start). |

Standard ASP.NET Core precedence applies: an environment variable (`:` written as `__`) beats `appsettings.json`, which beats the default in the code. `ResilienceConfigurationTests` and `CallResilienceConfigurationTests` fail if any of these copies differs from the code's defaults, so a documented value is always the value in effect.

Deliberate choices:

- EF Core's own `EnableRetryOnFailure` is **not** used: it does not support the user-initiated transactions that MassTransit's outbox and the services' `BeginOrJoinAsync` open. Retrying the whole message instead re-runs the whole unit of work, which is safe because every consumer is idempotent and publishes through the outbox.
- No circuit breaker or kill switch pauses a consumer during an outage: stopping an endpoint can lose the retries in flight, and the bounded pool and the capped pauses already limit the load on a recovering database.
- Redelivery stays in-process (no RabbitMQ delayed-message plugin to install). The slot a message occupies while it waits is bounded by the 7-minute window, far below RabbitMQ's 30-minute `consumer_timeout`.

### 25. Idempotent Order Placement

`POST /api/orders` is the one call a client cannot safely repeat blindly, so it accepts an `Idempotency-Key` (usage: [Order API](#retrying-safely-the-idempotency-key-header)). The key is stored in the `IdempotencyRecords` table of the Order database together with a fingerprint of the request (SHA-256 of the products, quantities and note, so JSON formatting, line order and the ignored `productName`/`unitPrice` fields do not matter) and the response that was sent.

- **Atomic.** The order, its outbox event, its read-model row and the key are saved in a single transaction, so there is never an order without its key (or the other way round) and never an order the saga does not know about.
- **No lock, no "in progress" state.** The unique index on `(UserId, Key)` decides when two copies run at once: both see "new key", one commits, the other gets a unique-key violation, rolls back completely and answers from the winner. A request that crashes midway therefore cannot leave a key stuck.
- **Only a placed order uses up the key.** Validation errors, an unavailable Product service or missing stock leave nothing behind.
- **Retention.** A background job deletes keys older than `Idempotency:RetentionHours` (24) in one statement, safe to run on every replica.

The rest of the order flow is idempotent by design as well: the saga starter skips an order that already has a saga, every Inventory and Payment consumer ignores a message it has processed (pattern 7), and cancelling twice returns the same result.

### 26. Resilient Calls Between Services

A service that is restarting, overloaded or silently hung is more dangerous to its callers than one that is simply down: every caller waits as long as the callee does, holds its own connections and threads meanwhile, and the slowness spreads upstream. Each call between services therefore has a deadline, retries only what is safe to repeat, and stops calling a service that keeps failing.

| Call | Deadline | Retries | Circuit breaker | When it still fails |
|---|---|---|---|---|
| **Order to Inventory** (gRPC stock check) | 3 s per attempt. The deadline travels to Inventory, which cancels the database query as well. | Up to 2, with 0.1–1 s of jittered backoff, **only after a connection failure** (`Unavailable`: Inventory restarting, a dropped connection). A slow answer is never retried: that adds load to a service that is already struggling. | Opens when half of the recent calls (at least 5 in 30 s) failed; calls then fail at once for 15 s before one trial call is let through. | The stale cache (5 min) answers; only an order for a product with no cached value at all is rejected, with a message that the product is temporarily unavailable (pattern 11). |
| **Order to Product** (price lookup) | 3 s per attempt, 10 s in total | Up to 2 on a 5xx, 408, 429 or network error | At least 5 calls in 30 s with half failing: open for 15 s | The order is refused with `503` and `Retry-After: 5`; a guessed or stale price is never used. |
| **Gateway to every service** | 30 s without response headers or without further bytes of the request or response, then `504 Gateway Timeout` and the request to the service is cancelled. Connecting is cut off after 5 s. | Up to 2, **only for `GET`/`HEAD` without a body and only after a connection failure**: the request never reached the service, or the connection broke before an answer. `POST`, `PUT`, `PATCH` and `DELETE` are never repeated, because the service may have processed them. A `5xx` answer is passed through as it is. | None, see below | The gateway answers `502` (service unreachable) or `504` (service silent). |

Three mechanisms keep this from making an incident worse:

- **Retry budget.** Retries may add at most 20 % extra requests on top of the original ones within a 10-second window (and always a minimum of 10, so a quiet service can still retry). Without a limit, a dependency that starts failing receives every caller's retries on top of its normal load, several times over, exactly when it is least able to cope. When the budget is used up the call fails at once. The Order service keeps one budget per dependency and the gateway one for all its routes; `shopeasy_downstream_retries_total{target,outcome}` and `shopeasy_gateway_retries_total{outcome}` show how many retries were made and how many were refused by the budget.
- **Stampede protection.** When the 30-second stock cache expires, every concurrent order for the same product would call Inventory. A single in-flight fetch per product (`StockFetchCoordinator`) serves all of them. The fetch runs with its own deadline, so one customer giving up does not fail the others, and a customer who disconnects only stops waiting.
- **Stale answers over errors.** An open circuit or a timeout never reaches the customer as an error while a cached stock level (up to 5 minutes old) exists. The saga's reservation inside Inventory remains the authoritative check (pattern 11), so a stale cache cannot oversell.

The circuit breaker state changes are counted in `shopeasy_downstream_circuit_transitions_total{target,state}`.

Deliberate choices:

- **No passive health checks or circuit breaker in the gateway.** Each cluster has a single destination; there is nothing to fail over to, and with every destination marked unhealthy YARP routes to all of them anyway. Timeouts and the retry budget are what protect the gateway. With several destinations per cluster (a scaled service behind a load balancer that YARP sees individually) passive health checks are the next step.
- **Only connection failures are retried, never "slow".** A timeout means the callee may be busy with the request; sending it again doubles the work. The same reasoning is why a `5xx` answer is not retried by the gateway.
- **.NET already retries on its own** when a pooled connection turns out to be closed by the server before the request was sent; the gateway's retry covers what is left (a connection that broke after the request was sent, a service that restarted).
- **Connections are replaced every two minutes** (`PooledConnectionLifetimeSeconds`), so that the gateway finds new replicas of a scaled service behind one DNS name; a connection that works would otherwise be kept forever and the load would stay on the first replicas.
- **The Idempotency-Key makes the one write that matters safe to repeat** by the client (pattern 25); the gateway never repeats it on its own.

Settings (`Resilience:Inventory`, `Resilience:Gateway`, `Resilience:RetryBudget`) are written out in `appsettings.json`, Compose, `.env.example` and the ConfigMap like the other resilience settings, and invalid values stop the service at start-up:

| Setting | Default | Effect |
|---|---|---|
| `Resilience:Inventory:TimeoutSeconds` | 3 | Deadline of one stock-check attempt (1–60) |
| `Resilience:Inventory:MaxRetries` | 2 | Retries after a connection failure (0–5; `0` turns retries off) |
| `Resilience:Inventory:BreakDurationSeconds` | 15 | How long the circuit stays open (1–600) |
| `Resilience:Gateway:ActivityTimeoutSeconds` | 30 | How long a service may stay silent before `504` (1–600). A cluster may set its own `HttpRequest:ActivityTimeout`, which wins. Raise it for a route that streams or runs a long report. |
| `Resilience:Gateway:ConnectTimeoutSeconds` | 5 | Opening a connection (1–60) |
| `Resilience:Gateway:PooledConnectionLifetimeSeconds` | 120 | Connections are replaced after this long (10–3600) |
| `Resilience:Gateway:GetRetries` | 2 | Retries of a `GET`/`HEAD` after a connection failure (0–5) |
| `Resilience:RetryBudget:Ratio` | 0.2 | Extra requests retries may add (0–1; `0` allows only the minimum) |
| `Resilience:RetryBudget:MinRetriesPerWindow` | 10 | Retries always allowed per window |
| `Resilience:RetryBudget:WindowSeconds` | 10 | Length of the sliding window (1–600) |

The Order service's Compose variables are `RESILIENCE_INVENTORY_*` and `RESILIENCE_RETRY_BUDGET_*`, the gateway's are `RESILIENCE_GATEWAY_*` and `RESILIENCE_RETRY_BUDGET_*` (examples in `.env.example`).

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Compose)
- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) (only needed for running tests / EF migrations locally)
- ~6 GB free RAM (the full stack runs ~15 containers)

### Run the entire system

```bash
git clone <your-repo-url>
cd ShopEasy

# Create your local config — every secret lives in .env (git-ignored), never in the repo
cp .env.example .env
#   → edit .env and replace every "change-me-*" value
#   → set BOOTSTRAP_ADMIN_EMAIL and BOOTSTRAP_ADMIN_PASSWORD: without them nobody can log in
#     as an administrator (see "How to Use It → 1. Log in")
#   → for local demos you can instead set SEED_DEMO_USERS=true, SEED_ADMIN_PASSWORD, SEED_CUSTOMER_PASSWORD
#     and EXPOSE_ACCESS_TOKEN=true (needed for the Swagger "Authorize" flow)

docker-compose up --build
```

First build takes a few minutes. When it settles, every service will have run its database migrations automatically against PostgreSQL.

> **Updating an existing checkout?** `git pull` never touches your `.env`. When `.env.example` gains a setting, add it to your `.env` yourself, then rebuild the service that uses it (for the first administrator: `docker-compose up -d --build identity`).

> Every setting and secret is explained in [Configuration & Secrets](#configuration--secrets).

### Verify it's up

Open the developer portal:

```
http://localhost:5000
```

### Stop it

```bash
docker-compose down
```

To also wipe the database volume:

```bash
docker-compose down -v
```

### Database migrations

Each service applies its EF Core migrations automatically at start-up. To change a schema, add a **new** migration from that service's folder (never delete and re-create `InitialCreate` on a database that holds data):

```bash
cd src/Services/ShopEasy.OrderService      # or ShopEasy.ProductService / InventoryService / PaymentService
dotnet ef migrations add <DescriptiveName> --output-dir Data/Migrations
# Identity lives in src/Identity/ShopEasy.Identity with the same command
```

This needs the `dotnet-ef` tool (`dotnet tool install --global dotnet-ef`) and a configured connection string (see *Running a service outside Docker* above).

---

## Configuration & Secrets

No secret lives in the repository. Docker Compose reads them from a local, git-ignored **`.env`** file; Kubernetes reads them from a Secret you create from a template.

### Docker Compose: the `.env` file

```bash
cp .env.example .env     # Windows cmd: copy .env.example .env
```

`docker-compose` refuses to start (and names the missing variable) if a required value is not set. Validate your file with `docker-compose config > /dev/null` (`> nul` on Windows).

| Variable | Required | Purpose |
|---|---|---|
| `POSTGRES_PASSWORD` | yes | Password of the `shopeasy` Postgres user. Also embedded in every service connection string, so **avoid `;` and `$`**. |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | yes | RabbitMQ credentials used by the broker and by Order, Inventory and Payment. |
| `RABBITMQ_ERLANG_COOKIE` | yes | Shared secret for RabbitMQ node clustering. Any long random string. |
| `GATEWAY_CLIENT_SECRET` | yes | OAuth2 client secret: Identity registers it, the Gateway presents it. |
| `ORDER_SERVICE_CLIENT_SECRET` | yes | OAuth2 client secret for the Order service's machine-to-machine token. |
| `PGADMIN_PASSWORD` | yes | pgAdmin login password (user `admin@shopeasy.com`). |
| `GRAFANA_ADMIN_PASSWORD` | yes | Grafana `admin` password. |
| `GRAFANA_ANONYMOUS_ENABLED` | no (`false`) | Allow read-only anonymous Grafana access. Keep `false` outside your laptop. |
| `SEED_DEMO_USERS` | no (`false`) | `true` creates `alice@shopeasy.com` (admin) and `bob@shopeasy.com` (customer) at Identity startup. **Local development only.** |
| `SEED_ADMIN_PASSWORD` / `SEED_CUSTOMER_PASSWORD` | only if seeding | Passwords for the demo users. Must satisfy the Identity policy: at least 8 characters with an uppercase letter, a lowercase letter and a digit. |
| `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_PASSWORD` | no (empty) | The **first administrator**: Identity creates this account at start-up, but only while no administrator exists yet (details under [How to Use It](#1-log-in)). The password must satisfy the Identity policy; it is a secret, so it lives here (or in a Kubernetes Secret) and nowhere in the repository. |
| `EXPOSE_ACCESS_TOKEN` | no (`false`) | `true` enables `GET /account/token` on the gateway, which returns the raw JWT so you can paste it into Swagger's *Authorize* button (it is not needed for the browser console examples in this README). Off by default because the BFF pattern exists to keep tokens out of the browser. While it is off, `/account/token` answers `404` with a JSON message explaining how to turn it on. After changing it, recreate the gateway: `docker-compose up -d gateway`. |

Generate strong random values, e.g. `openssl rand -base64 24`, or in PowerShell: `-join ((48..57)+(65..90)+(97..122) | Get-Random -Count 32 | % {[char]$_})`.

**Changing `POSTGRES_PASSWORD` after the first run:** Postgres only reads it when it creates an empty data volume. Either keep the old password, or wipe local data with `docker-compose down -v` and start again.

### Service configuration keys

Services read standard ASP.NET Core configuration, so every key can be an environment variable (`:` becomes `__`). Compose and the k8s manifests already set these; this is the reference if you deploy somewhere else.

| Key | Service | Notes |
|---|---|---|
| `ConnectionStrings__DefaultConnection` | all with a DB | **Required.** Each service has its own database. No default. |
| `IdentityServer__Authority` | all APIs, Gateway | Internal URL of Identity, e.g. `http://identity:8080`. |
| `IdentityServer__PublicUrl` | Gateway | Browser-facing Identity URL (used for login redirects), e.g. `http://localhost:5001`. |
| `Oidc__ClientSecret` | Gateway | **Required.** Must equal `Clients__Gateway__Secret`. |
| `Clients__Gateway__Secret`, `Clients__OrderService__Secret` | Identity | **Required.** Secrets of the registered OAuth2 clients. |
| `Clients__Gateway__PublicUrl` | Identity | Browser-facing gateway URL for redirect URIs. Default `http://localhost:5000`. |
| `Bootstrap__AdminEmail`, `Bootstrap__AdminPassword` | Identity | The first administrator, created at start-up while no admin exists (see "How to Use It"). Default empty = none. The password is a secret: environment or Secret only, never `appsettings.json`. |
| `Registration__Enabled` | Identity | Customer sign-up on the sign-in page (see "How to Use It"). Default on. |
| `RateLimiting__Login__*`, `RateLimiting__Registration__*` | Identity | `PermitLimit` and `WindowSeconds` for the sign-in and sign-up forms, per client. Defaults 10 per 60 s and 10 per 3600 s. |
| `Seed__Enabled`, `Seed__AdminPassword`, `Seed__CustomerPassword` | Identity | Demo users, see above. Default off. |
| `Features__ExposeAccessToken` | Gateway | Enables `/account/token`. Default off. |
| `RabbitMQ__Host`, `RabbitMQ__Username`, `RabbitMQ__Password` | Order, Inventory, Payment | Username and password are **required** (no `guest` fallback). |
| `RabbitMQ__Port` | Order | AMQP port used by the dead-letter tooling. Default `5672`. |
| `RabbitMQ__ManagementUiUrl` | Order | Optional, e.g. `http://localhost:15672`. Only adds a "View" link to the dead-letter page; the service never calls it. |
| `Metrics__Port` | all services | Port of the Prometheus metrics listener. Default `9091`; change it when running several services on one machine. |
| `ServiceAuth__ClientId`, `ServiceAuth__ClientSecret` | Order | **Required** when the service token is used. |
| `Services__ProductService` | Order | Product service base URL used for price lookup. Default `http://product:8080`. |
| `Services__InventoryGrpcService` | Order | Inventory's gRPC address for the stock check. Default `http://inventory:8090`. |
| `Redis__Connection` | Product, Inventory, Order | Redis endpoint, e.g. `redis:6379`. |
| `Telemetry__Endpoint` | all | OTLP endpoint (Jaeger), default `http://jaeger:4317`. |
| `Idempotency__RequireKey`, `Idempotency__RetentionHours` | Order | `Idempotency-Key` on `POST /api/orders`: reject requests without the header (default `false`) and how long a key is remembered (default `24` hours). See [Order API](#retrying-safely-the-idempotency-key-header). |
| `IdentityServer__RequireHttpsMetadata` | all APIs, Gateway | Fetch Identity's discovery document and signing keys only over HTTPS. Default `false`; switch on together with an `https://` `IdentityServer__Authority` (see [pattern 2](#2-authentication--authorization-oauth2--oidc--jwt)). |
| `Security__CookieSecurePolicy` | Gateway, Identity | `Always`, `SameAsRequest` (default) or `None`: when the session and login cookies get the Secure flag. Use `Always` behind HTTPS. A misspelt value stops the service. |
| `ForwardedHeaders__Enabled`, `ForwardedHeaders__KnownNetworks`, `ForwardedHeaders__KnownProxies` | Gateway, Identity | Believe the `X-Forwarded-*` headers of a reverse proxy, but only from the listed CIDR networks / addresses (comma-separated). Default off; enabling without listing the proxy stops the service. |
| `Resilience__Postgres__*` | all with a DB | Limits on how long a service waits for PostgreSQL and how many connections it opens (lock, idle-in-transaction, command and connect timeouts, pool size, TCP keep-alive). Defaults suit most setups and are written out in `appsettings.json`, Compose and the ConfigMap; see [pattern 24](#24-surviving-database-and-network-outages). |
| `Resilience__Consumers__*` | Order, Inventory, Payment | Retry schedule of the message consumers: patient retries for outages, quick retries for everything else. Same pattern. |
| `Resilience__Inventory__*` | Order | Deadline, retries and circuit breaker of the stock check against Inventory; see [pattern 26](#26-resilient-calls-between-services). |
| `Resilience__Gateway__*` | Gateway | Timeout for a silent service, connect timeout, connection lifetime and the retries of `GET` requests. Same pattern. |
| `Resilience__RetryBudget__*` | Order, Gateway | How many extra requests retries may add to a dependency. Same pattern. |
| `Seq__ServerUrl` | all | Where logs are shipped, default `http://seq:5341` (the Compose Seq). **Empty = off** (console only) — the Kubernetes manifests set it empty because they do not run Seq. |
| `Testing__ForcePaymentSuccess` | Payment | Test-only: forces simulated payments to succeed. |

### Kubernetes

```bash
cp k8s/secrets/secrets.example.yaml k8s/secrets/secrets.yaml   # secrets.yaml is git-ignored
# replace every CHANGE_ME (connection strings must embed the same postgres password)
kubectl apply -f k8s/secrets/secrets.yaml
```

For real clusters prefer an external secret store (External Secrets, Sealed Secrets, Vault) over a plain `Secret`.

### Running a service outside Docker (e.g. from Visual Studio)

The `appsettings*.json` files intentionally contain no connection strings or credentials. To run one service directly, supply them yourself, for example:

```bash
cd src/Services/ShopEasy.OrderService
dotnet user-secrets init
dotnet user-secrets set "ConnectionStrings:DefaultConnection" "Host=localhost;Database=shopeasy_orders;Username=shopeasy;Password=<POSTGRES_PASSWORD>"
dotnet user-secrets set "RabbitMQ:Username" "<RABBITMQ_USER>"
dotnet user-secrets set "RabbitMQ:Password" "<RABBITMQ_PASSWORD>"
```

(and point `RabbitMQ:Host`, `IdentityServer:Authority`, `Redis:Connection` at `localhost` or the published container ports.)

### Security notes

- **Monitoring pages and APIs need a login.** `/outbox`, `/sagas` and `/deadletters` on the gateway redirect to the login page, and `/api/outbox/*` on the Order service requires the `admin` role. Swagger UIs and metrics ports are still open in this project; put them behind your network controls in production.
- **Identity key protection.** IdentityServer's signing keys are generated at runtime (automatic key management) and stored **in PostgreSQL**, together with the ASP.NET Data Protection key ring and the issued grants (authorization codes, refresh tokens, consent), so tokens survive restarts and rolling updates and several Identity replicas share one state. The signing keys are encrypted with the Data Protection key ring, which sits in the same database: **without a key-encryption certificate, anyone who can read the database can read the keys** (ASP.NET Core logs `No XML encryptor configured`). Set `DataProtection__CertificatePath` (a PKCS#12 file) and `DataProtection__CertificatePassword` to encrypt the ring with a certificate you guard separately — the Kubernetes manifests require it, see [Identity key protection](#identity-key-protection-kubernetes). `DataProtection__PreviousCertificatePath` keeps the ring readable while you rotate the certificate. Nothing is committed to git.
- **Duende IdentityServer license.** The project runs unlicensed, which Duende allows for development and testing; in production you need a license (and automatic key management requires the Business or Enterprise tier — IdentityServer logs this at start-up). Without it, supply your own signing certificate instead of automatic key management.
- **Containers run unprivileged.** Every service image runs as the base image's non-root `app` user (UID 1654); Compose and Kubernetes also set a read-only root file system (only `/tmp` is writable), drop all Linux capabilities and forbid privilege escalation. CI starts the real containers like this, so it cannot silently stop working.
- **Secrets that were committed in the early history** of this repository (database password, client secrets, a signing key) must be treated as compromised: rotate them anywhere they were used.
- **Never enable** `SEED_DEMO_USERS`, `EXPOSE_ACCESS_TOKEN` or `GRAFANA_ANONYMOUS_ENABLED` on anything reachable from the internet.

---

## How to Use It

### 1. Log in

A fresh install has **no user accounts at all**, and there is no default password anywhere in the repository. The first administrator is created from configuration:

```bash
# in .env — choose your own values
BOOTSTRAP_ADMIN_EMAIL=you@example.com
BOOTSTRAP_ADMIN_PASSWORD=YourStr0ngPassword     # 8+ characters with a digit, a lower- and an upper-case letter

docker-compose up --build        # first start; on an existing stack: docker-compose up -d --build identity
```

Then open `http://localhost:5000` and sign in with that email and password. Identity logs `Created the first administrator …` when it has done so.

How it behaves:

- **Only while no administrator exists.** Once one does, both settings are ignored and never change an existing account, so remove the password from `.env` after your first sign-in. Changing it later has no effect.
- If the email already belongs to an account, that account is made the administrator and its password is left alone.
- A password that breaks the policy, or an email without a password, stops Identity's start-up with a message that says which setting is wrong (the password itself is never logged).
- It is safe to start several Identity replicas at once; exactly one account results.
- On Kubernetes the email is `Bootstrap__AdminEmail` in the ConfigMap and the password is the optional `bootstrap-admin-password` key in the `shopeasy-secrets` Secret.
- You can change a password you know (below), but there is no self-service "forgot password" reset yet (see the known limitations). If you lose the password, ask an administrator for a temporary one (see *Managing accounts* below); if you lost the only administrator's, delete that user from the Identity database (or wipe the local data with `docker-compose down -v`) and start again with new settings.

For local demos you can instead let Identity create two throw-away users with `SEED_DEMO_USERS=true`:

```
alice@shopeasy.com  → admin     (password = SEED_ADMIN_PASSWORD)
bob@shopeasy.com    → customer  (password = SEED_CUSTOMER_PASSWORD)
```

Because `alice` is an administrator, the first-administrator settings are then ignored. That includes a database that still holds the demo users from an earlier run: sign in as `alice` with your `SEED_ADMIN_PASSWORD`, or set `SEED_DEMO_USERS=false`, delete the two users from the `AspNetUsers` table of `shopeasy_identity` and restart Identity so that the settings apply. Never use the demo users outside your laptop.

**Changing your password:** signed in, open `/Account/ChangePassword` on Identity (`http://localhost:5001/Account/ChangePassword` with Compose; the Identity home page links to it) and give the current and the new password. A wrong current password counts toward the account lockout, and the form shares the sign-in budget below. A successful change keeps this browser signed in on Identity and ends everything else: the refresh tokens of the account are revoked, so the gateway session and any other app ask you to sign in again when their token next renews, and the other browser sessions of Identity end within a minute (its security check runs every minute instead of the default 30).

**Customers create their own account** with "Create an account" on the sign-in page. Such an account is always a `customer` (the page never takes a role from the request), follows the same password rules and is signed in straight away. There is no e-mail service yet, so the address is not verified and the account stays marked unconfirmed. When the address is already registered the page only says that the account could not be created, without giving the reason; without a confirmation e-mail a persistent visitor can still tell registered addresses apart, which the limits below slow down but do not prevent.

| Setting (environment variable) | Default | Meaning |
|---|---|---|
| `Registration__Enabled` (`REGISTRATION_ENABLED`) | `true` | `false` closes sign-up: the page answers `404` and the link disappears. |
| `RateLimiting__Login__PermitLimit` / `__WindowSeconds` (`RATE_LIMIT_LOGIN_ATTEMPTS` / `_WINDOW_SECONDS`) | 10 per 60 s | Sign-in and password-change form submissions one client may make. The account lockout still applies per account. |
| `RateLimiting__Registration__PermitLimit` / `__WindowSeconds` (`RATE_LIMIT_REGISTRATION_ATTEMPTS` / `_WINDOW_SECONDS`) | 10 per 3600 s | Sign-up form submissions one client may make. |

The limits are fixed windows per client address (an IPv6 client is counted per /64) and only apply to submitting these forms; the answer is `429` with a `Retry-After` header. Behind a proxy or ingress every visitor shares the proxy's address and its budget until `ForwardedHeaders` is enabled (see [Service configuration keys](#service-configuration-keys)). On Kubernetes these keys live in the ConfigMap, which the Identity Deployment reads.

**Managing accounts (administrators):** an administrator opens `/Admin/Users` on Identity (`http://localhost:5001/Admin/Users` with Compose; the Identity home page links to it for administrators) to list and search the accounts by email, give one the `admin` or the `customer` role, and disable or enable it. Everyone else gets a `403`, and anonymous visitors are sent to sign in. The rules that keep the system administrable: nobody changes their own role or disables their own account, and the last administrator that is not disabled can be neither demoted nor disabled. Accounts are disabled, not deleted, because orders refer to user ids. Disabling ends what the account holds: its refresh tokens at once and its Identity session within a minute; a disabled account cannot sign in or renew a token, and the sign-in page says it is locked or disabled. Enabling also lifts an ordinary lockout from too many wrong passwords. A role change reaches the services through the access token, so a token already issued keeps its old role until it expires or is renewed; the Identity session of the account ends within a minute so that its next sign-in carries the new role. **Temporary password:** for a user who cannot sign in (there is no e-mail to reset it by), the *Temporary password* button generates a random one-time password, shows it to the administrator once (the response is not cached and the password is not kept anywhere, so it cannot be shown again) and ends the account's sessions and refresh tokens; the old password stops working and an ordinary lockout is lifted, while a disabled account stays disabled. The user signs in with it and is taken to the change-password page before anything else, including before an application is given a token; once they have chosen their own password they continue to where they were going. The temporary password does not expire by itself, so hand it over over a channel you trust and have it used soon; an administrator may set one for another administrator (the case where no one else can get them back in) but not for their own account. Every change is logged with the administrator's and the account's ids, never a password. If no administrator is left, which only two administrators acting on each other at the very same instant could cause, the first-administrator settings promote an account again on the next start (two that disabled each other would have to be enabled in the database, since a disabled administrator still counts as one).

### 2. Place an order

From the browser console at `localhost:5000` (while logged in):

```javascript
fetch('/api/orders', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    items: [{
      productId: '00000000-0000-0000-0000-000000000005',
      quantity:  1
    }],
    notes: 'my first order'
  })
}).then(r => r.json()).then(console.log)
```

You only send **what** to buy. The product name and price (`89.99` for the seeded "4K Webcam", which has 100 in stock) are filled in by the server from the Product service. Any `productName` / `unitPrice` you add to an item is ignored. See the [Order API](#order-api) for all rules and error responses.

### 3. Watch the saga play out

Open the **Saga Monitor** at `http://localhost:5000/sagas`. Within a couple of seconds you'll see the order move through:

```
Started → Reserving → Payment → Completed
```

The Payment column shows `Succeeded`. Inventory quantity drops by 1.

### 4. List your orders (CQRS read model)

```javascript
fetch('/api/orders').then(r => r.json()).then(console.log)
```

This reads from the denormalized `OrderSummaries` table — fast, no joins.

---

## Order API

All endpoints require a logged-in user (cookie via the gateway, or a Bearer token directly against the Order service).

| Method & path | Description |
|---|---|
| `POST /api/orders` | Place an order. Rate limited to 10 per minute per user. Send an `Idempotency-Key` header to make a retry safe (see [Retrying safely](#retrying-safely-the-idempotency-key-header)). |
| `GET /api/orders?status=&page=&pageSize=` | List the current user's orders (CQRS read model). `pageSize` is clamped to 1-50. |
| `GET /api/orders/{orderId}` | One of the current user's orders. |
| `GET /api/orders/status-counts` | Order counts per status for the current user. |
| `DELETE /api/orders/{id}` | Cancel an order (owner or admin) through the saga. See [Cancelling an order](#cancelling-an-order). |

### `POST /api/orders`

```json
{
  "items": [
    { "productId": "00000000-0000-0000-0000-000000000005", "quantity": 2 }
  ],
  "notes": "leave at the door"
}
```

| Field | Rules |
|---|---|
| `items` | Required, 1 to **50** entries. |
| `items[].productId` | Required GUID of an existing, active product. |
| `items[].quantity` | Whole number from 1 to **100**. |
| `items[].productName`, `items[].unitPrice` | **Ignored.** Accepted only so older clients keep working. The server uses the Product service's name and price. |
| `notes` | Optional, at most **500** characters (trimmed). |

The same `productId` appearing in several items is **merged into one line** (quantities added); the merged quantity must still be at most 100.

**How an order is processed:** validate the request, look up every product in the Product service (name, price, active flag), check stock (Redis/gRPC, see pattern 11), then save the order, its outbox event, its read-model row and (if the request has one) its idempotency key in one transaction and return `201`. The saga then reserves stock and takes payment asynchronously.

| Status | Meaning |
|---|---|
| `201 Created` | Order accepted (status `Pending`). The saga then reserves stock, takes payment and commits the stock asynchronously; the order becomes `Confirmed` once all of that succeeded, or `Cancelled` if any step failed (stock short, payment declined, ...). The body contains the order with the **server-side** prices and `totalAmount`. |
| `400 Bad Request` | Invalid request, unknown product, inactive product, insufficient stock, or a malformed (or, when required, missing) `Idempotency-Key`. Body: `{ "message": "Order could not be placed.", "errors": ["..."] }` with every problem listed. |
| `401 Unauthorized` | Not logged in / invalid token. |
| `422 Unprocessable Entity` | The `Idempotency-Key` was already used for a *different* request. Use a new key for a different order. |
| `429 Too Many Requests` | Order rate limit exceeded. Through the gateway the response includes a `Retry-After` header. |
| `503 Service Unavailable` | The Product service could not be reached or answered in time, so the price cannot be trusted. **The order is rejected, nothing is saved.** The response has `Retry-After: 5`; retrying is safe. |

Example `400`:

```json
{
  "message": "Order could not be placed.",
  "errors": [
    "Item 1: quantity must be at least 1.",
    "Product 3f2c0c1e-0000-0000-0000-000000000099 does not exist."
  ]
}
```

#### Retrying safely: the `Idempotency-Key` header

A client that loses the connection or times out does not know whether its order was placed. Repeating the request without protection could place it twice (and charge twice). Send a unique `Idempotency-Key` with the request and send the **same key again** when you retry:

```bash
curl -X POST https://shop.example.com/api/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: 7b0c3f0e-5d5b-4d57-9f6e-2d5f3f6a9c11" \
  -d '{ "items": [ { "productId": "00000000-0000-0000-0000-000000000005", "quantity": 1 } ] }'
```

| Situation | Result |
|---|---|
| First request with this key | Processed normally: `201`, and the response is remembered. |
| The same request again | `201` with the **original** response (same order id and body) and an `Idempotency-Replayed: true` header. No second order, no second payment. The stock and the prices are not checked again, so the retry succeeds even if they changed in the meantime. |
| The same key with a different request (other items, quantities or notes) | `422`. |
| Two copies of the same request in flight at once | One order is placed; every copy gets that order back. |
| A request that was rejected (`400`) or could not be served (`503`) | Does not use up the key: repeat it with the same key once the problem is fixed. |
| No `Idempotency-Key` | Processed as before, with no protection against repeats. |

The key is 1 to 128 characters (letters, digits and `. _ ~ : -`; a UUID is the usual choice), is scoped to the logged-in user, and is remembered for 24 hours. Both can be tuned (`Idempotency__RetentionHours`), and `Idempotency__RequireKey=true` rejects order requests that carry no key once every client sends one. `DELETE /api/orders/{id}` needs no key: cancelling is idempotent by nature, repeating it returns the same result.

Replays are counted in the `shopeasy_orders_idempotent_replays_total{result}` metric (`replayed`, `key_reused_with_different_request`).

---

### Cancelling an order

`DELETE /api/orders/{id}` does not just flip a status. It hands the order to the saga, which releases the reserved stock and refunds a payment that still arrives, using the same compensation steps as a failed order. The owner or an admin can cancel; for anyone else the order looks like it does not exist (`404`).

| Where the order is when you cancel | What happens | Response |
|---|---|---|
| Saga not created yet (the outbox event is still in flight) | A cancelled saga is recorded, so the saga starter never reserves stock or takes a payment for it | `204 No Content`, order `Cancelled` |
| `Started` (nothing reserved or charged yet) | Cancelled on the spot | `204 No Content`, order `Cancelled` |
| `ReservingInventory` or `ProcessingPayment` | Saga moves to `ReleasingInventory` and sends `ReleaseInventoryCommand`. A payment that still succeeds is refunded (late-payment guard), a reservation that arrives late is released again. When Inventory confirms the release the order becomes `Cancelled` | `202 Accepted` `{ "status": "Cancelling", ... }`; poll `GET /api/orders/{id}` until `Cancelled` |
| `ReleasingInventory` (already being cancelled, e.g. after a payment failure) | Nothing new to do | `202 Accepted` |
| Already `Cancelled` / `Failed` | Nothing to do (repeating a cancel is safe) | `204 No Content` |
| `Completed` (paid, committed, order `Confirmed`) | Saga moves to `RefundingPayment` and sends `RefundPaymentCommand`. When Payment confirms the refund the saga sends `RestockInventoryCommand`; when Inventory confirms the units are back the order becomes `Cancelled` with its payment `Refunded` | `202 Accepted` `{ "status": "Cancelling", ... }`; poll `GET /api/orders/{id}` until `Cancelled` |
| `Completing` (paid, the stock commit not confirmed yet) | The request is kept (in the saga's `FailureReason`). When the commit is confirmed the saga refunds instead of completing, then restocks as above. The order is never shown as `Confirmed` | `202 Accepted` |
| `RefundingPayment` or `RestockingInventory` (already being cancelled) | Nothing new to do | `202 Accepted` |
| The order has shipped or been delivered | Not cancellable: there is no shipping or return flow yet | `409 Conflict` `{ "message": "..." }` |

The reason is stored on the saga and shown as the order's `failureReason` (`Cancelled by customer` / `Cancelled by admin`). If the release command is lost, the saga timeout service re-sends it (idempotent) up to 3 times before flagging the order for manual intervention. A lost refund or restock is re-sent the same way but never given up on, because cancelling the order on a guess would leave the customer's money or the stock in between: after five tries each further attempt is logged as critical, and the oldest-active-order gauge and its alert show the order.

---

## Inventory Stock Model

Each product has two numbers in the Inventory database:

| Field | Meaning |
|---|---|
| `Quantity` | Units physically in stock. Only decreases when an order is **committed** (or an admin adjusts stock). |
| `ReservedQty` | Units held for orders that are still in flight. |
| `AvailableQuantity` | `Quantity − ReservedQty` — what a new order can still reserve. |

Every change is also written to the `StockTransactions` ledger. For an order the ledger holds up to three kinds of rows, recognised by their exact `Reason` text (`Data/StockReasons.cs`): *Reserved for Order …*, *Committed for Order …*, *Released reservation for Order …*.

**Lifecycle of one order's stock**

```
reserve  → ReservedQty += n                      (Quantity unchanged)
   ├── commit  (payment succeeded) → Quantity −= n, ReservedQty −= n   → InventoryCommittedEvent
   │           └── impossible (no reservation / already released)      → InventoryCommitFailedEvent → saga refunds + cancels
   └── release (payment failed / timeout)       → ReservedQty −= n     → InventoryReleasedEvent
```

**Concurrency — no overselling.** Reserve, commit, release and the admin *adjust* endpoint all run in a database transaction and first lock the affected product rows with `SELECT … FOR UPDATE`, always in `ProductId` order (so two operations touching several products cannot deadlock). Two Inventory instances therefore cannot both read "1 left" and both reserve it: the second waits for the first, then sees the updated stock. The locking lives behind `IInventoryLocking` (`Data/InventoryLocking.cs`).

**Idempotency — safe to run twice.** The state checks happen *under* those locks, so even two copies of a message processed at the same moment cannot both act:

| Command | Duplicate / odd case | Result |
|---|---|---|
| Reserve | Order already reserved | No second reservation; `InventoryReservedEvent` re-sent |
| Reserve | Not enough stock / unknown product / quantity ≤ 0 | Nothing reserved; `InventoryReservationFailedEvent` |
| Reserve | Same product on several lines | Reserved once with the total |
| Commit | Already committed | No second deduction; `InventoryCommittedEvent` re-sent |
| Commit | Reservation was released, or never existed | `InventoryCommitFailedEvent` |
| Release | Already released | No second release; `InventoryReleasedEvent` re-sent |
| Release | Already **committed** | Refused (stock is already deducted); nothing published, error logged |
| Release | Nothing was reserved | `InventoryReleasedEvent` (nothing to undo) so the saga can finish |
| Release | A reserved product has no inventory record (a data inconsistency that should never happen) | Logged as an error; the other products are released, the release is still recorded for that product (so a repeat is a no-op) and `InventoryReleasedEvent` is published so the order still gets cancelled |

Transient failures (database unavailable, ...) are *not* reported as events: the message is retried and ends up in the `*_error` queue if it keeps failing (see Dead Letter Queues).

**Messages**

| Message | Direction | Queue / consumer |
|---|---|---|
| `ReserveInventoryCommand`, `CommitInventoryCommand`, `ReleaseInventoryCommand` | Order → Inventory | `inventory-reserve`, `inventory-commit`, `inventory-release` |
| `InventoryReservedEvent`, `InventoryReservationFailedEvent`, `InventoryReleasedEvent` | Inventory → Order | `order-saga-inventory-reserved`, `-failed`, `-released` |
| `InventoryCommittedEvent`, `InventoryCommitFailedEvent` | Inventory → Order | `order-saga-inventory-committed`, `order-saga-inventory-commit-failed` |

### Checking the concurrency behaviour yourself

The unit tests cover the logic (idempotency, state checks, quantity maths) with an in-memory database, and the PostgreSQL-backed tests (`Infrastructure/InventoryPostgresTests`, see [Testing](#testing)) prove the row locks automatically — 12 orders racing for 10 units reserve at most 3 times of 3. To see it with the running stack, log in as the admin at `http://localhost:5000` and paste this into the browser console. It leaves exactly 5 *Standing Desks* in stock and then places 10 orders for one desk each, all at the same moment (the order rate limit is 10 per minute, so run it once a minute):

```javascript
const id = '00000000-0000-0000-0000-000000000003';          // Standing Desk
const json = { 'Content-Type': 'application/json' };

// 1. set stock to exactly 5
const inv = await (await fetch(`/api/inventory/${id}`)).json();
await fetch(`/api/inventory/${id}/adjust`, { method: 'POST', headers: json,
  body: JSON.stringify({ quantityChange: 5 - inv.quantity, reason: 'concurrency test' }) });

// 2. 10 simultaneous orders for 1 desk each
const statuses = await Promise.all(Array.from({ length: 10 }, () =>
  fetch('/api/orders', { method: 'POST', headers: json,
    body: JSON.stringify({ items: [{ productId: id, quantity: 1 }] }) }).then(r => r.status)));
console.log('HTTP statuses', statuses);

// 3. wait for the sagas to finish, then look at the result
await new Promise(r => setTimeout(r, 20000));
console.log('stock', await (await fetch(`/api/inventory/${id}`)).json());
const orders = await (await fetch('/api/orders?pageSize=50')).json();
console.log(orders.items.filter(o => o.itemsSummary.includes('Standing')).map(o => o.statusName));
```

Expected: **at most 5 orders end up `Confirmed`**, the rest `Cancelled` (their `failureReason` says "Insufficient stock…"), and the stock ends at `quantity: 0, reservedQty: 0` — never negative, never more than 5 sold. (Some orders may already be rejected with `400` at acceptance time, based on the cached stock level; that is fine.) Payments succeed in the default Compose setup because `Testing__ForcePaymentSuccess` is on for the Payment service.

---

## Developer Portal & Monitoring

All reachable from the landing page at `http://localhost:5000`:

| Tool | URL | Credentials |
|---|---|---|
| **Developer Portal** | http://localhost:5000 | — |
| Outbox Monitor | http://localhost:5000/outbox | log in as an admin |
| **Saga Monitor** | http://localhost:5000/sagas | log in as an admin |
| Dead Letter Monitor | http://localhost:5000/deadletters | log in as an admin |
| RabbitMQ Management | http://localhost:15672 | `RABBITMQ_USER` / `RABBITMQ_PASSWORD` from `.env` |
| Jaeger (tracing) | http://localhost:16686 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana (dashboards) | http://localhost:3000 | `admin` / `GRAFANA_ADMIN_PASSWORD` from `.env` |
| Seq (logs) | http://localhost:5341 | — |
| Redis Insight | http://localhost:5540 | — |
| pgAdmin | http://localhost:5050 | `admin@shopeasy.com` / `PGADMIN_PASSWORD` from `.env` |
| Service Swagger | Product http://localhost:5101 · Inventory http://localhost:5103 · Payment http://localhost:5104 · Order on its assigned port (`docker compose port order 8080`) | — |

**pgAdmin connection** (to inspect databases): host `postgres`, port `5432`, user `shopeasy`, password = `POSTGRES_PASSWORD` from your `.env`.

### Following one order across services

Every log line and trace of one customer action shares a **correlation id**, and every line of an order's flow also carries its `OrderId`.

1. Place the order and read the id from the response header: `curl -i … | grep -i x-correlation-id` (or send your own: `-H "X-Correlation-Id: my-test-1"`).
2. In Seq (http://localhost:5341) search `CorrelationId = 'my-test-1'` — you see the gateway request, the saga steps in the Order service, the stock changes in Inventory and the payment in Payment, in order. `OrderId = '<guid>'` finds the same story from the order id, even for lines the id did not reach (for example a payment timeout fired by a background worker long after the request).
3. In Jaeger (http://localhost:16686) pick the `ShopEasy.OrderService` service and look at the trace: HTTP spans, one span per message and one per database command.

Orders placed before correlation ids existed have no id on their saga; their messages simply carry none, and `OrderId` still works.

### Working with dead letters

A message that fails all its retries is parked in an `<endpoint>_error` queue (for example `payment-process_error`). The Order service logs `DEAD LETTER ALERT: Queue … has N failed message(s)` every 30 seconds while a queue is non-empty.

1. Open http://localhost:5000/deadletters (as an admin), or call `GET /api/outbox/deadletters`, to see every error queue and its message count.
2. Find the cause — the Seq logs of the failing service, or the message and its `MT-Fault-*` headers in the RabbitMQ management UI.
3. Fix it, then click **Requeue** (or `POST /api/outbox/deadletters/{queue}/requeue`). Each message is copied back to its original queue, confirmed by the broker, and only then removed from the error queue, so a crash can duplicate a message but never lose one — and every consumer is idempotent.

| Response | Meaning |
|---|---|
| `200` `{ moved: n }` | `n` messages were moved back. |
| `400` | Not one of the known error queues (live queues and unknown names are refused). |
| `404` | That error queue does not exist — nothing has failed there. |
| `503` | RabbitMQ is not reachable; retry shortly. |

### Testing failure scenarios

The Saga Monitor is the best way to see the system's resilience. Try this:

```bash
# 1. Place an order, then immediately stop the payment service
docker-compose stop payment

# 2. Wait ~2 minutes — the saga times out, releases inventory, and cancels.
#    The Saga Monitor shows the order Cancelled with reason "Timed out waiting for payment".

# 3. Bring payment back
docker-compose start payment

# 4. The late payment is processed, the zombie guard detects the cancelled saga,
#    and issues a refund. The Payment column now shows "Refunded 💜".
```

To reliably force a successful payment for the refund test, set `Testing__ForcePaymentSuccess: "true"` on the payment service in `docker-compose.yml`.

---

## Testing

```bash
# Unit tests — need nothing but the .NET 8 SDK
dotnet test src/Tests/ShopEasy.UnitTests

# End-to-end test — needs the Compose stack running (see "End-to-end test" below)
dotnet test src/Tests/ShopEasy.EndToEndTests

# Older API tests that call single services directly (not run by CI)
dotnet test src/Tests/ShopEasy.IntegrationTests
```

**Unit tests — 665 test cases.** 611 run anywhere (they use the EF Core InMemory provider with a fresh database per test); the other 54 need a real PostgreSQL or RabbitMQ and are **skipped automatically** unless the environment variables below are set, so `dotnet test` is green on a machine without them. CI runs all 665 (see [CI/CD](#cicd)).

| Area | Where | What it covers |
|---|---|---|
| Saga state machine | `Sagas/SagaStateTransitionTests` | The full happy path (Started → … → Completed → order Confirmed only at the end), the compensation path, insufficient stock, duplicate events, unknown orders, and the timeout service for every stuck state (including a lost commit result being re-sent and a stuck commit being flagged for a human). |
| Order cancellation | `Sagas/CancellationTests` | Cancelling at every saga stage, who may cancel, a lost release command, and what the saga handlers do after a cancel (release confirmed, late payment refunded, late reservation released, timeout retries). |
| Commit flow | `Sagas/CommitFlowTests` | Completion only after `InventoryCommittedEvent`, refund on `InventoryCommitFailedEvent`, duplicate or late events never refund, cancel or release a finished order. |
| Saga starter and first handlers | `Sagas/SagaOrchestratorTests`, `Consumers/*` | Starting a saga (including idempotency), the reserved / payment consumers, outbox entry state. |
| Inventory consumers | `Inventory/*` | Reserve, commit and release including duplicates, out-of-order messages, shortages, multi-product orders and a missing inventory record. |
| Order placement | `Orders/*` | Request validation and limits, server-side price/name resolution, the Product service client (404, 5xx, network failure, malformed response). |
| Idempotent order placement (no servers needed) | `Orders/IdempotencyKeyTests`, `OrderPlacementIdempotencyTests`, `IdempotencyConfigurationTests` | The real `POST /api/orders` action on an in-memory database with the Product service and stock cache faked: header rules and the request fingerprint (order of lines, ignored fields and whitespace do not matter); a repeated request returns the original order and response without placing another or calling the catalog and stock again, even after the stock ran out or the price changed; a different request under the same key gets 422; keys are per user; a request that was rejected (400) or unserved (503) does not use up its key; a required key; the order, outbox event, read-model row and key all saved. The appsettings, Compose, `.env.example` and ConfigMap copies of the settings equal the code's defaults. |
| Messaging (no servers needed) | `Messaging/*` | The broker readiness gate (including against MassTransit's real health check registration), outbox delivery pausing while the bus is not ready, the dead-letter monitor and the requeue endpoint's responses. |
| Resilience (no servers needed) | `Resilience/*`, `Messaging/ConsumerResilienceTests` | Which failures count as transient (including EF Core's wrapping, nested and aggregate exceptions, and what must *not* count: a missing file, a cancellation); the connection-string limits and that anything written in the string wins; the retry schedule and its jitter; and the policy on a real MassTransit pipeline — an outage of more than the old four attempts is ridden out without a dead letter, the patient and quick retries never multiply, a bug gets exactly 3 retries and a broken rule none. `Resilience/ResilienceConfigurationTests` keeps the copies of the settings in each `appsettings.json`, `docker-compose.yml`, `.env.example` and the Kubernetes ConfigMap equal to the code's defaults.; the test bus of those consumer tests is started, published to and stopped under a 30 s limit on a thread of its own (`Support/TestBus`), so a stall, even one that blocks a thread, fails that one test with a clear message instead of holding up the whole run until the 5-minute hang detector gives up. |
| Resilient calls (no servers needed) | `Resilience/RetryBudgetTests`, `Orders/InventoryCallPolicyTests`, `StockFetchCoordinatorTests`, `InventoryCacheServiceTests`, `HttpRetryBudgetTests`, `CallResilienceConfigurationTests` | The retry budget (ratio and minimum, the window sliding, concurrent callers, invalid settings); the stock-check policy against scripted failures: only connection failures are retried, a slow or timed-out call is not, retries stop when the budget is used up, the circuit opens and recovers, a hung gRPC call is cancelled at the deadline and counted as a failure, and the caller's own cancellation is not mistaken for either; one fetch per product for any number of concurrent callers, and a waiting caller leaving does not stop it; the stock cache layers, including the stale answer when Inventory is unavailable; the Product client's retries and the budget. The appsettings, Compose, `.env.example` and ConfigMap copies of the settings equal the code's defaults. |
| Gateway proxying (no servers needed) | `Gateway/GatewayResilienceTests` | A real YARP gateway in front of fake services that misbehave: a `GET` over a broken connection succeeds on the retry, a `POST` over the same broken connection is never repeated, a `503` is passed through, a refused connection answers `502` quickly, a service that restarts during the retries is reached again, a service that never answers is cut off with `504` and the request to it is cancelled. The retry handler itself against scripted failures (which methods, which failures, the budget, a request body), the timeout filter and the settings validation. |
| Gateway session (no servers needed) | `Gateway/AccessTokenRenewalTests` | The refresh-token exchange (what is sent, a revoked token, an Identity that fails or is unreachable and is then left alone for a while without that period growing, one exchange for the parallel requests of a page) and what the session cookie does with the result: untouched while the token has time, renewed and rewritten when it is about to expire, signed out when Identity rejects it, kept when Identity cannot be reached. |
| First administrator (Identity) | `Identity/AdminBootstrapTests` | The roles always exist; the account is created, confirmed and able to sign in; a restart or a new password in the configuration never changes it; nothing is created when an administrator exists; an existing account with that email is promoted with its password untouched; a missing, weak or malformed value stops the start-up without printing the password; the settings are in appsettings, Compose, `.env.example` and Kubernetes. Against PostgreSQL: eight instances starting together end with one administrator and none fails. |
| Customer sign-up (Identity) | `Identity/SignUpTests` | A sign-up creates a customer who is signed in and sent back to where they came from, never an administrator; an address that is already registered (any letter case) is refused without saying why and without touching the account; weak passwords are refused with what to change and never echoed; closed sign-up answers `404`; the form fields are validated; the limits count only form submissions, per client (IPv6 per /64, IPv4-in-IPv6 as IPv4), separately for sign-in and sign-up, and give the budget back after the window; the settings are in appsettings, Compose, `.env.example` and the ConfigMap. |
| Password change (Identity) | `Identity/ChangePasswordTests` | The password is replaced, this browser stays signed in under the new security stamp and every refresh token of the account is revoked; a wrong current password is refused, counts toward the lockout and the lockout then blocks even the right one; a new password that breaks the policy or equals the current one is refused and never echoed; a deleted account is sent to sign in again; the form fields are validated; changing the password shares the sign-in rate-limit budget. |
| User management (Identity) | `Identity/UserManagementTests` | An account becomes an administrator or a customer (one role, its old session ended); nobody changes their own role or disables their own account; the last administrator that is not disabled is protected, a disabled colleague does not count; disabling locks the account out, ends its session, revokes its refresh tokens and nobody else's; enabling lets it sign in again and lifts an ordinary lockout; an unknown role or a vanished account is reported; the page is for administrators only, lists email, name, role and status in order, marks the viewer, searches by part of the email, pages, and reports what the rules answered. |
| Temporary password (Identity) | `Identity/TemporaryPasswordTests`, `Identity/ChangePasswordTests` | The generated password is long, mixed, free of look-alike characters and different every time, and meets the password policy; setting it replaces the old password, marks the account (once), ends its session and refresh tokens and nobody else's, lifts an ordinary lockout but keeps a disabled account disabled, is refused for the administrator's own account or a vanished one, and never reaches a message, an outcome's text or the log; the Users page shows it on the answer to that request only, with caching off and nothing in TempData, and marks accounts that still have to change it; until the password is changed a signed-in account is sent to the change-password page (with the address it was going to) from every page, the authorize endpoint included, while that page, sign-out and the end-session endpoint stay open and nobody else is affected; choosing a password lifts the requirement before the cookie is renewed and follows only a local return address. |
| Authorization (no servers needed) | `Security/*`, `Orders/TokenServiceTests`, `Orders/InventoryServiceCallTests` | Bearer-token validation with real signed tokens: another API's audience, expired, signed by somebody else, issued by somebody else, unsigned and garbage tokens are all refused, and claims keep the names Identity issued. The access table: every endpoint of the four APIs against seven kinds of caller, using the real controllers and policies, plus a check that no endpoint is missing from the table; the gRPC stock check accepts only a service's token. The policies themselves (scope matching, scope AND role, user token versus service token, closed by default). Identity's configuration: one resource per API, every scope in exactly one, the order-service client limited to `inventory.read`, no administration scopes for the mobile app. The service token: the grant and scope requested, reuse until it is about to expire, one request for concurrent callers, no hammering of an Identity that is down, the old token kept while it is still valid, a rejected token dropped. Forwarded headers (a listed proxy is believed, anyone else is not) and the cookie settings; the appsettings, Compose, `.env.example` and ConfigMap copies of the settings equal the code's defaults. |
| Logging, correlation, tracing (no servers needed) | `Messaging/LoggingTests`, `CorrelationFilterTests`, `TracingSourceTests` | The exact console line per service, the correlation id inside and outside a scope, the Seq setting; the id travelling through a real MassTransit pipeline (header → consumer → the messages it publishes), the HTTP hand-over from the gateway; MassTransit and Npgsql emitting spans under the source names the services subscribe to. |

### Tests that need PostgreSQL and RabbitMQ

What these check cannot be faked: row locks, transactions, the MassTransit outbox and AMQP queue operations. Every test creates and drops its own database / queues.

| Test class | What it proves |
|---|---|
| `Infrastructure/InventoryPostgresTests` | 12 orders of 3 units racing for 10 units reserve at most 3 times (the real `SELECT … FOR UPDATE`); the same command delivered 4 times at once reserves once; a consumer that publishes and then fails sends **nothing** and rolls everything back; `BeginOrJoinAsync` joins an open transaction and leaves the commit to its owner; a row lock that another process never releases makes the consumer hit the lock timeout again and again, and the reservation still goes through once the lock is gone. |
| `Infrastructure/PaymentPostgresTests` | A payment command through the outbox saves the payment and delivers the event; the same command twice charges once and still confirms. **Failure injection:** six dropped database connections in a row (more than the old policy survived) are ridden out and the order is charged exactly once, with one idempotency row and one result event; an outage that outlasts the patient retries ends in the error queue after exactly the configured attempts, with nothing half-written. |
| `Infrastructure/OrderIdempotencyPostgresTests` | Against the real unique index: eight copies of one request in flight at once place exactly one order (one outbox event, one read-model row, one key) and everybody receives that order; concurrent requests with different keys or without a key each place their own; concurrent requests reusing one key for different orders let one win and reject the rest with 422; a retry on another instance replays from the database; only keys older than the retention are deleted and a forgotten key is accepted as new. Checked to fail when the duplicate-key handling is removed. |
| `Infrastructure/PostgresResilienceTests` | What each connection limit does against a real server: a waiter behind a stuck row lock gives up at the lock timeout (and, as a control, hangs without it); a session left idle inside a transaction is ended and its locks released; a session terminated by the server counts as retryable; a runaway query is cancelled at the command timeout and the connection stays usable; with the pool used up a caller fails after the connect timeout and the server never sees more than `MaxPoolSize` connections. |
| `Infrastructure/OutboxDeliveryPostgresTests` | Ready bus → entries published and marked delivered; not ready → nothing published and no retry spent; a failing entry is retried, parked after 5 attempts and no longer blocks the others; deliveries earlier in a batch stay delivered when a later one fails; each entry's correlation id is restored while it is published. |
| `Infrastructure/SagaPostgresTests` | The saga row lock (`SELECT … FOR UPDATE`): twelve concurrent actors lose no update (and a control test shows that **without** the lock updates are lost); a payment result racing the timeout service, and a customer cancel racing the payment result, never leave a charged order both committed and released. Checked to fail when the lock is removed. |
| `Infrastructure/ErrorQueueClientTests` | Counting and moving messages between real queues over AMQP (body, headers and message id preserved), missing source / destination, broker unreachable. |
| `Infrastructure/IdentitySharedStorageTests` | What makes several Identity replicas behave like one, with each "replica" a separate service provider that shares only the database: both sign with the same key and it survives a restart; a login cookie protected by one is readable by the other; an authorization code stored by one is redeemed (and removed) at the other; grant filters, upsert, cleanup of expired grants; with a key-encryption certificate the key ring in the table is encrypted and a replica without the certificate cannot read it (and without a certificate it is stored in clear text — the test documents that default). |

Start throw-away servers and point the tests at them. They were verified against PostgreSQL 16 and RabbitMQ 3.12, with the RabbitMQ management plugin deliberately **not** enabled — the application does not need it:

```bash
docker run -d --name shopeasy-test-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:16-alpine
docker run -d --name shopeasy-test-mq -e RABBITMQ_DEFAULT_USER=test -e RABBITMQ_DEFAULT_PASS=test -p 55672:5672 rabbitmq:3.13-alpine

export SHOPEASY_TEST_POSTGRES="Host=localhost;Port=55432;Username=postgres;Password=test"
export SHOPEASY_TEST_RABBITMQ="amqp://test:test@localhost:55672"
dotnet test src/Tests/ShopEasy.UnitTests          # now 665 run, 0 skipped

docker rm -f shopeasy-test-pg shopeasy-test-mq
```

`SHOPEASY_TEST_POSTGRES` is a server connection string **without** a database name (the role needs `CREATEDB`). 

### End-to-end test

`src/Tests/ShopEasy.EndToEndTests` uses the running system the way a customer does: through the gateway only, against the real containers. It creates an account on Identity's sign-up page, signs in through the gateway's OpenID Connect redirects (the test never handles a token, only the session cookie) and then shops. Every service takes part: Identity, gateway, Product, Order and its saga, Inventory, Payment and RabbitMQ.

| Scenario | What it proves |
|---|---|
| A customer buys | An order whose request claims a price of one cent is priced by the catalog; it is accepted as `Pending`, the saga reserves the stock, takes the payment and the order becomes `Confirmed`; Inventory then holds fewer units by exactly the quantity ordered, Payment holds one charge for the total, the order is in the customer's list, and cancelling the confirmed order refunds the payment, puts the units back and ends it `Cancelled` (covered by its own end-to-end scenario). |
| The same request again | One `Idempotency-Key` placed twice gives the same order with `Idempotency-Replayed: true`, a different request under that key is refused (`422`), and one order and one unit off the shelf result. |
| An order that cannot be filled | An unknown product and a quantity above the stock are each refused (`400`) with the reason; nothing is saved and the stock is unchanged. |
| Who may see what | A visitor sees the catalog but not orders and cannot place one; a customer is known by the account they created; another customer is told the order does not exist (`404`) and may not read its payment (`403`); monitoring endpoints refuse a customer. |
| Order budgets | A customer who sends 10 order requests in a minute gets the eleventh refused (`429`), and another customer is not affected: both the gateway and the Order service limit per user, not per client address. |

CI runs it in the **Compose stack and end-to-end test** job, right after the containers are up, with the throw-away values of `.env.example`. Compose starts the payment provider in "always succeeds" mode (`Testing__ForcePaymentSuccess`), so a failure here means a broken system, not a declined payment. To run it yourself:

```bash
docker compose up -d --build --wait postgres rabbitmq redis identity product inventory payment order gateway
dotnet test src/Tests/ShopEasy.EndToEndTests
```

It expects the gateway at `http://localhost:5000` and Identity at `http://localhost:5001` (`E2E_GATEWAY_URL` and `E2E_IDENTITY_URL` change that). Each run creates three accounts, and sign-up is limited to 10 per hour per address (`RATE_LIMIT_REGISTRATION_ATTEMPTS`), so raise that limit in `.env` if you run it more than three times an hour. The accounts and orders stay in the database; `docker compose down -v` clears them.

Not covered, on purpose: a declined payment and the compensation after it (the provider is forced to succeed here; the saga tests and the PostgreSQL tests cover the compensation), and cancelling an order while the saga is still running (it depends on timing, and the saga tests cover every stage).

## CI/CD

Everything below is in `.github/` and runs on GitHub Actions. Nothing needs to be installed or configured by hand for the **CI** part; the **CD** part publishes images with the repository's built-in token.

### CI — `.github/workflows/ci.yml`

Runs on every pull request, every push to `main` and every `v*.*.*` tag. A new push to a pull request cancels the run still in progress; runs on `main` and on tags always finish, so that a quick second merge cannot stop the first one from publishing its images. A pull request that **only changes documentation** (`*.md`, `docs/`, the licence) skips the build, the tests and the container jobs, so it finishes in seconds; anything else, and every run on `main` or a tag, runs the full pipeline.

| Job | What it checks |
|---|---|
| **Detect changes** | Decides whether the pull request touches anything that is built, tested or deployed (plain `git diff`, no third-party action). Skipped jobs count as passed in **CI passed**, so the check can stay required for documentation-only changes. |
| **Build and test** | `dotnet restore/build` (Release) and all 665 tests against **PostgreSQL 16 and RabbitMQ 3.13 service containers** (RabbitMQ without the management plugin on purpose). A guard step **fails the job if any test was skipped**, so a mis-wired service container cannot turn the 54 infrastructure tests into silent no-ops. `CI=true` also turns the NuGet vulnerability audit into errors (below). A test that hangs is aborted after 5 minutes and named in the output instead of holding the job until its 25-minute limit. Test results are uploaded as an artifact; the counters appear in the job summary. |
| **Validate Compose and Kubernetes files** | `docker compose config` with `.env.example` (catches a new required variable that is missing from the example); `kubectl kustomize k8s` validated with `kubeconform -strict`; **deployment rules** on the rendered manifests (`.github/scripts/check-k8s-manifests.py`): the six service images come from GHCR and are pinned to a `sha-<commit>` tag (never `latest`, never `imagePullPolicy: Never`), their containers run as non-root with a read-only root file system, no privilege escalation, no capabilities and resource limits, they take the shared ConfigMap (so a setting changed there reaches every service), third-party images have a version tag, and the namespace denies ingress by default; and a check that every Dockerfile's .NET base image matches the projects' target framework (an image *build* succeeds with a wrong version, but a `net8.0` app cannot start on a .NET 10 runtime — so a base-image bump must show up as a red check). |
| **Compose stack and end-to-end test** | Builds all six service images from their Dockerfiles (so a Dockerfile that no longer builds fails here; nothing is pushed from a pull request) and **starts the real containers** (PostgreSQL, RabbitMQ, Redis and the six services), hardened as in production — non-root, read-only root file system, no capabilities — and waits for every health check. Then it proves each application container runs as a non-root user, that Identity serves a signing key it generated into PostgreSQL, and that the gateway answers. An image that builds but cannot start (the failure mode of the .NET 10 base-image bump) fails here. Last, it runs the [end-to-end test](#end-to-end-test) against those containers: a customer signs up, signs in, buys something and the order is confirmed, which no unit test can show because it needs every service, the broker and the databases working together. |
| **CI passed** | One aggregate status for the jobs above (a skipped job counts as passed) — the check to require in branch protection. |

**Dependency audit.** `Directory.Build.props` makes NuGet check every restored package, direct *and* transitive, against the advisory database. Locally a finding is a warning; **in CI it fails the build** (`NU1901`–`NU1904`). To fix one, upgrade the package, or pin a patched version of the transitive dependency with a direct `PackageReference` (see the `System.Text.Json` pin in `ShopEasy.Shared.csproj`). **Dependabot** (`.github/dependabot.yml`) opens weekly update pull requests for NuGet packages, the Dockerfile base images and the GitHub Actions; each runs the full CI. NuGet updates are grouped: one pull request for the OpenTelemetry, Serilog, MassTransit and test-tooling packages each, and one for every other minor or patch update across all projects; a major version arrives as its own pull request. Major bumps of the framework-bound packages **and of the .NET base images** are ignored on purpose (the services target .NET 8 LTS; moving to a newer .NET is a project decision that changes the `TargetFramework`, the Dockerfiles and the CI together), and so are major bumps of Swashbuckle and Serilog.AspNetCore and every major *and minor* bump of MassTransit: 8.5+ pulls RabbitMQ.Client 7, which removes the synchronous API the dead-letter tooling uses (the Order service stops compiling), and MassTransit 9 is no longer Apache-2.0 — both need a deliberate migration. If a base-image bump is merged anyway, the Dockerfile check above fails.

To run the same checks locally:

```bash
CI=true dotnet build ShopEasy.sln          # includes the vulnerability audit
dotnet test src/Tests/ShopEasy.UnitTests    # with the two SHOPEASY_TEST_* variables set (see Testing)
docker compose --env-file .env.example config --quiet
```

### CD — publishing images

After **CI passed** on `main` or on a version tag, the `publish-images` job builds the six service images (with the layer cache of earlier runs) and pushes them to the GitHub Container Registry as `ghcr.io/<owner>/shopeasy-<service>` (`identity`, `gateway`, `product`, `inventory`, `payment`, `order`), tagged:

| Tag | When |
|---|---|
| `sha-<short commit>` | every publish — immutable, what a deployment should reference |
| `latest` | pushes to `main` |
| `1.2.3` and `1.2` | when you push a tag `v1.2.3` |

To cut a release: `git tag v1.2.3 && git push origin v1.2.3`. Pull requests never publish. The job authenticates with the workflow's own `GITHUB_TOKEN` (permission `packages: write` on that job only), so there is **no secret to create**. A pushed image is private to the repository at first; make the package public (or grant the cluster pull access) under *Packages → package settings*.

Deploying to a cluster is intentionally **not** automated: it needs credentials for a specific environment. The Kubernetes manifests reference these published images, pinned to a `sha-<commit>` tag in `k8s/kustomization.yaml`; rolling out a build is `k8s/set-image-tag.sh sha-abc1234 && kubectl apply -k k8s` (see [Kubernetes Deployment](#kubernetes-deployment)).

### One-time repository settings (needs repository admin)

These cannot be done from a workflow file; they are GitHub settings:

1. **Settings → Actions → General**: Actions enabled; *Workflow permissions* may stay on the read-only default (the publish job asks for `packages: write` itself).
2. **Settings → Branches → add rule for `main`**: *Require a pull request before merging* and *Require status checks to pass* with the check **`CI passed`**. This is what turns the CI above from advice into a gate.
3. *(Optional)* **Settings → Code security**: enable *Dependabot alerts* and *secret scanning* (free on public repositories).

---

## Horizontal Scaling

The Order service is stateless and built to scale. The distributed outbox lock means duplicate instances never double-process messages.

```bash
# Scale to 3 instances
docker-compose up --build --scale order=3

# Scale up live, no downtime
docker-compose up --scale order=5 --no-recreate

# Scale back down
docker-compose up --scale order=2 --no-recreate
```

The gateway needs **no changes** — Docker's internal DNS round-robins across all instances behind `http://order:8080`. To verify only one instance claims each batch:

```bash
docker-compose logs order | findstr "acquired lock"
```

---

## Kubernetes Deployment

Full manifests live in `k8s/`. They deploy the images the CI/CD workflow publishes to GHCR with production orchestration: auto-scaling, self-healing, rolling updates, resource limits, containers that run unprivileged, and network policies.

| Path | What |
|---|---|
| `k8s/kustomization.yaml` | What `kubectl apply -k k8s` deploys, and the **one place** that says which images run: `ghcr.io/<owner>/shopeasy-<service>` pinned to a `sha-<commit>` tag (never `latest`). CI fails if a tag is not pinned. |
| `k8s/set-image-tag.sh` | `k8s/set-image-tag.sh sha-abc1234` points all six services at one published build. |
| `k8s/<service>/` | Deployment + Service per service (non-root user 1654, read-only root file system, all capabilities dropped, `/tmp` as the only writable path, resource requests/limits, health probes). |
| `k8s/network-policies/` | Default-deny ingress plus explicit allows (below). |
| `k8s/secrets/secrets.example.yaml` | Template for the `shopeasy-secrets` Secret you create yourself; it is deliberately not part of the kustomization. |

### Quick start

You need a cluster and `kubectl` (Docker Desktop, kind, minikube, or a managed cluster).

```bash
# 1. The namespace first, because the secrets live in it
kubectl apply -f k8s/namespace/namespace.yaml

# 2. Secrets (secrets.yaml is git-ignored): replace every CHANGE_ME
cp k8s/secrets/secrets.example.yaml k8s/secrets/secrets.yaml
kubectl apply -f k8s/secrets/secrets.yaml

# 3. The certificate that encrypts Identity's key ring (see "Identity key protection" below)

# 4. If the GHCR packages are private, let the cluster pull them (skip when they are public)
kubectl -n shopeasy create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io --docker-username=<github user> --docker-password=<token with read:packages>

# 5. Everything else, in one command
kubectl apply -k k8s

# 6. Watch it come up
kubectl get pods -n shopeasy -w
```

Gateway is exposed on NodePort `30000` (`http://localhost:30000`), Identity on `30001`. The pods start in no particular order and retry until PostgreSQL, RabbitMQ and Identity are ready.

On a **fork**, change `newName` in `k8s/kustomization.yaml` to your lowercase owner (`ghcr.io/<owner>/shopeasy-<service>`). To run **locally built** images (kind, minikube) instead, build them, load them into the cluster and use `kustomize edit set image shopeasy-order=shopeasy-order:dev` in a scratch copy — do not commit that; the CI rule rejects unpinned images.

### Rolling out a build

Every merge to `main` publishes images tagged `sha-<short commit>` (see CD above). Pick the commit, then:

```bash
k8s/set-image-tag.sh sha-abc1234
kubectl apply -k k8s
kubectl -n shopeasy rollout status deployment/identity deployment/gateway
```

Commit the changed `kustomization.yaml` (a pull request) so the repository always says what is deployed. Rolling back is the same command with the previous tag.

### Identity key protection (Kubernetes)

Identity stores its signing keys, the Data Protection key ring and issued grants in PostgreSQL, so replicas share them and a restart or rolling update invalidates nothing. The key ring protects the signing keys, so it must not sit in the database in clear text: the Identity Deployment loads a certificate from a Secret and encrypts the ring with it. Create it once (any PKCS#12 file works; a self-signed one is enough because it only encrypts):

```bash
PASSWORD="$(openssl rand -base64 24)"
openssl req -x509 -newkey rsa:3072 -nodes -days 3650 -subj "/CN=shopeasy-identity-keyring" \
  -keyout keyring.key -out keyring.crt
openssl pkcs12 -export -inkey keyring.key -in keyring.crt -out key-ring.pfx -passout "pass:$PASSWORD"

kubectl -n shopeasy create secret generic identity-dataprotection \
  --from-file=key-ring.pfx --from-literal=password="$PASSWORD"

shred -u keyring.key keyring.crt key-ring.pfx    # keep the PFX and password in your password manager or vault
```

Without that Secret the Identity pod stays in `ContainerCreating` (*secret "identity-dataprotection" not found*) — on purpose, rather than silently storing the keys unprotected. **Losing the certificate means losing the key ring**: Identity then creates new signing keys and every user signs in again. To rotate the certificate, mount the old one too and set `DataProtection__PreviousCertificatePath` (it must use the same password), roll out, and remove the old one later.

**Scaling Identity.** Deploy with one replica (it applies its database migrations at start-up), then `kubectl -n shopeasy scale deployment identity --replicas=2`.

**Upgrading from the file-based keys.** Earlier versions kept the signing key in a volume (`identity-keys` in Compose, a PVC in Kubernetes). After the upgrade Identity creates a new key in the database, so tokens signed by the old key stop validating and users sign in once more. Remove the old volume: `docker volume rm shopeasy_identity-keys` / `kubectl -n shopeasy delete pvc identity-keys-pvc`.

### Network policies

`k8s/network-policies/network-policies.yaml` denies all ingress in the namespace and then allows only what the services actually call: the **gateway** and **identity** from anywhere (the NodePorts; Identity is also called by every service), the gateway into the four APIs, Order into Inventory (HTTP and gRPC) and Product, and PostgreSQL, RabbitMQ and Redis only from the services that use them. The data stores may not start connections (DNS only).

> NetworkPolicies are enforced only if the cluster's network plugin supports them (Calico, Cilium, …). Docker Desktop's and minikube's defaults accept them and do nothing. Egress of the application pods is not restricted: it needs the list of external endpoints of *your* deployment. Behind an Ingress controller, narrow the "from anywhere" rule to the controller.

### What Kubernetes adds over Compose

| Docker Compose | Kubernetes |
|---|---|
| Manual restart | Self-healing (auto-restart crashed pods) |
| Manual `--scale` | HorizontalPodAutoscaler (CPU-based, 2–10 pods) |
| Single machine | Multi-node |
| Restart = downtime | Zero-downtime rolling updates |
| No resource caps | Enforced CPU/memory requests & limits |

### Tear down

```bash
kubectl delete namespace shopeasy
```

> **Note on migrations under multiple replicas:** the Order and Identity services swallow the `42P07` ("relation already exists") error so that when two replicas start simultaneously, the one that loses the migration race continues cleanly instead of crash-looping. The other services still rely on starting one replica first.

---

## Project Structure

```
ShopEasy/
├── docker-compose.yml
├── .env.example                   # copy to .env (git-ignored) — all secrets live there
├── Directory.Build.props          # NuGet vulnerability audit (errors in CI)
├── .github/
│   ├── workflows/ci.yml           # CI (change detection, build, tests, deployment-file checks, build and start the stack) + CD (publish to GHCR)
│   ├── scripts/                   # Dockerfile / Kubernetes / running-container rules the CI enforces
│   └── dependabot.yml
├── postgres-init.sql              # creates the 5 databases
├── prometheus.yml
├── prometheus-rules.yml           # alert rules (+ prometheus-rules.test.yml: their tests)
├── grafana/provisioning/          # auto-provisioned dashboards
├── k8s/                           # Kubernetes manifests (kubectl apply -k k8s)
│   ├── kustomization.yaml         # which images (pinned sha-<commit> tags) and resources
│   ├── set-image-tag.sh
│   ├── namespace/  secrets/  configmaps/  network-policies/
│   ├── postgres/  rabbitmq/  redis/
│   └── identity/ product/ inventory/ payment/ order/ gateway/
├── src/
│   ├── Identity/ShopEasy.Identity/
│   ├── Gateway/ShopEasy.Gateway/
│   ├── Shared/ShopEasy.Shared/    # contracts, middleware, cache, protos
│   │   ├── Contracts/             # events & commands
│   │   ├── Logging/               # one Serilog setup for every service (names, Seq, correlation id)
│   │   ├── Messaging/             # CorrelationContext + MassTransit correlation filters
│   │   ├── Protos/inventory.proto
│   │   ├── Services/CacheService.cs
│   │   └── Middleware/CorrelationIdMiddleware.cs
│   └── Services/
│       ├── ShopEasy.ProductService/
│       ├── ShopEasy.OrderService/
│       │   ├── Sagas/             # orchestrator + consumers
│       │   ├── Services/          # outbox delivery + broker readiness, error-queue (AMQP) client,
│       │   │                      #   dead-letter monitor, timeout, gRPC client, inventory cache,
│       │   │                      #   product catalog client, order request validation/pricing
│       │   ├── ReadModels/        # CQRS OrderSummary
│       │   └── Controllers/       # Commands + Queries (CQRS split)
│       ├── ShopEasy.InventoryService/
│       │   ├── Grpc/              # gRPC service implementation
│       │   ├── Data/              # DbContext, StockReasons (ledger kinds), InventoryLocking (FOR UPDATE),
│       │   │                      #   UnitOfWork (join the MassTransit outbox transaction)
│       │   └── Consumers/         # reserve / commit / release
│       └── ShopEasy.PaymentService/
│           ├── Consumers/         # process / refund
│           └── Data/              # DbContext, UnitOfWork (join the MassTransit outbox transaction)
└── src/Tests/
    ├── ShopEasy.UnitTests/        # unit tests + Infrastructure/ (needs PostgreSQL / RabbitMQ, see Testing)
    ├── ShopEasy.EndToEndTests/    # a customer signs up and buys, against the running Compose stack
    └── ShopEasy.IntegrationTests/ # older tests of single services (not run by CI)
```

---

## Troubleshooting

**Currency shows as `¤` instead of `$`** — cosmetic locale issue in the container; ignore.

**Prometheus targets DOWN** — ensure `prometheus.yml` and the Grafana provisioning files exist on disk *before* `docker-compose up` (Docker mounts files; a missing file becomes a directory).

**gRPC fails with `HTTP_1_1_REQUIRED`** — Inventory must expose gRPC on a dedicated HTTP/2-only port (8090). REST stays on 8080. The env var `DOTNET_SYSTEM_NET_HTTP_SOCKETSHTTPHANDLER_HTTP2UNENCRYPTEDSUPPORT=1` enables cleartext HTTP/2.

**Order migration crash on multi-instance start** — handled in code (swallows `42P07`); if you see it, ensure you rebuilt the Order image.

**`docker-compose --scale` fails with "container name" warning** — the Order service must not have a fixed `container_name` and must expose a dynamic port (`- "8080"`, not `- "5102:8080"`).

**`docker-compose up` fails with "Set X in .env"** — a required variable is missing from your `.env`. Compare it with `.env.example`.

**Services cannot log in to Postgres after you edited `.env`** — Postgres only applies `POSTGRES_PASSWORD` when it first creates the data volume. Restore the old password, or wipe local data with `docker-compose down -v`.

**`/sagas`, `/outbox` or `/deadletters` redirect to a login page** — expected: they require a login (as an admin to see data). No account exists until you create one: set `BOOTSTRAP_ADMIN_EMAIL` and `BOOTSTRAP_ADMIN_PASSWORD` in `.env` and run `docker-compose up -d --build identity` (see [How to Use It](#1-log-in)).

**I set `BOOTSTRAP_ADMIN_*` but cannot sign in** — check `docker-compose logs identity | grep -i administrator`. `Created the first administrator …` means it worked (check the email and password you typed). `An administrator already exists …` means an admin was there before, so the settings were ignored; this is also the case after ever running with `SEED_DEMO_USERS=true` (sign in as `alice@shopeasy.com`, or wipe the data with `docker-compose down -v`). Nothing in the log at all usually means the identity image was not rebuilt after editing `.env`.

**`/account/token` returns 404 with a message** — it is disabled unless `EXPOSE_ACCESS_TOKEN=true` in `.env` (local development only). Set it, then run `docker-compose up -d gateway` to recreate the gateway container with the new value, and open `/account/token` again while logged in.

**`/account/me` shows `userId: null` or empty `roles`** — your login cookie was created by an older gateway build. Sign out at `/account/logout` and log in again so the session picks up the `sub` and `role` claims. (The services themselves always read the user and role from the access token, so API calls were not affected; the visible effects were this endpoint and the gateway's per-user / admin rate limits.)

**`429 Too Many Requests` when listing or cancelling orders** — only placing an order (`POST /api/orders`) has the strict 10-per-minute limit; other order calls use the normal per-user limit of 100 per minute. If you still see `429`, you are exceeding that, or you are running an older gateway image: `docker-compose up -d --build gateway`.

**`POST /api/orders` returns 503 "Product catalog is temporarily unavailable"** — the Order service could not get a price from the Product service (down, slow, or its circuit breaker is open). Check `docker-compose ps product` and its logs, then retry; nothing was saved.

**`POST /api/orders` returns 400 "Product ... does not exist" / "no longer available"** — the `productId` is not in the Product catalog, or the product was deactivated. List valid ids with `GET /api/v1/products`.

**`DELETE /api/orders/{id}` returns `409`** — the order has shipped or been delivered; there is no shipping or return flow yet. **It returns `202`** — the cancellation was accepted and the saga is releasing stock; the order shows `Cancelled` shortly after (check `/sagas`, or the `inventory-release` queue if it takes longer than a couple of minutes).

**An order stays `Pending` and the saga sits in `Completing`** — payment succeeded and the saga is waiting for the Inventory service to confirm the stock commit. Check that the `inventory` container is up and that `inventory-commit` / `inventory-commit_error` in the RabbitMQ UI are empty. The timeout service re-sends the commit every ~2 minutes; the commit is idempotent, so this is safe.

**An order was `Cancelled` and refunded although payment had succeeded** — the Inventory service reported `InventoryCommitFailedEvent`: the reservation had been released (for example by a timeout) before the commit arrived. The saga ends in `Failed`, the order is cancelled and the payment is refunded. The reason is on the saga (`/sagas`) and in the order's `failureReason`.

**Orders stay `Pending` and the Order log says `Outbox: message bus is not ready — delivery paused`** — the Order service cannot publish because RabbitMQ is unreachable or one of its queues is not declared yet. Entries stay in the outbox (check `/outbox`) and are delivered automatically, in order, once the log says `message bus is ready — delivery enabled`; nothing needs to be done unless it persists, in which case check the `rabbitmq` container and the `RabbitMQ__*` settings.

**Dead letter page shows nothing / `503 RabbitMQ is not reachable`** — the Order service reaches RabbitMQ over AMQP (port 5672), not through the management UI. Check `RabbitMQ__Host`, `RabbitMQ__Port`, the credentials and that the `rabbitmq` container is healthy. The management UI (15672) being disabled does not affect it.

**Payment or Inventory processed a command but the next step never happened** — the result event is stored in the service's `OutboxMessage` table until it is delivered. `SELECT count(*) FROM "OutboxMessage";` in `shopeasy_payments` / `shopeasy_inventory` should be 0; rows that stay mean RabbitMQ was unreachable at that moment. They are delivered when the broker is back and the command is redelivered — the stock/payment change itself is not repeated. If the command ended up in an `_error` queue instead, requeue it from the Dead Letter Monitor page; the saga timeout service also re-sends commands that never got an answer.

**CI fails with `NU1903` (or `NU1902`, `NU1904`, `NU1901`)** — a package has a published security advisory. Run `dotnet list ShopEasy.sln package --vulnerable --include-transitive` to see which, then upgrade it or pin a patched version (see CI/CD → Dependency audit). It is only a warning on your machine; CI makes it an error on purpose.

**CI fails in "Fail if any test was skipped"** — the PostgreSQL / RabbitMQ tests did not run, so their service containers are not reachable. Look at the *Initialize containers* step of the job; this guard exists so that never passes silently.

**Log lines show `[-]` instead of an id** — the line was written outside a request or message (start-up, a timer). Search by `OrderId` instead, or find the line's neighbours with the same `Service`.

**No logs in Seq on Kubernetes** — expected: the manifests set `Seq__ServerUrl` empty because Seq is part of the Compose stack only. Use `kubectl logs`, or set `Seq__ServerUrl` in the ConfigMap to your own Seq.

**Redis down crashes EF migrations** — the Redis connection string uses `abortConnect=false` and the multiplexer is registered so startup doesn't depend on Redis being up.

---

## Production Hardening Roadmap & Known Limitations

This project is being hardened step by step. Status:

| # | Step | Status |
|---|---|---|
| 1 | Repo hygiene: `.gitignore`, `.dockerignore`, no build output / IDE files / keys in git | Done |
| 2 | Secrets out of the repo (`.env`, k8s Secret template), monitoring endpoints behind auth, no hardcoded credentials | Done |
| 3 | Server-side pricing and request validation on `POST /api/orders` | Done |
| 4 | Inventory correctness: row locking on reserve/commit/release/adjust, idempotent commit and release, saga waits for the commit result (refunds if the commit is impossible) | Done |
| 5 | Order cancellation through the saga (release stock, refund a late payment), plus row locking on every saga transition | Done |
| 6 | Replace RabbitMQ management-API polling with a bus-ready check and AMQP dead-letter tooling; MassTransit transactional inbox/outbox for Payment/Inventory events | Done |
| 7 | Observability (per-service log names, configurable Seq, correlation id across HTTP, outbox, saga and RabbitMQ, MassTransit + Npgsql traces), CI/CD on GitHub Actions (build, all tests incl. PostgreSQL/RabbitMQ, deployment-file validation, image builds, publish to GHCR, Dependabot), vulnerable dependencies patched and gated, saga row-locking tests on PostgreSQL, saga tests re-enabled | Done |
| 8 | Non-root images (no SQLite, no `chmod 777`), read-only containers started by CI, Kubernetes with pinned published image tags and no local-image assumptions, Identity keys / key ring / grants shared in PostgreSQL (several replicas), network policies | Done |
| 9 | Resilience and API safety: consumers ride out database and network outages and every database wait is bounded (pattern 24); order placement is idempotent (pattern 25); calls between services have deadlines, budgeted retries and circuit breakers, and the gateway times out silent services (pattern 26); every API checks audience, scope and role, and the internal gRPC call needs a service identity (pattern 2). An end-to-end test of the order journey runs in CI. Still to do: business-level observability and contract tests between the services | In progress |

**Known limitations today** (documented so nobody is surprised):

- A shipped or delivered order cannot be cancelled, and there is no return or partial-refund flow yet. A paid order that is cancelled before it ships is refunded in full.
- If the saga orchestrator itself fails three times on a saga it marks the saga `Failed` but does not update the order, which stays `Pending` until the customer cancels it (cancelling a `Failed` saga fixes the order).
- The Order service's saga handlers still publish their commands *after* committing (they do not use the MassTransit outbox). A crash in that gap is recovered by the saga timeout service, which re-sends the command; every consumer is idempotent.
- The Inventory consumers invalidate the Redis stock cache while MassTransit's transaction is still open (the commit follows immediately after the consumer returns). In that few-millisecond window a reader can re-cache the old stock level for up to its 30-second TTL. The cache is advisory — the saga's reservation is the authoritative check.
- A message published to an exchange that has no queue bound yet is dropped by the broker, so a command sent before its consuming service has started for the first time is lost until the saga timeout re-sends it. (Docker Compose and Kubernetes start order makes this rare.)
- The MassTransit inbox only remembers a message for its 30-minute duplicate-detection window; beyond that, duplicates are caught by the business-level checks (pattern 7).
- Inventory endpoints process one message at a time per instance (`ConcurrentMessageLimit = 1`). Correctness no longer depends on it (row locks do), so it can be raised for throughput.
- `InventoryDbContext` has no unique database constraint on the ledger; idempotency relies on the row locks.
- The CI only becomes a *gate* once branch protection requires the `CI passed` check (a one-time repository setting, see CI/CD). Until then it reports but does not block.
- The older tests in `src/Tests/ShopEasy.IntegrationTests` call single services directly and are not run by CI; the end-to-end test and the unit-test project (with real PostgreSQL and RabbitMQ) are. The end-to-end test covers the happy path and the refusals, not a declined payment or a cancel while the saga runs.
- Images are published to GHCR and the manifests pin them by commit, but nothing deploys them automatically (that needs credentials for a specific cluster), and the pinned tag in `k8s/kustomization.yaml` is bumped by hand (`k8s/set-image-tag.sh`). No container image vulnerability scan (Trivy) or code scanning (CodeQL) runs yet.
- The Kubernetes manifests were validated (schema and the deployment rules above) and the same hardening runs in CI under Compose, but they have not been applied to a live cluster in CI. NetworkPolicies need a network plugin that enforces them, and egress of the application pods is not restricted.
- The gateway keeps its login cookie's Data Protection keys in memory/on its own file system, so a restart signs users out and it cannot run several replicas yet (Identity already can).
- Without `DataProtection__CertificatePath`, Identity's key ring — and with it the signing keys — is stored in the database in clear text; the Kubernetes manifests require the certificate, Compose (local development) does not. Production use of Duende IdentityServer needs a license (see Security notes).
- PostgreSQL, RabbitMQ and Redis in `k8s/` are single-replica Deployments (with a PVC at most), so they are not highly available. Use a managed service or an operator for production data stores.
- A customer cancel (`DELETE /api/orders/{id}`) is logged and published under the *cancel request's* correlation id, not the id of the request that placed the order; `OrderId` links the two.
- Migrations run at service start-up; with several replicas the race is only handled for the Order and Identity services.
- A database outage longer than the patient-retry window (about 7 minutes by default) sends the messages that were in flight to the `*_error` queues; requeue them once the database is back (pattern 14). While it lasts, every consumer slot retries in parallel (bounded by the pool size and the consumers' concurrency); there is no circuit breaker that pauses consumption.
- The gateway has no circuit breaker or passive health check: each cluster has a single destination, so there is nothing to fail over to (pattern 26). The retry budget and the timeouts bound the damage, but a dead service is still attempted on every request and answered with `502` after the connection fails. The Order service's `InventoryClient` (REST) still has its older, fixed Polly settings; it is registered but not used for the stock check.
- Redis calls have no explicit timeouts of their own: a Redis server that accepts connections and never answers stalls each cache lookup until the StackExchange.Redis defaults (5 s) apply.
- Identity has no self-service "forgot password" reset (it needs an e-mail service, which does not exist yet) and no e-mail verification: customers can sign up and change a password they know, administrators can manage roles, disable accounts and hand out a one-time temporary password, but a user cannot recover a forgotten password on their own, and a temporary password does not expire by itself. The first administrator comes from the configuration.
- Swagger UIs and Prometheus metrics ports are unauthenticated.
- Traffic between the services is plain HTTP inside the network, so bearer tokens (a user's lasts one hour, a service's 15 minutes) cross it in the clear. Network policies limit who can reach whom; TLS on every service or a mesh with mutual TLS is needed to protect the tokens in transit.
- The gateway forwards one access token that names every API in its audience, so a service that receives it could replay it against another API. Scopes and roles still limit what it can do there, but only separate tokens per API (token exchange) would close that.

---

## License

MIT
