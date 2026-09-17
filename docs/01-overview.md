# Overview and Requirements

[English] | [日本語](ja/01-overview.md)

This document defines the goals, requirements, and non-goals of the tasuki project.  
The architecture is defined in [02-architecture.md](02-architecture.md), public APIs in [03-api.md](03-api.md), and the development plan in [04-plan.md](04-plan.md).

## Goals

tasuki is an embedded **durable workflow engine** for Go applications (an execution foundation capable of continuing workflow execution seamlessly across process crashes and restarts).  
The name originates from the Japanese *tasuki* (襷)—the sash passed from runner to runner in ekiden long-distance relay races. Just as runners hand off the tasuki across legs of a race, workers hand over execution state to drive workflows to completion across restarts and process boundaries.

tasuki provides the developer experience pioneered by Temporal—authoring business logic as standard code without manually writing retries, timers, or state persistence—without operating a separate dedicated server cluster.

While Temporal is powerful, running it requires operating multiple separate services (frontend, history, matching, worker), their underlying database, and optionally Elasticsearch for visibility. For small teams or services, this operational complexity and infrastructure cost often outweigh the benefits of a workflow engine.

tasuki takes the inverted approach: the engine runs as an in-process library within the application, and persistence co-locates directly on the application's existing database. Zero additional infrastructure is required.

```go
// Minimal setup running with zero additional infrastructure
w := tasuki.NewWorker(postgres.NewBackend(pool), tasuki.WorkerOptions{})
tasuki.RegisterWorkflow(w, OrderWorkflow)
tasuki.RegisterActivity(w, ChargePayment)
w.Start(ctx)
```

## Problems to Solve

Server-centric workflow architectures introduce three major adoption barriers:

- **Infrastructure complexity**: Deploying, monitoring, and upgrading multiple services and dedicated databases.
- **Infrastructure cost**: Continuously running server clusters (or managed cloud fees) regardless of workload volume.
- **Developer ergonomics gap**: Running local servers or containers in local development and CI, raising testing friction.

In an embedded library format, these hurdles become: `go get`, pass the existing database connection, and use an in-memory backend for unit tests.

## Use Cases

The primary target use cases are multi-step operations that cannot be cleanly expressed with a basic single-shot job queue:

- Order fulfillment (payment processing, inventory allocation, shipment dispatch, compensation upon failure)
- External API integration with retry and timeout management (payment gateways, notification services)
- Long-delayed workflows (trial expiration follow-up after 14 days, approval wait states)
- Event-driven progression driven by external signals (e.g., user onboarding flows)
- Cron-like recurring schedules with multi-step executions per run

## Functional Requirements

Priorities are designated as **must** (essential for initial release), **should** (needed shortly after initial release), and **could** (future enhancement).

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1 | Define workflows as standard Go functions | must |
| FR-2 | Resume workflows from the point of interruption upon process restart | must |
| FR-3 | Automatically retry activities (side effects) with backoff and maximum attempts | must |
| FR-4 | Sleep for extended periods (days, months) using durable timers | must |
| FR-5 | Deduplicate workflow starts using workflow IDs (idempotent start) | must |
| FR-6 | Safely share execution across multiple application instances on the same database | must |
| FR-7 | Progress workflows via external signals | should |
| FR-8 | Execute activities concurrently and wait for results (Future / Select equivalent) | should |
| FR-9 | Spawn child workflows | should |
| FR-10 | Cancel or terminate workflows | should |
| FR-11 | Version workflows to support code modifications to running instances (GetVersion equivalent) | should |
| FR-12 | Define cron schedules for recurring executions | should |
| FR-13 | Reset history and continue execution via ContinueAsNew | should |
| FR-14 | Search and inspect execution history and instance state | should |
| FR-15 | View instance state via a Web UI | could |

## Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR-1 | Depend on only a single external data store (PostgreSQL reference implementation) |
| NFR-2 | Exactly-once workflow state transitions (crashes or dual execution do not corrupt history) |
| NFR-3 | At-least-once activity execution, clearly stated as an API and documentation contract |
| NFR-4 | Horizontally scale workers simply by adding application process instances |
| NFR-5 | Graceful shutdown (waiting for in-flight tasks and releasing leases early) |
| NFR-6 | Write unit tests without databases (in-memory backend) with virtual clock timer fast-forwarding |
| NFR-7 | Metrics hooks (OpenTelemetry) and structured logging (slog) |
| NFR-8 | Support Go 1.24+ with minimal dependencies. Separate database drivers/SDKs into dedicated backend Go modules |
| NFR-9 | Pluggable persistence via a backend interface, targeting RDBMS (PostgreSQL, MySQL, MariaDB, SQLite), NewSQL (Spanner, TiDB), and Document stores (DynamoDB, Firestore) |

## Non-Goals

The following areas are intentionally out of scope:

- Multi-language SDKs (Go-only)
- Multi-tenant namespaces, authentication, and authorization
- Dedicated search clusters like Elasticsearch (direct SQL / store queries suffice)
- Cross-region active replication (delegated to underlying database replication)
- General-purpose messaging or pub/sub event bus
- Temporal API compatibility or data migration tooling

## Comparison with Existing Products

| Product | Architecture | Additional Infra | Execution Model | Notes |
|---|---|---|---|---|
| Temporal / Cadence | Dedicated servers | 4 server services + DB (+ ES) | Event sourcing and replay | Richest feature set; high operational burden |
| go-workflows | Go library | DB (SQLite/MySQL/Redis) | Event sourcing and replay | Closest architectural precedent |
| durabletask-go | Go library | SQLite, etc. | Event sourcing and replay | Azure Durable Task Go implementation |
| DBOS Transact | Library | PostgreSQL | Step memoization & resident execution | Workflows remain resident as goroutines |
| Inngest / Hatchet / Restate | Server (single binary available) | Server + storage | Step execution | Self-hosting still requires running a server |
| River | Go library | PostgreSQL | Job queue | Not an orchestrator; reference for queueing patterns |

The feasibility of an embedded library workflow engine has been established by prior works such as go-workflows and durabletask-go. tasuki builds on these ideas with the following core design priorities:

- An explicitly specified, store-agnostic exactly-once state transition protocol, verified across all backends using a single compliance test suite (the central theme of [02-architecture.md](02-architecture.md))
- Fully type-safe APIs leveraging Go generics (no `interface{}` or `any` in public APIs)
- A minimal determinism constraint surface from day one (concurrency within workflows is restricted to engine primitives)
- First-class testing ergonomics (in-memory backend, virtual clock) built into the foundation

## Terminology

- **Workflow**: A deterministic Go function defining orchestration logic. Contains no side effects; only coordinates activity invocation order and branching.
- **Activity**: A Go function performing side effects (API calls, DB writes). Retried automatically under an at-least-once execution contract.
- **Instance**: A single execution of a workflow definition, uniquely identified by a workflow ID.
- **Journal**: An append-only sequence of execution history events per instance. Serves as replay input and the foundation of durability.
- **Replay**: Re-running a workflow function from the beginning and returning recorded events to reconstruct state up to the point of interruption.
- **Task**: The unit of execution claimed by workers. Divided into workflow tasks (advancing orchestration one turn) and activity tasks.
- **Worker**: An in-process component that polls for and executes tasks.
- **Backend**: The storage abstraction responsible for persistence and concurrency control.
- **Lease**: Time-bounded exclusivity granted to a worker on a claimed task. Expired tasks become reclaimable by other workers.
- **Signal**: An asynchronous message sent from external callers to a running workflow instance.
