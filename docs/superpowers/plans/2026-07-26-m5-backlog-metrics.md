# M5 Task Backlog Metrics Implementation Plan

> **For agentic workers:** Prefer **inline execution**. One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.

**Goal:** Add `CountClaimableTasks` to all backends and sample claimable backlog as an OTel Gauge from the Worker.

**Architecture:** Required Backend method returns per-queue counts matching Claim eligibility. Worker records `tasuki.tasks.backlog` once per poll tick when Metrics is set.

**Tech Stack:** Go, OpenTelemetry metric gauges, existing SQL/DDB/Firestore query patterns.

**Spec:** [docs/superpowers/specs/2026-07-26-m5-backlog-metrics-design.md](../specs/2026-07-26-m5-backlog-metrics-design.md)

---

### Task 1: Spec + Plan

- [x] Spec + this plan + `.claude/tasks/todo.md`
- Commit + PR `m5/task-1-backlog-metrics-docs`

### Task 2: Interface + memory + backendtest + metrics + worker

**Files:** `backend/backend.go`, `backend/memory/memory.go`, `backendtest/suite.go`, `observability/metrics.go`, `worker.go`, tests

- Add `CountClaimableTasks` to interface
- memory implementation
- backendtest case
- Gauge + `RecordBacklog`
- Worker sample in `tick`

### Task 3: SQL + Spanner

**Files:** `backend/sqlite`, `mysql`, `postgres`, `spanner`

### Task 4: DynamoDB + Firestore

**Files:** `backend/dynamodb`, `backend/firestore`

### Task 5: Observability docs

**Files:** `docs/05-observability.md`, `docs/04-plan.md`
