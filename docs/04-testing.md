# Testing Strategy and Risk Mitigations

[English] | [日本語](ja/04-testing.md)

This document defines the testing strategy, verification methodologies, and risk mitigations for tasuki.
As an embedded durable workflow engine, tasuki must guarantee exactly-once state transitions and data integrity even across process crashes, network partitions, and database concurrency races.

## Core Verification Principles

- **Protocol Verification First**: The core correctness invariants (state transition protocol, concurrency control, and lease mechanics) are codified into a shared backend compliance test suite. Every backend implementation is verified against this identical suite.
- **Chaos and Fault Injection**: Correctness is verified not only under clean execution but under hostile conditions, including unexpected process termination (`kill -9`) and database race conditions.
- **Runnable Documentation Tests**: To prevent documentation and example code from drifting out of sync with engine internals, example workflows and API snippets are compiled and executed as part of CI.

## Testing Strategy

| Layer | Focus | Scope & Methodology |
|---|---|---|
| **Unit & Replay** | Golden replay verification | Verifies that workflow functions produce identical command sequences when replayed against recorded journals. Ensures virtual clocks (`wftest`) accelerate timer tests instantaneously without real-time delays. |
| **Compliance Suite** | Backend implementation contracts | Verifies persistence contracts across all supported stores: mutual exclusion during claims, reclaim after lease expiration, absence of missed wakeups (I1), `next_seq` fencing, duplicate completion handling, signal-task races, and capability limits. |
| **Chaos E2E** | Crash recovery & durability | Subject concurrent worker processes to repeated random `kill -9` signals under heavy workflow loads. Verifies that all instances conclude with terminal consistency and uncorrupted journals. |
| **Continuous Verification** | Race detection & fuzzing | All test runs execute with the Go race detector (`-race`). Continuous fuzz testing exercises payload codecs and synthetic replay inputs (`go test -fuzz=Fuzz ./codec ./internal/engine`). |
| **CI Matrix** | Multi-environment validation | Automated CI runs matrix tests across PostgreSQL, MySQL / MariaDB, Spanner (emulator), DynamoDB (DynamoDB Local), Firestore (emulator), and zero-dependency backends (SQLite, In-Memory). |

### Backend Compliance Test Suite

The compliance test suite (`backendtest`) defines a unified specification that every persistence backend must satisfy. Any newly implemented backend is required to pass this suite without modification.

Key verified invariants include:
1. **Claim Exclusivity**: Leased tasks cannot be claimed by concurrent workers until the lease duration expires.
2. **Missed-Wakeup Prevention (I1)**: Concurrent execution of task deletion and inbox enqueueing never drops or indefinitely delays task execution.
3. **Optimistic Fencing (`next_seq` CAS)**: Advancements attempted against stale journal sequences fail with conflict errors.
4. **Duplicate Completion Protection**: Duplicate activity completions or completions arriving after workflow termination are safely ignored or rejected.
5. **Capabilities Budgeting**: On stores declaring `Capabilities.MaxAdvancementEffects`, fan-out commands exceeding limits are safely chunked across multiple turns.

### Chaos and Fault Resilience

Chaos tests simulate hostile real-world failures by executing distributed workflows across multiple OS processes while injecting non-graceful crashes:
- Workers are killed abruptly using `SIGKILL` (`kill -9`) during active workflow advancement and activity execution.
- Replacement workers pick up unrenewed leases and resume workflows from the latest recorded journal state.
- Post-run invariants verify that no journal entries are duplicated, skipped, or corrupted, and final workflow results match expected golden values.

### Continuous Integration

CI (`.github/workflows/ci.yml`) runs on every pull request and push to `main`, plus a nightly schedule (`0 3 * * *` UTC, also triggerable via `workflow_dispatch`):

| Job | What it gates |
|---|---|
| `lint` | `staticcheck` is clean on the root and backend modules |
| `vuln` | Advisory `govulncheck` over root and backend modules (non-blocking: stdlib findings track the toolchain) |
| `root` | `go test ./... -race`; fuzz seed corpora run here as ordinary unit tests |
| `backend-*` | Per-backend suites with `-race` |
| `chaos-*` | `kill -9` chaos suites with `-race`, including the zero-infrastructure `chaos-sqlite` job |
| `cover` | Root coverage summary and artifact |
| `gowork-check` | `GOWORK=off` independence, the workspace `tasuki_all` build, and `go test -tags tasuki_all ./contrib/... ./examples/...` |
| `fuzz-nightly` | Nightly only: 60s of `-fuzz -fuzztime` per codec/engine fuzz target |

CI uses Go `1.27.x` across all jobs (see `docs/10-modules.md`).

## Risks and Mitigations

The table below outlines technical risks identified in durable workflow execution and the architectural defenses built into tasuki:

| Risk | Impact | Architectural Mitigation |
|---|---|---|
| **Determinism violations** | Workflow instance quarantined as `stuck` | Three-layer defense: structural prevention via `*workflow.Context`, static analyzer detecting non-deterministic calls (`time.Now`, `go`, `rand`), and runtime journal matching with operational retry. |
| **Journal bloat** | Replay latency and database storage pressure | Configurable warning threshold (`JournalWarnThreshold`), in-memory sticky journal cache, and architectural support for `workflow.ContinueAsNew`. |
| **Hot instances** | Throughput bottleneck from turn serialization | Instances process workflow tasks sequentially. Mitigated via child workflows (`ExecuteChild`), key-based sharding, and documented partitioning guidelines. |
| **Database load on primary application DB** | Performance interference with application tables | Support for separate database connections, schemas, or stores; early visibility via `tasuki.tasks.backlog` metric and autovacuum tuning guidelines. |
| **Database semantics variance** (e.g., lock waits vs CAS aborts) | Missed wakeups or lost exclusivity | Guarantees formulated as formal invariants (e.g., I1) and validated uniformly across all backends via the shared compliance suite. |
| **Low transaction write caps** (DynamoDB, Firestore) | Exceeding transaction batch operation limits | Declared via `Capabilities.MaxAdvancementEffects`; the engine commits prefix commands within budget and defers remaining fan-outs to subsequent turns. |
| **Lack of authoritative database clock** | Premature lease expirations or extra retries | Safety is decoupled from clock drift using CAS and atomic task deletion; drift margins are configurable. |
| **Function rename incompatibilities** | Running instances fail replay | Explicit registration names via `worker.WithName` are supported and recommended in documentation. |
| **Payload schema evolution** | Deserialization failures on old journal events | Additive-only schema evolution rules documented; pluggable codecs support custom versioning and encryption. |
| **Duplicate activity execution** | External side-effect inconsistency | Activity contracts mandate at-least-once execution; stable `IdempotencyKey` is provided to ensure deduplication at external endpoints. |
