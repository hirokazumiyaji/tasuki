# Development Plan

[English] | [日本語](ja/04-plan.md)

This document defines milestones, testing strategies, and risk mitigations for tasuki.  
Estimates represent single-engineer effort in weeks and serve as relative prioritization weights rather than strict deadlines.

## Methodology

- **Test-Driven Development**: Acceptance criteria for each milestone are expressed as automated tests. A milestone is complete when its test suite is green.
- **Protocol Verification First**: The core correctness invariants (state transition protocol) are codified into a shared backend compliance test suite, starting with in-memory and PostgreSQL, and applied to each subsequent backend.
- **Working Demos**: Each milestone culminates in a runnable example under `examples/` to prevent documentation examples from drifting out of sync.

## Milestones

### M0: Execution Model Validation (~1–2 weeks)

Validate the highest technical risk: ensuring the journal replay execution model in Go is ergonomic and practical.

Scope:
- Core journal and replay mechanics (command matching, determinism violation detection)
- `*workflow.Context`, `Execute`, `Sleep`, and goroutine suspension via `runtime.Goexit`
- In-memory backend and virtual clock (`wftest` prototype)

Acceptance Criteria:
- Multi-step workflows successfully resume from partial journals (durability unit tests)
- Determinism violations (mismatched command sequences from code edits) are quarantined as `stuck`
- User-space `defer` and `recover` do not intercept or corrupt suspension
- A 7-day sleep test finishes instantaneously via the virtual clock

### M1: PostgreSQL Backend & Minimal Viable Product (~2–3 weeks)

Build the minimal viable foundation capable of surviving crashes across multiple processes sharing a single database.

Scope:
- Schema and migrations, state transition transactions, and `SKIP LOCKED` task claims
- Automatic lease renewal, retries with exponential backoff, and timer execution
- Client APIs (`Start`, `Result`, `Get`, `Terminate`) and idempotent start by instance ID
- Graceful shutdown and shared backend compliance test suite

Acceptance Criteria:
- Compliance test suite passes against both in-memory and PostgreSQL implementations (including missed-wakeup races, fencing, duplicate completion, and ignored post-terminal completions)
- Chaos tests withstand repeated random `kill -9` signals across multiple worker processes, verifying all instances finish with correct terminal results and uncorrupted journals
- README quickstart runs out of the box

### M2: Expressiveness (~2–3 weeks)

Complete the core programming model defined in [03-api.md](03-api.md).

Scope:
- `ExecuteAsync`, `SleepAsync`, `Await`, `AwaitAll`
- Signals, child workflows, `SideEffect`, `NewUUID`, `Now`
- `GetVersion`, cooperative cancellation, `ContinueAsNew`, `NonRetryable`

Acceptance Criteria:
- All code examples in [03-api.md](03-api.md) compile and run in CI as documentation tests
- Compliance test suite expands to cover signal-versus-task races, child workflow completions, and cancellation compensation

### M3: Operability (~2–3 weeks)

Deliver operational tooling and developer ergonomics necessary for production readiness.

Scope:
- SQLite backend (for local development and single-process applications)
- Cron schedules
- OpenTelemetry metrics and structured logging (slog), with documented metric dictionaries
- Static determinism analyzer (detecting `time.Now`, `go` statements, `rand`)
- `List` and `GetJournal` for operational inspection; expanded `examples/`

Acceptance Criteria:
- SQLite implementation passes the backend compliance test suite
- Duplicate schedule triggering is mitigated by instance ID deduplication
- Static analyzer detects representative violations from the determinism checklist

### M4: Backend Expansion (Phased)

Expand store support by adapting the compliance test suite and chaos testing to new databases:

1. **MySQL / MariaDB**: Lock-based claim via `FOR UPDATE SKIP LOCKED`; singleton enforced via generated column and unique index
2. **TiDB**: Verified against MySQL backend compatibility (TSO clock)
3. **Spanner**: First conditional-update implementation without row locking
4. **DynamoDB**: First implementation using `Capabilities.MaxAdvancementEffects` (100-item transaction limit)
5. **Firestore**: Document-based transactions and snapshot listeners

Acceptance Criteria:
- Each backend passes the full compliance test suite and chaos testing
- CI runs against official emulators or local instances (Docker containers for MySQL, Spanner emulator, DynamoDB Local, Firestore emulator)

### M5: Performance & Scaling (Continuous)

Measurement-driven optimizations:

- Store notification mechanisms (PostgreSQL `LISTEN`/`NOTIFY`, DynamoDB / Firestore cross-process wakeups, in-process pub/sub hub) to eliminate polling latency *(Implemented)*
- Batch claiming and batch advancement (`CommitAdvancements`), in-memory sticky journal cache *(Implemented)*
- At-rest payload encryption codec, standalone web UI (`contrib/ui`) *(Implemented)*

### M6: Expressiveness & Operational Extensions (Shipped)

- Read-only queries (`workflow.SetQueryHandler`, `tasuki.Query`)
- Request-response synchronous updates (`workflow.SetUpdateHandler`, `tasuki.Update`)
- Signal deduplication (`WithDedupeID`) and batch signals (`SignalBatch`)
- Incompatible worker Nack during rolling deployments (`IncompatibleRetryDelay`)
- Search attributes (indexed for `List`) and memos (display metadata)
- In-process local activities (`ExecuteLocal`) and activity start-to-close timeouts (`WithStartToCloseTimeout`)

## Testing Strategy

| Layer | Focus |
|---|---|
| Unit | Golden replay tests (verifying identical command sequences are produced against recorded journals) |
| Compliance Suite | Verifies backend implementation contracts across all stores: mutual exclusion during claims, reclaim after lease expiration, absence of missed wakeups (I1), `next_seq` fencing, duplicate completion handling |
| Chaos E2E | Multiple concurrent workers subject to repeated random `kill -9`; verifies terminal invariants and absence of journal corruption |
| Continuous Verification | `-race` detector enabled across all tests; fuzz testing for codec parsing and synthetic replay inputs (`go test -fuzz=Fuzz ./codec ./internal/engine`) |
| CI Matrix | Containerized PostgreSQL, MySQL, Spanner emulator, DynamoDB Local, Firestore emulator, with SQLite and memory running without external dependencies |

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Determinism violations | Instance stuck in execution | Three-layer defense: structural prevention via `*workflow.Context`, static analyzer, and runtime detection with operational retry |
| Journal bloat | Replay latency and database storage pressure | Warning threshold (`JournalWarnThreshold`), `ContinueAsNew` guidelines, and in-memory sticky cache |
| Hot instances | Throughput bottleneck from turn serialization | Architectural guidelines for child workflows and key-based sharding; explicit non-goal for high-frequency stream aggregation |
| Database load on application DB | Performance interference with primary application | Independent schema/database configuration support; early visibility via `tasuki.tasks.backlog` metric |
| Differences in database semantics (e.g., `ON CONFLICT` waits vs optimistic aborts) | Missed wakeups or lost exclusivity | Formulate guarantees as formal invariants (I1) and verify all backends against the shared compliance suite |
| Low write batch limits (DynamoDB, Firestore) | Exceeding transaction operation caps | Declare limits via `Capabilities.MaxAdvancementEffects`; engine commits prefix commands and defers remaining fan-outs |
| Lack of authoritative database clock | Premature lease expirations or extra retries | Decouple safety from clock drift using CAS and delete exclusivity; allow configurable drift margins |
| Function rename incompatibilities | Running instances quarantined as stuck | Recommend and document explicit registration names (`tasuki.WithName`) |
| Payload schema evolution | Deserialization failures | Document additive-only schema evolution rules; support pluggable codecs |
| Duplicate activity execution side effects | External system inconsistency | Explicitly document at-least-once contract; provide stable `IdempotencyKey` |

## Next Actions

1. Continuous Quality Maintenance (maintaining CI matrix, coverage reporting, and documentation freshness)
2. Release Preparation (semantic version tagging, CHANGELOG generation, and public README polish)
