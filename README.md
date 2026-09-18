# tasuki

English | [日本語](README.ja.md)

A durable workflow engine designed to be embedded directly into Go applications.  
The name comes from the Japanese *tasuki* (襷)—the sash passed from runner to runner in ekiden relay races. Just as runners hand off the tasuki to carry their team across long distances, workers hand over execution state to complete workflows across process restarts and crashes.

tasuki provides a Temporal-like developer experience—writing business workflows as standard code without manually building retries, timers, or state persistence—without requiring a dedicated server cluster.  
The engine runs as an in-process library within your application, and persistence co-locates directly on the application's existing data store. Zero additional infrastructure is needed.

Pluggable backend storage interface with PostgreSQL as the reference implementation, and support for MySQL / MariaDB, SQLite, Spanner, TiDB, DynamoDB, and Firestore.

## Status

- **M4 (Backend Expansion)** complete. PostgreSQL, SQLite, MySQL / MariaDB / TiDB, Spanner, DynamoDB, and Firestore are verified against compliance and chaos test suites.
- **M5 (Performance & Scaling)** and **M6 (Expressiveness & Operability)** are implemented, including Query, Signal deduplication, Worker incompatible Nack, SearchAttributes, Memo, LocalActivity, StartToClose Timeout, Synchronous Update, and SignalBatch.
- **Quality Sprint** completed (split CI matrix, test coverage reporting, test suite fill, and documentation freshness). See [docs/04-testing.md](docs/04-testing.md).
- **Benchmark Foundation**: `go run ./cmd/bench` (memory / postgres / sqlite).
- **Wakeup & Notification**: PostgreSQL Worker wakes on `LISTEN`/`NOTIFY` (channel `tasuki_tasks`), keeping `PollInterval` as fallback and cadence for timers/schedules. memory, sqlite, mysql, and spanner share in-process wakeups within the same `Backend` (`backend/hub`). DynamoDB uses `wf_wake` (Streams enabled) + wake item polling, and Firestore uses `wf_notify` document snapshots for cross-process wakeups (all notifications are hints; correctness is guaranteed by Claim / GetInstance).
- **Terminal Result Notification**: Client `Result` wakes up on workflow termination via postgres (`tasuki_terminal`), hub, or cross-process channels.
- **Sticky Journal Cache**: Worker uses an in-memory sticky journal cache per instance (matching `next_seq`, diff reads via `GetJournal`) to eliminate full history re-reads on replay.
- **Durable Actor Model**: tasuki models each workflow instance as a durable actor while retaining the deterministic journal replay execution model.
- **Journal Growth Warning**: Worker emits a Warn log and metric when journal event count reaches or exceeds `JournalWarnThreshold` (default 10,000; negative disables). Long-running workflows should reset history using `workflow.ContinueAsNew` ([docs/02-architecture.md](docs/02-architecture.md), [docs/03-api.md](docs/03-api.md)).
- **Activity Heartbeats**: Long-running activities can record progress and extend leases via `activity.RecordHeartbeat`, retrieved on retry with `GetHeartbeatDetails`.
- **Start-To-Close Timeout**: Activity single-attempt execution time limit is set via `workflow.WithStartToCloseTimeout` (exceeding it is treated as a regular failure eligible for retry).
- **Workflow Query**: Read-only queries to workflows are handled with `workflow.SetQueryHandler` and `tasuki.Query` (same Worker process).
- **Workflow Update**: Synchronous request-response updates to running workflows use `workflow.SetUpdateHandler` and `tasuki.Update` (supports optional `WithUpdateID` for idempotency).
- **Signal Deduplication**: `Client.Signal` supports `WithDedupeID` for instance-scoped idempotent resends.
- **Signal Batching**: Atomic bulk signals to a single instance via `Client.SignalBatch` (with optional per-item dedupe ID).
- **Incompatible Worker Nack**: During rolling deployments, if an older worker encounters an unrecognized journal event or unregistered workflow/activity, it Nacks the task so a newer worker can pick it up (`IncompatibleRetryDelay`).
- **Search Attributes**: String attributes attached via `WithSearchAttributes` (Start) and `workflow.UpsertSearchAttributes` (runtime) allow exact-match filtering in `Client.List`.
- **Memos**: Annotations attached via `WithMemo` / `workflow.UpsertMemo` provide visible metadata in `Get` (not filtered in `List`).
- **Local Activity**: Synchronous execution within the same Worker process via `workflow.ExecuteLocal` (no task queue, no retry; results recorded in journal).
- **Payload Encryption**: At-rest payload encryption via `codec.Encrypted` (AES-256-GCM with key rotation; configured on Worker `Codec` and Client `WithCodec`).

## Quickstart

PostgreSQL:

```bash
docker compose up -d postgres
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run ./examples/m1-postgres/
```

MySQL:

```bash
docker compose up -d mysql
export TASUKI_MYSQL_DSN='tasuki:tasuki@tcp(localhost:3306)/tasuki?parseTime=true&loc=UTC'
go run ./examples/m4-mysql/
```

TiDB (uses `backend/mysql`):

```bash
docker compose up -d tidb
export TASUKI_TIDB_DSN='root@tcp(127.0.0.1:4000)/tasuki?parseTime=true&loc=UTC'
export TASUKI_MYSQL_DSN="$TASUKI_TIDB_DSN"
go run ./examples/m4-mysql/
```

Spanner (Emulator):

```bash
docker compose up -d spanner
export SPANNER_EMULATOR_HOST=localhost:9010
export TASUKI_SPANNER_DSN=projects/tasuki/instances/tasuki/databases/tasuki
go run ./examples/m4-spanner/
```

DynamoDB (Local):

```bash
docker compose up -d dynamodb
export TASUKI_DYNAMODB_ENDPOINT=http://localhost:8000
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
go run ./examples/m4-dynamodb/
```

Firestore (Emulator):

```bash
docker compose up -d firestore
export FIRESTORE_EMULATOR_HOST=localhost:8086
export TASUKI_FIRESTORE_PROJECT=tasuki
go run ./examples/m4-firestore/
```

SQLite (no external infrastructure required):

```bash
go run ./examples/m3-sqlite/
```

Determinism Static Analysis:

```bash
go run ./analyzers/determinism/cmd/determinism -- ./...
```

Flags non-deterministic calls inside workflow functions (`*workflow.Context` receivers:
`go` statements; `time.Now/Since/Until/Sleep/After/AfterFunc/NewTimer/NewTicker/Tick`
(use `workflow.Now` for timestamps, `workflow.Sleep` for timers); `math/rand`,
`math/rand/v2`, `crypto/rand` (use `workflow.SideEffect` or `workflow.NewUUID`);
`os.Getenv/LookupEnv/Environ/Hostname/Getpid/Getppid/Getwd/Executable` and `os.Args`
(pass values via inputs or activities); `sync`, `sync/atomic`, `runtime`;
`net`, `net/http`, `os/exec` (do I/O in activities); channel operations (`select`,
send/receive, `make(chan ...)`, use `workflow.Execute`/`ExecuteAsync` and `workflow.Await`)
and `range` over maps (iteration order is random). Closures passed to
`workflow.SideEffect`/`NewUUID`/`SetQueryHandler`/`SetUpdateHandler` are excluded.
Helpers called from workflows are not analyzed; keep them deterministic.

## Web UI (contrib)

Instance list and journal viewer. The details page allows running instances to be Canceled, Terminated, or Signaled (Cancel and Terminate require confirmation checkboxes; Signal accepts name + JSON payload; all are CSRF-protected. Cancel is cooperative cancellation, Terminate is immediate termination).

By default, the UI listens only on loopback (`127.0.0.1:8080`) with defensive HTTP timeouts (ReadHeader 5s / Read 10s / Write 15s / Idle 60s). Exposing externally requires an authentication token. Unauthenticated external exposure requires explicit `--allow-unauthenticated-external` opt-in (dangerous):

```bash
go run ./contrib/ui/cmd/tasuki-ui -backend=memory -addr=127.0.0.1:8080
export TASUKI_UI_TOKEN='change-me'
go run ./contrib/ui/cmd/tasuki-ui -backend=memory -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"
# open http://127.0.0.1:8080

# External exposure (with token auth)
go run ./contrib/ui/cmd/tasuki-ui -backend=memory -addr=0.0.0.0:8080 -token="$TASUKI_UI_TOKEN"

# Shared deployment example (TLS termination + token auth): terminate TLS at a reverse proxy, run UI on loopback + token
# Caddy example:
# example.com {
#   reverse_proxy 127.0.0.1:8080
# }

docker compose up -d postgres
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=postgres -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export TASUKI_SQLITE_PATH=./tasuki.db
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=sqlite -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export TASUKI_MYSQL_DSN='tasuki:tasuki@tcp(localhost:3306)/tasuki?parseTime=true'
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=mysql -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export TASUKI_SPANNER_DSN='projects/p/instances/i/databases/d'
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=spanner -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export TASUKI_DYNAMODB_ENDPOINT=http://localhost:8000
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=dynamodb -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export FIRESTORE_EMULATOR_HOST=localhost:8081
export TASUKI_FIRESTORE_PROJECT=tasuki
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=firestore -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"
```

## Benchmarks

Measures end-to-end throughput (completed instances / sec from start of submission to full completion) and latency distributions (p50 / p95 / p99).  
By default, benchmarks do not delete existing data (only deleted with `--reset` + `TASUKI_ALLOW_RESET=1`). Repetitive runs on the same store are isolated via `--run-id`:

```bash
go run ./cmd/bench -backend=memory -instances=200 -workers=4
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -claim-limit=50 -activity-concurrency=8 -workflow-concurrency=8
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -scenario=mixed -run-id=try1
go run ./cmd/bench -backend=memory -instances=50 -workers=2 -scenario=long-history

# Destructive reset (verify target store first)
TASUKI_ALLOW_RESET=1 go run ./cmd/bench -backend=memory -instances=200 -workers=4 --reset

docker compose up -d postgres
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run -tags tasuki_all ./cmd/bench -backend=postgres -instances=200 -workers=4

export TASUKI_SQLITE_PATH=./bench.db
go run -tags tasuki_all ./cmd/bench -backend=sqlite -instances=200 -workers=4
```

- `WorkerOptions.ClaimLimit` (default 10) controls the number of tasks claimed per tick (via `-claim-limit` in bench).
- In all stores, multiple workflow advancements within a tick can be batched into `CommitAdvancements` (DynamoDB falls back sequentially when exceeding 100 TransactWrite items).
- `WorkerOptions.ActivityConcurrency` (default 1) controls concurrency for claimed activities (`-activity-concurrency` in bench).
- `WorkerOptions.WorkflowConcurrency` (default 1) controls concurrency for claimed workflows (the same instance is always serialized within a process; `-workflow-concurrency` in bench).

Baseline results and measurement notes: [docs/06-bench-baseline.md](docs/06-bench-baseline.md).

## Tests

```bash
go test ./... -race
cd backend/postgres && go test ./...
cd backend/mysql && go test ./...
cd backend/spanner && go test ./...
cd backend/dynamodb && go test ./...
cd backend/firestore && go test ./...
cd backend/sqlite && go test ./...
go test ./chaos/ -timeout 5m
```

Observability guide: [docs/05-observability.md](docs/05-observability.md)

## Design Documents

| Document | Description |
|---|---|
| [docs/01-overview.md](docs/01-overview.md) | Goals, requirements, non-goals, comparison with existing products, terminology |
| [docs/02-architecture.md](docs/02-architecture.md) | Execution model, exactly-once state transition protocol, data model, notifications & wakeups |
| [docs/03-api.md](docs/03-api.md) | Public API, code examples, determinism constraints, test support |
| [docs/04-testing.md](docs/04-testing.md) | Testing strategy, verification methodologies, risks and mitigations |
| [docs/05-observability.md](docs/05-observability.md) | Logging, OpenTelemetry metrics, troubleshooting guide |
| [docs/06-bench-baseline.md](docs/06-bench-baseline.md) | Benchmark baseline measurements and reproduction guide |
| [docs/07-retention.md](docs/07-retention.md) | Data retention and purging of completed instances |
| [docs/08-fair-dispatch.md](docs/08-fair-dispatch.md) | Fair dispatch and isolation of high-volume workflows |
| [docs/09-limits.md](docs/09-limits.md) | Backend atomicity budgets and limits |
| [docs/10-modules.md](docs/10-modules.md) | Go module layout, versioning, and publishing guidelines |
