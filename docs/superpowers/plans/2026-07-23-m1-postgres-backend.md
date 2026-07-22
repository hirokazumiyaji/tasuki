# M1 PostgreSQL バックエンドと実用最小 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do **not** use subagents (user preference: inline execution). One commit + one PR per task; merge to `main` before the next task.

**Goal:** 単一 PostgreSQL を共有する複数プロセスでクラッシュに耐えて動く最小構成を作り、バックエンド適合テストをインメモリと PostgreSQL の両方でグリーンにする。

**Architecture:** M0 のエンジン／Worker はそのまま使い、`backend.Backend` を契約として拡張する。適合テスト（`backendtest`）が契約を検証し、`backend/memory` と独立モジュール `backend/postgres`（pgx）が同じスイートを通過する。Client に `Result` / `Terminate`、Worker にリトライ・リース延長・graceful shutdown を足す。

**Tech Stack:** Go 1.24+、`jackc/pgx/v5`、Docker の PostgreSQL（テスト／CI）、標準 `testing` + `-race`。

**Spec:** [docs/04-plan.md](../../04-plan.md) M1、[docs/02-architecture.md](../../02-architecture.md)、[docs/03-api.md](../../03-api.md)

---

## File structure

| Path | Responsibility |
|---|---|
| `backend/backend.go` | Backend インターフェース拡張（Migrate, Terminate, ExtendLease, RetryActivity, GetJournal, ReleaseLease） |
| `backend/types.go` | 型の不足分（RetryPolicy on NewTask など） |
| `backend/memory/*.go` | 拡張メソッドのインメモリ実装 |
| `backendtest/*.go` | 適合テストスイート（両バックエンドで実行） |
| `backend/postgres/` | 独立 Go モジュール + スキーマ + Backend 実装 |
| `client.go` | `Handle.Result`, `Client.Terminate` |
| `worker.go` | リトライ／バックオフ、リース延長、shutdown 時リース解放 |
| `examples/m1-postgres/` | クイックスタート例 |
| `README.md` | クイックスタート |
| `.github/workflows/ci.yml` | `go test` + Postgres サービス |

---

### Task 1: Extend Backend interface + memory stubs

**Files:**
- Modify: `backend/backend.go`, `backend/types.go`, `backend/errors.go`, `backend/memory/memory.go`
- Create: `backend/memory/extra_test.go`

- [ ] **Step 1: Write failing tests for new methods**

```go
// backend/memory/extra_test.go
package memory_test

func TestMemory_TerminateIgnoresLateComplete(t *testing.T) {
	// Create instance, schedule activity via CommitAdvancement, Terminate,
	// then CompleteActivity must return ErrSuperseded or be ignored (no inbox / not running ensure).
}

func TestMemory_ExtendLeaseAndRetryActivity(t *testing.T) {
	// Claim activity, ExtendLease moves visible_at forward,
	// RetryActivity sets visible_at to future backoff time without completing.
}

func TestMemory_GetJournal(t *testing.T) {
	// After create, GetJournal returns workflow_started; afterSeq filters.
}
```

- [ ] **Step 2: Run to verify fail** — methods missing.

- [ ] **Step 3: Extend interface**

```go
type Backend interface {
	Migrate(ctx context.Context) error
	Capabilities() Capabilities

	CreateInstance(ctx context.Context, inst NewInstance) error
	GetInstance(ctx context.Context, id string) (*Instance, error)
	GetJournal(ctx context.Context, id string, afterSeq int64) ([]journal.Event, error)
	TerminateInstance(ctx context.Context, id string) error

	ClaimTasks(ctx context.Context, req ClaimRequest) ([]Task, error)
	ExtendLease(ctx context.Context, taskID int64, d time.Duration) error
	ReleaseLease(ctx context.Context, taskID int64) error // visible_at = now for graceful shutdown
	LoadWorkflow(ctx context.Context, instanceID string) (*WorkflowState, error)
	CommitAdvancement(ctx context.Context, adv Advancement) error
	CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error
	RetryActivity(ctx context.Context, taskID int64, visibleAt time.Time) error
	FireDueTimers(ctx context.Context, limit int) (int, error)
}
```

Add to `NewTask`: `MaxAttempts int` (0 = unlimited for M1 default later).

Implement on memory:
- `Migrate` no-op
- `TerminateInstance`: status=terminated, delete tasks+timers for instance
- `CompleteActivity`: if instance not running, delete task and return nil or ErrSuperseded without inbox (prefer ErrSuperseded if task gone; if task exists but not running, delete without inbox)
- `ExtendLease` / `ReleaseLease` / `RetryActivity` / `GetJournal`

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit + PR** `m1/task-1-backend-interface` → merge

---

### Task 2: Conformance suite — fencing, double-complete, terminate

**Files:**
- Create: `backendtest/suite.go`, `backendtest/suite_test.go` (memory driver)
- Modify: memory if gaps found

- [ ] **Step 1: Write suite**

```go
// backendtest/suite.go
package backendtest

type Factory func(t *testing.T) backend.Backend

func Run(t *testing.T, newBackend Factory) {
	t.Run("CreateDuplicate", func(t *testing.T) { ... })
	t.Run("CommitAdvancementConflict", func(t *testing.T) { ... }) // stale ExpectedSeq → ErrConflict
	t.Run("DoubleCompleteSuperseded", func(t *testing.T) { ... })
	t.Run("TerminateIgnoresLateComplete", func(t *testing.T) { ... })
	t.Run("FireTimerWakesWorkflow", func(t *testing.T) { ... })
}
```

Memory driver:

```go
// backend/memory/conform_test.go
func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) backend.Backend {
		b := memory.New()
		b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		return b
	})
}
```

For clock-dependent tests, define optional interface:

```go
type ClockSetter interface {
	SetNow(time.Time)
	Now() time.Time
}
```

Suite uses it when available; PostgreSQL uses `SELECT now()` (real time / short sleeps).

- [ ] **Step 2–4:** Implement suite + fix memory until green.

- [ ] **Step 5: PR** `m1/task-2-conformance-suite`

---

### Task 3: Conformance — claim exclusivity + wakeup race (I1)

**Files:**
- Create: `backendtest/concurrency.go`
- Modify: `backend/memory/memory.go` if needed for I1 semantics

- [ ] **Step 1: Tests**

```go
t.Run("ClaimExclusive", ...)
// Two goroutines ClaimTasks same kind/queue/limit=1; each task ID claimed at most once per lease window.

t.Run("WakeupRaceNoLostWakeup", ...)
// Goroutine A: CommitAdvancement deleting workflow task while inbox empty at read time.
// Goroutine B: CompleteActivity (ensure workflow task) concurrently.
// After both finish, if inbox non-empty and status=running, a workflow task must exist (I1).
```

Memory I1: when CompleteActivity runs, if workflow task is being "deleted" in another goroutine holding the mutex serially — for memory, serialize with mutex so I1 holds by holding lock across delete+ensure. For true race simulation on memory, use a hook/barrier:

```go
// Optional for tests only on memory:
type CommitHook interface {
	SetCommitHook(func())
}
```

Or: document that memory passes I1 via mutex atomicity; PostgreSQL test is the real proof. Suite still runs a concurrent CompleteActivity + CommitAdvancement stress loop (N=50) and asserts I1 afterwards.

- [ ] **Step 5: PR** `m1/task-3-conformance-concurrency`

---

### Task 4: PostgreSQL module scaffold + Migrate

**Files:**
- Create: `backend/postgres/go.mod`, `migrate.go`, `migrate.sql` (or embedded), `postgres.go`, `migrate_test.go`
- Create: `docker-compose.yml` (postgres:16)

- [ ] **Step 1: Failing migrate test** (skip if `TASUKI_POSTGRES_DSN` empty)

```go
func TestMigrate(t *testing.T) {
	dsn := os.Getenv("TASUKI_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TASUKI_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	...
	if err := b.Migrate(ctx); err != nil { t.Fatal(err) }
	if err := b.Migrate(ctx); err != nil { t.Fatal(err) } // idempotent
}
```

- [ ] **Step 2:** Start postgres:

```bash
docker compose up -d
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
```

- [ ] **Step 3: Implement** schema from 02-architecture (instances, journal, inbox, tasks, timers). Skip schedules table in M1 (M3). Embed SQL with `embed.FS`. Use `pgxpool`. Module:

```
module github.com/hirokazumiyaji/tasuki/backend/postgres
go 1.24
require (
  github.com/hirokazumiyaji/tasuki v0.0.0
  github.com/jackc/pgx/v5 v5.x
)
replace github.com/hirokazumiyaji/tasuki => ../..
```

- [ ] **Step 5: PR** `m1/task-4-postgres-migrate`

---

### Task 5: PostgreSQL Create/Load/Claim/Commit/Complete/Fire/Terminate

**Files:**
- Create: `backend/postgres/backend.go` (or split files)
- Create: `backend/postgres/conform_test.go`

- [ ] **Step 1:** Wire `backendtest.Run` for postgres (skip without DSN). Fail until methods implemented.

- [ ] **Step 2–4:** Implement all Backend methods with SQL matching architecture:

- `ClaimTasks`: `FOR UPDATE SKIP LOCKED` CTE
- `CommitAdvancement`: single Tx, CAS on `next_seq`, journal insert, effects, delete inbox/task, ensure workflow task `ON CONFLICT DO NOTHING`
- `CompleteActivity`: DELETE task; if 0 rows → ErrSuperseded; else inbox + ensure
- `FireDueTimers`: SKIP LOCKED delete timer + inbox + ensure
- `TerminateInstance`: status terminated; delete tasks/timers
- Payload columns: store activity name/input as jsonb `{"name","input"}` in task payload; journal payload as jsonb (use `json.RawMessage` / bytes)

Clock: use `SELECT now()` inside LoadWorkflow / Claim for authoritative time. Conformance tests that need timer fire use short durations (`50ms`) + `time.Sleep` instead of SetNow when ClockSetter absent.

- [ ] **Step 5: PR** `m1/task-5-postgres-backend`

---

### Task 6: Client Result + Terminate

**Files:**
- Modify: `client.go`
- Create: `client_test.go`

- [ ] **Step 1:**

```go
func TestHandle_Result(t *testing.T) {
	// memory backend + worker; Start; Result blocks until completed; returns typed output
}

func TestClient_Terminate(t *testing.T) {
	// Start long sleep workflow; Terminate; Get status terminated; Result returns error
}
```

- [ ] **Step 3:**

```go
func (h *Handle) Result(ctx context.Context) ([]byte, error) // or generic Result[O]
func Result[O any](ctx context.Context, h *Handle) (O, error)

func (c *Client) Terminate(ctx context.Context, id string) error
```

Poll interval default 200ms per 03-api. Terminal statuses: completed → unmarshal result; failed/canceled/terminated/stuck → error.

- [ ] **Step 5: PR** `m1/task-6-client-result-terminate`

---

### Task 7: Activity retry / backoff + lease extension

**Files:**
- Modify: `workflow/execute.go` or options for RetryPolicy; `worker.go`; `backend/types.go` (`RetryPolicy` on schedule)
- Create: `worker_retry_test.go`

- [ ] **Step 1:**

```go
func TestWorker_RetriesRetryableActivity(t *testing.T) {
	// Activity fails twice then succeeds; workflow gets success; journal has one activity_completed only
}

func TestWorker_NonRetryable(t *testing.T) {
	// tasuki.NonRetryable(err) → activity_failed immediately
}
```

- [ ] **Step 3:**
- Add `tasuki.NonRetryable(error) error` and `workflow.RetryPolicy` / `WithRetry` on Execute (M0 Execute takes name string — add optional opts or store policy in scheduled payload).
- Minimal: encode retry policy in activity_scheduled payload: `{"input":...,"retry":{"max_attempts":3,...}}`.
- Worker on activity error: if attempts < max → `RetryActivity` with backoff; else `CompleteActivity` failed.
- Lease extension: while activity runs longer than LeaseDuration/2, call `ExtendLease` on a ticker.

- [ ] **Step 5: PR** `m1/task-7-retry-lease`

---

### Task 8: Graceful shutdown releases leases

**Files:**
- Modify: `worker.go`
- Create: `worker_shutdown_test.go`

- [ ] **Step 1:** Claim task, start slow activity, Shutdown with timeout; assert task `visible_at <= now` so another worker can claim (ReleaseLease on in-flight task IDs tracked by worker).

- [ ] **Step 3:** Track `inFlight map[int64]struct{}`; on Shutdown cancel poll, wait in-flight with ctx, then `ReleaseLease` for any still held.

- [ ] **Step 5: PR** `m1/task-8-graceful-shutdown`

---

### Task 9: Chaos test (postgres)

**Files:**
- Create: `chaos/chaos_test.go` (build tag `chaos` or env-gated)
- Create: `chaos/worker_main` or use `go test` subprocess helper

- [ ] **Step 1:** Test starts N=3 worker processes (or goroutines with shared postgres — process kill is stronger). Prefer subprocess:

```go
// spawn `go run ./chaos/cmd/worker` with DSN; randomly kill -9; restart; 
// assert all instance IDs complete with expected result; journal seq contiguous no dup types at same seq
```

Practical M1 version (acceptable if documented):
- Multi-goroutine workers on one postgres with random `runtime.Goexit` / cancel mid-handle is weaker.
- Prefer: `t.Setenv` + exec worker binary; SIGKILL; verify invariants.

Assert:
- Every instance status completed
- Result equals expected
- Journal seqs are 1..n contiguous per instance

- [ ] **Step 5: PR** `m1/task-9-chaos`

---

### Task 10: README quickstart + example + CI

**Files:**
- Create: `examples/m1-postgres/main.go`
- Modify: `README.md`
- Create: `.github/workflows/ci.yml`
- Create: `docker-compose.yml` (if not in Task 4)

- [ ] **Step 1:** Document:

```bash
docker compose up -d
export TASUKI_POSTGRES_DSN=postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable
go run ./examples/m1-postgres/
go test ./... 
cd backend/postgres && go test ./...
```

CI: services postgres, run root tests + postgres module tests with DSN.

- [ ] **Step 5: PR** `m1/task-10-quickstart-ci`

---

## M1 acceptance checklist

| Criterion | Task |
|---|---|
| Conformance on memory | 2, 3 |
| Conformance on postgres | 5 |
| Wakeup race / fencing / double-complete / terminate | 2, 3, 5 |
| Chaos kill -9 | 9 |
| README quickstart works | 10 |

---

## Self-review

1. **Spec coverage:** M1 scope items mapped to tasks 1–10. Schedules/cron deferred to M3. Signals deferred to M2.
2. **Module split:** postgres is a separate module so root `go.mod` stays free of pgx.
3. **Clock:** memory uses SetNow; postgres tests use short real sleeps when ClockSetter unavailable.

## Execution notes

- Merge each PR to main before starting the next.
- TDD: red → green → refactor.
- No subagents.
- After Task 10: `go test ./... -race` and postgres suite with Docker.
