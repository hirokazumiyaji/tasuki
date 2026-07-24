# M5 PostgreSQL Task Notify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Wake postgres Workers via `LISTEN`/`NOTIFY` when tasks become claimable, keeping `PollInterval` as fallback.

**Architecture:** Optional `backend.TaskNotifier`; only `backend/postgres` implements it with a dedicated LISTEN connection. Worker type-asserts and selects on notify channel **or** ticker. `pg_notify('tasuki_tasks','')` after enqueue/ensure/release paths.

**Tech Stack:** Go, pgx/pgxpool, existing Worker + postgres backend, `cmd/bench` for manual A/B.

**Spec:** [docs/superpowers/specs/2026-07-24-m5-postgres-notify-design.md](../specs/2026-07-24-m5-postgres-notify-design.md)

## Global Constraints

- Notifications are hints only; correctness stays in Claim/leases
- Do not add `Subscribe` to required `Backend` interface
- Channel name: `tasuki_tasks`
- No RetryActivity future-visible notify; ticker covers timers/schedules
- Other backends untouched except Worker type assert
- Every commit / merge subject includes `[skip ci]`

---

## File structure

| Path | Responsibility |
|---|---|
| `backend/notifier.go` | `TaskNotifier` interface |
| `backend/postgres/notify.go` | `Subscribe`, LISTEN loop, reconnect, `notifyTasks` helper |
| `backend/postgres/backend.go` / `schedule.go` | Call `notifyTasks` at emit sites |
| `backend/postgres/notify_test.go` | Subscribe + CreateInstance wake test |
| `worker.go` | Select on notify **or** ticker |
| `README.md` | One-line postgres notify note |
| `.claude/tasks/todo.md` | M5 notify checklist |

---

### Task 1: Spec + Plan

**Files:**
- Create: `docs/superpowers/specs/2026-07-24-m5-postgres-notify-design.md`
- Create: `docs/superpowers/plans/2026-07-24-m5-postgres-notify.md` (this file)
- Modify: `.claude/tasks/todo.md`

- [ ] Reset todo for M5 postgres notify Tasks 1–5
- [ ] Commit + PR `m5/task-1-postgres-notify-spec` `[skip ci]`; merge

---

### Task 2: `TaskNotifier` + postgres `Subscribe` + CreateInstance notify

**Files:**
- Create: `backend/notifier.go`, `backend/postgres/notify.go`, `backend/postgres/notify_test.go`
- Modify: `backend/postgres/backend.go` (`CreateInstance` commit path), optionally `postgres.go` if Close needs to track listeners (prefer cancel via Subscribe ctx only)

**Interfaces:**
- Produces:
  - `type TaskNotifier interface { Subscribe(ctx context.Context) (<-chan struct{}, error) }`
  - `func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error)`
  - `func (b *Backend) notifyTasks(ctx context.Context)` — best-effort `SELECT pg_notify('tasuki_tasks','')` on pool

- [ ] **Step 1: Add interface** `backend/notifier.go`:

```go
package backend

import "context"

// TaskNotifier is an optional Backend capability for wake hints.
type TaskNotifier interface {
	Subscribe(ctx context.Context) (<-chan struct{}, error)
}
```

- [ ] **Step 2: Failing test** `backend/postgres/notify_test.go`:

```go
func TestSubscribe_WakesOnCreateInstance(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	// Migrate, Reset
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := b.Subscribe(subCtx)
	// CreateInstance with unique id
	select {
	case <-ch:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("expected notify")
	}
}
```

- [ ] **Step 3: Implement `notify.go`**

Pattern:

1. `notifyTasks(ctx)`: `b.pool.Exec(ctx, \`SELECT pg_notify('tasuki_tasks', '')\`)` ignore error (log optional).
2. `Subscribe`:
   - `ch := make(chan struct{}, 1)`
   - goroutine: connect via `pgx.ConnectConfig` using `b.pool.Config().ConnConfig.Copy()` (set unique application_name if useful)
   - `LISTEN tasuki_tasks`
   - loop `WaitForNotification(subCtx)`; on success non-blocking send to `ch`
   - on error: backoff sleep, reconnect while `subCtx` alive
   - on `subCtx.Done`: close conn, return (do not close `ch` from multiple goroutines unsafely — leave open or close once with sync.Once)
3. After successful `CreateInstance` `tx.Commit`, call `b.notifyTasks(ctx)`.

- [ ] **Step 4: Run test**

```bash
cd backend/postgres && go test -count=1 -run TestSubscribe_WakesOnCreateInstance -v
```

Expected: PASS (needs `TASUKI_POSTGRES_DSN`)

- [ ] **Step 5: Commit + PR** `m5/task-2-postgres-subscribe` `[skip ci]`; merge

---

### Task 3: Wire remaining notify call sites

**Files:** `backend/postgres/backend.go`, `backend/postgres/schedule.go`

**Interfaces:** Consumes `notifyTasks`

Call `b.notifyTasks(ctx)` (or `tx` then post-commit) after successful commit when:

- CommitAdvancement inserted activity/child/ensure WF tasks (simplest: **always** notify after successful CommitAdvancement — slightly noisier, fewer misses)
- CompleteActivity success
- SendToInbox success (after ensure)
- FireDueTimers if fired count > 0 **or** always after function returns with work
- ClaimDueSchedules if any started
- ReleaseLease success

Prefer: after `tx.Commit` / successful Exec, one `notifyTasks` call.

Skip: RetryActivity, ExtendLease.

- [ ] Grep all `INSERT INTO wf_tasks` and ensure paths; add notify
- [ ] Re-run `TestSubscribe_WakesOnCreateInstance` + quick smoke Start path still notifies
- [ ] Commit + PR `m5/task-3-postgres-notify-sites` `[skip ci]`; merge

---

### Task 4: Worker loop integration

**Files:** `worker.go` (and a small worker test with a fake notifier if easy)

**Interfaces:** Consumes `backend.TaskNotifier`

- [ ] Change `loop` to:

```go
func (w *Worker) loop(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.opts.PollInterval)
	defer ticker.Stop()

	var wake <-chan struct{}
	if n, ok := w.backend.(backend.TaskNotifier); ok {
		ch, err := n.Subscribe(ctx)
		if err != nil {
			w.opts.Logger.Warn("task notify subscribe failed", "err", err)
		} else {
			wake = ch
		}
	}

	for {
		w.tick(ctx)
		if wake == nil {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
	}
}
```

- [ ] Run memory tests (ticker-only path):

```bash
go test ./ -count=1 -run 'TestWorker|TestClient' 
# or targeted existing tests that use memory
go test ./bench/ -count=1
```

- [ ] Optional: fake `TaskNotifier` in a unit test that blocks ticker for long poll and asserts faster completion — nice-to-have, not required if postgres integration test + manual bench suffice

- [ ] Commit + PR `m5/task-4-worker-notify` `[skip ci]`; merge

---

### Task 5: Conform / chaos / README / todo

**Files:** `README.md`, `.claude/tasks/todo.md`

- [ ] `cd backend/postgres && go test ./... -count=1` (conform + notify)
- [ ] Chaos postgres if DSN available: `go test ./chaos/ -count=1 -run TestChaos_KillWorkers` (or existing name)
- [ ] README status/bench area: note that postgres Workers LISTEN on `tasuki_tasks`; `PollInterval` remains fallback and timer cadence
- [ ] Manual bench note in PR body:

```bash
go run ./cmd/bench -backend=postgres -poll=1s -instances=50 -workers=2
```

Expect higher throughput vs pre-notify baseline with same flags.

- [ ] Mark all todos done; commit + PR `m5/task-5-postgres-notify-docs` `[skip ci]`; merge

---

## Acceptance checklist

- [ ] `backend.TaskNotifier` exists; postgres implements it
- [ ] NOTIFY on CreateInstance + other enqueue/ensure/release paths
- [ ] Worker wakes on notify or ticker
- [ ] `TestSubscribe_WakesOnCreateInstance` green
- [ ] postgres conform (+ chaos) green
- [ ] README documents behavior
- [ ] Other backends unchanged (no new required methods)

## Spec coverage

| Spec item | Task |
|---|---|
| TaskNotifier interface | 2 |
| Subscribe + LISTEN reconnect | 2 |
| CreateInstance notify | 2 |
| Other emit sites | 3 |
| Worker select loop | 4 |
| Conform/chaos/README/bench note | 5 |
| Non-goals (Result, other stores, triggers) | omitted |
