# M5 SQLite Task / Terminal Notify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Wake Workers and `Client.Result` on `backend/sqlite` via in-process `TaskNotifier` / `TerminalNotifier` when callers share one `*sqlite.Backend`.

**Architecture:** Copy the memory subscriber-list pattern into `sqlite.Backend` (`notify.go` + fields). Call `notifyTasks` / `notifyTerminal` **after successful commit** on the same emit sites as postgres/memory. No shared hub; no cross-process wake.

**Tech Stack:** Go, `database/sql` + modernc sqlite, existing `backend.TaskNotifier` / `TerminalNotifier`.

**Spec:** [docs/superpowers/specs/2026-07-25-m5-sqlite-notify-design.md](../specs/2026-07-25-m5-sqlite-notify-design.md)

## Global Constraints

- Same `*sqlite.Backend` instance only
- Notifications are hints; correctness stays in Claim / GetInstance
- Do not close subscribe channels on cancel (memory race avoidance)
- No notify on `RetryActivity` / `ExtendLease`
- Every commit / merge subject includes `[skip ci]`

---

## File structure

| Path | Responsibility |
|---|---|
| `docs/superpowers/specs/2026-07-25-m5-sqlite-notify-design.md` | Approved design |
| `docs/superpowers/plans/2026-07-25-m5-sqlite-notify.md` | This plan |
| `backend/sqlite/notify.go` | Subscribe*, notify helpers |
| `backend/sqlite/sqlite.go` | `notifyMu`, subscriber fields |
| `backend/sqlite/backend.go` | Emit-site calls after commit |
| `backend/sqlite/schedule.go` | `ClaimDueSchedules` notify |
| `backend/sqlite/notify_test.go` | Wake tests |
| `README.md` | Mention sqlite in-process notify |
| `.claude/tasks/todo.md` | Checklist |

---

### Task 1: Spec + Plan

**Files:**
- Create: `docs/superpowers/specs/2026-07-25-m5-sqlite-notify-design.md`
- Create: `docs/superpowers/plans/2026-07-25-m5-sqlite-notify.md` (this file)
- Modify: `.claude/tasks/todo.md`

- [ ] **Step 1: Reset todo**

```markdown
# tasuki M5 SQLite Notify

## タスク

- [x] Task 1: Spec + Plan
- [ ] Task 2: Subscribe + CreateInstance / Terminate wake
- [ ] Task 3: Remaining emit sites + README
```

- [ ] **Step 2: Commit + PR** `m5/task-1-sqlite-notify-docs`

```bash
git commit -m "$(cat <<'EOF'
Add sqlite notify design and plan. [skip ci]

EOF
)"
```

---

### Task 2: `Subscribe` / `SubscribeTerminal` + CreateInstance / Terminate

**Files:**
- Create: `backend/sqlite/notify.go`, `backend/sqlite/notify_test.go`
- Modify: `backend/sqlite/sqlite.go`, `backend/sqlite/backend.go` (`CreateInstance`, `TerminateInstance`)

**Interfaces:**
- Produces:
  - `func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error)`
  - `func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error)`
  - `func (b *Backend) notifyTasks()`
  - `func (b *Backend) notifyTerminal(instanceID string)`

- [ ] **Step 1: Failing tests** `backend/sqlite/notify_test.go`:

```go
package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

func openNotifyBackend(t *testing.T) *sqlite.Backend {
	t.Helper()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "notify.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSubscribe_WakesOnCreateInstance(t *testing.T) {
	ctx := context.Background()
	b := openNotifyBackend(t)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := b.Subscribe(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "notify-wake-1", Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected notify after CreateInstance")
	}
}

func TestSubscribeTerminal_WakesOnTerminate(t *testing.T) {
	ctx := context.Background()
	b := openNotifyBackend(t)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := b.SubscribeTerminal(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	const id = "notify-term-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: id, Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got != id {
			t.Fatalf("payload=%q want %q", got, id)
		}
	case <-time.After(time.Second):
		t.Fatal("expected terminal notify after TerminateInstance")
	}
}

func TestSubscribe_CancelStopsDelivery(t *testing.T) {
	ctx := context.Background()
	b := openNotifyBackend(t)
	subCtx, cancel := context.WithCancel(ctx)
	ch, err := b.Subscribe(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "notify-cancel-1", Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
		t.Fatal("did not expect wake after cancel")
	case <-time.After(50 * time.Millisecond):
	}
}
```

- [ ] **Step 2: Run — expect FAIL**

```bash
go test ./backend/sqlite/ -count=1 -run 'TestSubscribe' -v
```

- [ ] **Step 3: Implement `backend/sqlite/notify.go`**

Copy structure from `backend/memory/notify.go` (same package-local `taskSub` / `terminalSub`, no channel close on cancel).

- [ ] **Step 4: Add fields on `Backend` in `sqlite.go`**

```go
import "sync"

type Backend struct {
	db *sql.DB

	notifyMu     sync.Mutex
	taskSubs     []*taskSub
	terminalSubs []*terminalSub
}
```

- [ ] **Step 5: Wire CreateInstance / TerminateInstance**

```go
func (b *Backend) CreateInstance(ctx context.Context, inst backend.NewInstance) error {
	// ... existing withTx body unchanged ...
	err := withTx(ctx, b.db, func(conn *sql.Conn) error { ... })
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) TerminateInstance(ctx context.Context, id string) error {
	// ... existing beginImmediate / updates ...
	if err := commitConn(ctx, conn); err != nil {
		return err
	}
	b.notifyTerminal(id)
	return nil
}
```

- [ ] **Step 6: Run — expect PASS**

```bash
go test ./backend/sqlite/ -count=1 -run 'TestSubscribe' -v
```

- [ ] **Step 7: Commit + PR** `m5/task-2-sqlite-notify-subscribe`

```bash
git commit -m "$(cat <<'EOF'
Add in-process TaskNotifier and TerminalNotifier to sqlite. [skip ci]

EOF
)"
```

---

### Task 3: Remaining emit sites + terminal commit + README

**Files:**
- Modify: `backend/sqlite/backend.go`, `backend/sqlite/schedule.go`, `backend/sqlite/notify_test.go`, `README.md`, `.claude/tasks/todo.md`

- [ ] **Step 1: Add terminal commit test** (mirror memory; use `openNotifyBackend`)

```go
func TestSubscribeTerminal_WakesOnCommitTerminal(t *testing.T) {
	ctx := context.Background()
	b := openNotifyBackend(t)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := b.SubscribeTerminal(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	const id = "notify-term-commit"
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: id, Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(tasks))
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{{
			Seq: st.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: []byte(`"ok"`),
		}},
		Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got != id {
			t.Fatalf("payload=%q want %q", got, id)
		}
	case <-time.After(time.Second):
		t.Fatal("expected terminal notify after CommitAdvancement")
	}
}
```

- [ ] **Step 2: Wire emit sites (after successful commit only)**

| Method | After success |
|---|---|
| `CommitAdvancements` | `notifyTasks()`; for each adv with `Terminal != nil`, `notifyTerminal(id)` |
| `CompleteActivity` | after `commitConn` succeeds → `notifyTasks()` (including late-complete path that commits without enqueue: still OK to notify, or only when status was running — prefer notify when commit succeeds after enqueue path; for late-complete `commitConn` without enqueue, **skip** notify by tracking a `wake bool`) |
| `SendToInbox` | after both withTx succeed → `notifyTasks()` if first tx enqueued / status running (simplest: always `notifyTasks` after successful SendToInbox — duplicates OK) |
| `FireDueTimers` | after commit if `n > 0` → `notifyTasks()` |
| `ReleaseLease` | after rows affected > 0 → `notifyTasks()` |
| `ClaimDueSchedules` | after commit if `len(out) > 0` (or claimed non-empty) → `notifyTasks()` |
| `RetryActivity` / `ExtendLease` | **no** |

`CommitAdvancements` pattern:

```go
func (b *Backend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	if len(advs) == 0 {
		return nil
	}
	err := withTx(ctx, b.db, func(conn *sql.Conn) error {
		for _, adv := range advs {
			if err := b.commitAdvancementConn(ctx, conn, adv); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	b.notifyTasks()
	for _, adv := range advs {
		if adv.Terminal != nil {
			b.notifyTerminal(adv.InstanceID)
		}
	}
	return nil
}
```

`CompleteActivity` pattern — set `wake := status == "running"` before enqueue; after successful `commitConn`, if `wake { b.notifyTasks() }`.

`FireDueTimers`:

```go
if err := commitConn(ctx, conn); err != nil {
	return 0, err
}
if n > 0 {
	b.notifyTasks()
}
return n, nil
```

`ReleaseLease`:

```go
if n == 0 {
	return backend.ErrNotFound
}
b.notifyTasks()
return nil
```

`ClaimDueSchedules`: after successful `commitConn`, if `len(out) > 0` (or claimed was non-empty before empty early return) call `notifyTasks()`. Empty due path that only commits: no notify.

`SendToInbox`:

```go
if err != nil {
	return err
}
if err := withTx(... ensure ...); err != nil {
	return err
}
b.notifyTasks()
return nil
```

- [ ] **Step 3: README** — update notify lines:

```markdown
PostgreSQL Worker は `LISTEN`/`NOTIFY`（チャネル `tasuki_tasks`）で起床し、`PollInterval` はフォールバックおよびタイマー／スケジュール用。memory / sqlite は同一 `Backend` インスタンス内の `TaskNotifier` で同様に起床する。
Client の `Result` は postgres（`tasuki_terminal`）および memory / sqlite（プロセス内）で終端時に起床できる（未対応ストアは従来どおりポーリング）。
```

- [ ] **Step 4: Run tests**

```bash
go test ./backend/sqlite/ -count=1
```

Expected: PASS

- [ ] **Step 5: Commit + PR** `m5/task-3-sqlite-notify-emit-sites`

```bash
git commit -m "$(cat <<'EOF'
Wire sqlite notify emit sites and document in-process wake. [skip ci]

EOF
)"
```

---

## Spec coverage (self-review)

| Spec item | Task |
|---|---|
| TaskNotifier + TerminalNotifier on sqlite | 2 |
| Coalesce buffer 1; cancel unregister; no close | 2 |
| CreateInstance / Terminate | 2 |
| Remaining emit sites | 3 |
| Tests | 2, 3 |
| README | 3 |
| No hub / no memory refactor | all |

## Placeholder scan

No TBD. Emit-site table and concrete Commit/Release/Fire snippets included.
