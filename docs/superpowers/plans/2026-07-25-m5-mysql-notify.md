# M5 MySQL Task / Terminal Notify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Wake Workers and `Client.Result` on `backend/mysql` via in-process `TaskNotifier` / `TerminalNotifier` when callers share one `*mysql.Backend`.

**Architecture:** Copy the memory/sqlite subscriber-list pattern into `mysql.Backend` (`notify.go` + fields). Call `notifyTasks` / `notifyTerminal` **after successful commit** (and after mysql’s post-commit ensure pass for `CommitAdvancements`). No shared hub; no cross-process wake.

**Tech Stack:** Go, `database/sql` + go-sql-driver/mysql, existing `backend.TaskNotifier` / `TerminalNotifier`.

**Spec:** [docs/superpowers/specs/2026-07-25-m5-mysql-notify-design.md](../specs/2026-07-25-m5-mysql-notify-design.md)

## Global Constraints

- Same `*mysql.Backend` instance only
- Notifications are hints; correctness stays in Claim / GetInstance
- Do not close subscribe channels on cancel
- No notify on `RetryActivity` / `ExtendLease`
- Tests skip without `TASUKI_MYSQL_DSN`
- Every commit / merge subject includes `[skip ci]`

---

## File structure

| Path | Responsibility |
|---|---|
| `docs/superpowers/specs/2026-07-25-m5-mysql-notify-design.md` | Approved design |
| `docs/superpowers/plans/2026-07-25-m5-mysql-notify.md` | This plan |
| `backend/mysql/notify.go` | Subscribe*, notify helpers |
| `backend/mysql/mysql.go` | `notifyMu`, subscriber fields |
| `backend/mysql/backend.go` | Emit-site calls after commit |
| `backend/mysql/schedule.go` | `ClaimDueSchedules` notify |
| `backend/mysql/notify_test.go` | Wake tests (`dsnOrSkip`) |
| `README.md` | Mention mysql in-process notify |
| `.claude/tasks/todo.md` | Checklist |

---

### Task 1: Spec + Plan

**Files:**
- Create: `docs/superpowers/specs/2026-07-25-m5-mysql-notify-design.md`
- Create: `docs/superpowers/plans/2026-07-25-m5-mysql-notify.md` (this file)
- Modify: `.claude/tasks/todo.md`

- [ ] **Step 1: Reset todo**

```markdown
# tasuki M5 MySQL Notify

## タスク

- [x] Task 1: Spec + Plan
- [ ] Task 2: Subscribe + CreateInstance / Terminate wake
- [ ] Task 3: Remaining emit sites + README
```

- [ ] **Step 2: Commit + PR** `m5/task-1-mysql-notify-docs`

```bash
git commit -m "$(cat <<'EOF'
Add mysql notify design and plan. [skip ci]

EOF
)"
```

---

### Task 2: `Subscribe` / `SubscribeTerminal` + CreateInstance / Terminate

**Files:**
- Create: `backend/mysql/notify.go`, `backend/mysql/notify_test.go`
- Modify: `backend/mysql/mysql.go`, `backend/mysql/backend.go`

**Interfaces:**
- Produces:
  - `func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error)`
  - `func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error)`
  - `func (b *Backend) notifyTasks()`
  - `func (b *Backend) notifyTerminal(instanceID string)`

- [ ] **Step 1: Failing tests** `backend/mysql/notify_test.go`:

```go
package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/mysql"
)

func openNotifyBackend(t *testing.T) *mysql.Backend {
	t.Helper()
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Skip(err.Error())
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
	case <-time.After(2 * time.Second):
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
	case <-time.After(2 * time.Second):
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

- [ ] **Step 2: Run — expect FAIL** (or Skip without DSN)

```bash
go test ./backend/mysql/ -count=1 -run 'TestSubscribe' -v
```

- [ ] **Step 3: Implement `backend/mysql/notify.go`**

Copy structure from `backend/sqlite/notify.go` (no channel close on cancel).

- [ ] **Step 4: Add fields on `Backend` in `mysql.go`**

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
func (b *Backend) CreateInstance(...) error {
	err := withTx(...)
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) TerminateInstance(...) error {
	// ... existing ...
	if err := commitConn(ctx, conn); err != nil {
		return err
	}
	b.notifyTerminal(id)
	return nil
}
```

- [ ] **Step 6: Run — expect PASS** (or Skip)

```bash
go test ./backend/mysql/ -count=1 -run 'TestSubscribe' -v
```

- [ ] **Step 7: Commit + PR** `m5/task-2-mysql-notify-subscribe`

```bash
git commit -m "$(cat <<'EOF'
Add in-process TaskNotifier and TerminalNotifier to mysql. [skip ci]

EOF
)"
```

---

### Task 3: Remaining emit sites + terminal commit + README

**Files:**
- Modify: `backend/mysql/backend.go`, `backend/mysql/schedule.go`, `backend/mysql/notify_test.go`, `README.md`, `.claude/tasks/todo.md`

- [ ] **Step 1: Add terminal commit test** (mirror sqlite; use `openNotifyBackend` + `journal`)

- [ ] **Step 2: Wire emit sites (after successful commit only)**

| Method | After success |
|---|---|
| `CommitAdvancements` | After main `withTx` **and** the existing post-commit ensure loop succeed → `notifyTasks()`; then for each `Terminal != nil`, `notifyTerminal(id)` |
| `CompleteActivity` | After `commitConn` on enqueue path → `notifyTasks()`; late-complete (not running) → no notify |
| `SendToInbox` | After all withTx succeed → `notifyTasks()` |
| `FireDueTimers` | After commit if `n > 0` → `notifyTasks()` |
| `ReleaseLease` | After rows affected > 0 → `notifyTasks()` |
| `ClaimDueSchedules` | After commit if `len(out) > 0` → `notifyTasks()` |
| `RetryActivity` / `ExtendLease` | **no** |

`CommitAdvancements` — notify **after** the second-pass ensure loop:

```go
err := withTx(...) // commit all advs
if err != nil {
	return err
}
for _, adv := range advs {
	if err := withTx(... ensure ...); err != nil {
		return err
	}
}
b.notifyTasks()
for _, adv := range advs {
	if adv.Terminal != nil {
		b.notifyTerminal(adv.InstanceID)
	}
}
return nil
```

- [ ] **Step 3: README**

```markdown
PostgreSQL Worker は `LISTEN`/`NOTIFY`（チャネル `tasuki_tasks`）で起床し、`PollInterval` はフォールバックおよびタイマー／スケジュール用。memory / sqlite / mysql は同一 `Backend` インスタンス内の `TaskNotifier` で同様に起床する。
Client の `Result` は postgres（`tasuki_terminal`）および memory / sqlite / mysql（プロセス内）で終端時に起床できる（未対応ストアは従来どおりポーリング）。
```

- [ ] **Step 4: Run tests**

```bash
go test ./backend/mysql/ -count=1 -run 'TestSubscribe|TestCommitAdvancements|TestMigrate' -timeout 60s
```

Expected: PASS or Skip without DSN; compile must succeed either way:

```bash
go test ./backend/mysql/ -count=1 -c -o /dev/null
```

- [ ] **Step 5: Commit + PR** `m5/task-3-mysql-notify-emit-sites`

```bash
git commit -m "$(cat <<'EOF'
Wire mysql notify emit sites and document in-process wake. [skip ci]

EOF
)"
```

---

## Spec coverage (self-review)

| Spec item | Task |
|---|---|
| TaskNotifier + TerminalNotifier on mysql | 2 |
| Coalesce; cancel unregister; no close | 2 |
| CreateInstance / Terminate | 2 |
| Remaining emit sites | 3 |
| Tests (skip without DSN) | 2, 3 |
| README | 3 |
| No hub | all |

## Placeholder scan

No TBD. CommitAdvancements second-pass ordering called out explicitly.
