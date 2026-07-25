# M5 Memory Task / Terminal Notify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Wake Workers and `Client.Result` on `backend/memory` via in-process `TaskNotifier` / `TerminalNotifier`, keeping poll intervals as fallback.

**Architecture:** `memory.Backend` keeps subscriber lists; `Subscribe` / `SubscribeTerminal` register buffered channels (size 1); helpers `notifyTasks` / `notifyTerminal` fan-out with coalesce after mutations that mirror postgres emit sites. Lock: snapshot subscribers under mutex, send outside store lock.

**Tech Stack:** Go stdlib only; existing `backend.TaskNotifier` / `TerminalNotifier`; Worker/Client already type-assert.

**Spec:** [docs/superpowers/specs/2026-07-25-m5-memory-notify-design.md](../specs/2026-07-25-m5-memory-notify-design.md)

## Global Constraints

- Notifications are hints only; correctness stays in Claim / GetInstance
- Do not add Subscribe to required `Backend` interface
- Memory only; other backends untouched
- No notify on `RetryActivity` future `visible_at` or `ExtendLease`
- Every commit / merge subject includes `[skip ci]`

---

## File structure

| Path | Responsibility |
|---|---|
| `docs/superpowers/specs/2026-07-25-m5-memory-notify-design.md` | Approved design |
| `docs/superpowers/plans/2026-07-25-m5-memory-notify.md` | This plan |
| `backend/memory/notify.go` | Subscribe*, subscriber lists, notify helpers |
| `backend/memory/memory.go` | Call notify at emit sites; struct fields if needed |
| `backend/memory/schedule.go` | `ClaimDueSchedules` → notifyTasks when instance created |
| `backend/memory/notify_test.go` | Wake / terminal / cancel tests |
| `README.md` | Mention memory in-process notify |
| `.claude/tasks/todo.md` | Checklist |

---

### Task 1: Spec + Plan

**Files:**
- Create: `docs/superpowers/specs/2026-07-25-m5-memory-notify-design.md`
- Create: `docs/superpowers/plans/2026-07-25-m5-memory-notify.md` (this file)
- Modify: `.claude/tasks/todo.md`

- [ ] **Step 1: Reset todo**

```markdown
# tasuki M5 Memory Notify

## タスク

- [x] Task 1: Spec + Plan
- [ ] Task 2: Subscribe + CreateInstance / Terminate wake
- [ ] Task 3: Remaining emit sites + README
```

- [ ] **Step 2: Commit + PR**

Branch: `m5/task-1-memory-notify-docs`

```bash
git add docs/superpowers/specs/2026-07-25-m5-memory-notify-design.md \
  docs/superpowers/plans/2026-07-25-m5-memory-notify.md \
  .claude/tasks/todo.md
git commit -m "$(cat <<'EOF'
Add memory notify design and plan. [skip ci]

EOF
)"
git push -u origin HEAD
gh pr create --title "M5 Task 1: memory notify spec + plan" --body "$(cat <<'EOF'
## Summary
- Design and plan for in-process TaskNotifier / TerminalNotifier on memory backend

## Test plan
- [ ] Docs-only review

EOF
)"
gh pr merge --merge --subject "Merge pull request #N from m5/task-1-memory-notify-docs [skip ci]"
git checkout main && git pull
```

---

### Task 2: `Subscribe` / `SubscribeTerminal` + CreateInstance / Terminate

**Files:**
- Create: `backend/memory/notify.go`, `backend/memory/notify_test.go`
- Modify: `backend/memory/memory.go` (`Backend` fields; `CreateInstance` / `TerminateInstance` call helpers)

**Interfaces:**
- Consumes: `backend.TaskNotifier`, `backend.TerminalNotifier`
- Produces:
  - `func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error)`
  - `func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error)`
  - `func (b *Backend) notifyTasks()`
  - `func (b *Backend) notifyTerminal(instanceID string)`

- [ ] **Step 1: Failing tests** `backend/memory/notify_test.go`:

```go
package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

func TestSubscribe_WakesOnCreateInstance(t *testing.T) {
	b := memory.New()
	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := b.Subscribe(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(context.Background(), backend.NewInstance{
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
	b := memory.New()
	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := b.SubscribeTerminal(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	const id = "notify-term-1"
	if err := b.CreateInstance(context.Background(), backend.NewInstance{
		ID: id, Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(context.Background(), id); err != nil {
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
	b := memory.New()
	subCtx, cancel := context.WithCancel(context.Background())
	ch, err := b.Subscribe(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	// Allow unregister goroutine to run.
	time.Sleep(20 * time.Millisecond)
	if err := b.CreateInstance(context.Background(), backend.NewInstance{
		ID: "notify-cancel-1", Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("did not expect wake after cancel")
		}
	case <-time.After(50 * time.Millisecond):
		// closed or silent both OK; must not deliver a value
	}
}
```

- [ ] **Step 2: Run tests — expect FAIL** (Subscribe undefined)

```bash
go test ./backend/memory/ -count=1 -run 'TestSubscribe' -v
```

- [ ] **Step 3: Implement `backend/memory/notify.go`**

```go
package memory

import "context"

type taskSub struct {
	ch chan struct{}
}

type terminalSub struct {
	ch chan string
}

func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)
	sub := &taskSub{ch: ch}
	b.notifyMu.Lock()
	b.taskSubs = append(b.taskSubs, sub)
	b.notifyMu.Unlock()
	go func() {
		<-ctx.Done()
		b.removeTaskSub(sub)
		close(ch)
	}()
	return ch, nil
}

func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error) {
	ch := make(chan string, 1)
	sub := &terminalSub{ch: ch}
	b.notifyMu.Lock()
	b.terminalSubs = append(b.terminalSubs, sub)
	b.notifyMu.Unlock()
	go func() {
		<-ctx.Done()
		b.removeTerminalSub(sub)
		close(ch)
	}()
	return ch, nil
}

func (b *Backend) removeTaskSub(sub *taskSub) {
	b.notifyMu.Lock()
	defer b.notifyMu.Unlock()
	out := b.taskSubs[:0]
	for _, s := range b.taskSubs {
		if s != sub {
			out = append(out, s)
		}
	}
	b.taskSubs = out
}

func (b *Backend) removeTerminalSub(sub *terminalSub) {
	b.notifyMu.Lock()
	defer b.notifyMu.Unlock()
	out := b.terminalSubs[:0]
	for _, s := range b.terminalSubs {
		if s != sub {
			out = append(out, s)
		}
	}
	b.terminalSubs = out
}

func (b *Backend) notifyTasks() {
	b.notifyMu.Lock()
	subs := append([]*taskSub(nil), b.taskSubs...)
	b.notifyMu.Unlock()
	for _, s := range subs {
		select {
		case s.ch <- struct{}{}:
		default:
		}
	}
}

func (b *Backend) notifyTerminal(instanceID string) {
	b.notifyMu.Lock()
	subs := append([]*terminalSub(nil), b.terminalSubs...)
	b.notifyMu.Unlock()
	for _, s := range subs {
		select {
		case s.ch <- instanceID:
		default:
		}
	}
}
```

- [ ] **Step 4: Add fields on `Backend` in `memory.go`**

```go
type Backend struct {
	mu sync.Mutex
	// ... existing fields ...

	notifyMu     sync.Mutex
	taskSubs     []*taskSub
	terminalSubs []*terminalSub
}
```

- [ ] **Step 5: Wire CreateInstance / TerminateInstance**

In `CreateInstance`, after successful create (before return nil), call `b.notifyTasks()` **outside** `b.mu` if currently locked — pattern:

```go
func (b *Backend) CreateInstance(_ context.Context, inst backend.NewInstance) error {
	b.mu.Lock()
	err := b.createInstanceLocked(inst)
	b.mu.Unlock()
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}
```

In `TerminateInstance`, after successful status change unlock then `b.notifyTerminal(id)`.

Note: `createInstanceLocked` is also used from `CommitAdvancements` (children) and `ClaimDueSchedules` — those paths must call `notifyTasks` at the outer API level (Task 3), not inside `createInstanceLocked`, to avoid nested notify under `b.mu` and double-notify from CreateInstance. **Do not** call notify inside `createInstanceLocked`.

- [ ] **Step 6: Run tests — expect PASS**

```bash
go test ./backend/memory/ -count=1 -run 'TestSubscribe' -v
```

- [ ] **Step 7: Commit + PR** `m5/task-2-memory-notify-subscribe`

```bash
git commit -m "$(cat <<'EOF'
Add in-process TaskNotifier and TerminalNotifier to memory. [skip ci]

EOF
)"
```

---

### Task 3: Remaining emit sites + terminal commit + README

**Files:**
- Modify: `backend/memory/memory.go` (`CommitAdvancements`, `CompleteActivity`, `SendToInbox`, `FireDueTimers`, `ReleaseLease`)
- Modify: `backend/memory/schedule.go` (`ClaimDueSchedules`)
- Modify: `backend/memory/notify_test.go` (terminal commit test)
- Modify: `README.md`
- Modify: `.claude/tasks/todo.md`

**Interfaces:**
- Consumes: helpers from Task 2
- Produces: notify calls on all spec emit sites

- [ ] **Step 1: Add test** for terminal commit:

```go
func TestSubscribeTerminal_WakesOnCommitTerminal(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
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

(import `journal` as needed)

- [ ] **Step 2: Wire emit sites** (always notify **after** unlocking `b.mu`)

| Method | After success |
|---|---|
| `CommitAdvancements` | `notifyTasks()`; for each adv with `Terminal != nil`, `notifyTerminal(adv.InstanceID)` |
| `CompleteActivity` | if ensure ran / returned nil after enqueue → `notifyTasks()` (including late-complete no-op: **no** notify when instance not running and no enqueue — match postgres spirit: only when task may become claimable) |
| `SendToInbox` | after ensure → `notifyTasks()` |
| `FireDueTimers` | if fired count > 0 → `notifyTasks()` |
| `ReleaseLease` | after setting visible → `notifyTasks()` |
| `ClaimDueSchedules` | if any `Created` or any due processed that enqueued → `notifyTasks()` (simplest: if `len(out) > 0` notify) |
| `RetryActivity` | **no** |
| `ExtendLease` | **no** |

Pattern for locked methods:

```go
func (b *Backend) ReleaseLease(_ context.Context, taskID int64) error {
	b.mu.Lock()
	t, ok := b.tasks[taskID]
	if !ok {
		b.mu.Unlock()
		return backend.ErrNotFound
	}
	t.visibleAt = b.now
	t.workerID = ""
	b.mu.Unlock()
	b.notifyTasks()
	return nil
}
```

For `CommitAdvancements`:

```go
func (b *Backend) CommitAdvancements(_ context.Context, advs []backend.Advancement) error {
	if len(advs) == 0 {
		return nil
	}
	b.mu.Lock()
	for _, adv := range advs {
		if err := b.preflightAdvancementLocked(adv); err != nil {
			b.mu.Unlock()
			return err
		}
	}
	var terminals []string
	for _, adv := range advs {
		if err := b.commitAdvancementLocked(adv); err != nil {
			b.mu.Unlock()
			return err
		}
		if adv.Terminal != nil {
			terminals = append(terminals, adv.InstanceID)
		}
	}
	b.mu.Unlock()
	b.notifyTasks()
	for _, id := range terminals {
		b.notifyTerminal(id)
	}
	return nil
}
```

- [ ] **Step 3: README** — update lines about notify:

```markdown
PostgreSQL Worker は `LISTEN`/`NOTIFY`（チャネル `tasuki_tasks`）で起床し、`PollInterval` はフォールバックおよびタイマー／スケジュール用。memory は同一プロセス内の `TaskNotifier` で同様に起床する。
Client の `Result` は postgres（`tasuki_terminal`）および memory（プロセス内）で終端時に起床できる（未対応ストアは従来どおりポーリング）。
```

- [ ] **Step 4: Run tests**

```bash
go test ./backend/memory/ -count=1
go test ./ -count=1 -run 'Worker|Client' -timeout 60s
```

Expected: PASS

- [ ] **Step 5: Commit + PR** `m5/task-3-memory-notify-emit-sites`

```bash
git commit -m "$(cat <<'EOF'
Wire memory notify emit sites and document in-process wake. [skip ci]

EOF
)"
```

- [ ] Mark Task 3 done in `.claude/tasks/todo.md`

---

## Spec coverage (self-review)

| Spec item | Task |
|---|---|
| TaskNotifier + TerminalNotifier on memory | 2 |
| Coalesce buffer 1, cancel unregister | 2 |
| CreateInstance / Terminate notify | 2 |
| Commit / CompleteActivity / SendToInbox / FireDueTimers / ClaimDueSchedules / ReleaseLease | 3 |
| No RetryActivity / ExtendLease notify | 3 (explicit non-call) |
| Tests wake + terminal + cancel | 2, 3 |
| README | 3 |
| Other backends untouched | all |

## Placeholder scan

No TBD / “similar to Task N” without code. Emit-site table + concrete CommitAdvancements / ReleaseLease snippets included.
