# M5 In-Process Notify Hub Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Extract in-process Task/Terminal wake fan-out into `backend/hub` and wire memory / sqlite / mysql / spanner / dynamodb / firestore to it without changing emit sites or postgres.

**Architecture:** `hub.Hub` owns subscriber lists + coalesce send. Each store holds `*hub.Hub`, delegates `Subscribe*`, and keeps `notifyTasks` / `notifyTerminal` as one-line wrappers.

**Tech Stack:** Go stdlib only for hub.

**Spec:** [docs/superpowers/specs/2026-07-26-m5-notify-hub-design.md](../specs/2026-07-26-m5-notify-hub-design.md)

## Global Constraints

- Semantics identical to current per-store notify.go
- Do not close channels on cancel
- postgres untouched
- Every commit / merge subject includes `[skip ci]`

---

## File structure

| Path | Responsibility |
|---|---|
| `backend/hub/hub.go` | Hub type + Subscribe/Notify |
| `backend/hub/hub_test.go` | Unit tests |
| `backend/{memory,sqlite,mysql,spanner,dynamodb,firestore}/notify.go` | Thin wrappers |
| `backend/*/New` constructors | `hub: hub.New()` |
| Remove old subscriber fields from each Backend struct |
| `README.md` | One-line hub note |
| `.claude/tasks/todo.md` | Checklist |

---

### Task 1: Spec + Plan

**Files:**
- Create: `docs/superpowers/specs/2026-07-26-m5-notify-hub-design.md`
- Create: `docs/superpowers/plans/2026-07-26-m5-notify-hub.md` (this file)
- Modify: `.claude/tasks/todo.md`

```markdown
# tasuki M5 Notify Hub

## タスク

- [x] Task 1: Spec + Plan
- [ ] Task 2: backend/hub + unit tests
- [ ] Task 3: Wire six stores + README
```

- [ ] Commit + PR `m5/task-1-notify-hub-docs`

```bash
git commit -m "$(cat <<'EOF'
Add notify hub design and plan. [skip ci]

EOF
)"
```

---

### Task 2: `backend/hub` + unit tests

**Files:**
- Create: `backend/hub/hub.go`, `backend/hub/hub_test.go`

- [ ] **Step 1: Failing tests** `backend/hub/hub_test.go`:

```go
package hub_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/hub"
)

func TestSubscribe_WakesOnNotifyTasks(t *testing.T) {
	h := hub.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := h.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.NotifyTasks()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected wake")
	}
}

func TestSubscribeTerminal_DeliversID(t *testing.T) {
	h := hub.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := h.SubscribeTerminal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.NotifyTerminal("i1")
	select {
	case got := <-ch:
		if got != "i1" {
			t.Fatalf("got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected terminal")
	}
}

func TestSubscribe_CancelStopsDelivery(t *testing.T) {
	h := hub.New()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := h.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	h.NotifyTasks()
	select {
	case <-ch:
		t.Fatal("unexpected wake after cancel")
	case <-time.After(50 * time.Millisecond):
	}
}
```

- [ ] **Step 2: Implement `backend/hub/hub.go`** — move logic from `backend/mysql/notify.go` into `Hub` methods (`NotifyTasks` / `NotifyTerminal` exported). Package `hub`.

- [ ] **Step 3: Run**

```bash
go test ./backend/hub/ -count=1 -v
```

Expected: PASS

- [ ] **Step 4: Commit + PR** `m5/task-2-notify-hub-pkg`

```bash
git commit -m "$(cat <<'EOF'
Add backend/hub for in-process task and terminal wake. [skip ci]

EOF
)"
```

---

### Task 3: Wire six stores + README

**Files:**
- Modify each store: replace fan-out `notify.go` with wrappers; add `hub *hub.Hub` field; init in `New`
- Stores: `memory`, `sqlite`, `mysql`, `spanner`, `dynamodb`, `firestore`
- Modify: `README.md`, `.claude/tasks/todo.md`

**Per store checklist:**

1. Import `github.com/hirokazumiyaji/tasuki/backend/hub`
2. On `Backend` struct: remove `notifyMu`, `taskSubs`, `terminalSubs`; add `hub *hub.Hub`
3. In constructor (`New` / `memory.New`): set `hub: hub.New()`
4. Replace `notify.go` body with:

```go
package <store>

import "context"

func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	return b.hub.Subscribe(ctx)
}
func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error) {
	return b.hub.SubscribeTerminal(ctx)
}
func (b *Backend) notifyTasks() { b.hub.NotifyTasks() }
func (b *Backend) notifyTerminal(instanceID string) { b.hub.NotifyTerminal(instanceID) }
```

5. Do **not** change emit call sites (`notifyTasks()` / `notifyTerminal(...)` remain).

**memory.New** currently returns `&Backend{...maps...}` — add `hub: hub.New()`.

**README** — adjust notify sentence:

```markdown
PostgreSQL Worker は `LISTEN`/`NOTIFY`（チャネル `tasuki_tasks`）で起床し、`PollInterval` はフォールバックおよびタイマー／スケジュール用。他ストアは同一 `Backend` 内のプロセス内 wakeup（`backend/hub`）で同様に起床する。
Client の `Result` は postgres（`tasuki_terminal`）および他ストア（プロセス内）で終端時に起床できる。
```

- [ ] **Verify**

```bash
go test ./backend/hub/ ./backend/memory/ ./backend/sqlite/ ./backend/mysql/ ./backend/spanner/ ./backend/dynamodb/ ./backend/firestore/ -count=1 -run 'TestSubscribe|TestHub' -timeout 90s
go test ./backend/memory/ ./backend/hub/ -count=1
```

Expected: compile + PASS (or Skip for stores needing DSN/emulator).

- [ ] **Commit + PR** `m5/task-3-notify-hub-wire-stores`

```bash
git commit -m "$(cat <<'EOF'
Wire in-process stores to backend/hub for notify fan-out. [skip ci]

EOF
)"
```

---

## Spec coverage (self-review)

| Spec item | Task |
|---|---|
| hub package API | 2 |
| Hub unit tests | 2 |
| Six stores delegate | 3 |
| postgres untouched | 3 (no edits) |
| README | 3 |

## Placeholder scan

No TBD. Wrapper code and constructor field listed explicitly.
