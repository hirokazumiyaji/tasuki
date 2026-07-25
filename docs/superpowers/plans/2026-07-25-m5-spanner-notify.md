# M5 Spanner Task / Terminal Notify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Wake Workers and `Client.Result` on `backend/spanner` via in-process `TaskNotifier` / `TerminalNotifier` when callers share one `*spanner.Backend`.

**Architecture:** Copy the mysql/sqlite subscriber-list pattern into `spanner.Backend` (`notify.go` + fields). Call `notifyTasks` / `notifyTerminal` **after successful ReadWriteTransaction** (and after Spanner’s post-commit ensure pass for `CommitAdvancements`). No shared hub.

**Tech Stack:** Go, Cloud Spanner client, existing `backend.TaskNotifier` / `TerminalNotifier`.

**Spec:** [docs/superpowers/specs/2026-07-25-m5-spanner-notify-design.md](../specs/2026-07-25-m5-spanner-notify-design.md)

## Global Constraints

- Same `*spanner.Backend` instance only
- Notifications are hints; correctness stays in Claim / GetInstance
- Do not close subscribe channels on cancel
- No notify on `RetryActivity` / `ExtendLease`
- Tests skip without `TASUKI_SPANNER_DSN` + `SPANNER_EMULATOR_HOST`
- Every commit / merge subject includes `[skip ci]`

---

## File structure

| Path | Responsibility |
|---|---|
| `docs/superpowers/specs/2026-07-25-m5-spanner-notify-design.md` | Approved design |
| `docs/superpowers/plans/2026-07-25-m5-spanner-notify.md` | This plan |
| `backend/spanner/notify.go` | Subscribe*, notify helpers |
| `backend/spanner/spanner.go` | `notifyMu`, subscriber fields |
| `backend/spanner/backend.go` | Emit-site calls after commit |
| `backend/spanner/schedule.go` | `ClaimDueSchedules` notify |
| `backend/spanner/notify_test.go` | Wake tests (`dsnOrSkip` + short timeout New/Migrate/Reset) |
| `README.md` | Mention spanner in-process notify |
| `.claude/tasks/todo.md` | Checklist |

---

### Task 1: Spec + Plan

**Files:**
- Create: `docs/superpowers/specs/2026-07-25-m5-spanner-notify-design.md`
- Create: `docs/superpowers/plans/2026-07-25-m5-spanner-notify.md` (this file)
- Modify: `.claude/tasks/todo.md`

- [ ] Reset todo for Tasks 1–3; commit + PR `m5/task-1-spanner-notify-docs`

```bash
git commit -m "$(cat <<'EOF'
Add spanner notify design and plan. [skip ci]

EOF
)"
```

---

### Task 2: `Subscribe` / `SubscribeTerminal` + CreateInstance / Terminate

**Files:**
- Create: `backend/spanner/notify.go`, `backend/spanner/notify_test.go`
- Modify: `backend/spanner/spanner.go`, `backend/spanner/backend.go`

- [ ] **Step 1: Failing tests** — mirror mysql notify tests; helper like `openBatchBackend` in `batch_test.go` (5s timeout, Skip on New/Migrate/Reset failure). Unique instance IDs: `notify-wake-1`, `notify-term-1`, `notify-cancel-1`.

- [ ] **Step 2: Implement `notify.go`** — copy from `backend/mysql/notify.go` (package `spanner`).

- [ ] **Step 3: Fields on `Backend`**

```go
type Backend struct {
	client *spanner.Client
	dsn    string

	notifyMu     sync.Mutex
	taskSubs     []*taskSub
	terminalSubs []*terminalSub
}
```

- [ ] **Step 4: Wire CreateInstance / TerminateInstance**

```go
func (b *Backend) CreateInstance(...) error {
	_, err := b.client.ReadWriteTransaction(...)
	if isAlreadyExists(err) {
		return backend.ErrAlreadyExists
	}
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) TerminateInstance(...) error {
	_, err := b.client.ReadWriteTransaction(...)
	if err != nil {
		return err // map ErrNotFound as today
	}
	b.notifyTerminal(id)
	return nil
}
```

(Preserve existing error mapping inside the txn; only notify on success.)

- [ ] **Step 5: Run**

```bash
go test ./backend/spanner/ -count=1 -c -o /dev/null
go test ./backend/spanner/ -count=1 -run 'TestSubscribe' -v -timeout 30s
```

Expected: compile OK; PASS or Skip without emulator.

- [ ] **Step 6: Commit + PR** `m5/task-2-spanner-notify-subscribe`

```bash
git commit -m "$(cat <<'EOF'
Add in-process TaskNotifier and TerminalNotifier to spanner. [skip ci]

EOF
)"
```

---

### Task 3: Remaining emit sites + terminal commit + README

**Files:**
- Modify: `backend/spanner/backend.go`, `backend/spanner/schedule.go`, `backend/spanner/notify_test.go`, `README.md`, `.claude/tasks/todo.md`

- [ ] **Step 1: Terminal commit test** — mirror mysql `TestSubscribeTerminal_WakesOnCommitTerminal`.

- [ ] **Step 2: Wire emit sites (after successful txn only)**

| Method | After success |
|---|---|
| `CommitAdvancements` | After main txn **and** post-commit ensure loop → `notifyTasks()`; then `notifyTerminal` for each Terminal |
| `CompleteActivity` | After successful txn on enqueue path → `notifyTasks()`; late-complete (not running) → no |
| `SendToInbox` | After all success → `notifyTasks()` |
| `FireDueTimers` | After success if `n > 0` → `notifyTasks()` |
| `ReleaseLease` | After success → `notifyTasks()` |
| `ClaimDueSchedules` | After success if any due / `len(out) > 0` → `notifyTasks()` |
| `RetryActivity` / `ExtendLease` | **no** |

`CommitAdvancements` pattern (notify after ensure loop, same as mysql):

```go
err := b.withRW(...) // commit all
if err != nil {
	return err
}
for _, adv := range advs {
	if err := b.withRW(... ensure ...); err != nil {
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
PostgreSQL Worker は `LISTEN`/`NOTIFY`（チャネル `tasuki_tasks`）で起床し、`PollInterval` はフォールバックおよびタイマー／スケジュール用。memory / sqlite / mysql / spanner は同一 `Backend` インスタンス内の `TaskNotifier` で同様に起床する。
Client の `Result` は postgres（`tasuki_terminal`）および memory / sqlite / mysql / spanner（プロセス内）で終端時に起床できる（未対応ストアは従来どおりポーリング）。
```

- [ ] **Step 4: Verify**

```bash
go test ./backend/spanner/ -count=1 -c -o /dev/null
go test ./backend/spanner/ -count=1 -run 'TestSubscribe' -timeout 60s
```

- [ ] **Step 5: Commit + PR** `m5/task-3-spanner-notify-emit-sites`

```bash
git commit -m "$(cat <<'EOF'
Wire spanner notify emit sites and document in-process wake. [skip ci]

EOF
)"
```

---

## Spec coverage (self-review)

| Spec item | Task |
|---|---|
| TaskNotifier + TerminalNotifier on spanner | 2 |
| CreateInstance / Terminate | 2 |
| Remaining emit sites | 3 |
| Tests (skip without emulator) | 2, 3 |
| README | 3 |
| No hub | all |

## Placeholder scan

No TBD. CommitAdvancements ensure-loop ordering called out.
