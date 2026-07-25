# M5 DynamoDB / Firestore Task / Terminal Notify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Wake Workers and `Client.Result` on DynamoDB and Firestore via in-process `TaskNotifier` / `TerminalNotifier` when callers share one Backend instance per store.

**Architecture:** Copy the mysql/spanner subscriber-list pattern into each of `dynamodb.Backend` and `firestore.Backend`. Notify after successful writes; for CommitAdvancements notify after the whole API returns nil (including DynamoDB sequential fallback and post-ensure loops). No Streams/listen; no hub.

**Tech Stack:** Go; AWS SDK DynamoDB; Firestore client; existing notifier interfaces.

**Spec:** [docs/superpowers/specs/2026-07-25-m5-dynamodb-firestore-notify-design.md](../specs/2026-07-25-m5-dynamodb-firestore-notify-design.md)

## Global Constraints

- Same Backend instance only (per store)
- Do not close subscribe channels on cancel
- No notify on `RetryActivity` / `ExtendLease`
- DynamoDB tests skip without `TASUKI_DYNAMODB_ENDPOINT`; Firestore without `FIRESTORE_EMULATOR_HOST`
- Every commit / merge subject includes `[skip ci]`

---

## File structure

| Path | Responsibility |
|---|---|
| Spec + this plan | Design / plan |
| `backend/dynamodb/notify.go` + fields in `dynamodb.go` | Subscribe*, helpers |
| `backend/dynamodb/backend.go` / `schedule.go` | Emit sites |
| `backend/dynamodb/notify_test.go` | Wake tests |
| `backend/firestore/notify.go` + fields in `firestore.go` | Subscribe*, helpers |
| `backend/firestore/backend.go` / `schedule.go` | Emit sites |
| `backend/firestore/notify_test.go` | Wake tests |
| `README.md` | All-stores notify note |
| `.claude/tasks/todo.md` | Checklist |

---

### Task 1: Spec + Plan

**Files:**
- Create: `docs/superpowers/specs/2026-07-25-m5-dynamodb-firestore-notify-design.md`
- Create: `docs/superpowers/plans/2026-07-25-m5-dynamodb-firestore-notify.md` (this file)
- Modify: `.claude/tasks/todo.md`

```markdown
# tasuki M5 DynamoDB / Firestore Notify

## タスク

- [x] Task 1: Spec + Plan
- [ ] Task 2: DynamoDB notify (full)
- [ ] Task 3: Firestore notify (full) + README
```

- [ ] Commit + PR `m5/task-1-ddb-fs-notify-docs`

```bash
git commit -m "$(cat <<'EOF'
Add dynamodb/firestore notify design and plan. [skip ci]

EOF
)"
```

---

### Task 2: DynamoDB notify (full)

**Files:**
- Create: `backend/dynamodb/notify.go`, `backend/dynamodb/notify_test.go`
- Modify: `backend/dynamodb/dynamodb.go`, `backend/dynamodb/backend.go`, `backend/dynamodb/schedule.go`

- [ ] **Step 1: Tests** — mirror spanner/mysql notify tests; helper via `endpointOrSkip` + `dynamodb.New` + Migrate + Reset (Skip on Reset failure). Cover CreateInstance, Terminate, CommitTerminal, Cancel.

- [ ] **Step 2: `notify.go`** — copy from `backend/mysql/notify.go` (package `dynamodb`).

- [ ] **Step 3: Fields on `Backend` in `dynamodb.go`**

```go
notifyMu     sync.Mutex
taskSubs     []*taskSub
terminalSubs []*terminalSub
```

- [ ] **Step 4: Wire emit sites**

| Method | After success |
|---|---|
| `CreateInstance` | after TransactWrite OK → `notifyTasks()` |
| `TerminateInstance` | after success → `notifyTerminal(id)` |
| `CommitAdvancement` / `commitAdvancementOnce` | Prefer notify from `CommitAdvancements` and from `CommitAdvancement` path: when `CommitAdvancement` calls `commitAdvancementOnce`, notify after that returns nil (`notifyTasks` + terminal if set). When `CommitAdvancements` succeeds (batched or sequential fallback), notify once `notifyTasks` then terminal for each adv with `Terminal`. Avoid double-notify: **do not** notify inside `commitAdvancementOnce` if only called from `CommitAdvancements`; instead: |
| | - Change `CommitAdvancement` to call `CommitAdvancements([]adv)` (already does for firestore; dynamodb currently calls `commitAdvancementOnce` — switch `CommitAdvancement` to `CommitAdvancements` like other stores **or** notify only in outer wrappers). **Preferred:** make `CommitAdvancement` → `CommitAdvancements`; put notify only at end of `CommitAdvancements` (and in sequential fallback path before `return nil`). For `len==1` early return through `commitAdvancementOnce`, still notify after that call inside `CommitAdvancements`. |
| `CompleteActivity` | after enqueue path success → `notifyTasks()` |
| `SendToInbox` | after success → `notifyTasks()` |
| `FireDueTimers` | if `n > 0` → `notifyTasks()` |
| `ReleaseLease` | after success → `notifyTasks()` |
| `ClaimDueSchedules` | if any due / `len(out) > 0` → `notifyTasks()` |

Concrete `CommitAdvancements` end:

```go
// after successful batch path ensures, or after sequential fallback loop:
b.notifyTasks()
for _, adv := range advs {
	if adv.Terminal != nil {
		b.notifyTerminal(adv.InstanceID)
	}
}
return nil
```

Also change:

```go
func (b *Backend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	return b.CommitAdvancements(ctx, []backend.Advancement{adv})
}
```

And for `len(advs)==1` branch that currently returns `commitAdvancementOnce` directly, keep that call then notify:

```go
if len(advs) == 1 {
	if err := b.commitAdvancementOnce(ctx, advs[0]); err != nil {
		return err
	}
	b.notifyTasks()
	if advs[0].Terminal != nil {
		b.notifyTerminal(advs[0].InstanceID)
	}
	return nil
}
```

Sequential fallback: after the loop succeeds, notify the same way (do not return nil before notify).

- [ ] **Step 5: Verify**

```bash
go test ./backend/dynamodb/ -count=1 -c -o /dev/null
go test ./backend/dynamodb/ -count=1 -run 'TestSubscribe' -timeout 60s
```

- [ ] **Step 6: Commit + PR** `m5/task-2-dynamodb-notify`

```bash
git commit -m "$(cat <<'EOF'
Add in-process TaskNotifier and TerminalNotifier to dynamodb. [skip ci]

EOF
)"
```

---

### Task 3: Firestore notify (full) + README

**Files:**
- Create: `backend/firestore/notify.go`, `backend/firestore/notify_test.go`
- Modify: `backend/firestore/firestore.go`, `backend/firestore/backend.go`, `backend/firestore/schedule.go`, `README.md`, `.claude/tasks/todo.md`

- [ ] **Step 1: Tests** — same suite; `emulatorOrSkip` + `firestore.New` + Migrate + Reset.

- [ ] **Step 2: `notify.go` + Backend fields** — mirror dynamodb/mysql.

- [ ] **Step 3: Wire emit sites** — same table as Task 2. `CommitAdvancements`: notify after txn + ensure loop (existing post-txn ensure). `CompleteActivity` / `SendToInbox` / `FireDueTimers` / `ReleaseLease` / `ClaimDueSchedules` / `CreateInstance` / `TerminateInstance` as above.

- [ ] **Step 4: README**

```markdown
PostgreSQL Worker は `LISTEN`/`NOTIFY`（チャネル `tasuki_tasks`）で起床し、`PollInterval` はフォールバックおよびタイマー／スケジュール用。他ストア（memory / sqlite / mysql / spanner / dynamodb / firestore）は同一 `Backend` インスタンス内の `TaskNotifier` で同様に起床する。
Client の `Result` は postgres（`tasuki_terminal`）および他ストア（プロセス内）で終端時に起床できる。
```

- [ ] **Step 5: Verify**

```bash
go test ./backend/firestore/ -count=1 -c -o /dev/null
go test ./backend/firestore/ -count=1 -run 'TestSubscribe' -timeout 60s
```

- [ ] **Step 6: Commit + PR** `m5/task-3-firestore-notify-readme`

```bash
git commit -m "$(cat <<'EOF'
Add in-process notify to firestore and document all-store wake. [skip ci]

EOF
)"
```

---

## Spec coverage (self-review)

| Spec item | Task |
|---|---|
| DynamoDB TaskNotifier + TerminalNotifier + emit sites | 2 |
| Firestore TaskNotifier + TerminalNotifier + emit sites | 3 |
| CommitAdvancements notify-after-success (incl. DDB fallback) | 2 |
| Tests skip without emulator | 2, 3 |
| README all stores | 3 |
| No Streams/listen / no hub | all |

## Placeholder scan

No TBD. DynamoDB `CommitAdvancement` → `CommitAdvancements` / single-adv notify path specified.
