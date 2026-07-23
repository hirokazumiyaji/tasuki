# M4 Firestore Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Do **not** use subagents. One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Firestore バックエンドを Emulator 上で適合・カオスし、M4 バックエンド拡充を完了する。

**Architecture:** 独立モジュール `backend/firestore`。マルチコレクション。Claim はクエリ + トランザクション内条件付き更新。Workflow singleton は doc ID `WF#<instance_id>`。前進は Firestore トランザクション。`Capabilities.MaxAdvancementEffects: 400`（既存 worker inbox clamp を流用）。

**Tech Stack:** Go 1.26+、`cloud.google.com/go/firestore`、Firestore emulator（compose）、既存 `backendtest` + `chaos`。

**Spec:** [docs/superpowers/specs/2026-07-23-m4-firestore-design.md](../specs/2026-07-23-m4-firestore-design.md)

---

## File structure

| Path | Responsibility |
|---|---|
| `backend/firestore/` | モジュール、Backend、conform |
| `docker-compose.yml` | `firestore` emulator |
| `chaos/` | `TASUKI_BACKEND=firestore` |
| `examples/m4-firestore/` | クイックスタート |
| `go.work` | `./backend/firestore` |
| `README.md` | M4 完了ステータス |

### Env

```bash
docker compose up -d firestore
export FIRESTORE_EMULATOR_HOST=localhost:8080
export TASUKI_FIRESTORE_PROJECT=tasuki
```

---

### Task 1: Spec + Plan

**Files:**
- Create: `docs/superpowers/specs/2026-07-23-m4-firestore-design.md`
- Create: `docs/superpowers/plans/2026-07-23-m4-firestore.md`
- Modify: `.claude/tasks/todo.md`, `README.md`

- [ ] Mark design Approved; add this plan; reset todo
- [ ] PR `m4/plan-firestore` `[skip ci]`

### Task 2: Compose emulator + module scaffold

**Files:**
- Modify: `docker-compose.yml`, `go.work`
- Create: `backend/firestore/go.mod`, `firestore.go` (`New`, `Migrate` no-op/ensure, `Reset`, `Close`), `migrate_test.go`

- [ ] Emulator service; verify `FIRESTORE_EMULATOR_HOST` connectivity
- [ ] Module `github.com/hirokazumiyaji/tasuki/backend/firestore`
- [ ] `Reset` deletes all docs in known collections (batch)
- [ ] Smoke: New + Reset + Migrate idempotent
- [ ] PR `m4/task-2-firestore-scaffold` `[skip ci]`

### Task 3: Backend core + Capabilities

**Files:** `backend/firestore/backend.go`, `helpers.go`

- [ ] Capabilities `{MaxAdvancementEffects: 400}`
- [ ] CreateInstance, ClaimTasks (query + txn conditional), LoadWorkflow, CommitAdvancement (txn + post-commit ensure), CompleteActivity, RetryActivity, ExtendLease, ReleaseLease, FireDueTimers, Terminate, GetJournal
- [ ] Smoke create → claim → commit
- [ ] PR `m4/task-3-firestore-core` `[skip ci]`

### Task 4: Inbox, schedules, List, children

**Files:** `backend/firestore/schedule.go`, `backend.go`

- [ ] SendToInbox + post-commit ensure
- [ ] Schedules (Upsert/Get/Pause/ClaimDue)
- [ ] ListInstances; children + ParentNotify
- [ ] PR `m4/task-4-firestore-inbox-schedules` `[skip ci]`

### Task 5: Conformance green

**Files:** `backend/firestore/conform_test.go`

- [ ] `backendtest.Run` green on emulator
- [ ] PR `m4/task-5-firestore-conform` `[skip ci]`

### Task 6: Chaos + example + README (M4 complete)

**Files:**
- Modify: `chaos/cmd/worker/main.go`, `README.md`
- Create: `chaos/firestore_chaos_test.go`, `examples/m4-firestore/main.go`

- [ ] Chaos kill-workers
- [ ] Example quickstart
- [ ] README: M4 全ストア完了; 次は M5 候補
- [ ] PR `m4/task-6-firestore-chaos-readme` `[skip ci]`

---

## Acceptance checklist

| Criterion | Task |
|---|---|
| Emulator + scaffold | 2 |
| Full Backend + Capabilities | 3–4 |
| backendtest green | 5 |
| Chaos + example + README / M4 done | 6 |

## Execution notes

- TDD; merge each PR before next; `[skip ci]`
- Prefer HTTPS push; no subagents
- Reference: `backend/dynamodb` (Capabilities + clamp), `backend/spanner` (I1 post-ensure)
