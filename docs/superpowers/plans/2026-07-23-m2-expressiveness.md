# M2 表現力 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Do **not** use subagents. One commit + one PR per task; merge to `main` before the next.

**Goal:** 03-api.md のワークフロー API を完成させ、シグナル／子ワークフロー／キャンセル等を適合テスト付きで動かす。

**Architecture:** M1 のジャーナル再実行エンジンを拡張する。`Future` で非同期コマンドを表現し、`Await` で最初の完了を待つ。シグナルは `ref_seq` なし完了イベントとして inbox 経由で取り込む。子は親の `CommitAdvancement.Children` で原子作成し、終端時に親 inbox へ通知する。

**Tech Stack:** Go 1.24+、既存 memory / postgres バックエンド、`backendtest` 拡張。

**Spec:** [docs/04-plan.md](../../04-plan.md) M2、[docs/03-api.md](../../03-api.md)、[docs/02-architecture.md](../../02-architecture.md)

**Note:** `NonRetryable` は M1 で実装済み。

---

## File structure

| Path | Responsibility |
|---|---|
| `journal/event.go` | `child_scheduled` / `child_completed` / `child_failed` |
| `workflow/future.go` | `Future[O]`, `Awaitable` |
| `workflow/async.go` | `ExecuteAsync`, `SleepAsync`, `Await`, `AwaitAll` |
| `workflow/sideeffect.go` | `SideEffect`, `NewUUID`, `Now` |
| `workflow/signal.go` | `ReceiveSignal`, `ReceiveSignalWithTimeout` |
| `workflow/child.go` | `ExecuteChild`, `ExecuteChildAsync` |
| `workflow/version.go` | `GetVersion` |
| `workflow/continue.go` | `ContinueAsNew` |
| `workflow/info.go` | `Info` / `WorkflowInfo` |
| `backend/*.go` + memory/postgres | `SendToInbox`; Children + ParentNotify in commit |
| `client.go` | `Signal`, `Cancel` |
| `worker.go` | canceled terminal, ContinueAsNew, child notify |
| `backendtest/m2.go` | signal race, child notify, cancel compensation |
| `doctest/` or `docs/examples_test.go` | 03-api コード例のコンパイル／実行 |

---

### Task 1: Journal child event types + Future/Awaitable

**Files:** `journal/event.go`, `journal/event_test.go`, `workflow/future.go`, `workflow/future_test.go`

- [ ] Add `TypeChildScheduled`, `TypeChildCompleted`, `TypeChildFailed`; update `IsCommand` / `IsCompletion`
- [ ] `Future[O]` with `seq`, `ready`, `value`, `err`; `Get` awaits completion or suspends; `Awaitable` interface for Await
- [ ] Commit + PR `m2/task-1-future`

### Task 2: ExecuteAsync, SleepAsync, Await, AwaitAll

**Files:** `workflow/async.go`, `workflow/async_test.go`, engine tests

- [ ] `ExecuteAsync` schedules activity without waiting; returns Future
- [ ] `SleepAsync` schedules timer; returns Future
- [ ] `Await` returns index of first completed; suspends if none ready; canceled → ErrCanceled
- [ ] `AwaitAll` waits all; returns first error
- [ ] Refactor `Execute`/`Sleep` to use Async+Get internally (optional DRY)
- [ ] PR `m2/task-2-async-await`

### Task 3: SideEffect, NewUUID, Now, Info

**Files:** `workflow/sideeffect.go`, `workflow/info.go`, tests

- [ ] `Now` records `now_recorded` command with ctx.now
- [ ] `SideEffect` runs fn only when recording (not replay); stores value
- [ ] `NewUUID` via SideEffect
- [ ] `Info(ctx)` from context fields (set by engine: InstanceID, Name, StartedAt)
- [ ] Engine/worker passes instance info into Context
- [ ] PR `m2/task-3-sideeffect-now`

### Task 4: SendToInbox + Client.Signal / Cancel

**Files:** `backend/backend.go`, memory, postgres, `client.go`, tests

- [ ] `SendToInbox(ctx, instanceID, ev)` — insert inbox + ensure workflow task if running
- [ ] `Client.Signal(ctx, id, name, payload)` → signal_received
- [ ] `Client.Cancel(ctx, id)` → cancel_requested
- [ ] PR `m2/task-4-signal-cancel-client`

### Task 5: ReceiveSignal (+ timeout)

**Files:** `workflow/signal.go`, `workflow/context.go`, wftest/env Signal helper

- [ ] Consume unmatched `signal_received` by name in seq order (no command emitted)
- [ ] Suspend if none; timeout via SleepAsync+Await pattern for `ReceiveSignalWithTimeout`
- [ ] Canceled → ErrCanceled
- [ ] PR `m2/task-5-receive-signal`

### Task 6: GetVersion (marker skip)

**Files:** `workflow/version.go`, `journal/match.go` or context replay

- [ ] First visit records version_marker with max; replay returns recorded; missing marker returns min
- [ ] Replay skips unread version_marker when matching other commands (old code path)
- [ ] PR `m2/task-6-getversion`

### Task 7: Cancel semantics + compensation in worker

**Files:** `worker.go`, `workflow/*`, tests

- [ ] After cancel_requested ingested, wait APIs return ErrCanceled; Execute still works
- [ ] Terminal: if returned error is/wraps ErrCanceled → status `canceled` + workflow_canceled event
- [ ] Test: cancel mid-sleep, run compensation activity, return ErrCanceled
- [ ] PR `m2/task-7-cancel-compensation`

### Task 8: Child workflows

**Files:** `workflow/child.go`, worker, backend Children/ParentNotify

- [ ] `ExecuteChild` / Async: command child_scheduled; Advancement.Children creates child instance + workflow task
- [ ] Child terminal CommitAdvancement sets ParentNotify → parent inbox child_completed/failed
- [ ] Parent Future completes on child_* event
- [ ] Default child ID `{parentID}:{seq}`
- [ ] PR `m2/task-8-child-workflows`

### Task 9: ContinueAsNew

**Files:** `workflow/continue.go`, worker

- [ ] `ContinueAsNew[I](ctx, in) error` returns sentinel error
- [ ] Worker: terminal continued_as_new; create new instance with same name/queue/new input; ID strategy `{id}:{run}` or replace — use `{originalID}~{n}` or docs: new execution with same workflow ID after terminate old — **use** new ID `{id}:{seq}` and document; simpler M2: `WithContinueAsNewID` default `{id}~continued-{seq}`
- [ ] Prefer: end old as `continued`, start new with same ID is impossible (PK). Use `{id}/run-{n}` stored in failure/result metadata. **Simplest:** new instance ID = `fmt.Sprintf("%s~%d", oldID, termSeq)`
- [ ] PR `m2/task-9-continue-as-new`

### Task 10: Conformance M2 cases + doctest + README

**Files:** `backendtest/m2.go`, `doctest/api_example_test.go`, README, CI

- [ ] Signal vs task processing race (SendToInbox concurrent with CommitAdvancement) — I1 still holds
- [ ] Child completion notifies parent
- [ ] Cancel compensation path (may be worker-level test in root package)
- [ ] Doc example test compiling OrderWorkflow-style snippet (adapted to name-based or func-based Execute)
- [ ] README status → M2 complete
- [ ] PR `m2/task-10-conform-doctest`

---

## Acceptance checklist

| Criterion | Task |
|---|---|
| ExecuteAsync / SleepAsync / Await / AwaitAll | 2 |
| Signals | 4, 5, 10 |
| Child workflows | 8, 10 |
| SideEffect / NewUUID / Now | 3 |
| GetVersion | 6 |
| Cancel + compensation | 7, 10 |
| ContinueAsNew | 9 |
| NonRetryable | done (M1) |
| Doc example compiles | 10 |

## Execution notes

- TDD; merge each PR before next
- Keep activity `Execute` string-name API; add func-name overload in Task 10 if needed for doctest
- No subagents
