# M3 運用性 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Do **not** use subagents. One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message MUST include `[skip ci]` (GitHub Actions minutes exhausted).

**Goal:** 本番投入と開発体験に必要な周辺（SQLite、cron、観測性、静的解析、状態閲覧）を揃え、M3 受入条件を自動テストでグリーンにする。

**Architecture:** `backend.Backend` に `ListInstances` / スケジュール操作を足し、SQLite を独立モジュールで適合スイート通過させる。cron は `{scheduleID}:{scheduledAt}` のインスタンス ID 重複排除で exactly-once。メトリクスは OpenTelemetry、ログは既存 `slog` フックを実運用レベルへ。解析器は `go/analysis` で `workflow` 関数内の `time.Now` / `go` / `rand` を検出する。

**Tech Stack:** Go 1.24+、`modernc.org/sqlite`（CGO なし）、`go.opentelemetry.io/otel`、`golang.org/x/tools/go/analysis`、既存 memory / postgres。

**Spec:** [docs/04-plan.md](../../04-plan.md) M3、[docs/02-architecture.md](../../02-architecture.md)、[docs/03-api.md](../../03-api.md)

---

## File structure

| Path | Responsibility |
|---|---|
| `backend/backend.go` + `types.go` | `ListInstances`, schedule types, `ClaimDueSchedules` / upsert |
| `backend/memory` | List + schedules |
| `backend/postgres` | List + `wf_schedules` migrate + schedule ops |
| `backend/sqlite/` | 独立モジュール、スキーマ、Backend、conform |
| `backendtest/m3.go` | schedule double-fire / List など |
| `client.go` | `GetJournal`, `List`, schedule client API |
| `worker.go` / schedule poller | due schedule → CreateInstance |
| `observability/` or worker hooks | OTel metrics + slog fields |
| `docs/05-observability.md` | メトリクス一覧 |
| `analyzers/determinism/` | `go vet` 互換解析器 |
| `examples/m3-sqlite/` | ローカルデモ |
| `README.md` | M3 完了ステータス |

---

### Task 1: Plan

**Files:** `docs/superpowers/plans/2026-07-23-m3-operability.md`, `.claude/tasks/todo.md`

- [ ] Add this plan; reset todo for M3
- [ ] PR `m3/plan-operability` — commit message includes `[skip ci]`

### Task 2: Client GetJournal + ListInstances

**Files:** `backend/types.go`, `backend/backend.go`, memory, postgres, `client.go`, tests

- [ ] Add `InstanceFilter` (`Status`, optional `Name`, `Limit`, `Offset`)
- [ ] `Backend.ListInstances(ctx, f) ([]Instance, error)`
- [ ] `Client.GetJournal(ctx, id)` → afterSeq 0; `Client.List(ctx, f)`
- [ ] Memory + postgres implement List; tests
- [ ] PR `m3/task-2-list-journal` `[skip ci]`

### Task 3: Schedule types + memory ClaimDueSchedules

**Files:** `backend/*`, memory, client upsert helpers

- [ ] Types: `Schedule`, `NewSchedule` (`ID`, `Cron`, `Workflow`, `Queue`, `Input`, `Paused`)
- [ ] `UpsertSchedule`, `GetSchedule`, `PauseSchedule`, `ClaimDueSchedules(limit) ([]DueSchedule, error)`
  - Claim: lock due rows, compute instance ID `{scheduleID}:{nextRunAt.UTC().Format(time.RFC3339)}`, create instance (ignore AlreadyExists), advance `next_run_at`, return claimed
- [ ] Cron parsing: use `github.com/robfig/cron/v3` (seconds optional off; standard 5-field)
- [ ] Memory implementation + unit tests (virtual clock)
- [ ] PR `m3/task-3-schedule-memory` `[skip ci]`

### Task 4: Postgres schedules + worker poller

**Files:** postgres schema/migrate, postgres backend, `worker.go`, client

- [ ] Add `wf_schedules` table (02-architecture)
- [ ] Implement schedule ops; SKIP LOCKED on due rows
- [ ] Worker polls `ClaimDueSchedules` alongside timers
- [ ] `Client.UpsertSchedule` / `PauseSchedule`
- [ ] Test: double claim → second CreateInstance AlreadyExists → still one running instance
- [ ] PR `m3/task-4-schedule-postgres-worker` `[skip ci]`

### Task 5: SQLite module scaffold + migrate

**Files:** `backend/sqlite/` (new module), `go.work`, schema

- [ ] Module `github.com/hirokazumiyaji/tasuki/backend/sqlite` with `modernc.org/sqlite`
- [ ] Embed schema (instances, journal, inbox, tasks, timers, schedules) — SQLite types
- [ ] `New(path)`, `Migrate`, `Close`, `Reset` for tests
- [ ] PR `m3/task-5-sqlite-scaffold` `[skip ci]`

### Task 6: SQLite Backend + conformance

**Files:** `backend/sqlite/*.go`, `conform_test.go`

- [ ] Implement full `Backend` (including M2 SendToInbox FOR UPDATE equivalent via single-writer / BEGIN IMMEDIATE)
- [ ] Pass `backendtest.Run` (includes M2)
- [ ] PR `m3/task-6-sqlite-backend` `[skip ci]`

### Task 7: Observability (slog + OpenTelemetry)

**Files:** `options.go`, `worker.go`, `observability/metrics.go`, `docs/05-observability.md`, tests

- [ ] Wire `WorkerOptions.Logger` (default `slog.Default()`) on claim/complete/stuck/retry
- [ ] OTel meters: `tasuki.workflow.tasks`, `tasuki.activity.tasks`, `tasuki.workflow.completed`, `tasuki.workflow.failed`, `tasuki.workflow.stuck`, `tasuki.activity.retries` (counters); optional histogram for task duration
- [ ] Document metric names / attributes in `docs/05-observability.md`
- [ ] PR `m3/task-7-observability` `[skip ci]`

### Task 8: Determinism analyzer

**Files:** `analyzers/determinism/`, tests with `analysistest`

- [ ] Analyzer flags in functions taking `*workflow.Context`: `time.Now`/`time.Since`, `go` stmt, `math/rand` / `crypto/rand` usage (allowlist SideEffect callee later if needed)
- [ ] `go test` with testdata; document `go vet -vettool=...` usage in README or docs
- [ ] PR `m3/task-8-determinism-analyzer` `[skip ci]`

### Task 9: Examples + README + backendtest M3 schedule race

**Files:** `examples/m3-sqlite/`, `backendtest/m3.go`, README, CI note

- [ ] Conformance: schedule double-fire dedup (memory + sqlite; postgres if easy)
- [ ] Example: sqlite file DB, schedule or simple workflow
- [ ] README → M3 complete; mention `[skip ci]` not required long-term
- [ ] PR `m3/task-9-example-readme` `[skip ci]`

---

## Acceptance checklist

| Criterion | Task |
|---|---|
| SQLite passes conformance | 5, 6 |
| Schedule double-fire dedup | 3, 4, 9 |
| Analyzer detects Now / go / rand | 8 |
| List / GetJournal | 2 |
| OTel + slog documented | 7 |
| Example works | 9 |

## Execution notes

- TDD; merge each PR before next
- All commits: include `[skip ci]` in subject or body
- No subagents
- Prefer HTTPS git push if SSH agent fails
- SQLite: pure Go driver; single-writer makes I1 easier but still assert in suite
