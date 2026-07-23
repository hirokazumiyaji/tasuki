# M4 バックエンド拡充 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Do **not** use subagents. One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]` (GitHub Actions minutes exhausted).
>
> Merge example: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** MySQL / MariaDB バックエンドを適合テストとカオスでグリーンにし、M4 の最初のストア追加を完了する。続くストア（TiDB → Spanner → DynamoDB → Firestore）は同じパターンで段階追加する。

**Architecture:** PostgreSQL 参照実装と同じロック方式（`FOR UPDATE SKIP LOCKED`）。ワークフロータスク singleton は MySQL 8 の生成列 + 一意インデックスで表現する。独立 Go モジュール `backend/mysql`（`go-sql-driver/mysql`）。DSN 環境変数 `TASUKI_MYSQL_DSN`。

**Tech Stack:** Go 1.24+、`github.com/go-sql-driver/mysql`、Docker `mysql:8` / `mariadb:11`、既存 `backendtest` + `chaos`。

**Spec:** [docs/04-plan.md](../../04-plan.md) M4、[docs/02-architecture.md](../../02-architecture.md)

**Phase scope (this plan):** MySQL / MariaDB only. TiDB+ は後続プラン。

---

## File structure

| Path | Responsibility |
|---|---|
| `backend/mysql/` | 独立モジュール、スキーマ、Backend、conform |
| `docker-compose.yml` | mysql サービス追加 |
| `chaos/` | DSN 切替で mysql でも回せるように（任意） |
| `examples/m4-mysql/` | クイックスタート |
| `README.md` | M4 MySQL ステータス |

### Schema notes (MySQL)

- `wf_tasks`: `wf_singleton` 生成列 = `CASE WHEN kind = 'workflow' THEN instance_id ELSE NULL END`、`UNIQUE (wf_singleton)`
- 日時は `DATETIME(6)` / `TIMESTAMP(6)`、比較はサーバー時計
- Claim: `SELECT ... FOR UPDATE SKIP LOCKED`（MySQL 8.0.1+）
- SendToInbox / CommitAdvancement: インスタンス行 `SELECT ... FOR UPDATE` で I1（postgres と同様）

---

### Task 1: Plan

**Files:** `docs/superpowers/plans/2026-07-23-m4-mysql-backend.md`, `.claude/tasks/todo.md`, README link

- [ ] Add this plan; reset todo for M4 MySQL
- [ ] PR `m4/plan-mysql` — commit + merge subject include `[skip ci]`

### Task 2: MySQL module scaffold + migrate

**Files:** `backend/mysql/` (new module), `go.work`, `schema.sql`

- [ ] Module `github.com/hirokazumiyaji/tasuki/backend/mysql`
- [ ] Schema with generated column singleton; Migrate / Reset / New(dsn)
- [ ] Migrate idempotent test（要 `TASUKI_MYSQL_DSN`）
- [ ] PR `m4/task-2-mysql-scaffold` `[skip ci]`

### Task 3: MySQL Backend core (instances, tasks, timers)

**Files:** `backend/mysql/*.go`

- [ ] CreateInstance, ClaimTasks, LoadWorkflow, CommitAdvancement, CompleteActivity, FireDueTimers, Terminate, leases
- [ ] Unit / manual smoke against docker mysql
- [ ] PR `m4/task-3-mysql-core` `[skip ci]`

### Task 4: Inbox, schedules, List, Children/ParentNotify

**Files:** `backend/mysql/`

- [ ] SendToInbox + I1 lock; schedules; ListInstances; child/parent
- [ ] PR `m4/task-4-mysql-inbox-schedules` `[skip ci]`

### Task 5: Conformance suite green

**Files:** `backend/mysql/conform_test.go`, docker-compose mysql service

- [ ] `backendtest.Run` passes on MySQL 8
- [ ] Optional: MariaDB service / same DSN docs
- [ ] PR `m4/task-5-mysql-conform` `[skip ci]`

### Task 6: Chaos + example + README

**Files:** `chaos/` (mysql DSN), `examples/m4-mysql/`, README

- [ ] Chaos against MySQL (reuse suite with DSN env)
- [ ] Example quickstart; README → M4 MySQL complete; note TiDB+ next
- [ ] PR `m4/task-6-mysql-example` `[skip ci]`

---

## Acceptance checklist (MySQL phase)

| Criterion | Task |
|---|---|
| Schema + migrate | 2 |
| Full Backend | 3, 4 |
| backendtest green | 5 |
| Chaos green | 6 |
| Example / README | 6 |

## Execution notes

- TDD; merge each PR before next
- Commits: `[skip ci]` in subject
- Merges: `gh pr merge --merge --subject "Merge pull request #N from <head> [skip ci]"`
- Prefer HTTPS git push if SSH agent fails
- No subagents
- Local: `docker compose up -d mysql` with `TASUKI_MYSQL_DSN=tasuki:tasuki@tcp(localhost:3306)/tasuki?parseTime=true`
