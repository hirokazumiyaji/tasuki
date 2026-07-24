# M5 Claim Batch Size Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Configurable `ClaimLimit` on workers (default 10) plus bench flag, sequential execution unchanged.

**Architecture:** Plumb `WorkerOptions.ClaimLimit` into existing `ClaimTasks` `Limit`; expose via `cmd/bench -claim-limit`.

**Tech Stack:** Existing Worker / bench / flags.

**Spec:** [docs/superpowers/specs/2026-07-24-m5-claim-limit-design.md](../specs/2026-07-24-m5-claim-limit-design.md)

## Global Constraints

- Default ClaimLimit = 10 (same as today’s hardcode)
- Sequential handle after claim
- No Backend interface changes
- Every commit / merge subject includes `[skip ci]`

---

## File structure

| Path | Responsibility |
|---|---|
| `options.go` | `ClaimLimit` field + default |
| `worker.go` | Use `ClaimLimit` in tick |
| `bench/runner.go` | Pass `Config.ClaimLimit` to workers |
| `cmd/bench/main.go` | `-claim-limit` flag |
| `README.md` | Document option / flag |
| `.claude/tasks/todo.md` | Task list |

---

### Task 1: Spec + Plan

**Files:** spec + this plan + todo

- [ ] Commit + PR `m5/task-1-claim-limit-spec` `[skip ci]`; merge

---

### Task 2: WorkerOptions + Worker tick

**Files:** `options.go`, `worker.go`

- [ ] Add `ClaimLimit int` to `WorkerOptions`
- [ ] In `withDefaults`, if `ClaimLimit <= 0` set `10`
- [ ] In `tick`, use `w.opts.ClaimLimit` for both ClaimTasks calls
- [ ] Run `go test . ./bench/ -count=1`
- [ ] Commit + PR `m5/task-2-claim-limit-worker` `[skip ci]`; merge

---

### Task 3: Bench flag + README

**Files:** `bench/runner.go`, `cmd/bench/main.go`, `README.md`, todo

- [ ] `Config.ClaimLimit`; pass to `WorkerOptions`
- [ ] `-claim-limit` flag (default 0)
- [ ] README: `WorkerOptions.ClaimLimit` / `-claim-limit` (default 10)
- [ ] Smoke: `go run ./cmd/bench -backend=memory -claim-limit=50 -instances=50`
- [ ] Mark todos done; commit + PR `m5/task-3-claim-limit-bench-readme` `[skip ci]`; merge

---

## Acceptance checklist

- [ ] Default behavior unchanged (limit 10)
- [ ] Configurable larger claims
- [ ] Bench flag works
- [ ] README updated
