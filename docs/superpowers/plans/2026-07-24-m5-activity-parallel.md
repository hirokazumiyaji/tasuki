# M5 Activity Parallelism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Bounded parallel `handleActivity` in Worker tick via `ActivityConcurrency` (default 1) + bench flag.

**Architecture:** Semaphore + WaitGroup inside tick after activity claim; workflows stay sequential.

**Tech Stack:** Existing Worker / bench.

**Spec:** [docs/superpowers/specs/2026-07-24-m5-activity-parallel-design.md](../specs/2026-07-24-m5-activity-parallel-design.md)

## Global Constraints

- Default ActivityConcurrency = 1
- Workflows remain sequential
- Every commit / merge subject includes `[skip ci]`

---

### Task 1: Spec + Plan

- [ ] Docs + todo; PR `m5/task-1-activity-parallel-spec`; merge

### Task 2: WorkerOptions + tick parallelism

**Files:** `options.go`, `worker.go`

- [ ] Add `ActivityConcurrency`; default 1
- [ ] Parallel activity loop with semaphore
- [ ] `go test . ./bench/ -count=1`
- [ ] PR `m5/task-2-activity-parallel-worker`; merge

### Task 3: Bench flag + README

**Files:** `bench/runner.go`, `cmd/bench/main.go`, `README.md`, todo

- [ ] `-activity-concurrency` + Config field
- [ ] README note
- [ ] Smoke bench; PR `m5/task-3-activity-parallel-bench-readme`; merge

---

## Acceptance checklist

- [ ] Default sequential unchanged
- [ ] Concurrency > 1 works
- [ ] Bench + README
