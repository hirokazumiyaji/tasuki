# M5 Workflow Parallelism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Bounded parallel `handleWorkflow` via `WorkflowConcurrency` (default 1) with per-instance mutex + bench flag.

**Architecture:** Mirror activity parallelism; add instance mutex map; workflows wait before activities in tick.

**Tech Stack:** Existing Worker / bench.

**Spec:** [docs/superpowers/specs/2026-07-24-m5-workflow-parallel-design.md](../specs/2026-07-24-m5-workflow-parallel-design.md)

## Global Constraints

- Default WorkflowConcurrency = 1
- Same instance_id serialized in-process
- Every commit / merge subject includes `[skip ci]`

---

### Task 1: Spec + Plan

- [ ] Docs + todo; PR; merge

### Task 2: WorkerOptions + tick + instance mutex

**Files:** `options.go`, `worker.go` (or `worker_parallel.go` for mutex helpers)

- [ ] `WorkflowConcurrency` default 1
- [ ] `instanceMu(id) *sync.Mutex` helper
- [ ] Parallel workflow claim loop; then existing activity loop
- [ ] `go test . ./bench/ -count=1`
- [ ] PR; merge

### Task 3: Bench + README

- [ ] `-workflow-concurrency` + Config
- [ ] README; mark todos done; PR; merge

---

## Acceptance checklist

- [ ] Default sequential unchanged
- [ ] Concurrency > 1 + instance mutex
- [ ] Bench + README
