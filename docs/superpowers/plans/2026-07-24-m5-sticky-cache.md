# M5 Sticky Journal Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Per-Worker sticky journal cache with `LoadWorkflowHead` + `GetJournal(afterSeq)` to cut full journal reads.

**Architecture:** Required `Backend.LoadWorkflowHead`; Worker map keyed by instance ID validated with `next_seq`; full reload on inconsistency.

**Tech Stack:** Existing backends + Worker; conform via `backendtest`.

**Spec:** [docs/superpowers/specs/2026-07-24-m5-sticky-cache-design.md](../specs/2026-07-24-m5-sticky-cache-design.md)

## Global Constraints

- Sticky is per-Worker only; default on
- Inbox + Now always from `LoadWorkflowHead`
- Invalidate on conflict / terminal / seq anomaly
- Every commit / merge subject includes `[skip ci]`
- One PR per task; merge before next

---

## File structure

| Path | Responsibility |
|---|---|
| `backend/backend.go` | Declare `LoadWorkflowHead` |
| `backend/*/…` | Implement Head (all stores) |
| `backendtest/` | Assert Head vs LoadWorkflow fields |
| `worker.go` / `sticky.go` | Cache + load helper |
| `README.md` | Note |
| `.claude/tasks/todo.md` | Checklist |

---

### Task 1: Spec + Plan

- [ ] Commit docs + todo; PR `m5/task-1-sticky-spec`; merge

---

### Task 2: `LoadWorkflowHead` interface + memory + postgres

**Files:** `backend/backend.go`, `backend/memory/memory.go`, `backend/postgres/backend.go`, `backendtest` helper or suite addition

- [ ] Add method to interface
- [ ] memory + postgres implementations (extract from LoadWorkflow)
- [ ] Test: Head.Journal empty; Inbox/NextSeq/Now/Instance match LoadWorkflow
- [ ] PR `m5/task-2-sticky-head-memory-pg`; merge

---

### Task 3: Remaining backends (sqlite, mysql, spanner, dynamodb, firestore)

- [ ] Implement `LoadWorkflowHead` on each (prefer DRY with LoadWorkflow)
- [ ] Smoke/conform still compile; run available conform tests
- [ ] PR `m5/task-3-sticky-head-stores`; merge

---

### Task 4: Worker sticky cache

**Files:** `sticky.go` (or inside `worker.go`), `worker.go` `handleWorkflow`

- [ ] Cache map + mutex on Worker
- [ ] `loadWorkflowState(ctx, id) (*WorkflowState, error)` implementing spec algorithm
- [ ] Update cache after successful commit; delete on conflict/terminal
- [ ] `go test . ./bench/ -count=1`
- [ ] PR `m5/task-4-sticky-worker`; merge

---

### Task 5: README + todo complete + postgres/memory verification

- [ ] README note
- [ ] Mark todos done
- [ ] PR `m5/task-5-sticky-docs`; merge

---

## Acceptance checklist

- [ ] All backends implement `LoadWorkflowHead`
- [ ] Worker uses sticky path
- [ ] Conform green (memory + postgres minimum)
- [ ] README updated
