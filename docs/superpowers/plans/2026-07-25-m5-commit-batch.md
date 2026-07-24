# M5 Commit Batching Implementation Plan

> Inline. One PR per task. `[skip ci]` on commits/merges.

**Goal:** Collect workflow advancements per tick and flush via optional `AdvancementBatcher` (postgres one txn).

**Spec:** [../specs/2026-07-25-m5-commit-batch-design.md](../specs/2026-07-25-m5-commit-batch-design.md)

### Task 1: Spec + Plan docs

### Task 2: Interface + postgres `CommitAdvancements`

- Add `backend.AdvancementBatcher`
- Refactor postgres `CommitAdvancement` → `applyAdvancement` + `CommitAdvancements`
- Test: two instances batch commit; conflict rolls back both

### Task 3: Worker collect-then-flush

- `handleWorkflow` returns pending commit
- `tick` collects, `flushWorkflowCommits`
- Existing worker tests green

### Task 4: README note
