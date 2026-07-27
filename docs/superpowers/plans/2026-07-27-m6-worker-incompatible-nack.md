# M6 Worker Incompatible-Task Nack Implementation Plan

> Inline execution. One commit + PR per task. Every commit/merge subject includes `[skip ci]`.

**Goal:** Nack incompatible workflow/activity tasks instead of terminal stuck / activity fail during rolling deploys.

**Spec:** [docs/superpowers/specs/2026-07-27-m6-worker-incompatible-nack-design.md](../specs/2026-07-27-m6-worker-incompatible-nack-design.md)

### Task 1: Spec + Plan
### Task 2: `Backend.NackTask` on all stores + backendtest
### Task 3: Worker options + incompatible paths + integration tests
### Task 4: Docs (`03-api`, `04-plan`, README) + metrics hook

## Notes

- `NackTask(ctx, taskID, visibleAt)`: any kind; set `visible_at`, clear `worker_id`.
- `IncompatibleRetryDelay`: `0` → default 5s; `< 0` → immediate.
- Nack on: unregistered workflow, determinism stuck, unregistered activity.
- Optional: `RetryActivity` delegates to `NackTask` after kind check.
