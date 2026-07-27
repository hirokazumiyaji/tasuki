# M6 Signal Dedupe ID Implementation Plan

> Inline execution. One commit + PR per task. Every commit/merge subject includes `[skip ci]`.

**Goal:** Optional per-instance signal dedupe via `WithDedupeID` and `wf_signal_dedupe`.

**Spec:** [docs/superpowers/specs/2026-07-27-m6-signal-dedupe-design.md](../specs/2026-07-27-m6-signal-dedupe-design.md)

### Task 1: Spec + Plan
### Task 2: Backend `SendToInbox(..., dedupeID)` + memory + Client `WithDedupeID` + tests
### Task 3: SQL (postgres / sqlite / mysql) + Spanner schema and paths
### Task 4: DynamoDB + Firestore
### Task 5: Docs (`03-api`, `02-architecture`, `04-plan`, README)

## Notes

- Signature: `SendToInbox(ctx, instanceID, ev, dedupeID string)`; empty `dedupeID` = legacy.
- Update all call sites (`client.Signal` / `Cancel`, `backendtest`).
- On unique conflict: return `nil`, no inbox insert, no extra wakeup.
- Delete `wf_signal_dedupe` rows for the instance on terminal (`CommitAdvancement` terminal + `TerminateInstance`).
