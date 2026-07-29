# M5 Memory CommitAdvancements Design

**Date:** 2026-07-25  
**Status:** Approved (autonomous)  
**Parent:** [2026-07-25-m5-commit-batch-design.md](./2026-07-25-m5-commit-batch-design.md)

## Goal

Implement `AdvancementBatcher` on the memory backend so worker batch flush uses one lock for the whole wave.

## Approach

- Extract `commitAdvancementLocked` (caller holds `b.mu`)
- `CommitAdvancements`: under one lock, **preflight** all (instance exists, ExpectedSeq, workflow task), then apply all via `commitAdvancementLocked`
- Preflight-then-apply gives all-or-nothing for CAS/task conflicts (matches postgres tests)

## Acceptance

- [x] `CommitAdvancements` on memory
- [x] Batch OK + conflict leaves both running
