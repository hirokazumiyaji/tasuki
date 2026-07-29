# M5 SQL CommitAdvancements (sqlite + mysql)

**Date:** 2026-07-25  
**Status:** Approved (autonomous)  
**Parent:** [2026-07-25-m5-commit-batch-design.md](./2026-07-25-m5-commit-batch-design.md)

## Goal

Implement `backend.AdvancementBatcher` on sqlite and mysql so worker collect-then-flush batches without falling back to per-advancement transactions.

## Approach

Reuse existing `commitAdvancementConn`. Wrap N advancements in one `withTx`. `CommitAdvancement` delegates to batch-of-one.

MySQL: keep post-commit `ensureWorkflowTaskIfInbox` second pass for each instance in the batch (I1 / TiDB).

## Verification

- sqlite tempfile: batch OK + conflict rolls back both
- mysql (when `TASUKI_MYSQL_DSN` set): same

## Acceptance

- [x] Both implement `CommitAdvancements`
- [x] Worker already uses batcher when len>1
- [x] Tests as above
