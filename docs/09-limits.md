# Backend Limits and Budgets

[English] | [日本語](ja/09-limits.md)

Atomicity budgets per storage backend and behavior when operation limits are exceeded.

## `MaxAdvancementEffects`

`backend.Capabilities.MaxAdvancementEffects` indicates the maximum number of state transition effects that can be committed atomically in a single workflow advancement. A value of 0 indicates unlimited (applicable to in-memory, PostgreSQL, MySQL, SQLite, and Spanner).

| Backend | Limit | Rationale |
|---|---|---|
| DynamoDB | 80 | Safety margin against the 100-item `TransactWriteItems` limit (instance CAS 1 + task delete 1 + 18 reserved buffer) |
| Firestore | 400 | Safety margin against the 500-operation transaction limit |
| Others | 0 (unlimited) | Atomicity guaranteed by single SQL transaction |

Operation Count Calculation (DynamoDB equivalent):

```
2 (instance CAS + task delete)
+ len(NewEvents) (journal puts)
+ len(ActivityTasks) (task puts)
+ len(Timers)
+ len(DrainedInbox) (inbox deletes; ingested events are counted in NewEvents, totaling 2 ops per inbox event)
+ 3 * len(Children) (child instance, journal event, task)
+ (ParentNotify ? 1 : 0)
```

Signal-dedupe rows (`wf_signal_dedupe`) are intentionally **not** counted above.

## Signal Dedupe and Terminal Cleanup (Firestore / Spanner)

`wf_signal_dedupe` accumulates one row per `DedupeID` for the life of the instance. Deleting one row per key inside the committing transaction would make terminal commits scale with history: an instance with 600 dedupe keys needs 600+ writes, breaching Firestore's 500-write transaction limit (budget 400) and stressing Spanner's commit mutation limit. The same applies to `TerminateInstance` sweeping `wf_tasks` / `wf_timers` / `wf_signal_dedupe` in one commit.

- Terminal advancements commit only the bounded effects (instance status, journal puts, activity/timer puts, inbox deletes, children, parent notify, one task delete) inside the transaction. Dedupe keys are swept **after commit** in paged batches: Firestore deletes `wf_signal_dedupe` with 400-doc `Batch` commits in a `Limit` loop, Spanner deletes in 500-key paged read-write transactions, DynamoDB already paged post-commit. The sweep is best-effort; anything left by a crash is reaped by retention purge. Terminal instances are immutable, so the non-transactional sweep cannot race with advancement commits.
- `TerminateInstance` flips `status` to `terminated` in one small transaction (one read + one update), then awaits a paged sweep of `wf_tasks` / `wf_timers` / `wf_signal_dedupe` before returning, so `SendToInbox` with a previously seen `DedupeID` correctly inserts anew and claimed tasks observe no leftovers. Inbox/journal rows (if any) are left for purge.
- Spanner `PurgeInstances` works per instance in paged transactions: each child table (`wf_tasks`, `wf_timers`, `wf_signal_dedupe`, `wf_inbox`, `wf_journal`) is swept in 500-key `LIMIT` pages, the `wf_inbox_seq` + `wf_instances` rows go last, and a second child sweep reaps writers that committed between the first sweep and the instance delete.
- Compliance: `backendtest` `TerminalLargeDedupe` seeds 600 dedupe rows (over the 500 cap) and asserts both `TerminateInstance` and a terminal `CommitAdvancement` succeed and clear dedupe keys.

## Fanout Advancement Strategy

When a suspended workflow advancement exceeds the configured budget, the Worker commits only the prefix of new commands that fits within the budget, deferring the remainder to the next turn's replay. This ensures intermediate states remain strictly consistent between the journal and task tables.

An immediate follow-up workflow task is guaranteed via `EnsureWorkflowTask`, committed atomically within the same transaction where supported (e.g. in-place singleton task updates on DynamoDB; upserts in Firestore/SQL). Even if the process crashes immediately after committing, no deferred replay commands are lost, and upcoming activity completions trigger follow-up turns to keep execution moving forward.

- A fan-out of 50 `ExecuteAsync` calls (~102 DynamoDB operations) is split across approximately 2 commits.
- A fan-out of 100 calls is similarly split across turns without duplicating scheduled events or tasks (idempotency is guaranteed by journal `attribute_not_exists` conditions and prefix matching during replay).
- In mixed workloads with inbox, timer, and child operations, the same formula applies. If undrained inbox messages alone exceed the budget, the drain cannot be truncated and triggers a diagnostic error (adjust `MaxPerInstance` or inbox batch sizes).

## Operations Exceeding Budgets

- If an advancement containing a terminal transition (completed, failed, canceled, continued_as_new, or stuck) exceeds the budget, it cannot be truncated and returns a diagnostic error (e.g., `tasuki: terminal advancement needs 120 ops, budget 80`). Workers validate terminal turns in advance. Workflows must structure fan-outs to fit within single-tick budgets or delegate large batches to child workflows.
- If `SendToInboxBatch` exceeds the budget, it returns `ErrBatchTooLarge` (`InboxBatchLimit` = `MaxAdvancementEffects / 4`, or 100 when unlimited).

## Scan Costs and Considerations (DynamoDB)

`ListInstances` in DynamoDB performs table scans. While sorting over $M$ matching instances is optimized to $O(M \log M)$ in-memory, the underlying scan consumes read capacity units (RCU) across the entire table. Even requests with small `Limit` parameters scan underlying items. Avoid frequent full-table list queries in large-scale production DynamoDB deployments without dedicated secondary indexes.
