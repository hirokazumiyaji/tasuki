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

## Fanout Advancement Strategy

When a suspended workflow advancement exceeds the configured budget, the Worker commits only the prefix of new commands that fits within the budget, deferring the remainder to the next turn's replay. This ensures intermediate states remain strictly consistent between the journal and task tables.

An immediate follow-up workflow task is guaranteed via `EnsureWorkflowTask`, committed atomically within the same transaction where supported (e.g. in-place singleton task updates on DynamoDB; upserts in Firestore/SQL). Even if the process crashes immediately after committing, no deferred replay commands are lost, and upcoming activity completions trigger follow-up turns to keep execution moving forward.

- A fan-out of 50 `ExecuteAsync` calls (~102 DynamoDB operations) is split across approximately 2 commits.
- A fan-out of 100 calls is similarly split across turns without duplicating scheduled events or tasks (idempotency is guaranteed by journal `attribute_not_exists` conditions and prefix matching during replay).
- In mixed workloads with inbox, timer, and child operations, the same formula applies. If undrained inbox messages alone exceed the budget, the drain cannot be truncated and triggers a diagnostic error (adjust `MaxPerInstance` or inbox batch sizes).

## Operations Exceeding Budgets

- If an advancement containing a terminal transition (completed, failed, canceled, continued_as_new, or stuck) exceeds the budget, it cannot be truncated and returns a diagnostic error (e.g., `tasuki: terminal advancement needs 120 ops, budget 80`). Workers validate terminal turns in advance. Workflows must structure fan-outs to fit within single-tick budgets or delegate large batches to child workflows.
- If `SendToInboxBatch` exceeds the budget, it returns `ErrBatchTooLarge` (`InboxBatchLimit` = `MaxAdvancementEffects / 4`, or 100 when unlimited).
- A combined `CommitAdvancements` batch whose total DynamoDB operations exceed the 100-item transaction limit is rejected with a sizing error before anything is applied (never partially): split the batch or reduce per-advancement fanout. Firestore/Spanner commit batches in a single transaction and fail atomically without a sequential fallback.

## Scan Costs and Considerations (DynamoDB)

`ListInstances` in DynamoDB performs table scans. While sorting over $M$ matching instances is optimized to $O(M \log M)$ in-memory, the underlying scan consumes read capacity units (RCU) across the entire table. Even requests with small `Limit` parameters scan underlying items. Avoid frequent full-table list queries in large-scale production DynamoDB deployments without dedicated secondary indexes.
