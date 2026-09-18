# Observability

[English] | [日本語](ja/05-observability.md)

Reference dictionary of structured logs and OpenTelemetry metrics emitted by tasuki workers.

## Logging (slog)

Events emitted to `WorkerOptions.Logger` (defaults to `slog.Default()`):

| Event | Level | Key Attributes |
|---|---|---|
| Workflow task started | Debug | `instance_id`, `task_id`, `workflow` |
| Workflow completed | Info | `instance_id`, `status` |
| Workflow stuck | Warn | `instance_id`, `error` |
| Journal size warning | Warn | `instance_id`, `workflow`, `journal_events`, `threshold` |
| Activity task started | Debug | `instance_id`, `task_id`, `activity`, `attempt` |
| Activity retry scheduled | Info | `instance_id`, `activity`, `attempt`, `visible_at` |
| Activity permanent failure | Warn | `instance_id`, `activity`, `error` |
| Activity panic recovered | Warn | `panic` (and `activity` for local activities) |
| Store operation failed | Warn | `op`, `err` (and `instance_id`, etc.) |
| Store contention / shutdown canceled | Debug | `op`, `err` |
| Orphan task recovered | Info | `n` |
| Truncated fanout | Info | `instance_id`, `kept_commands`, `total_commands`, `budget` |
| Shutdown lease release failure / timeout | Warn | `task_id` / `remaining`, `error` |

## Metrics (OpenTelemetry)

Meter Name: `github.com/hirokazumiyaji/tasuki`

| Name | Type | Description |
|---|---|---|
| `tasuki.workflow.tasks` | Counter | Total processed workflow tasks |
| `tasuki.activity.tasks` | Counter | Total processed activity tasks |
| `tasuki.workflow.terminal` | Counter | Total workflow terminal transitions |
| `tasuki.activity.retries` | Counter | Total scheduled activity retries |
| `tasuki.workflow.journal_warnings` | Counter | Total occurrences of journal size warnings |
| `tasuki.tasks.backlog` | Gauge | Count of claimable tasks per queue (attributes `kind`, `queue`). Sampled at most once per `BacklogSampleInterval` (default 10s) |
| `tasuki.worker.incompatible_nacks` | Counter | Total Nacked tasks due to incompatible workers (attribute `reason`) |
| `tasuki.worker.store_errors` | Counter | Total failed store operations (attribute `op`: `fire_timers`, `claim_schedules`, `claim_workflow`, `claim_activity`, `commit_workflow`, `complete_activity`, `retry_activity`, `release_lease`, `extend_lease`, `recover_tasks`) |

Configure metrics by passing `observability.NewMetrics()` to `WorkerOptions.Metrics`. When unset, metrics recording is disabled (noop).  
If the global OpenTelemetry `MeterProvider` is not configured, the default noop meter is used.  
The backlog gauge is only recorded when `Metrics` is configured, calling `CountClaimableTasks` to inspect database queue depth (task execution proceeds even if sampling fails).
Sampling is throttled by `WorkerOptions.BacklogSampleInterval` (default 10s, coarser than the 1s `PollInterval` default); set a smaller positive value for fresher gauges or a negative value to disable sampling entirely.

## Alerting Criteria and Troubleshooting

- **Sustained increase in `tasuki.worker.store_errors{op="claim_workflow"|"claim_activity"}` + workflow stall**: Investigate database connectivity, credentials, or rate limits. Inspect `op` and `err` in the `store operation failed` logs and verify database availability and `TASUKI_*_DSN`.
- **Increase in `op="fire_timers"`**: Indicates timer table bloat or missing indexes. In PostgreSQL, check VACUUM status on `wf_timers(fire_at)`.
- **Increase in `op="commit_workflow"` without `ErrConflict`**: Indicates a persistent database write error. Normal concurrency contention (`ErrConflict` / `ErrSuperseded` in debug logs) is expected under load and should not trigger alerts.
- **Increase in `op="complete_activity"` or `"retry_activity"`**: Indicates database issues during activity completion. Distinguish from `activity failed` (which represents expected business failures).
- **Increase in `op="release_lease"` or `"extend_lease"`**: Indicates lease renewal failures during shutdown or within long-running activities. Timeouts exceeding `ShutdownReleaseTimeout` (default 5s) log warnings; the tasks will be safely recovered by peer workers upon lease expiry.
- **Increase in `op="recover_tasks"`**: Scan failure during orphan task recovery. Check permissions and throttling in DynamoDB / Firestore.
- Standard optimistic lock conflicts (`ErrConflict`, `ErrSuperseded`) and shutdown cancellations (`context.Canceled`) are logged at Debug level only and excluded from `store_errors`. Only persistent errors increment error counters, with labels bounded to the fixed `op` vocabulary.

## PostgreSQL Dead Tuples and Table Bloat

Tables `wf_tasks` and `wf_inbox` undergo high-frequency INSERT, UPDATE, and DELETE operations via `ClaimTasks` and `CompleteActivity`.  
In PostgreSQL's MVCC implementation, updates and deletes produce dead tuples. If autovacuum cannot keep pace, table and index bloat degrades query performance. Because `ClaimTasks` scans indexes via `FOR UPDATE SKIP LOCKED`, accumulated dead tuples manifest as degraded task claim latency.

tasuki applies the following reloptions to high-churn tables during `Migrate`:

```sql
-- Identical to backend/postgres/migrations/000002_vacuum_tuning.up.sql
ALTER TABLE wf_tasks SET (
    autovacuum_vacuum_scale_factor = 0.05,  -- Default 0.2: Trigger VACUUM at 5% dead tuples
    autovacuum_vacuum_cost_limit = 1000,    -- Default 200: Clear more dead tuples per round
    fillfactor = 80                          -- Reserve page space for index-free HOT updates
);
ALTER TABLE wf_inbox SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000);
ALTER TABLE wf_instances SET (autovacuum_vacuum_scale_factor = 0.05, fillfactor = 80);
ALTER TABLE wf_signal_dedupe SET (autovacuum_vacuum_scale_factor = 0.05);
```

Verify active settings in `pg_class`:

```sql
SELECT relname, reloptions FROM pg_class WHERE relname LIKE 'wf_%';
```

### Monitoring Queries

Execute monitoring queries via `postgres.Pool()` or database monitoring agents:

**Dead Tuple Ratio**: Review autovacuum settings if `dead_ratio` persistently exceeds 0.1 (10%):

```sql
SELECT relname, n_live_tup, n_dead_tup,
       n_dead_tup::float / GREATEST(n_live_tup, 1) AS dead_ratio,
       last_autovacuum
FROM pg_stat_user_tables
WHERE relname LIKE 'wf_%'
ORDER BY n_dead_tup DESC;
```

**Bloat Ratio**: Measure wasted space using the `pgstattuple` extension during troubleshooting:

```sql
CREATE EXTENSION IF NOT EXISTS pgstattuple;
SELECT * FROM pgstattuple('wf_tasks');
```

Track total relation sizes over time. While autovacuum reclaims space for new rows, already bloated tables do not shrink automatically on disk. If `pg_total_relation_size` grows monotonically without plateauing, schedule online table reorganization using `pg_repack` or `VACUUM FULL`.

Mitigate table growth long-term by combining automated retention purging ([07-retention.md](07-retention.md)) with `ContinueAsNew` for long-running workflows.
