# Data Retention

[English] | [日本語](ja/07-retention.md)

In long-running production systems, completed workflow instances and their journals accumulate over time, consuming storage. `Backend.PurgeInstances` provides an administrative interface to bulk-delete terminated instances and their associated data.

## API

```go
n, err := backend.PurgeInstances(ctx, olderThan time.Duration, statuses []string, limit int) (int, error)
```

Parameters:

- **`olderThan`**: Only instances whose completion timestamp is older than `now - olderThan` are eligible for deletion.
- **`statuses`**: Target terminal statuses to purge. If empty (nil), defaults to `{completed, failed, terminated, canceled}`. `continued` can also be specified.
- **`limit`**: Maximum number of instances to delete in a single invocation. If $\le 0$, defaults to `backend.DefaultPurgeLimit` (1000).

The return value `n` is the number of instances successfully deleted.

Active instances (`running` and `stuck`) are strictly excluded from deletion. Passing these active statuses in `statuses` returns an error, preventing accidental deletion of active workflows due to misconfiguration.

## Deleted Data

For each purged instance, all corresponding rows/items are removed:

| Table / Collection | Description |
|---|---|
| `wf_instances` | Core instance state and metadata |
| `wf_journal` | Historical execution journal events |
| `wf_inbox` | Pending inbox messages and signals |
| `wf_tasks` | Lingering tasks (e.g. late-arriving activity completions) |
| `wf_timers` | Timers scheduled for future firing |
| `wf_signal_dedupe` | Signal deduplication keys |

SQL-based backends (PostgreSQL, MySQL, SQLite, Spanner) execute purges within a single transaction per batch. If a batch fails midway, all changes in that batch are rolled back.

Because DynamoDB and Firestore lack multi-table distributed transactions across disparate entities, purges are performed on a best-effort, per-instance basis. If an operation fails midway, orphan rows may remain for that instance. If strict cleanup is required in document stores, archiving data prior to purging is recommended.

## Operational Pattern

`PurgeInstances` should be executed periodically from a dedicated background job or cron process, separated from worker execution loops:

```go
func retentionLoop(ctx context.Context, b backend.Backend) error {
    ticker := time.NewTicker(time.Hour)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            for {
                n, err := b.PurgeInstances(ctx, 30*24*time.Hour, nil, backend.DefaultPurgeLimit)
                if err != nil {
                    return err
                }
                if n < backend.DefaultPurgeLimit {
                    break // All eligible instances have been purged
                }
            }
        }
    }
}
```

As long as `n == limit`, more eligible instances remain. Repeat the call in batches to prevent transaction timeouts or lock saturation.

## Impact

Regularly purging terminated instances prevents `wf_instances` and `wf_journal` tables and their indexes from expanding indefinitely. Because query performance for operations like `ListInstances` depends on table cardinality, retention policies should be calibrated alongside storage monitoring (see [05-observability.md](05-observability.md)).
