# Fair Dispatch

[English] | [日本語](ja/08-fair-dispatch.md)

When a single large workflow spawns a burst of activity tasks in a short time window, it can monopolize the queue and delay tasks belonging to other workflows (Head-of-Line Blocking).  
`ClaimRequest.MaxPerInstance` places an upper bound on the number of tasks claimable per workflow instance in a single claim batch, preventing starvation.

## Mechanics

By default, task claims return tasks in order of `visible_at` (FIFO).  
Setting `MaxPerInstance` preserves FIFO ordering while enforcing that at most $N$ tasks from any single instance are returned in a given batch. Additional tasks from that instance remain in the queue for subsequent polling ticks.

```text
Queue: [flood-1, flood-2, flood-3, victim-1]   (FIFO order)

Limit=3, MaxPerInstance=0 -> flood-1, flood-2, flood-3   # victim is delayed
Limit=3, MaxPerInstance=1 -> flood-1, victim-1           # victim is admitted in the first batch
```

If the batch cannot be completely filled because remaining tasks belong to instances that reached their quota, the partial batch is returned immediately.  
Because excess tasks become claimable again on the next polling tick, high-volume instances are throttled to fair concurrency without blocking queue progress for other workflows.

To find runnable tasks when high-volume tasks block the queue head, candidates are scanned in FIFO order using pagination. The page window size is determined by `FairOverfetch(limit)`, scanning ahead until the batch is satisfied or candidates are exhausted.

## Usage

Configure on a per-worker basis:

```go
w := tasuki.NewWorker(b, tasuki.WorkerOptions{
    Queues:         []string{"default"},
    MaxPerInstance: 1,
})
```

When calling backend claim APIs directly (e.g. in custom batch executors), provide `ClaimRequest.MaxPerInstance`.

Supported backends: PostgreSQL, MySQL, SQLite, in-memory.  
*(Spanner, DynamoDB, and Firestore do not currently support fair dispatch; the parameter is ignored and claims fall back to standard FIFO).*

## Workload Isolation Strategies

While fair dispatch mitigates starvation, the primary tool for workload isolation is queue partitioning. Directing resource-intensive workflows to a dedicated queue isolates their impact entirely:

```go
// Direct heavy batch workloads to the dedicated "bulk" queue
tasuki.Start(ctx, c, BulkWorkflow, in, tasuki.WithQueue("bulk"))

// Run separate worker pools for standard and bulk queues
tasuki.NewWorker(b, tasuki.WorkerOptions{Queues: []string{"default"}})
tasuki.NewWorker(b, tasuki.WorkerOptions{Queues: []string{"bulk"}, ClaimLimit: 50})
```

Queue backlog can be tracked per queue using the `tasuki.tasks.backlog` metric ([05-observability.md](05-observability.md)) and `CountClaimableTasks`, making it easy to drive autoscaling for bulk workers.

Recommended practices:

- **Consistently Heavy Workloads**: Separate into dedicated queues with independent worker pools.
- **Occasionally Spiky Workloads**: Use `MaxPerInstance` on shared queues to cap burst consumption.
- **Differentiated Priorities**: Combine queue partitioning with allocated worker capacities.

## Note on Task-Level Priority Columns

While adding a per-task priority column (`ORDER BY priority DESC, visible_at`) is technically feasible, it is intentionally deferred:
- Priority schemes require mechanisms to prevent starvation of low-priority tasks (aging algorithms) alongside APIs for dynamic priority adjustment.
- Combining separate queues with `MaxPerInstance` cleanly addresses known production requirements without adding schema overhead.