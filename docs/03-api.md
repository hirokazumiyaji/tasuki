# Public API Design

[English] | [日本語](ja/03-api.md)

This document defines the public API of tasuki from the perspective of library consumers.  
The module path is `github.com/hirokazumiyaji/tasuki`, and the root package name is `tasuki`.

## Design Principles

- **Compile-Time Type Safety via Generics**: The public API does not expose `interface{}` or `any`. Input and output types are verified at compile time.
- **Dedicated Workflow Context**: Workflows accept `*workflow.Context` instead of standard `context.Context`. This type separation turns accidental calls to blocking I/O functions (which require `context.Context`) into compile errors.
- **Package Functions for Parameterized Operations**: Because Go does not support generic methods on structs, generic operations such as `Execute` and `Start` are provided as package-level functions.
- **Pluggable Persistence**: The storage backend is abstracted behind `backend.Backend`. Workflow and activity code is completely decoupled from database specifics and requires no changes when switching backends.

## Minimal Configuration Example

```go
package main

import (
    "context"
    "time"

    "github.com/hirokazumiyaji/tasuki"
    "github.com/hirokazumiyaji/tasuki/activity"
    "github.com/hirokazumiyaji/tasuki/backend/postgres"
    "github.com/hirokazumiyaji/tasuki/workflow"
    "github.com/jackc/pgx/v5/pgxpool"
)

type OrderInput struct{ OrderID string }
type OrderResult struct{ InvoiceID string }

type ChargeInput struct{ OrderID string }
type ChargeResult struct {
    InvoiceID     string
    CustomerEmail string
}
type ShipInput struct{ OrderID string }
type ShipResult struct{ TrackingID string }
type MailInput struct{ To string }

// Workflow: Contains strictly deterministic orchestration logic.
func OrderWorkflow(ctx *workflow.Context, in OrderInput) (OrderResult, error) {
    charge, err := workflow.Execute[ChargeInput, ChargeResult](ctx, "ChargePayment", ChargeInput{OrderID: in.OrderID},
        workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 5}))
    if err != nil {
        return OrderResult{}, err
    }

    // Execute shipping and email notification concurrently
    ship := workflow.ExecuteAsync[ShipInput, ShipResult](ctx, "ShipOrder", ShipInput{OrderID: in.OrderID})
    mail := workflow.ExecuteAsync[MailInput, struct{}](ctx, "SendReceiptMail", MailInput{To: charge.CustomerEmail})
    if _, err := ship.Get(ctx); err != nil {
        return OrderResult{}, err
    }
    if _, err := mail.Get(ctx); err != nil {
        return OrderResult{}, err
    }

    // Sleep durably for 7 days until follow-up
    if err := workflow.Sleep(ctx, 7*24*time.Hour); err != nil {
        return OrderResult{}, err
    }
    if _, err := workflow.Execute[MailInput, struct{}](ctx, "SendFollowUpMail", MailInput{To: charge.CustomerEmail}); err != nil {
        return OrderResult{}, err
    }
    return OrderResult{InvoiceID: charge.InvoiceID}, nil
}

// Activity: Encapsulates side effects. Implemented idempotently for at-least-once execution.
func ChargePayment(ctx context.Context, in ChargeInput) (ChargeResult, error) {
    key := activity.GetInfo(ctx).IdempotencyKey
    return paymentClient.Charge(ctx, in.OrderID, key)
}

func main() {
    ctx := context.Background()
    pool, _ := pgxpool.New(ctx, "postgres://...")

    w := tasuki.NewWorker(postgres.NewBackend(pool), tasuki.WorkerOptions{})
    tasuki.RegisterWorkflow(w, OrderWorkflow)
    tasuki.RegisterActivity(w, ChargePayment)
    tasuki.RegisterActivity(w, ShipOrder)
    tasuki.RegisterActivity(w, SendReceiptMail)
    tasuki.RegisterActivity(w, SendFollowUpMail)
    w.Start(ctx)
    defer w.Shutdown(ctx)

    // Starting workflows can be performed from any process (Clients do not require workers)
    c := tasuki.NewClient(postgres.NewBackend(pool))
    h, _ := tasuki.Start(ctx, c, OrderWorkflow, OrderInput{OrderID: "order-123"},
        tasuki.WithID("order-123"))
    res, _ := h.Result(ctx)
    _ = res
}
```

## Workflow In-Scope API

Functions provided by the `workflow` package:

| Function | Description |
|---|---|
| `Execute[I, O](ctx, fn, in, opts...) (O, error)` | Runs an activity and waits for completion (`WithRetry`, `WithStartToCloseTimeout`) |
| `ExecuteLocal[I, O](ctx, name, in) (O, error)` | Executes an activity synchronously on the same worker, recording results directly in the journal (no task queue, no retry) |
| `ExecuteAsync[I, O](ctx, fn, in, opts...) *Future[O]` | Schedules an activity asynchronously and returns a `Future` |
| `ExecuteChild[I, O](ctx, wf, in, opts...) (O, error)` | Runs a child workflow and waits for completion (`ExecuteChildAsync` also available) |
| `Sleep(ctx, d) error` / `SleepUntil(ctx, t) error` | Suspends execution using a durable timer |
| `SleepAsync(ctx, d) *Future[struct{}]` | Starts a durable timer as a `Future` (useful for Select timeouts) |
| `Now(ctx) time.Time` | Returns the recorded current time that remains constant across replays |
| `ReceiveSignal[T](ctx, name) (T, error)` | Waits for a signal with the specified name |
| `ReceiveSignalWithTimeout[T](ctx, name, d) (T, bool, error)` | Waits for a signal with a duration timeout (second return value indicates receipt) |
| `SideEffect[T](ctx, fn) (T, error)` | Executes a non-deterministic value generator once and records the result in the journal |
| `NewUUID(ctx) (string, error)` | Returns a deterministic UUID (syntactic sugar over `SideEffect`) |
| `Await(ctx, futures...) (int, error)` | Waits until any of the provided futures complete, returning the index of the first completion |
| `AwaitAll(ctx, futures...) error` | Waits until all futures complete, returning the first encountered error |
| `GetVersion(ctx, changeID, min, max) int` | Evaluates version branches to support backward-compatible code evolution on running instances |
| `ContinueAsNew[I](ctx, in) error` | Ends the current instance and restarts execution with a fresh history (used as a return error) |
| `Info(ctx) WorkflowInfo` | Returns instance metadata (ID, workflow name, start time) |
| `SetQueryHandler[I, O](ctx, name, fn)` | Registers a read-only query handler (queries leave no journal events) |
| `SetUpdateHandler[I, O](ctx, name, fn)` | Registers a synchronous update handler for running instances (`Execute` and `Sleep` are permitted) |
| `UpsertSearchAttributes(ctx, attrs)` | Merges string key-value attributes for instance filtering (empty values remove keys) |
| `UpsertMemo(ctx, attrs)` | Merges display annotations for instance inspection (empty values remove keys) |

`SetQueryHandler` must be registered at a deterministic position during replay. Query handlers must never record new commands (no `Execute` or `Sleep`).

`SetUpdateHandler` must similarly be registered at a deterministic position. Handlers receive `*workflow.Context` and may call standard workflow operations such as `Execute` and `Sleep`. Updates are triggered using `tasuki.Update` (within the same worker process) and support optional `WithUpdateID` for idempotent resends. While an update turn is processing, the main workflow routine does not advance.

`UpsertSearchAttributes` is recorded as a command event in the journal, with the full merged attribute map as its payload. Calling it during query execution is rejected.

`UpsertMemo` is similarly recorded as a command event, but memo fields are intended for display metadata and are not indexed for `List` filtering.

`ExecuteLocal` runs registered activities synchronously within the workflow task turn. Unlike regular activities, it does not enqueue an activity task and does not perform retries. It is ideal for short, highly reliable operations. The result or error is recorded directly as a `local_activity` event, and the runner function is skipped during replay.

The worker extends the workflow task lease (`ExtendLease` at half the `LeaseDuration`) while the turn runs, so long replays and local activities that exceed `LeaseDuration` are not reclaimed by a peer and executed twice. Renewal covers the turn through the batch commit, not just `handleWorkflow`. Local activities run with the worker turn's `context.Context`, so `Shutdown` cancels a running local activity and the canceled turn is abandoned (lease released for a peer) instead of being committed as a workflow failure. Set `WorkerOptions.LocalActivityTimeout` to bound a single `ExecuteLocal` invocation: the timeout is enforced outside the call, so even an activity that ignores cancellation returns a deadline error on time (its late result is discarded).

For long-running or looping workflows, calling `ContinueAsNew` when event counts reach thousands is strongly recommended to bound history size.

`Future[O]` exposes `Get(ctx) (O, error)`. All futures implement the non-generic `Awaitable` interface, allowing heterogeneous types to be awaited together in `Await`.

### Concurrency Model

A workflow function executes sequentially within a single goroutine per task turn (see [02-architecture.md](02-architecture.md)).  
What executes concurrently are external activities and child workflows. Concurrency in workflow logic is expressed using `ExecuteAsync`, `Await`, and `AwaitAll`. Workflows must not spawn goroutines (`go` statements) or use channels. For larger units of independent concurrency, split execution into child workflows.

Concentrating high-frequency events onto a single workflow instance serializes execution and creates a throughput bottleneck (hot instances). Use key-based partitioning or child workflows instead (see the "Hot Instances and Partitioning Guidelines" section in [02-architecture.md](02-architecture.md)).

Timeouts for asynchronous operations can be expressed by combining a task future with a timer future in `Await`:

```go
f := workflow.ExecuteAsync[SlowInput, SlowOutput](ctx, "CallSlowAPI", in)
t := workflow.SleepAsync(ctx, 10*time.Minute)
idx, err := workflow.Await(ctx, f, t)
if err != nil {
    return out, err
}
if idx == 1 {
    return out, ErrAPITimeout // Timer finished first
}
```

### Cancellation Handling

Once a cancellation request is ingested from the inbox, future blocking calls (`Sleep`, `Future.Get`, `ReceiveSignal`, `Await`) immediately return `workflow.ErrCanceled`.  
`Execute` remains callable after cancellation to allow workflows to run compensation activities (e.g., issuing refunds or cleaning up resources) before exiting. When the workflow returns, if the returned error wraps `workflow.ErrCanceled`, the instance transitions to `canceled`; otherwise, it transitions to standard `completed` or `failed`.

### `GetVersion` and Code Evolution

Modifying the sequence of commands in code paths already traversed by active instances causes determinism violations. Use `GetVersion` to introduce branching changes safely:

```go
v := workflow.GetVersion(ctx, "add-fraud-check", 1, 2)
if v >= 2 {
    if _, err := workflow.Execute[CheckInput, CheckResult](ctx, "FraudCheck", in); err != nil {
        return out, err
    }
}
```

`GetVersion` operates according to the following rules:
- Upon first execution, it records `max` as a `version_marker` in the journal and returns that value in subsequent replays.
- During replays of code paths traversed before the change (where no marker exists), it returns `min`.
- `version_marker` events are ignored by older workers that predate the marker. If the resulting branch generates different commands, subsequent command matching safely catches the violation.

During rolling deployments, older workers may encounter tasks containing newer history events. Rather than failing the instance or marking it `stuck`, workers Nack the task, delaying its visibility (`IncompatibleRetryDelay`, default 5s) so a newer worker can claim it.

## Determinism Constraints

Workflow functions must produce the exact same sequence of commands given the same execution history.

| Forbidden | Permitted Alternative |
|---|---|
| `time.Now()`, `time.Since()` | `workflow.Now(ctx)` |
| `time.Sleep()`, `time.After()` | `workflow.Sleep()`, `workflow.SleepAsync()` |
| `rand`, UUID generation | `workflow.SideEffect()`, `workflow.NewUUID()` |
| `go` statements, channels, `sync` package | `ExecuteAsync` + `Await`, Child workflows |
| Map iteration order dependencies | Sort map keys before iterating |
| Network, filesystem, or database I/O | Move logic into activities |
| Global mutable state, reading environment variables | Pass via workflow input or `SideEffect` |
| Side effects inside `defer` statements | Only pure logic allowed; defers run on every suspension |

While I/O and goroutines are structurally prevented by the type system via `*workflow.Context`, time calls and map iteration cannot be prevented by types alone. A `go vet`-compatible static analyzer is provided (`analyzers/determinism`). Any runtime violation that bypasses static analysis is detected during replay command matching, safely quarantining the instance into `stuck`.

## Retries and Errors

Activity retry behavior is configured via `RetryPolicy`:

```go
type RetryPolicy struct {
    InitialInterval    time.Duration // Default: 1s
    BackoffCoefficient float64       // Default: 2.0
    MaxInterval        time.Duration // Default: 1m
    MaxAttempts        int           // Default: 0 (unlimited)
}
```

By default, retries are unlimited (matching Temporal's convention) so that transient outages do not fail workflows. To bound retries, configure `MaxAttempts` or `WithStartToCloseTimeout`.

`WithStartToCloseTimeout(d)` sets a maximum execution duration for a **single attempt** (`d <= 0` disables the limit). If an attempt exceeds this duration, it fails with `"activity start-to-close timeout"`, triggering standard retry backoff according to the `RetryPolicy`. The worker cancels the `context.Context` passed to the activity.

Non-retriable errors (such as invalid user input) should be wrapped with `tasuki.NonRetryable(err)`. The worker immediately halts retries and returns the error to the workflow.

## Activity Definition and Idempotency

Activities are standard Go functions taking a `context.Context`:

```go
func ChargePayment(ctx context.Context, in ChargeInput) (ChargeResult, error)
```

Execution metadata is retrieved using `activity.GetInfo(ctx)`:

```go
type Info struct {
    InstanceID     string
    ActivityName   string
    Attempt        int    // 1-indexed
    TaskID         int64
    IdempotencyKey string // Derived from InstanceID and schedule sequence; stable across retries
}
```

Long-running activities can invoke `activity.RecordHeartbeat(ctx, details)` to extend their lease and record progress. On retry, previous progress can be retrieved using `activity.GetHeartbeatDetails(ctx, &dest)` (returns `activity.ErrNoDetails` if none exists).

Because activities follow an **at-least-once** execution contract, side effects must be idempotent. Use `IdempotencyKey` as an external idempotency key or database unique constraint.

## Client API

```go
c := tasuki.NewClient(backend)

h, err := tasuki.Start(ctx, c, OrderWorkflow, in, tasuki.WithID("order-123"))
h, err := tasuki.Start(ctx, c, OrderWorkflow, in, tasuki.WithID("order-123"),
    tasuki.WithSearchAttributes(map[string]string{"tenant": "acme", "order_id": "42"}))
h, err := tasuki.Start(ctx, c, OrderWorkflow, in, tasuki.WithID("order-123"),
    tasuki.WithMemo(map[string]string{"note": "vip"}))

res, err := h.Result(ctx)                    // Awaits terminal completion
err = c.Signal(ctx, "order-123", "approve", payload)
err = c.Signal(ctx, "order-123", "approve", payload, tasuki.WithDedupeID("pay-42"))
err = c.SignalBatch(ctx, "order-123", []tasuki.SignalItem{
    {Name: "approve", Payload: payload, DedupeID: "pay-42"},
    {Name: "note", Payload: note},
})
err = c.Cancel(ctx, "order-123")             // Cooperative cancellation
err = c.Terminate(ctx, "order-123")          // Immediate termination
info, err := c.Get(ctx, "order-123")         // Status, result, failure, search attributes, memo
events, err := c.GetJournal(ctx, "order-123") // Execution history
list, err := c.List(ctx, tasuki.InstanceFilter{Status: tasuki.StatusStuck})
list, err := c.List(ctx, tasuki.InstanceFilter{
    Status: tasuki.StatusRunning,
    SearchAttributes: map[string]string{"tenant": "acme"},
})
```

- `WithSearchAttributes` attaches string metadata at start time. `List` filters search attributes using exact-match AND queries.
- `WithMemo` attaches arbitrary display metadata visible in `Get` (not filtered in `List`).
- `Signal` with `WithDedupeID` prevents duplicate delivery of the same signal identifier within an instance.
- `SignalBatch` atomically delivers multiple signals to an instance.
- `Start` is idempotent on instance ID: if an instance with the given ID already exists, it returns `tasuki.ErrAlreadyStarted` along with a valid handle to the existing instance.

### Query

To inspect derived state from running or completed workflows without mutating history, use `tasuki.Query`:

```go
out, err := tasuki.Query[struct{}, int](ctx, w, "order-123", "count", struct{}{})
```

Queries replay history in-memory up to the current point and invoke the registered query handler without claiming tasks or updating sequence numbers.

### Update

To send synchronous request-response mutations to running instances, use `tasuki.Update`:

```go
out, err := tasuki.Update[ReviseIn, ReviseOut](ctx, w, "order-123", "revise", in,
    tasuki.WithUpdateID("rev-42"))
```

Updates enqueue an `update_requested` event to the inbox. The worker executes the registered `SetUpdateHandler`, which can invoke activities or sleep. Completed updates commit as `update_completed` events.

## Registration and Naming

Workflow and activity names are stored in the database and serve as matching keys during replay. By default, names are derived from function reflection (e.g., `OrderWorkflow`). In production, explicit names are recommended to safeguard against accidental refactoring breakages:

```go
tasuki.RegisterWorkflow(w, OrderWorkflow, tasuki.WithName("order"))
```

## Serialization

Payload serialization is handled via the `Codec` interface:

```go
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
}
```

The default implementation uses `encoding/json`. Field additions to payload structs are backward-compatible; renaming or deleting fields on running instances should be avoided.

### Payload Encryption

An `Encrypted` codec is provided to encrypt payloads (inputs, results, signal payloads, side effects) at rest using AES-256-GCM. Outputs are formatted as JSON envelopes, compatible with SQL `jsonb` columns:

```go
keys, err := codec.StaticKeys("2026-07", map[string][]byte{
    "2026-07": currentKey, // 32 bytes
    "2026-01": oldKey,     // Preserved for decrypting historical payloads
})
enc := codec.Encrypted(codec.JSON(), keys)

w := tasuki.NewWorker(b, tasuki.WorkerOptions{Codec: enc})
c := tasuki.NewClient(b, tasuki.WithCodec(enc))
```

Key rotation is supported by specifying a new primary key while retaining historical keys in the keyring. Unencrypted payloads lacking envelope markers fall back to plaintext reading, allowing encryption to be enabled on existing deployments without data migration.

## Testing Support

The `wftest` package enables unit testing workflows without databases and with virtual clocks:

```go
func TestOrderWorkflow(t *testing.T) {
    env := wftest.New(t)
    wftest.RegisterActivity(env, ChargePayment)
    wftest.MockActivity(env, ShipOrder, func(ctx context.Context, in ShipInput) (ShipResult, error) {
        return ShipResult{TrackingID: "t-1"}, nil
    })

    res, err := wftest.Run(env, OrderWorkflow, OrderInput{OrderID: "o-1"})
    if err != nil {
        t.Fatal(err)
    }
    if res.InvoiceID == "" {
        t.Error("invoice id is empty")
    }
}
```

Virtual clocks automatically fast-forward to the earliest pending timer when no tasks are runnable, completing long-running workflows instantly. Signals can be injected at any time using `env.Signal(name, payload)`.

## Worker Options

```go
type WorkerOptions struct {
    Queues                 []string      // Default: ["default"]
    WorkflowSlots          int           // Default: 100 concurrent workflow tasks
    ActivitySlots          int           // Default: 100 concurrent activities
    PollInterval           time.Duration // Default: 1s
    LeaseDuration          time.Duration // Default: 30s
    WorkerID               string        // Default: hostname + random suffix
    Codec                  Codec         // Default: JSON
    Logger                 *slog.Logger  // Default: slog.Default()
    JournalWarnThreshold   int           // Default: 10000; negative disables
    IncompatibleRetryDelay time.Duration // Default: 5s; negative redisplays immediately
    LocalActivityTimeout   time.Duration // Default: 0 (no limit); bounds one ExecuteLocal call
}
```

## Schema Validation and Migrations

Schema migrations are managed per backend. The PostgreSQL backend uses versioned migration files under `backend/postgres/migrations/` (see the [PostgreSQL Migrations README](../backend/postgres/migrations/README.md)).

Workers verify database schemas at startup if the backend implements `backend.SchemaValidator`. If required tables are missing, the worker logs an error and avoids starting the polling loop. This validation can be disabled using `WorkerOptions.DisableSchemaValidation`.

Applications can explicitly trigger validation using `tasuki.ValidateSchema(ctx, backend)`.

- `w.Start(ctx)` starts task polling loops asynchronously and returns immediately.
- `w.Shutdown(ctx)` gracefully halts new task acquisition, waits for in-flight tasks within the context deadline, and releases task leases so peer workers can claim them without waiting for expiration.
