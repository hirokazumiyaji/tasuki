# M6 Activity Heartbeat Design

**Date:** 2026-07-26  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) 将来候補「ハートビート付き長時間アクティビティ」; [docs/03-api.md](../../03-api.md) `activity.Info`  
**Decisions:** New `activity` package; `RecordHeartbeat` extends lease and persists details on the activity task; `GetHeartbeatDetails` reads last details on retry; auto `extendLeaseLoop` remains as safety net; heartbeat is optional (activities need not call it).

## Goal

Let long-running activities signal liveness and checkpoint progress so (1) leases stay alive via explicit heartbeats and (2) retries can resume from last heartbeat details.

## Non-goals

- HeartbeatTimeout that fails the activity if silent (can follow later)
- Changing workflow APIs or journal event types
- Removing automatic lease extension

## API (`activity` package)

```go
type Info struct {
    InstanceID     string
    ActivityName   string
    Attempt        int    // 1-based
    TaskID         int64
    IdempotencyKey string // "{instanceID}/{seq}"
}

func Info(ctx context.Context) Info
func RecordHeartbeat(ctx context.Context, details any) error
func GetHeartbeatDetails(ctx context.Context, dest any) error // ErrNoDetails if none
```

Worker injects an internal heartbeat handle into `ctx` before calling the activity.

## Backend

```go
// RecordHeartbeat extends the lease and stores details for later GetHeartbeatDetails.
RecordHeartbeat(ctx context.Context, taskID int64, lease time.Duration, details []byte) error
```

`Task` gains `HeartbeatDetails []byte` (loaded on Claim).

Storage: `wf_tasks.heartbeat` (SQL/Spanner column or DDB/FS attribute). Migrate adds column when missing.

`RecordHeartbeat` with `details == nil` still extends lease (same as ExtendLease).

## Worker

- Before `act.fn`: `ctx = activity.WithEnv(ctx, env)` where env has Info, codec, backend, lease duration
- `RecordHeartbeat`: codec.Marshal(details) → `backend.RecordHeartbeat`
- Keep `extendLeaseLoop` (half lease) as fallback for activities that do not heartbeat
- IdempotencyKey = `fmt.Sprintf("%s/%d", instanceID, seq)`

## Tests

- memory: activity records heartbeat details; after RetryActivity + re-claim, `GetHeartbeatDetails` returns them
- RecordHeartbeat extends visible_at
- Info fields populated

## Docs

- `docs/03-api.md`: document RecordHeartbeat / GetHeartbeatDetails
- `docs/04-plan.md`: move heartbeat out of pure future list (implemented)
- README short note if needed

## Acceptance

- [ ] `activity` package with API above
- [ ] Backend.RecordHeartbeat + Task.HeartbeatDetails on all stores
- [ ] Worker wiring + memory integration test
- [ ] Docs updated
