# M6 Workflow Update Implementation Plan

> Inline execution. One commit + PR per task. Every commit/merge subject includes `[skip ci]`.

**Goal:** Sync Workflow Updates via Worker: `SetUpdateHandler` + `tasuki.Update`, optional `WithUpdateID`, handlers may use full workflow APIs under cooperative single-goroutine rules.

**Architecture:** Inbox `update_requested` → journal; commands `update_accepted` / `update_completed`. Worker, after main replay reaches a wait, runs/re-enters the in-flight or next Update handler on the same `Context`.

**Spec:** [docs/superpowers/specs/2026-07-29-m6-workflow-update-design.md](../specs/2026-07-29-m6-workflow-update-design.md)

### Task 1: Spec + Plan
### Task 2: Journal types + Context update handlers + engine/unit tests (record accept/complete, replay)
### Task 3: `tasuki.Update` + Worker dispatch + idempotent Update ID + integration tests
### Task 4: Docs

## Notes

### Journal (`journal/event.go`)

```go
TypeUpdateRequested Type = "update_requested" // IsCompletion
TypeUpdateAccepted  Type = "update_accepted"  // IsCommand
TypeUpdateCompleted Type = "update_completed"  // IsCommand
```

### Payload helpers (`workflow/update.go`)

```go
type updateRequestPayload struct {
    ID    string          `json:"id"`
    Input json.RawMessage `json:"input"`
}
type updateAcceptedPayload struct {
    ID string `json:"id"`
}
type updateCompletedPayload struct {
    ID     string          `json:"id"`
    Result json.RawMessage `json:"result,omitempty"`
    Error  string          `json:"error,omitempty"`
}

func SetUpdateHandler[I, O any](ctx *Context, name string, fn func(*Context, I) (O, error))
```

Context fields: `updateHandlers map[string]...`, helpers to find pending requested / in-flight accepted, `DispatchUpdates()` internal used by Worker/engine after main run.

### Engine / Worker

Prefer extending `engine.RunAt` result or post-processing in Worker:

1. Run main `wf.fn` as today.
2. If suspended or before terminal: call `workflow.DispatchPendingUpdate(wctx)` which:
   - Finds incomplete accepted update in events+new commands, or next unconsumed `update_requested`
   - Records accepted if needed via `recordOrReplay`
   - Invokes handler (may `Goexit` suspend)
3. Append any new commands from the handler into the advancement.

Careful with `Goexit`: Dispatch must run in the same workflow goroutine model as today (likely nested `engine` helper that runs handler in a goroutine with recover, like `RunAt`).

### Client API (`update.go` in root package)

```go
func Update[I, O any](ctx context.Context, w *Worker, instanceID, name string, in I, opts ...UpdateOption) (O, error)
func WithUpdateID(id string) UpdateOption
```

Loop: ensure request → `w.PollOnce` / wait on task notifier → scan journal for `update_completed` with id.

### Idempotency

Before inbox insert: `GetJournal` / `LoadWorkflow` scan for completed or accepted with same id. Completed → unmarshal result. Accepted → skip insert and wait. Else insert with dedupe key = updateID if backend SendToInbox dedupe can be reused (`WithDedupeID`-style on a dedicated path or pass updateID as dedupeID for this event type only).

### Tests

- `workflow/update_test.go`: handler records complete; Execute inside handler suspends then completes on second run
- `worker_update_test.go`: end-to-end Update; WithUpdateID duplicate; unknown handler

### Docs

`docs/03-api.md`, README, `docs/02-architecture.md` journal table, `docs/04-plan.md`
