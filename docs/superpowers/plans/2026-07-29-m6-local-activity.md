# M6 Local Activity Implementation Plan

> Inline execution. One commit + PR per task. Every commit/merge subject includes `[skip ci]`.

**Goal:** Same-worker sync local activities via `workflow.ExecuteLocal` with journaled results (no activity task queue, no retries).

**Architecture:** New journal command `local_activity` (SideEffect-like). On first record, Worker-injected `LocalActivityRunner` invokes a registered activity; on replay, return stored result/error. No backend schema or `CommitAdvancement` special cases.

**Spec:** [docs/superpowers/specs/2026-07-29-m6-local-activity-design.md](../specs/2026-07-29-m6-local-activity-design.md)

### Task 1: Spec + Plan
### Task 2: Journal + `ExecuteLocal` + Context runner hook + unit tests
### Task 3: Worker LocalRunner wiring + integration tests
### Task 4: Docs (`03-api`, README, architecture journal note)

## Notes

### Journal (`journal/event.go`)

```go
TypeLocalActivity Type = "local_activity"
// IsCommand() includes TypeLocalActivity
```

### Payload (`workflow/local_activity.go`)

```go
type localActivityPayload struct {
	Input  json.RawMessage `json:"input,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}
```

### API

```go
func ExecuteLocal[I, O any](ctx *Context, activityName string, in I) (O, error)

type LocalActivityRunner func(name string, input []byte) (result []byte, err error)

func (c *Context) SetLocalActivityRunner(r LocalActivityRunner)
```

Behavior:
- Replay: `recordOrReplay` → unmarshal payload → if `Error != ""` return error, else unmarshal `Result` to `O`
- Record: require runner; `runner(name, inputBytes)` → build payload → `recordOrReplay`
- Missing runner when recording: return `fmt.Errorf("local activity runner not configured")` (or package-level `ErrLocalActivityRunnerMissing`)
- Unregistered activity: runner returns error → stored in payload `Error` → returned to workflow

### Worker (`worker.go`)

Before `engine.RunAt` / in Query path:

```go
wctx.SetLocalActivityRunner(func(name string, input []byte) ([]byte, error) {
	act, err := w.reg.activity(name)
	if err != nil {
		return nil, err
	}
	return act.fn(context.Background(), input) // match existing activityEntry invoke shape
})
```

Inspect `activityEntry` in `registry.go` for the exact invoke signature (likely already `[]byte` in/out via codec inside the entry).

Do **not** enqueue tasks; do **not** set heartbeat Info unless trivial.

### Tests

- `workflow/local_activity_test.go`: record with fake runner; replay does not call runner; error path
- Integration: memory Worker + RegisterActivity + ExecuteLocal in workflow; unregistered fails

### Docs

- `docs/03-api.md` table + short contrast with `Execute`
- README one-liner
- `docs/02-architecture.md` journal type list if present
