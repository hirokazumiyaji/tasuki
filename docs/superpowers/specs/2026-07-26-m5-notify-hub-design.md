# M5 In-Process Notify Hub Design

**Date:** 2026-07-26  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（ストアの通知機構）; follow-up to per-store in-process notify  
**Decisions:** Extract fan-out only into `backend/hub`; stores keep emit-site calls via thin wrappers; postgres LISTEN/NOTIFY unchanged; semantics identical to existing per-store `notify.go`.

## Goal

Deduplicate the in-process `TaskNotifier` / `TerminalNotifier` subscriber fan-out currently copied across memory, sqlite, mysql, spanner, dynamodb, and firestore by introducing a small shared `backend/hub` package. Behavior stays **hints only**; correctness remains Claim / GetInstance.

## Non-goals

- Changing postgres LISTEN/NOTIFY implementation
- Changing emit timing, Worker/Client APIs, or `backend.TaskNotifier` / `TerminalNotifier` interface shapes
- DynamoDB Streams / Firestore listen
- Adding Subscribe to the required `Backend` interface
- Performance SLO gates

## Package API

```go
package hub

// Hub is an in-process coalesce fan-out for task and terminal wake hints.
type Hub struct { /* unexported mutex + subscriber slices */ }

func New() *Hub

func (h *Hub) Subscribe(ctx context.Context) (<-chan struct{}, error)
func (h *Hub) SubscribeTerminal(ctx context.Context) (<-chan string, error)
func (h *Hub) NotifyTasks()
func (h *Hub) NotifyTerminal(instanceID string)
```

| Item | Value |
|---|---|
| Task channel | `chan struct{}`, buffer 1 |
| Terminal channel | `chan string`, buffer 1 |
| Coalesce | `select { case ch <- v: default: }` |
| Cancel | unregister subscriber; **do not close** channel (avoid send-after-close with snapshot fan-out) |
| Errors | `Subscribe*` returns `nil` |

## Store integration

For each of: `memory`, `sqlite`, `mysql`, `spanner`, `dynamodb`, `firestore`:

1. Hold `hub *hub.Hub` on `Backend` (construct with `hub.New()` in `New` / `memory.New`).
2. Remove duplicated subscriber fields (`notifyMu`, `taskSubs`, `terminalSubs`) and old `notify.go` fan-out types.
3. Keep thin methods so emit sites stay untouched:

```go
func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	return b.hub.Subscribe(ctx)
}
func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error) {
	return b.hub.SubscribeTerminal(ctx)
}
func (b *Backend) notifyTasks() { b.hub.NotifyTasks() }
func (b *Backend) notifyTerminal(id string) { b.hub.NotifyTerminal(id) }
```

Thin wrappers may live in a small `notify.go` per store or next to `New` — either is fine; prefer one short `notify.go` for consistency.

**postgres:** no change (does not use hub).

## Verification

- New `backend/hub` unit tests: wake on `NotifyTasks`, terminal payload on `NotifyTerminal`, cancel stops delivery.
- Existing per-store `TestSubscribe*` remain green (or skip without emulator/DSN as today).
- `go test` compile for all backends.

## Docs

- README: one short note that in-process wake is shared via `backend/hub` (postgres remains LISTEN/NOTIFY).

## Acceptance

- [x] `backend/hub` with API above
- [x] Six in-process stores delegate Subscribe/notify to hub
- [x] No behavior change vs previous per-store notify
- [x] Hub unit tests + store subscribe tests green/skip
- [x] postgres untouched
- [x] README note

## Follow-ups (out of scope)

- Sharing coalesce helpers with postgres listen loops
- Cross-process Streams/listen
