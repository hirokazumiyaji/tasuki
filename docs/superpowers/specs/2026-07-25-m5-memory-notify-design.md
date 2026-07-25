# M5 Memory Task / Terminal Notify Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（ストアの通知機構）; follow-up to postgres task / Result notify  
**Decisions:** Scope = `backend/memory` only; both `TaskNotifier` and `TerminalNotifier`; Approach = in-process subscriber list on `memory.Backend` with coalesce (buffer 1); emit sites mirror postgres; polling remains fallback.

## Goal

Reduce Worker / `Client.Result` latency on the in-memory backend by waking subscribers when tasks become claimable or instances become terminal. Notifications are **hints only**; correctness remains in `ClaimTasks` / leases / `GetInstance`. Timers and due schedules still rely on the poll ticker unless a write path already enqueues a visible task (same as postgres).

## Non-goals

- SQLite / MySQL / DynamoDB / Firestore / Spanner notifiers
- Adding `Subscribe` / `SubscribeTerminal` to the required `Backend` interface
- Shared `backend/hub` package (YAGNI until a second in-process store needs it)
- Callback registration APIs that bypass the existing type-assert capability
- Per-queue channels
- Removing `PollInterval` / Client poll fallback
- Numeric SLO gates in CI
- Cross-process wake (memory is single-process by definition)

## Optional capability (unchanged)

Reuse existing interfaces in `backend/notifier.go`:

```go
type TaskNotifier interface {
	Subscribe(ctx context.Context) (<-chan struct{}, error)
}

type TerminalNotifier interface {
	SubscribeTerminal(ctx context.Context) (<-chan string, error)
}
```

Only `backend/memory.Backend` gains implementations in this slice (postgres already has both).

## Mechanism

| Item | Value |
|---|---|
| Transport | In-process fan-out under `memory.Backend` mutex |
| Task channel | `chan struct{}`, buffer 1 |
| Terminal channel | `chan string` (instance id), buffer 1 |
| Coalesce | `select { case ch <- v: default: }` — drop if busy |
| Lifecycle | `Subscribe*` registers; ctx cancel unregisters and closes the channel |
| Errors | `Subscribe*` returns setup error only (always `nil` for memory unless Backend is closed — if no Close gate exists, always `nil`) |

Worker / Client already type-assert these interfaces; no Worker API changes.

## When to notify (memory)

Emit after a successful mutation that can make a task newly claimable soon, or that sets a terminal status. Prefer calling small helpers (`notifyTasks()`, `notifyTerminal(id)`) from the same sites postgres uses.

### Tasks (`notifyTasks`)

| Operation | Notify? |
|---|---|
| `CreateInstance` (WF task insert) | yes |
| `CommitAdvancement` / `CommitAdvancements` (activity / child WF / ensure WF task) | yes |
| `CompleteActivity` → inbox + ensure WF | yes |
| `SendToInbox` → ensure WF | yes |
| `FireDueTimers` → enqueue / ensure | yes |
| `ClaimDueSchedules` → started instance tasks | yes |
| `ReleaseLease` (immediate visibility) | yes |
| `RetryActivity` with future `visible_at` | **no** (ticker) |
| `ExtendLease` | no |

### Terminal (`notifyTerminal(instanceID)`)

| Operation | Notify? |
|---|---|
| `CommitAdvancement(s)` with `Terminal` | yes, payload = instance id |
| `TerminateInstance` | yes |
| Non-terminal advancement | no |
| `CompleteActivity` alone | no |

## Implementation sketch

```go
// notify.go (new) or fields on Backend
type taskSub struct {
	ch chan struct{}
}
type terminalSub struct {
	ch chan string
}

func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) { ... }
func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error) { ... }
func (b *Backend) notifyTasks() { /* fan-out under lock or after unlock */ }
func (b *Backend) notifyTerminal(id string) { ... }
```

**Locking:** Prefer snapshot subscriber slices under `b.mu`, then send **outside** the lock (or use a separate `notifyMu`) so notify does not hold the store lock during channel send. Dropping on full buffer is OK.

**Cancel:** A goroutine watches `ctx.Done()`, removes the subscriber, closes the channel. Unregister must be idempotent.

## Verification

- New memory tests:
  - After `Subscribe`, `CreateInstance` yields ≥1 wake within a short timeout
  - After `SubscribeTerminal`, `TerminateInstance` delivers the instance id
  - Terminal commit via `CommitAdvancement` with `Terminal` delivers id
  - Cancelled subscription does not receive further wakes (and does not panic on notify)
- Existing memory conformance / schedule tests remain green
- Other backends unchanged

## Docs

- README: extend the notify sentence to mention memory (in-process) alongside postgres LISTEN/NOTIFY
- Spec is the source of truth

## Acceptance

- [ ] `memory.Backend` implements `TaskNotifier` and `TerminalNotifier`
- [ ] Notify on listed task / terminal paths
- [ ] Coalesced buffered channels; cancel unregisters
- [ ] Unit/integration tests above green
- [ ] README note
- [ ] No required API changes on other backends or Worker/Client

## Follow-ups (out of scope)

- SQLite in-process notifier (same pattern)
- Other store notifiers (MySQL, DynamoDB Streams, Firestore)
- Shared hub package if/when a second in-process store lands
- Timer deadline notify without a write
