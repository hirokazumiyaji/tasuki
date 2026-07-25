# M5 SQLite Task / Terminal Notify Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（ストアの通知機構）; follow-up to [memory notify](2026-07-25-m5-memory-notify-design.md)  
**Decisions:** Scope = `backend/sqlite` only, same `*sqlite.Backend` instance; both `TaskNotifier` and `TerminalNotifier`; Approach = in-process subscriber list copied from memory pattern (no shared hub); emit sites mirror postgres/memory; polling remains fallback.

## Goal

Reduce Worker / `Client.Result` latency on SQLite when the application shares one `*sqlite.Backend` across Worker and Client in-process. Notifications are **hints only**; correctness remains in `ClaimTasks` / leases / `GetInstance`.

## Non-goals

- Cross-process or multi-`Backend` wake (separate SQLite connections / processes)
- Extracting `backend/hub` or refactoring memory to share code in this slice
- MySQL / DynamoDB / Firestore / Spanner notifiers
- Adding `Subscribe` / `SubscribeTerminal` to the required `Backend` interface
- Per-queue channels
- Removing `PollInterval` / Client poll fallback
- Numeric SLO gates in CI

## Optional capability (unchanged)

Reuse `backend/notifier.go`:

```go
type TaskNotifier interface {
	Subscribe(ctx context.Context) (<-chan struct{}, error)
}

type TerminalNotifier interface {
	SubscribeTerminal(ctx context.Context) (<-chan string, error)
}
```

`backend/sqlite.Backend` gains both implementations in this slice (postgres and memory already have them).

## Mechanism

| Item | Value |
|---|---|
| Transport | In-process fan-out under a dedicated `notifyMu` on `sqlite.Backend` |
| Task channel | `chan struct{}`, buffer 1 |
| Terminal channel | `chan string` (instance id), buffer 1 |
| Coalesce | `select { case ch <- v: default: }` — drop if busy |
| Lifecycle | `Subscribe*` registers; ctx cancel unregisters; **do not close** channels (avoid send-after-close with snapshot fan-out; same as memory) |
| Errors | `Subscribe*` returns `nil` error for setup |

Worker / Client already type-assert these interfaces; no Worker API changes.

## When to notify (sqlite)

Emit after successful mutation that can make a task newly claimable, or that sets a terminal status. Helpers: `notifyTasks()`, `notifyTerminal(id)`. Call **after** DB commit / unlock of any long transaction.

### Tasks (`notifyTasks`)

| Operation | Notify? |
|---|---|
| `CreateInstance` (WF task insert) | yes |
| `CommitAdvancement` / `CommitAdvancements` | yes |
| `CompleteActivity` → inbox + ensure WF | yes (when enqueue / ensure path runs) |
| `SendToInbox` → ensure WF | yes (when running + ensure) |
| `FireDueTimers` → enqueue / ensure | yes if fired count > 0 |
| `ClaimDueSchedules` → started instance tasks | yes if any due processed |
| `ReleaseLease` (immediate visibility) | yes |
| `RetryActivity` with future `visible_at` | **no** |
| `ExtendLease` | no |

### Terminal (`notifyTerminal(instanceID)`)

| Operation | Notify? |
|---|---|
| `CommitAdvancement(s)` with `Terminal` | yes |
| `TerminateInstance` | yes |
| Non-terminal advancement | no |
| `CompleteActivity` alone | no |

## Implementation sketch

```go
// backend/sqlite/notify.go
func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error)
func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error)
func (b *Backend) notifyTasks()
func (b *Backend) notifyTerminal(instanceID string)
```

Fields on `Backend` (in `sqlite.go`): `notifyMu`, `taskSubs`, `terminalSubs` — mirror `backend/memory/notify.go` structure.

**Locking:** Snapshot subscriber slices under `notifyMu`, send outside that lock. Do not hold DB transaction locks during channel send.

## Verification

- New sqlite tests (use existing DSN/temp DB helpers):
  - After `Subscribe`, `CreateInstance` yields ≥1 wake within a short timeout
  - After `SubscribeTerminal`, `TerminateInstance` delivers instance id
  - Terminal `CommitAdvancement` delivers id
  - Cancelled subscription does not receive further wakes
- Existing sqlite conformance / schedule tests remain green
- Other backends unchanged

## Docs

- README: extend notify sentence to mention sqlite (same `Backend` instance, in-process) alongside postgres and memory

## Acceptance

- [ ] `sqlite.Backend` implements `TaskNotifier` and `TerminalNotifier`
- [ ] Notify on listed task / terminal paths
- [ ] Coalesced buffered channels; cancel unregisters without closing ch
- [ ] Unit/integration tests above green
- [ ] README note
- [ ] No required API changes on other backends or Worker/Client
- [ ] No shared hub / no memory refactor in this slice

## Follow-ups (out of scope)

- Shared hub if duplication becomes painful
- Multi-connection SQLite wake
- Other store notifiers
