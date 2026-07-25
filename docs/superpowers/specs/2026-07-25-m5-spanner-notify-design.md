# M5 Spanner Task / Terminal Notify Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（ストアの通知機構）; follow-up to [mysql notify](2026-07-25-m5-mysql-notify-design.md)  
**Decisions:** Scope = `backend/spanner` only, same `*spanner.Backend` instance; both `TaskNotifier` and `TerminalNotifier`; Approach = in-process subscriber list copied from memory/sqlite/mysql pattern (no shared hub); emit sites mirror other stores; polling remains fallback.

## Goal

Reduce Worker / `Client.Result` latency on Spanner when the application shares one `*spanner.Backend` across Worker and Client in-process. Notifications are **hints only**; correctness remains in `ClaimTasks` / leases / `GetInstance`.

## Non-goals

- Cross-process or multi-`Backend` wake
- Extracting `backend/hub` or refactoring other stores in this slice
- DynamoDB Streams / Firestore listen
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

`backend/spanner.Backend` gains both implementations in this slice (postgres, memory, sqlite, and mysql already have them).

## Mechanism

| Item | Value |
|---|---|
| Transport | In-process fan-out under a dedicated `notifyMu` on `spanner.Backend` |
| Task channel | `chan struct{}`, buffer 1 |
| Terminal channel | `chan string` (instance id), buffer 1 |
| Coalesce | `select { case ch <- v: default: }` — drop if busy |
| Lifecycle | `Subscribe*` registers; ctx cancel unregisters; **do not close** channels |
| Errors | `Subscribe*` returns `nil` error for setup |

Worker / Client already type-assert these interfaces; no Worker API changes.

## When to notify (spanner)

Emit after successful mutation that can make a task newly claimable, or that sets a terminal status. Helpers: `notifyTasks()`, `notifyTerminal(id)`. Call **after** the Spanner read/write transaction (or mutation) commits successfully.

### Tasks (`notifyTasks`)

| Operation | Notify? |
|---|---|
| `CreateInstance` (WF task insert) | yes |
| `CommitAdvancement` / `CommitAdvancements` | yes |
| `CompleteActivity` → inbox + ensure WF | yes (when enqueue path runs) |
| `SendToInbox` | yes after successful send/ensure |
| `FireDueTimers` | yes if fired count > 0 |
| `ClaimDueSchedules` | yes if any due processed |
| `ReleaseLease` | yes |
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
// backend/spanner/notify.go
func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error)
func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error)
func (b *Backend) notifyTasks()
func (b *Backend) notifyTerminal(instanceID string)
```

Fields on `Backend`: `notifyMu`, `taskSubs`, `terminalSubs` — mirror `backend/mysql/notify.go`.

## Verification

- New spanner tests (skip if emulator / project unavailable, same as other spanner tests):
  - After `Subscribe`, `CreateInstance` yields ≥1 wake within a short timeout
  - After `SubscribeTerminal`, `TerminateInstance` delivers instance id
  - Terminal `CommitAdvancement` delivers id
  - Cancelled subscription does not receive further wakes
- Existing spanner conformance / batch tests remain green
- Other backends unchanged

## Docs

- README: extend notify sentence to mention spanner (same `Backend` instance, in-process) alongside postgres, memory, sqlite, and mysql

## Acceptance

- [ ] `spanner.Backend` implements `TaskNotifier` and `TerminalNotifier`
- [ ] Notify on listed task / terminal paths
- [ ] Coalesced buffered channels; cancel unregisters without closing ch
- [ ] Unit/integration tests above green (or skip without emulator)
- [ ] README note
- [ ] No required API changes on other backends or Worker/Client
- [ ] No shared hub / no other-store refactor in this slice

## Follow-ups (out of scope)

- Shared hub if duplication becomes painful
- DynamoDB Streams / Firestore listen
- Multi-connection wake
