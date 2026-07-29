# M5 MySQL Task / Terminal Notify Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（ストアの通知機構）; follow-up to [sqlite notify](2026-07-25-m5-sqlite-notify-design.md)  
**Decisions:** Scope = `backend/mysql` only, same `*mysql.Backend` instance; both `TaskNotifier` and `TerminalNotifier`; Approach = in-process subscriber list copied from memory/sqlite pattern (no shared hub); emit sites mirror postgres/memory/sqlite; polling remains fallback.

## Goal

Reduce Worker / `Client.Result` latency on MySQL when the application shares one `*mysql.Backend` across Worker and Client in-process. Notifications are **hints only**; correctness remains in `ClaimTasks` / leases / `GetInstance`.

## Non-goals

- Cross-process or multi-`Backend` wake (separate MySQL connections / processes / native DB notify)
- Extracting `backend/hub` or refactoring memory/sqlite to share code in this slice
- DynamoDB Streams / Firestore listen / Spanner notifiers
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

`backend/mysql.Backend` gains both implementations in this slice (postgres, memory, and sqlite already have them).

## Mechanism

| Item | Value |
|---|---|
| Transport | In-process fan-out under a dedicated `notifyMu` on `mysql.Backend` |
| Task channel | `chan struct{}`, buffer 1 |
| Terminal channel | `chan string` (instance id), buffer 1 |
| Coalesce | `select { case ch <- v: default: }` — drop if busy |
| Lifecycle | `Subscribe*` registers; ctx cancel unregisters; **do not close** channels |
| Errors | `Subscribe*` returns `nil` error for setup |

Worker / Client already type-assert these interfaces; no Worker API changes.

## When to notify (mysql)

Emit after successful mutation that can make a task newly claimable, or that sets a terminal status. Helpers: `notifyTasks()`, `notifyTerminal(id)`. Call **after** DB commit succeeds.

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
// backend/mysql/notify.go
func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error)
func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error)
func (b *Backend) notifyTasks()
func (b *Backend) notifyTerminal(instanceID string)
```

Fields on `Backend`: `notifyMu`, `taskSubs`, `terminalSubs` — mirror `backend/sqlite/notify.go`.

## Verification

- New mysql tests (skip if DSN unavailable, same as other mysql tests):
  - After `Subscribe`, `CreateInstance` yields ≥1 wake within a short timeout
  - After `SubscribeTerminal`, `TerminateInstance` delivers instance id
  - Terminal `CommitAdvancement` delivers id
  - Cancelled subscription does not receive further wakes
- Existing mysql conformance / batch tests remain green
- Other backends unchanged

## Docs

- README: extend notify sentence to mention mysql (same `Backend` instance, in-process) alongside postgres, memory, and sqlite

## Acceptance

- [x] `mysql.Backend` implements `TaskNotifier` and `TerminalNotifier`
- [x] Notify on listed task / terminal paths
- [x] Coalesced buffered channels; cancel unregisters without closing ch
- [x] Unit/integration tests above green (or skip without DSN)
- [x] README note
- [x] No required API changes on other backends or Worker/Client
- [x] No shared hub / no memory/sqlite refactor in this slice

## Follow-ups (out of scope)

- Shared hub if duplication becomes painful
- Multi-connection / MySQL-native notify
- Other store notifiers (DynamoDB Streams, Firestore)
