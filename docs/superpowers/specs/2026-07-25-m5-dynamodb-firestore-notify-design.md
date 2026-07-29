# M5 DynamoDB / Firestore Task / Terminal Notify Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（ストアの通知機構）; follow-up to [spanner notify](2026-07-25-m5-spanner-notify-design.md)  
**Decisions:** Scope = both `backend/dynamodb` and `backend/firestore`, same Backend instance each; both `TaskNotifier` and `TerminalNotifier`; Approach = in-process subscriber list copied per store (no shared hub); emit sites mirror other stores; polling remains fallback. DynamoDB Streams / Firestore listen are out of scope.

## Goal

Reduce Worker / `Client.Result` latency on DynamoDB and Firestore when the application shares one Backend instance across Worker and Client in-process. Notifications are **hints only**; correctness remains in `ClaimTasks` / leases / `GetInstance`.

## Non-goals

- DynamoDB Streams, Firestore realtime listeners, or any cross-process wake
- Extracting `backend/hub` or refactoring other stores in this slice
- Adding `Subscribe` / `SubscribeTerminal` to the required `Backend` interface
- Per-queue channels
- Removing `PollInterval` / Client poll fallback
- Numeric SLO gates in CI

## Optional capability (unchanged)

Reuse `backend/notifier.go`. Each of `dynamodb.Backend` and `firestore.Backend` gains both implementations (postgres, memory, sqlite, mysql, and spanner already have them).

## Mechanism (per store)

| Item | Value |
|---|---|
| Transport | In-process fan-out under a dedicated `notifyMu` on each Backend |
| Task channel | `chan struct{}`, buffer 1 |
| Terminal channel | `chan string` (instance id), buffer 1 |
| Coalesce | `select { case ch <- v: default: }` — drop if busy |
| Lifecycle | `Subscribe*` registers; ctx cancel unregisters; **do not close** channels |
| Errors | `Subscribe*` returns `nil` error for setup |

Worker / Client already type-assert these interfaces; no Worker API changes.

## When to notify

Emit after successful mutation that can make a task newly claimable, or that sets a terminal status. Helpers: `notifyTasks()`, `notifyTerminal(id)`. Call **after** the store write / transaction commits successfully.

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
// backend/dynamodb/notify.go  and  backend/firestore/notify.go
func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error)
func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error)
func (b *Backend) notifyTasks()
func (b *Backend) notifyTerminal(instanceID string)
```

Fields on each Backend: `notifyMu`, `taskSubs`, `terminalSubs` — mirror `backend/mysql/notify.go`.

**DynamoDB note:** `CommitAdvancements` may fall back to sequential `commitAdvancementOnce` when TransactWrite ops exceed 100; notify once after the whole batch API returns success (not per-item during fallback), and emit terminal notifies for each adv that had `Terminal != nil` and was committed successfully. If sequential fallback fails mid-batch, do not notify for the failed call (partial commits may already be durable — same as today’s sequential semantics; still call `notifyTasks` / terminal only when the outer `CommitAdvancements` returns nil).

**Firestore note:** notify after `RunTransaction` succeeds and after any post-txn `ensureWorkflowTask` loops complete (same ordering as mysql/spanner).

## Verification

- New tests per package (skip if local endpoint / emulator unavailable):
  - After `Subscribe`, `CreateInstance` yields ≥1 wake within a short timeout
  - After `SubscribeTerminal`, `TerminateInstance` delivers instance id
  - Terminal `CommitAdvancement` delivers id
  - Cancelled subscription does not receive further wakes
- Existing conform / batch tests remain green
- Other backends unchanged

## Docs

- README: extend notify sentence so dynamodb / firestore are listed with memory / sqlite / mysql / spanner (all stores now have a notifier; postgres remains LISTEN/NOTIFY)

## Acceptance

- [x] `dynamodb.Backend` and `firestore.Backend` implement `TaskNotifier` and `TerminalNotifier`
- [x] Notify on listed task / terminal paths for both
- [x] Coalesced buffered channels; cancel unregisters without closing ch
- [x] Unit/integration tests above green (or skip without emulator)
- [x] README note covering all stores
- [x] No Streams/listen; no hub; no Worker/Client API changes

## Follow-ups (out of scope)

- DynamoDB Streams / Firestore listen for multi-process wake
- Shared hub if duplication becomes painful
