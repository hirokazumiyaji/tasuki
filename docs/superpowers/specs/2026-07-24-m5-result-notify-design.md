# M5 Client Result Terminal Notify Design

**Date:** 2026-07-24  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（タスク獲得と Result の通知化）; follow-up to postgres task notify  
**Decisions:** Approach A — optional `TerminalNotifier`, channel `tasuki_terminal` with instance-id payload, Client filters; ticker remains fallback.

## Goal

Wake `Result` waiters when an instance reaches a terminal status on PostgreSQL via `LISTEN`/`NOTIFY`, reducing latency from `Client.pollInterval` (default 200ms). Notifications are hints; correctness remains `GetInstance`.

## Non-goals

- Per-instance LISTEN channels
- Backend-side waiter fan-out registry (Approach C — later)
- Merging with `tasuki_tasks` / `TaskNotifier`
- Non-postgres stores
- CI latency gates

## Optional capability

```go
package backend

// TerminalNotifier is an optional Backend capability for Result wake hints.
type TerminalNotifier interface {
	// SubscribeTerminal delivers instance IDs that may have become terminal.
	// Cancelling ctx ends the subscription.
	SubscribeTerminal(ctx context.Context) (<-chan string, error)
}
```

Only `backend/postgres` implements this in this slice.

## Channel

| Item | Value |
|---|---|
| Name | `tasuki_terminal` |
| Payload | `instance_id` string |
| Coalesce | OK (duplicate wakes fine) |

## When to NOTIFY (postgres)

After successful commit that sets a **terminal** status:

| Path | Notify? |
|---|---|
| `CommitAdvancement` with `Terminal` (`completed`/`failed`/`canceled`/`stuck`/`continued`/…) | yes, payload = `InstanceID` |
| `TerminateInstance` | yes |
| Non-terminal `CommitAdvancement` | no |
| `CompleteActivity` alone | no |

Helper: `notifyTerminal(ctx, instanceID)` via `SELECT pg_notify('tasuki_terminal', $1)`.

## LISTEN

Dedicated pgx connection (same pattern as `Subscribe` for tasks). Reconnect with backoff on failure. Cancel ctx closes connection.

## Client.Result

If `backend.(TerminalNotifier)`:

1. `SubscribeTerminal(ctx)` for the Result call’s lifetime (per-call subscribe in v1)
2. Loop: poll `GetInstance`; on non-terminal, `select` on `ctx.Done()`, ticker, or wake channel
3. On wake: if payload non-empty and ≠ handle ID, ignore; else re-poll immediately
4. Subscribe error → ticker-only (log optional)

If no `TerminalNotifier`, keep current ticker-only loop.

## Verification

- postgres test: subscribe → run workflow to completion → receive matching instance id (or wake then GetInstance sees terminal)
- memory / non-notifier: existing Result behavior
- postgres conform + chaos still green

## Docs

README: Client `Result` can wake on `tasuki_terminal` when using postgres.

## Acceptance

- [x] `TerminalNotifier` interface
- [x] postgres SubscribeTerminal + notify on terminal paths
- [x] `Result` uses notify or ticker
- [x] Test + README
- [x] No required Backend method added

## Follow-ups

- Shared LISTEN + fan-out (Approach C)
- Payload-less wake-all mode
- Other stores
