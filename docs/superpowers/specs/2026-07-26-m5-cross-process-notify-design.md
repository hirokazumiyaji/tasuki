# M5 Cross-Process Notify (DynamoDB / Firestore) Design

**Date:** 2026-07-26  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5 通知; deferred from in-process hub slice  
**Decisions:** Keep in-process `hub` for same Backend; add store-backed wake so **another process** with its own Backend can Subscribe. Hints only. Polling remains fallback.

## Goal

Workers / Clients in separate processes wake early when DynamoDB or Firestore backends mutate tasks or terminals.

## Non-goals

- Changing postgres LISTEN/NOTIFY
- Removing PollInterval
- Exactly-once delivery of wake hints
- Shared hub across processes

## Mechanism

### Shared

Emit sites stay the same (`notifyTasks` / `notifyTerminal`). Each call:

1. `hub.Notify*` (same-process subscribers)
2. Persist a wake marker (cross-process)

`Subscribe*` returns a channel fed by **hub OR store listener** (coalesced buffer-1).

### Firestore

| Kind | Document | Write |
|---|---|---|
| tasks | `wf_notify/tasks` | `{n: Increment(1), at: server time}` |
| terminal | `wf_notify/terminal` | `{n: Increment(1), id: instanceID, at: ...}` |

`Subscribe` / `SubscribeTerminal`: `Snapshots` on the doc; ignore initial snapshot; on change coalesce-send. Also fan-in hub channel.

### DynamoDB

| Table | Keys | Stream |
|---|---|---|
| `wf_wake` | `pk` (S) = `tasks` \| `terminal` | `NEW_IMAGE` enabled on create |

Writes: `UpdateItem` `ADD n :one` (and `SET id = :id` for terminal).

`Subscribe*`: in-process hub fan-in **plus** a consistent `GetItem` poll (~100ms) on the wake item (works with DynamoDB Local). The table enables DynamoDB Streams (`NEW_IMAGE`) so operators can attach Lambda / external consumers; the library itself uses the wake-item poller for Subscribe reliability.

## Tests

- Existing same-Backend notify tests remain green (hub path)
- New test (emulator): Backend A Subscribe, Backend B CreateInstance / Terminate → A wakes (skip without emulator)
- DynamoDB Local: skip Streams test if stream ARN unavailable

## Docs

- README: DDB/FS support cross-process wake via wake markers / Streams; postgres remains LISTEN/NOTIFY
- Update prior “out of scope” notes in older specs only if needed via README

## Acceptance

- [x] Firestore snapshot cross-process wake
- [x] DynamoDB wf_wake + Streams (or documented skip)
- [x] Hub path unchanged for same-process
- [x] README note
