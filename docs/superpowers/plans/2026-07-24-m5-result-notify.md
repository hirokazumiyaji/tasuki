# M5 Client Result Terminal Notify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Postgres `TerminalNotifier` + Client `Result` wakes on terminal NOTIFY with ticker fallback.

**Architecture:** Optional interface; channel `tasuki_terminal`; notify after terminal commits; Result filters by instance id.

**Tech Stack:** pgx LISTEN (existing notify.go patterns), Client Result loop.

**Spec:** [docs/superpowers/specs/2026-07-24-m5-result-notify-design.md](../specs/2026-07-24-m5-result-notify-design.md)

## Global Constraints

- Hints only; GetInstance remains source of truth
- Do not add SubscribeTerminal to required Backend
- Every commit / merge subject includes `[skip ci]`

---

### Task 1: Spec + Plan

- [ ] Docs + todo; PR; merge

### Task 2: TerminalNotifier + postgres SubscribeTerminal + terminal notify

**Files:** `backend/notifier.go` (or new file), `backend/postgres/notify.go`, emit sites in `backend.go`, test

- [ ] Interface `TerminalNotifier`
- [ ] `SubscribeTerminal` + `notifyTerminal`
- [ ] Call after terminal CommitAdvancement / TerminateInstance
- [ ] Test wake on terminal
- [ ] PR; merge

### Task 3: Client.Result + README

**Files:** `client.go`, `README.md`, todo

- [ ] Result select on terminal wake or ticker
- [ ] README; PR; merge

---

## Acceptance checklist

- [ ] Interface + postgres impl
- [ ] Result wake path
- [ ] Tests + README
