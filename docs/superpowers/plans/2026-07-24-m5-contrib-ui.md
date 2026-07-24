# M5 Contrib Read-only Web UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Read-only instance list + journal detail UI in `contrib/ui` with CLI for memory/postgres.

**Architecture:** `NewHandler(client)` serves embedded HTML; CLI opens backend and listens.

**Tech Stack:** net/http, embed, existing tasuki.Client.

**Spec:** [docs/superpowers/specs/2026-07-24-m5-contrib-ui-design.md](../specs/2026-07-24-m5-contrib-ui-design.md)

## Global Constraints

- Read-only; contrib only; `[skip ci]` on commits/merges
- One PR per task

---

### Task 1: Spec + Plan

- [ ] Docs + todo; PR; merge

### Task 2: Handler + templates + httptest

**Files:** `contrib/ui/*.go`, embedded HTML/CSS

- [ ] `NewHandler`, `/` and `/instances/{id}`
- [ ] httptest smoke with memory backend + seeded instances
- [ ] PR; merge

### Task 3: CLI + README

**Files:** `contrib/ui/cmd/tasuki-ui/main.go`, README, todo

- [ ] memory/postgres flags
- [ ] README section
- [ ] Smoke `go run`; PR; merge

---

## Acceptance checklist

- [ ] Handler + CLI
- [ ] Tests + README
