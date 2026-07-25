# M5 Codec + Replay Fuzz Design

**Date:** 2026-07-26  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) テスト戦略「codec とリプレイ入力の fuzz」

## Goal

Add `go test -fuzz` targets so malformed codec payloads and short synthetic journals cannot panic the process.

## Scope

- `codec`: JSON round-trip fuzz; Encrypted encrypt/decrypt + garbage ciphertext
- `internal/engine`: bounded synthetic journal + tiny workflow; no panic (stuck/suspend/complete OK)

## Non-goals

- External fuzzing infra / CI long runs (seed corpus + unit invocation is enough)
- Exhaustive journal grammar generation
