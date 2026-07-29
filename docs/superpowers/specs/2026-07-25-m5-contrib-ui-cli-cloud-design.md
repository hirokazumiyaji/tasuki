# M5 Contrib UI CLI cloud backends Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [2026-07-25-m5-contrib-ui-cli-sqlite-mysql-design.md](./2026-07-25-m5-contrib-ui-cli-sqlite-mysql-design.md)  
**Decisions:** Add spanner, dynamodb, firestore to `tasuki-ui`; env aligned with chaos worker; Migrate on start; no Reset.

## Goal

Complete `tasuki-ui` backend coverage for remaining M4 stores.

## Non-goals

- Shared opener package; auth; bench CLI sync

## CLI

`-backend=memory|postgres|sqlite|mysql|spanner|dynamodb|firestore`

| Backend | Env | Notes |
|---|---|---|
| spanner | `TASUKI_SPANNER_DSN` (required) | Migrate |
| dynamodb | `TASUKI_DYNAMODB_ENDPOINT` (optional), prefix via existing `TASUKI_DYNAMODB_TABLE_PREFIX` | Migrate; empty endpoint = AWS default |
| firestore | `TASUKI_FIRESTORE_PROJECT` (optional; package default) | Migrate no-op; use `FIRESTORE_EMULATOR_HOST` for emulator |

## Verification

- Missing spanner DSN → error
- Unknown backend still errors
- README examples

## Acceptance

- [x] Three backends wired + README
- [x] Handler unchanged
