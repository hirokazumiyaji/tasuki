# M5 Encrypted Codec Design

**Date:** 2026-07-24  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（暗号化 Codec）, [docs/03-api.md](../../03-api.md) §シリアライゼーション  
**Decisions:** JSON envelope (jsonb-safe); AES-256-GCM only (32-byte keys); `Keyring` with primary + lookup for rotation; plaintext fallback on missing marker; `NewClient` gains variadic options with `WithCodec` (codec was hardcoded).

## Goal

At-rest encryption for every payload the engine persists (workflow inputs, results, activity inputs/results, signal payloads, side effects) via a `Codec` wrapper. Stdlib crypto only — no new dependencies. Output must be valid JSON because the postgres backend stores payloads in `jsonb` columns.

## Non-goals

- KMS / secret-manager integration (`Keyring` is an interface; callers bring their own)
- Encrypting metadata: instance IDs, workflow/activity names, queues, timestamps stay plaintext
- Searchable encryption; `List` filters never touch payloads
- Compression

## Envelope

```json
{"tasuki_enc":1,"kid":"2026-07","n":"<base64 nonce>","ct":"<base64 ciphertext+tag>"}
```

- Valid JSON object → storable in `jsonb` (jsonb may reorder keys / reformat; only decrypted equality is guaranteed, never byte equality)
- `tasuki_enc` is the envelope marker; payloads without it are treated as plaintext and passed to the inner codec (enables turning encryption on while plaintext payloads remain; keep the keyring for as long as encrypted payloads remain)
- Nonce is 12 random bytes per encryption; GCM tag is appended to `ct` (Go `Seal`)
- Random nonce makes ciphertext nondeterministic; safe because command matching compares type+name only, never payload bytes

## API (`codec` package)

```go
type Keyring interface {
    Primary() (id string, key []byte)       // key for new writes
    Lookup(id string) (key []byte, ok bool) // decryption keys incl. rotated-out
}

func StaticKeys(primaryID string, keys map[string][]byte) (Keyring, error) // validates 32-byte keys
func Encrypted(inner Codec, keys Keyring) Codec
```

- `Marshal`: `inner.Marshal` → seal with primary → envelope JSON
- `Unmarshal`: envelope marker present → `Lookup(kid)` → open → `inner.Unmarshal(plain)`; no marker → `inner.Unmarshal(data)`
- Errors: unknown `kid`, tampered `ct` (GCM auth), non-32-byte key at use

## Client wiring

`NewClient(b backend.Backend, opts ...ClientOption) *Client` (backward compatible), `WithCodec(codec.Codec)` in `options.go`. Worker side already exists (`WorkerOptions.Codec`). Client and Worker must be configured with the same codec.

## Rotation

Add the new key as primary, keep old keys in `Lookup`. Old payloads decrypt by `kid`; new writes use the new key. No re-encryption pass required.

## Testing

- Unit: roundtrip; envelope is JSON with marker and without plaintext substring; plaintext fallback; rotation via `kid`; unknown `kid`; tamper detection; `StaticKeys` validation; non-32-byte key at use
- E2E (memory): worker + client with `Encrypted` complete a workflow; journal payloads carry the marker
- Postgres (env-gated): encrypted payload round-trips through `CreateInstance` → `GetJournal` including jsonb reformatting
