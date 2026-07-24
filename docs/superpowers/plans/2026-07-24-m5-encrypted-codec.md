# M5 Encrypted Codec Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** At-rest encryption for all persisted payloads via `codec.Encrypted` (AES-256-GCM JSON envelope) with key rotation, plus a client codec option.

**Architecture:** `Codec` wrapper in the existing `codec` package; envelope is a JSON object so jsonb columns accept it; `Keyring` resolves keys by id for rotation; `NewClient` becomes variadic to accept `WithCodec`.

**Tech Stack:** stdlib only (`crypto/aes`, `crypto/cipher`, `crypto/rand`, `encoding/json`).

**Spec:** [docs/superpowers/specs/2026-07-24-m5-encrypted-codec-design.md](../specs/2026-07-24-m5-encrypted-codec-design.md)

## Global Constraints

- No new module dependencies
- Envelope marker field is exactly `tasuki_enc`
- AES-256 only: every key is 32 bytes
- Plaintext fallback: `Unmarshal` without marker delegates to inner codec unchanged
- Every commit / merge subject includes `[skip ci]`; one PR per task, merged before the next

---

## File structure

| Path | Responsibility |
|---|---|
| `codec/encrypted.go` | `Keyring`, `StaticKeys`, `Encrypted`, envelope |
| `codec/encrypted_test.go` | Unit tests |
| `options.go` | `ClientOption`, `WithCodec` |
| `client.go` | `NewClient` variadic |
| `encrypted_test.go` (root) | E2E on memory backend |
| `backend/postgres/encrypted_test.go` | jsonb roundtrip (env-gated) |
| `docs/03-api.md`, `README.md` | Docs |
| `.claude/tasks/todo.md` | Checklist |

---

### Task 1: Spec + Plan

**Files:** this plan, the spec, `.claude/tasks/todo.md` (new checklist), `go.work.sum` (untracked housekeeping)

- [ ] Branch `m5/task-1-encrypted-codec-spec`; commit docs + todo + `go.work.sum`
- [ ] PR, merge with `[skip ci]`

---

### Task 2: `codec.Encrypted` + `Keyring` (TDD)

**Files:** Create `codec/encrypted_test.go`, then `codec/encrypted.go`

**Interfaces produced:**
- `codec.Keyring` — `Primary() (string, []byte)`, `Lookup(string) ([]byte, bool)`
- `codec.StaticKeys(primaryID string, keys map[string][]byte) (Keyring, error)`
- `codec.Encrypted(inner Codec, keys Keyring) Codec`

- [ ] **Write failing tests** (`codec/encrypted_test.go`):

```go
package codec_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/codec"
)

type secretPayload struct {
	Msg string
	N   int
}

func key32(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func mustKeys(t *testing.T, primary string, keys map[string][]byte) codec.Keyring {
	t.Helper()
	kr, err := codec.StaticKeys(primary, keys)
	if err != nil {
		t.Fatal(err)
	}
	return kr
}

func TestEncrypted_RoundTrip(t *testing.T) {
	enc := codec.Encrypted(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	in := secretPayload{Msg: "top-secret", N: 42}
	data, err := enc.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out secretPayload
	if err := enc.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestEncrypted_EnvelopeIsJSONWithMarker(t *testing.T) {
	enc := codec.Encrypted(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	data, err := enc.Marshal(secretPayload{Msg: "top-secret"})
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("envelope is not valid JSON: %v", err)
	}
	if env["tasuki_enc"] != float64(1) || env["kid"] != "k1" {
		t.Fatalf("unexpected envelope: %v", env)
	}
	if strings.Contains(string(data), "top-secret") {
		t.Fatal("plaintext leaked into envelope")
	}
}

func TestEncrypted_PlaintextFallback(t *testing.T) {
	enc := codec.Encrypted(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	plain, _ := codec.JSON().Marshal(secretPayload{Msg: "legacy", N: 7})
	var out secretPayload
	if err := enc.Unmarshal(plain, &out); err != nil {
		t.Fatal(err)
	}
	if out.Msg != "legacy" || out.N != 7 {
		t.Fatalf("got %+v", out)
	}
}

func TestEncrypted_KeyRotation(t *testing.T) {
	old := codec.Encrypted(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	data, err := old.Marshal(secretPayload{Msg: "rotate-me"})
	if err != nil {
		t.Fatal(err)
	}
	rotated := codec.Encrypted(codec.JSON(), mustKeys(t, "k2", map[string][]byte{
		"k1": key32('a'),
		"k2": key32('b'),
	}))
	var out secretPayload
	if err := rotated.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.Msg != "rotate-me" {
		t.Fatalf("got %+v", out)
	}
	fresh, err := rotated.Marshal(secretPayload{Msg: "new"})
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(fresh, &env); err != nil {
		t.Fatal(err)
	}
	if env["kid"] != "k2" {
		t.Fatalf("new writes must use primary key, got kid=%v", env["kid"])
	}
}

func TestEncrypted_UnknownKeyID(t *testing.T) {
	writer := codec.Encrypted(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	data, err := writer.Marshal(secretPayload{Msg: "x"})
	if err != nil {
		t.Fatal(err)
	}
	reader := codec.Encrypted(codec.JSON(), mustKeys(t, "k2", map[string][]byte{"k2": key32('b')}))
	var out secretPayload
	err = reader.Unmarshal(data, &out)
	if err == nil || !strings.Contains(err.Error(), "unknown key id") {
		t.Fatalf("want unknown key id error, got %v", err)
	}
}

func TestEncrypted_TamperDetected(t *testing.T) {
	enc := codec.Encrypted(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	data, err := enc.Marshal(secretPayload{Msg: "x"})
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Enc int    `json:"tasuki_enc"`
		KID string `json:"kid"`
		N   []byte `json:"n"`
		CT  []byte `json:"ct"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatal(err)
	}
	env.CT[0] ^= 0xff
	tampered, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	var out secretPayload
	if err := enc.Unmarshal(tampered, &out); err == nil {
		t.Fatal("tampered ciphertext must not decrypt")
	}
}

func TestStaticKeys_Validation(t *testing.T) {
	if _, err := codec.StaticKeys("missing", map[string][]byte{"k1": key32('a')}); err == nil {
		t.Fatal("primary not in keys must error")
	}
	if _, err := codec.StaticKeys("k1", map[string][]byte{"k1": []byte("short")}); err == nil {
		t.Fatal("non-32-byte key must error")
	}
}

type badKeyring struct{}

func (badKeyring) Primary() (string, []byte)      { return "bad", []byte("short") }
func (badKeyring) Lookup(string) ([]byte, bool)   { return []byte("short"), true }

func TestEncrypted_BadKeySizeAtUse(t *testing.T) {
	enc := codec.Encrypted(codec.JSON(), badKeyring{})
	if _, err := enc.Marshal(secretPayload{}); err == nil || !strings.Contains(err.Error(), "32 bytes") {
		t.Fatalf("want 32-byte key error, got %v", err)
	}
}
```

- [ ] Run: `go test ./codec/ -run Encrypted -v` → FAIL (undefined: codec.Encrypted 等)
- [ ] **Implement** `codec/encrypted.go`:

```go
package codec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
)

// Keyring resolves AES-256 keys for the encrypted codec.
// Primary is used for new writes. Lookup must keep resolving rotated-out
// keys for as long as payloads encrypted with them remain in the store.
type Keyring interface {
	Primary() (id string, key []byte)
	Lookup(id string) (key []byte, ok bool)
}

type staticKeyring struct {
	primary string
	keys    map[string][]byte
}

// StaticKeys returns a fixed Keyring. Every key must be 32 bytes (AES-256).
func StaticKeys(primaryID string, keys map[string][]byte) (Keyring, error) {
	if _, ok := keys[primaryID]; !ok {
		return nil, fmt.Errorf("codec: primary key %q not in keys", primaryID)
	}
	cp := make(map[string][]byte, len(keys))
	for id, k := range keys {
		if len(k) != 32 {
			return nil, fmt.Errorf("codec: key %q must be 32 bytes, got %d", id, len(k))
		}
		cp[id] = append([]byte(nil), k...)
	}
	return &staticKeyring{primary: primaryID, keys: cp}, nil
}

func (s *staticKeyring) Primary() (string, []byte) { return s.primary, s.keys[s.primary] }

func (s *staticKeyring) Lookup(id string) ([]byte, bool) {
	k, ok := s.keys[id]
	return k, ok
}

// envelope is valid JSON so it can live in jsonb columns; []byte fields
// serialize as base64 strings.
type envelope struct {
	Enc int    `json:"tasuki_enc"`
	KID string `json:"kid"`
	N   []byte `json:"n"`
	CT  []byte `json:"ct"`
}

type encryptedCodec struct {
	inner Codec
	keys  Keyring
}

// Encrypted wraps inner so every payload is stored as an AES-256-GCM JSON
// envelope. Payloads without the envelope marker are passed through to
// inner, so encryption can be enabled while plaintext payloads remain.
func Encrypted(inner Codec, keys Keyring) Codec {
	return &encryptedCodec{inner: inner, keys: keys}
}

func (c *encryptedCodec) Marshal(v any) ([]byte, error) {
	plain, err := c.inner.Marshal(v)
	if err != nil {
		return nil, err
	}
	kid, key := c.keys.Primary()
	aead, err := newAEAD(kid, key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return json.Marshal(envelope{Enc: 1, KID: kid, N: nonce, CT: aead.Seal(nil, nonce, plain, nil)})
}

func (c *encryptedCodec) Unmarshal(data []byte, v any) error {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil || env.Enc == 0 {
		return c.inner.Unmarshal(data, v)
	}
	key, ok := c.keys.Lookup(env.KID)
	if !ok {
		return fmt.Errorf("codec: unknown key id %q", env.KID)
	}
	aead, err := newAEAD(env.KID, key)
	if err != nil {
		return err
	}
	plain, err := aead.Open(nil, env.N, env.CT, nil)
	if err != nil {
		return fmt.Errorf("codec: decrypt with key %q: %w", env.KID, err)
	}
	return c.inner.Unmarshal(plain, v)
}

func newAEAD(kid string, key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("codec: key %q must be 32 bytes, got %d", kid, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
```

- [ ] Run: `go test ./codec/ -race -count=1` → PASS
- [ ] Commit; PR `m5/task-2-encrypted-codec`; merge

---

### Task 3: Client `WithCodec` + E2E

**Files:** Modify `options.go`, `client.go`; Create `encrypted_test.go` (root), `backend/postgres/encrypted_test.go`

**Interfaces produced:**
- `tasuki.ClientOption`, `tasuki.WithCodec(codec.Codec) ClientOption`
- `tasuki.NewClient(b backend.Backend, opts ...ClientOption) *Client`

- [ ] **Write failing E2E test** (`encrypted_test.go`, root, package `tasuki_test`):

```go
package tasuki_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/codec"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestEncryptedCodec_EndToEnd(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	kr, err := codec.StaticKeys("k1", map[string][]byte{"k1": bytes.Repeat([]byte{'a'}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	enc := codec.Encrypted(codec.JSON(), kr)

	upper := func(ctx context.Context, s string) (string, error) { return s + "!", nil }

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond, Codec: enc})
	tasuki.RegisterActivity(w, upper, tasuki.WithName("upper"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, in string) (string, error) {
		return workflow.Execute[string, string](wctx, "upper", in)
	}, tasuki.WithName("EncWF"))

	c := tasuki.NewClient(b, tasuki.WithCodec(enc))
	h, err := tasuki.Start(ctx, c, "EncWF", "secret-input", tasuki.WithID("enc-1"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	defer w.Shutdown(ctx)

	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := tasuki.Result[string](rctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if out != "secret-input!" {
		t.Fatalf("got %q", out)
	}

	events, err := c.GetJournal(ctx, "enc-1")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, ev := range events {
		if len(ev.Payload) == 0 {
			continue
		}
		if ev.Type == journal.TypeWorkflowStarted || ev.Type == journal.TypeActivityScheduled ||
			ev.Type == journal.TypeActivityCompleted || ev.Type == journal.TypeWorkflowCompleted {
			var env map[string]any
			if err := json.Unmarshal(ev.Payload, &env); err != nil {
				t.Fatalf("payload of %s is not JSON: %v", ev.Type, err)
			}
			if env["tasuki_enc"] != float64(1) {
				t.Fatalf("payload of %s is not encrypted: %s", ev.Type, ev.Payload)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no payloads checked")
	}
}
```

- [ ] Run: `go test . -run EncryptedCodec -v` → FAIL (NewClient に opts がない / WithCodec 未定義)
- [ ] **Implement** — `options.go` に追加:

```go
type ClientOption func(*Client)

// WithCodec sets the codec used for inputs, signals, and results.
// It must match the codec configured on the workers.
func WithCodec(c codec.Codec) ClientOption {
	return func(cl *Client) { cl.codec = c }
}
```

`client.go` の `NewClient` を差し替え:

```go
func NewClient(b backend.Backend, opts ...ClientOption) *Client {
	c := &Client{backend: b, codec: codec.JSON(), pollInterval: 200 * time.Millisecond}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
```

- [ ] Run: `go test . -run EncryptedCodec -race -v` → PASS、`go test ./... -race -count=1` → PASS
- [ ] **Postgres jsonb roundtrip test** (`backend/postgres/encrypted_test.go`; DSN なしなら skip):

```go
package postgres_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/codec"
)

func TestEncryptedPayloadRoundTripsThroughJSONB(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	kr, err := codec.StaticKeys("k1", map[string][]byte{"k1": bytes.Repeat([]byte{'a'}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	enc := codec.Encrypted(codec.JSON(), kr)
	payload, err := enc.Marshal(map[string]string{"card": "4242"})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "enc-pg-1", Name: "wf", Queue: "default", Input: payload}); err != nil {
		t.Fatal(err)
	}
	events, err := b.GetJournal(ctx, "enc-pg-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("no journal events")
	}
	var out map[string]string
	if err := enc.Unmarshal(events[0].Payload, &out); err != nil {
		t.Fatalf("decrypt after jsonb roundtrip: %v", err)
	}
	if out["card"] != "4242" {
		t.Fatalf("got %v", out)
	}
}
```

- [ ] Run: `cd backend/postgres && TASUKI_POSTGRES_DSN=... go test ./... -race -count=1` → PASS
- [ ] Commit; PR `m5/task-3-encrypted-wiring`; merge

---

### Task 4: Docs

**Files:** Modify `docs/03-api.md`（シリアライゼーション節に「暗号化」小節を追加）, `README.md`（ステータスに 1 行）, `.claude/tasks/todo.md`（チェック）

- [ ] 03-api.md の指針 2 項目の直後に追加:

```markdown
### 暗号化

保存されるペイロード全体（入力、結果、イベント、シグナル）を AES-256-GCM で暗号化する `Encrypted` コーデックを同梱する。
出力は鍵 ID とノンスを含む JSON 封筒であり、jsonb カラムにもそのまま保存できる。

​```go
keys, err := codec.StaticKeys("2026-07", map[string][]byte{
    "2026-07": currentKey, // 32 バイト
    "2026-01": oldKey,     // ローテーション済みの鍵も復号用に残す
})
enc := codec.Encrypted(codec.JSON(), keys)

w := tasuki.NewWorker(b, tasuki.WorkerOptions{Codec: enc})
c := tasuki.NewClient(b, tasuki.WithCodec(enc))
​```

運用規則を四つ定める。

- Worker と Client に同じコーデックを設定する。
- ローテーションは primary の切り替えで行い、旧鍵は該当ペイロードが残る間 `Lookup` に残す（再暗号化は不要）。
- 封筒マーカーのないペイロードは平文として読むため、既存インスタンスが残るストアでも有効化できる。
- インスタンス ID、ワークフロー名、キュー名、時刻は暗号化されない（メタデータは平文）。
```

- [ ] README ステータスに追記: `ペイロードの at-rest 暗号化は codec.Encrypted（AES-256-GCM、鍵ローテーション対応）。`
- [ ] todo 全チェック; Commit; PR `m5/task-4-encrypted-docs`; merge
