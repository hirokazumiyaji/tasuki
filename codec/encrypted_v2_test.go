package codec_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hirokazumiyaji/tasuki/codec"
)

type v2env struct {
	Enc int    `json:"tasuki_enc"`
	KID string `json:"kid"`
	N   []byte `json:"n"`
	CT  []byte `json:"ct"`
}

func mustV2Env(t *testing.T, data []byte) v2env {
	t.Helper()
	var env v2env
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("envelope is not valid JSON: %v", err)
	}
	return env
}

// sealV1 crafts a legacy Enc:1 envelope (nil AAD) for backward-compat tests.
func sealV1(t *testing.T, kid string, key, plain []byte) []byte {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := bytes.Repeat([]byte{0x11}, aead.NonceSize())
	data, err := json.Marshal(v2env{Enc: 1, KID: kid, N: nonce, CT: aead.Seal(nil, nonce, plain, nil)})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestEncryptedV2_MarshalWritesV2(t *testing.T) {
	enc := codec.EncryptedWithOptions(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}),
		codec.WithWriteVersion(codec.WriteVersionV2))
	data, err := enc.Marshal(secretPayload{Msg: "v2", N: 1})
	if err != nil {
		t.Fatal(err)
	}
	env := mustV2Env(t, data)
	if env.Enc != 2 {
		t.Fatalf("new writes must use Enc 2, got %d", env.Enc)
	}
	if env.KID != "k1" {
		t.Fatalf("got kid %q", env.KID)
	}
	var out secretPayload
	if err := enc.Unmarshal(data, &out); err != nil {
		t.Fatalf("v2 roundtrip: %v", err)
	}
	if out.Msg != "v2" || out.N != 1 {
		t.Fatalf("got %+v", out)
	}
}

func TestEncryptedV1_BackwardCompat(t *testing.T) {
	kr := mustKeys(t, "k1", map[string][]byte{"k1": key32('a')})
	reader := codec.Encrypted(codec.JSON(), kr)
	plain, err := codec.JSON().Marshal(secretPayload{Msg: "legacy-enc", N: 9})
	if err != nil {
		t.Fatal(err)
	}
	data := sealV1(t, "k1", key32('a'), plain)
	var out secretPayload
	if err := reader.Unmarshal(data, &out); err != nil {
		t.Fatalf("Enc:1 payload must remain decryptable: %v", err)
	}
	if out.Msg != "legacy-enc" || out.N != 9 {
		t.Fatalf("got %+v", out)
	}
	// Strict mode still accepts v1 envelopes; it only rejects non-envelopes.
	strict := codec.EncryptedStrict(codec.JSON(), kr)
	var out2 secretPayload
	if err := strict.Unmarshal(data, &out2); err != nil {
		t.Fatalf("strict must accept Enc:1: %v", err)
	}
	if out2 != out {
		t.Fatalf("got %+v want %+v", out2, out)
	}
}

func TestEncryptedV2_KidTamperRejected(t *testing.T) {
	// k1 and k2 share key bytes so only the v2 AAD (not key mismatch)
	// distinguishes the tampered envelope.
	same := key32('s')
	kr := mustKeys(t, "k1", map[string][]byte{"k1": same, "k2": append([]byte(nil), same...)})
	enc := codec.EncryptedWithOptions(codec.JSON(), kr, codec.WithWriteVersion(codec.WriteVersionV2))
	data, err := enc.Marshal(secretPayload{Msg: "bound-to-k1"})
	if err != nil {
		t.Fatal(err)
	}
	env := mustV2Env(t, data)
	if env.Enc != 2 {
		t.Fatalf("precondition: want Enc 2, got %d", env.Enc)
	}
	env.KID = "k2"
	tampered, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	var out secretPayload
	if err := enc.Unmarshal(tampered, &out); err == nil {
		t.Fatal("rewritten kid must be rejected for Enc:2 (AAD-bound)")
	}
	// Contrast: the same rewrite on a legacy v1 envelope is NOT detected
	// because v1 seals with nil AAD.
	plain, err := codec.JSON().Marshal(secretPayload{Msg: "bound-to-k1"})
	if err != nil {
		t.Fatal(err)
	}
	v1 := sealV1(t, "k1", same, plain)
	var v1env v2env
	if err := json.Unmarshal(v1, &v1env); err != nil {
		t.Fatal(err)
	}
	v1env.KID = "k2"
	v1tampered, err := json.Marshal(v1env)
	if err != nil {
		t.Fatal(err)
	}
	var v1out secretPayload
	if err := enc.Unmarshal(v1tampered, &v1out); err != nil {
		t.Fatalf("precondition: v1 kid rewrite decrypts (nil AAD), got %v", err)
	}
}

func TestEncryptedV2_CrossRecordSwapRejected(t *testing.T) {
	enc := codec.EncryptedWithOptions(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}),
		codec.WithWriteVersion(codec.WriteVersionV2))
	dataA, err := enc.Marshal(secretPayload{Msg: "record-A", N: 1})
	if err != nil {
		t.Fatal(err)
	}
	dataB, err := enc.Marshal(secretPayload{Msg: "record-B", N: 2})
	if err != nil {
		t.Fatal(err)
	}
	envA := mustV2Env(t, dataA)
	envB := mustV2Env(t, dataB)

	// Swap ciphertexts between the two records (keep each nonce).
	swappedCT := envA
	swappedCT.CT = envB.CT
	raw, err := json.Marshal(swappedCT)
	if err != nil {
		t.Fatal(err)
	}
	var out secretPayload
	if err := enc.Unmarshal(raw, &out); err == nil {
		t.Fatal("cross-record ct swap must be rejected")
	}

	// Swap nonces between the two records (keep each ciphertext).
	swappedN := envA
	swappedN.N = envB.N
	raw, err = json.Marshal(swappedN)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Unmarshal(raw, &out); err == nil {
		t.Fatal("cross-record nonce swap must be rejected")
	}
}

func TestEncrypted_StrictRejectsPlaintext(t *testing.T) {
	kr := mustKeys(t, "k1", map[string][]byte{"k1": key32('a')})
	plain, err := codec.JSON().Marshal(secretPayload{Msg: "legacy", N: 7})
	if err != nil {
		t.Fatal(err)
	}
	strict := codec.EncryptedStrict(codec.JSON(), kr)
	var out secretPayload
	err = strict.Unmarshal(plain, &out)
	if err == nil || !strings.Contains(err.Error(), "strict") {
		t.Fatalf("strict mode must reject non-envelope, got %v", err)
	}
	// Non-strict keeps the fallback.
	lax := codec.Encrypted(codec.JSON(), kr)
	var out2 secretPayload
	if err := lax.Unmarshal(plain, &out2); err != nil {
		t.Fatalf("non-strict must accept plaintext, got %v", err)
	}
	// Strict still accepts real envelopes.
	data, err := strict.Marshal(secretPayload{Msg: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	var out3 secretPayload
	if err := strict.Unmarshal(data, &out3); err != nil {
		t.Fatalf("strict must accept envelopes, got %v", err)
	}
	// WithStrict(false) behaves like the default.
	explicit := codec.EncryptedWithOptions(codec.JSON(), kr, codec.WithStrict(false))
	var out4 secretPayload
	if err := explicit.Unmarshal(plain, &out4); err != nil {
		t.Fatalf("WithStrict(false) must accept plaintext, got %v", err)
	}
}

func TestEncrypted_PlaintextFallbackCountsAndLogs(t *testing.T) {
	kr := mustKeys(t, "k1", map[string][]byte{"k1": key32('a')})
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	var ext atomic.Int64
	hooks := 0
	enc := codec.EncryptedWithOptions(codec.JSON(), kr,
		codec.WithLogger(log),
		codec.WithFallbackCounter(&ext),
		codec.WithFallbackHook(func() { hooks++ }),
	)
	plain, err := codec.JSON().Marshal(secretPayload{Msg: "legacy"})
	if err != nil {
		t.Fatal(err)
	}
	var out secretPayload
	if err := enc.Unmarshal(plain, &out); err != nil {
		t.Fatal(err)
	}
	if got := codec.PlaintextFallbacks(enc); got != 1 {
		t.Fatalf("internal fallback counter = %d, want 1", got)
	}
	if got := ext.Load(); got != 1 {
		t.Fatalf("external fallback counter = %d, want 1", got)
	}
	if hooks != 1 {
		t.Fatalf("fallback hook calls = %d, want 1", hooks)
	}
	if !strings.Contains(buf.String(), "plaintext fallback") {
		t.Fatalf("want Warn log for fallback, got %q", buf.String())
	}
	// Envelope inputs must not bump the counter.
	data, err := enc.Marshal(secretPayload{Msg: "enc"})
	if err != nil {
		t.Fatal(err)
	}
	var out2 secretPayload
	if err := enc.Unmarshal(data, &out2); err != nil {
		t.Fatal(err)
	}
	if got := codec.PlaintextFallbacks(enc); got != 1 {
		t.Fatalf("envelope must not bump fallback counter, got %d", got)
	}
}

func TestEncrypted_UnsupportedVersionRejected(t *testing.T) {
	enc := codec.Encrypted(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	raw := []byte(`{"tasuki_enc":99,"kid":"k1","n":"","ct":""}`)
	var out secretPayload
	err := enc.Unmarshal(raw, &out)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("want unsupported version error, got %v", err)
	}
	strict := codec.EncryptedStrict(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	if err := strict.Unmarshal(raw, &out); err == nil {
		t.Fatal("strict must also reject unknown versions")
	}
}

func TestEncrypted_DefaultWritesV1ForRollingUpgrades(t *testing.T) {
	enc := codec.Encrypted(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	data, err := enc.Marshal(secretPayload{Msg: "staged", N: 1})
	if err != nil {
		t.Fatal(err)
	}
	env := mustV2Env(t, data)
	if env.Enc != 1 {
		t.Fatalf("default writes must stay on Enc 1 until v2 is enabled, got %d", env.Enc)
	}
	// Default (v1) writes round-trip.
	var out secretPayload
	if err := enc.Unmarshal(data, &out); err != nil {
		t.Fatalf("v1 roundtrip: %v", err)
	}
	if out.Msg != "staged" || out.N != 1 {
		t.Fatalf("got %+v", out)
	}
	// Explicit V1 opt-in behaves the same as the default.
	v1 := codec.EncryptedWithOptions(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}),
		codec.WithWriteVersion(codec.WriteVersionV1))
	v1data, err := v1.Marshal(secretPayload{Msg: "explicit-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := mustV2Env(t, v1data).Enc; got != 1 {
		t.Fatalf("explicit V1 must emit Enc 1, got %d", got)
	}
}

func TestEncrypted_StagedRolloutInterop(t *testing.T) {
	kr := mustKeys(t, "k1", map[string][]byte{"k1": key32('a')})
	v1writer := codec.Encrypted(codec.JSON(), kr)
	v2writer := codec.EncryptedWithOptions(codec.JSON(), kr,
		codec.WithWriteVersion(codec.WriteVersionV2))
	// A v2-capable reader accepts both versions regardless of its own
	// write version, so phase 1 (all readers upgraded, still writing v1)
	// and phase 2 (v2 writes enabled) both decrypt.
	for _, tc := range []struct {
		name string
		data []byte
		want secretPayload
	}{
		{"v1", mustMarshal(t, v1writer, secretPayload{Msg: "phase-1", N: 1}), secretPayload{Msg: "phase-1", N: 1}},
		{"v2", mustMarshal(t, v2writer, secretPayload{Msg: "phase-2", N: 2}), secretPayload{Msg: "phase-2", N: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reader := range []struct {
				name string
				c    interface {
					Unmarshal([]byte, any) error
				}
			}{
				{"v1-writer-reader", v1writer},
				{"v2-writer-reader", v2writer},
			} {
				var out secretPayload
				if err := reader.c.Unmarshal(tc.data, &out); err != nil {
					t.Fatalf("%s cannot read %s payload: %v", reader.name, tc.name, err)
				}
				if out != tc.want {
					t.Fatalf("%s read %+v, want %+v", reader.name, out, tc.want)
				}
			}
		})
	}
}

func mustMarshal(t *testing.T, c interface {
	Marshal(any) ([]byte, error)
}, v any) []byte {
	t.Helper()
	data, err := c.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestEncrypted_InvalidWriteVersionRejected(t *testing.T) {
	enc := codec.EncryptedWithOptions(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}),
		codec.WithWriteVersion(99))
	if _, err := enc.Marshal(secretPayload{Msg: "bad"}); err == nil ||
		!strings.Contains(err.Error(), "unsupported write version") {
		t.Fatalf("invalid write version must fail Marshal, got %v", err)
	}
}
