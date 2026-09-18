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
	if env["tasuki_enc"] != float64(2) || env["kid"] != "k1" {
		t.Fatalf("unexpected envelope: %v", env)
	}
	if strings.Contains(string(data), "top-secret") {
		t.Fatal("plaintext leaked into envelope")
	}
}

func TestEncrypted_PlaintextFallback(t *testing.T) {
	enc := codec.Encrypted(codec.JSON(), mustKeys(t, "k1", map[string][]byte{"k1": key32('a')}))
	plain, err := codec.JSON().Marshal(secretPayload{Msg: "legacy", N: 7})
	if err != nil {
		t.Fatal(err)
	}
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

func (badKeyring) Primary() (string, []byte)    { return "bad", []byte("short") }
func (badKeyring) Lookup(string) ([]byte, bool) { return []byte("short"), true }

func TestEncrypted_BadKeySizeAtUse(t *testing.T) {
	enc := codec.Encrypted(codec.JSON(), badKeyring{})
	if _, err := enc.Marshal(secretPayload{}); err == nil || !strings.Contains(err.Error(), "32 bytes") {
		t.Fatalf("want 32-byte key error, got %v", err)
	}
}
