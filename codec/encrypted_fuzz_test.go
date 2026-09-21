package codec_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/hirokazumiyaji/tasuki/codec"
)

func FuzzEncryptedRoundTrip(f *testing.F) {
	key := bytes.Repeat([]byte{0x42}, 32)
	kr, err := codec.StaticKeys("k1", map[string][]byte{"k1": key})
	if err != nil {
		f.Fatal(err)
	}
	c := codec.Encrypted(codec.JSON(), kr)

	f.Add([]byte(``))
	f.Add([]byte(`hello`))
	f.Add([]byte(`{"n":1}`))
	f.Fuzz(func(t *testing.T, plain []byte) {
		type wrap struct {
			D []byte `json:"d"`
		}
		enc, err := c.Marshal(wrap{D: plain})
		if err != nil {
			t.Fatal(err)
		}
		var out wrap
		if err := c.Unmarshal(enc, &out); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.D, plain) {
			t.Fatalf("decrypt mismatch len=%d", len(plain))
		}
	})
}

func FuzzEncryptedGarbage(f *testing.F) {
	key := bytes.Repeat([]byte{0x7}, 32)
	kr, err := codec.StaticKeys("k1", map[string][]byte{"k1": key})
	if err != nil {
		f.Fatal(err)
	}
	c := codec.Encrypted(codec.JSON(), kr)

	f.Add([]byte(`not-json`))
	f.Add([]byte(`{"tasuki_enc":1,"kid":"k1","n":"AA","ct":"BB"}`))
	f.Add([]byte(`{"tasuki_enc":1,"kid":"missing","n":"","ct":""}`))
	// v2 seeds: unknown kid, empty envelope, and a tampered-kid envelope
	// shape (rejected: unknown key id or AAD mismatch).
	f.Add([]byte(`{"tasuki_enc":2,"kid":"missing","n":"","ct":""}`))
	f.Add([]byte(`{"tasuki_enc":2,"kid":"k1","n":"","ct":""}`))
	f.Add([]byte(`{"tasuki_enc":2,"kid":"tampered","n":"AAAAAAAAAAAAAAAA","ct":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		_ = c.Unmarshal(data, &v) // must not panic
	})
}

// anyKeyRing resolves every kid to the same key so kid-rewrite tests isolate
// AAD binding from key-mismatch failures.
type anyKeyRing struct {
	primary string
	key     []byte
}

func (r anyKeyRing) Primary() (string, []byte) { return r.primary, r.key }
func (r anyKeyRing) Lookup(string) ([]byte, bool) {
	return r.key, true
}

// FuzzEncryptedV2KidBinding seals one record then rewrites kid: any kid other
// than the original must be rejected for Enc:2 (kid is AAD-bound).
func FuzzEncryptedV2KidBinding(f *testing.F) {
	key := bytes.Repeat([]byte{0x9}, 32)
	c := codec.EncryptedWithOptions(codec.JSON(), anyKeyRing{primary: "k1", key: key},
		codec.WithWriteVersion(codec.WriteVersionV2))

	f.Add("k2")
	f.Add("tampered")
	f.Add("")
	f.Add("k1")
	f.Fuzz(func(t *testing.T, newKid string) {
		type wrap struct {
			D string `json:"d"`
		}
		enc, err := c.Marshal(wrap{D: "record"})
		if err != nil {
			t.Fatal(err)
		}
		var env struct {
			Enc int    `json:"tasuki_enc"`
			KID string `json:"kid"`
			N   []byte `json:"n"`
			CT  []byte `json:"ct"`
		}
		if err := json.Unmarshal(enc, &env); err != nil {
			t.Fatal(err)
		}
		if env.Enc != 2 {
			t.Fatalf("precondition: want Enc 2, got %d", env.Enc)
		}
		if newKid == env.KID {
			t.Skip("no tamper")
		}
		env.KID = newKid
		raw, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		var out wrap
		if err := c.Unmarshal(raw, &out); err == nil {
			t.Fatalf("rewritten kid %q must be rejected", newKid)
		}
	})
}

// FuzzEncryptedV2CrossRecord seals two records then swaps n/ct between them:
// mismatched pairs must be rejected for Enc:2.
func FuzzEncryptedV2CrossRecord(f *testing.F) {
	key := bytes.Repeat([]byte{0xA}, 32)
	kr, err := codec.StaticKeys("k1", map[string][]byte{"k1": key})
	if err != nil {
		f.Fatal(err)
	}
	c := codec.EncryptedWithOptions(codec.JSON(), kr, codec.WithWriteVersion(codec.WriteVersionV2))

	f.Add([]byte("alpha"), []byte("beta"))
	f.Add([]byte("a"), []byte("a"))
	f.Add([]byte(""), []byte("x"))
	f.Fuzz(func(t *testing.T, a, b []byte) {
		type wrap struct {
			D []byte `json:"d"`
		}
		encA, err := c.Marshal(wrap{D: a})
		if err != nil {
			t.Fatal(err)
		}
		encB, err := c.Marshal(wrap{D: b})
		if err != nil {
			t.Fatal(err)
		}
		var envA, envB struct {
			Enc int    `json:"tasuki_enc"`
			KID string `json:"kid"`
			N   []byte `json:"n"`
			CT  []byte `json:"ct"`
		}
		if err := json.Unmarshal(encA, &envA); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(encB, &envB); err != nil {
			t.Fatal(err)
		}
		// Swap ciphertexts (keep each nonce).
		swapped := envA
		swapped.CT = envB.CT
		// When both records happen to share nonce+ct (practically
		// impossible with random nonces) there is nothing to check.
		if bytes.Equal(swapped.N, envB.N) && bytes.Equal(swapped.CT, envB.CT) &&
			bytes.Equal(envA.N, envB.N) && bytes.Equal(envA.CT, envB.CT) {
			t.Skip("identical envelopes")
		}
		raw, err := json.Marshal(swapped)
		if err != nil {
			t.Fatal(err)
		}
		var out wrap
		if err := c.Unmarshal(raw, &out); err == nil {
			// Only acceptable when the swap is a no-op: same n and same ct.
			if !bytes.Equal(envA.N, envB.N) || !bytes.Equal(envA.CT, envB.CT) {
				t.Fatal("cross-record ct swap must be rejected")
			}
		}
		// Swap nonces (keep each ciphertext).
		swappedN := envA
		swappedN.N = envB.N
		raw, err = json.Marshal(swappedN)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Unmarshal(raw, &out); err == nil {
			if !bytes.Equal(envA.N, envB.N) || !bytes.Equal(envA.CT, envB.CT) {
				t.Fatal("cross-record nonce swap must be rejected")
			}
		}
	})
}
