package codec_test

import (
	"bytes"
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
	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		_ = c.Unmarshal(data, &v) // must not panic
	})
}
