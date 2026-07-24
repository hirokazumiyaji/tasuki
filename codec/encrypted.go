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

// envelope is a JSON object so it can live in jsonb columns; []byte fields
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
