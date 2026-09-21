package codec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
)

// Envelope versions for the encrypted codec.
//
//   - encV1 (1): legacy envelope, sealed with nil AAD. This is the default
//     write version so rolling upgrades stay safe: old readers (pre-v2)
//     can still decrypt new payloads.
//   - encV2 (2): hardened envelope, sealed with domain-separated AAD
//     binding the key id, so kid rewrites are detected by GCM
//     authentication. Readers accept both versions; opt into v2 writes
//     with WithWriteVersion(WriteVersionV2) only after every reader
//     (workers and replay tooling) understands v2.
const (
	encV1 = 1
	encV2 = 2
)

// Staged write versions for the encrypted codec. Readers always accept
// both v1 and v2 envelopes; the write version only controls what Marshal
// emits. Keep the default (V1) until all readers are upgraded, then opt
// into V2 writes.
const (
	WriteVersionV1 = encV1
	WriteVersionV2 = encV2
)

// encV2AADPrefix domain-separates the v2 additional authenticated data.
// The full AAD is encV2AADPrefix + kid.
const encV2AADPrefix = "tasuki/enc/v2|"

// encV2AAD returns the AAD binding a v2 envelope to its key id.
func encV2AAD(kid string) []byte { return []byte(encV2AADPrefix + kid) }

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

	strict       bool
	writeVersion int
	logger       *slog.Logger
	onFallback   func()
	extCounter   *atomic.Int64
	fallbacks    atomic.Int64
}

// EncryptedOption customizes an encrypted codec built with EncryptedWithOptions.
type EncryptedOption func(*encryptedCodec)

// WithStrict makes Unmarshal reject payloads without the envelope marker
// instead of passing them through to the inner codec. Default is false
// (legacy plaintext fallback with Warn log + counter).
func WithStrict(strict bool) EncryptedOption {
	return func(c *encryptedCodec) { c.strict = strict }
}

// WithWriteVersion sets the envelope version used for new writes.
// WriteVersionV1 (default) seals with nil AAD and stays readable by
// pre-v2 binaries; WriteVersionV2 seals with AAD binding the key id.
// Readers accept both versions regardless of this setting, so the safe
// rolling-upgrade order is: (1) deploy v2-capable readers while still
// writing V1, (2) opt into V2 writes once every reader understands v2.
// Any other value makes Marshal fail with an unsupported-version error.
func WithWriteVersion(v int) EncryptedOption {
	return func(c *encryptedCodec) { c.writeVersion = v }
}

// WithLogger sets the logger used for the plaintext-fallback Warn log.
// Defaults to slog.Default() when nil or unset.
func WithLogger(l *slog.Logger) EncryptedOption {
	return func(c *encryptedCodec) { c.logger = l }
}

// WithFallbackHook sets a hook invoked on every plaintext fallback
// (non-envelope input accepted via the inner codec). It is not called in
// strict mode or for envelope inputs. Useful for wiring custom metrics.
func WithFallbackHook(fn func()) EncryptedOption {
	return func(c *encryptedCodec) { c.onFallback = fn }
}

// WithFallbackCounter sets an external counter incremented on every
// plaintext fallback, in addition to the codec's internal counter readable
// via PlaintextFallbacks.
func WithFallbackCounter(ctr *atomic.Int64) EncryptedOption {
	return func(c *encryptedCodec) { c.extCounter = ctr }
}

// Encrypted wraps inner so every payload is stored as an AES-256-GCM JSON
// envelope. New writes default to the v1 envelope (Enc 1, nil AAD) so
// rolling upgrades stay safe: readers accept both v1 and v2, but old
// (pre-v2) binaries can only decrypt v1. Opt into v2 writes with
// EncryptedWithOptions(inner, keys, WithWriteVersion(WriteVersionV2))
// only after every reader understands v2; a v2 payload handed to an old
// reader fails authentication and that decode error would otherwise be
// committed as a terminal workflow failure instead of an incompatible-task
// Nack. Payloads without the envelope marker are passed through to inner,
// so encryption can be enabled while plaintext payloads remain; each
// fallback emits a Warn log and bumps the plaintext-fallback counter. Use
// EncryptedWithOptions with WithStrict(true) (or EncryptedStrict) to reject
// non-envelope payloads instead.
func Encrypted(inner Codec, keys Keyring) Codec {
	return EncryptedWithOptions(inner, keys)
}

// EncryptedWithOptions is like Encrypted with additional options
// (write version, strict mode, logger, fallback hook/counter).
// The default write version is WriteVersionV1.
func EncryptedWithOptions(inner Codec, keys Keyring, opts ...EncryptedOption) Codec {
	c := &encryptedCodec{inner: inner, keys: keys, writeVersion: encV1}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// EncryptedStrict wraps inner like Encrypted but rejects payloads without
// the envelope marker instead of falling back to plaintext.
// It keeps the default V1 write version; combine WithStrict(true) with
// WithWriteVersion(WriteVersionV2) via EncryptedWithOptions for v2 writes.
func EncryptedStrict(inner Codec, keys Keyring) Codec {
	return EncryptedWithOptions(inner, keys, WithStrict(true))
}

// PlaintextFallbacks reports how many non-envelope payloads c accepted via
// the plaintext fallback. It returns 0 for codecs that are not built by
// Encrypted/EncryptedWithOptions/EncryptedStrict.
func PlaintextFallbacks(c Codec) int64 {
	if ec, ok := c.(*encryptedCodec); ok {
		return ec.fallbacks.Load()
	}
	return 0
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
	switch c.writeVersion {
	case 0, encV1:
		// Default: legacy envelope readable by pre-v2 binaries.
		return json.Marshal(envelope{Enc: encV1, KID: kid, N: nonce, CT: aead.Seal(nil, nonce, plain, nil)})
	case encV2:
		return json.Marshal(envelope{Enc: encV2, KID: kid, N: nonce, CT: aead.Seal(nil, nonce, plain, encV2AAD(kid))})
	default:
		return nil, fmt.Errorf("codec: unsupported write version %d", c.writeVersion)
	}
}

func (c *encryptedCodec) Unmarshal(data []byte, v any) error {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil || env.Enc == 0 {
		if c.strict {
			return fmt.Errorf("codec: strict mode rejects non-envelope payload")
		}
		c.noteFallback()
		return c.inner.Unmarshal(data, v)
	}
	switch env.Enc {
	case encV2:
		key, ok := c.keys.Lookup(env.KID)
		if !ok {
			return fmt.Errorf("codec: unknown key id %q", env.KID)
		}
		aead, err := newAEAD(env.KID, key)
		if err != nil {
			return err
		}
		if len(env.N) != aead.NonceSize() {
			return fmt.Errorf("codec: decrypt with key %q: invalid nonce size %d", env.KID, len(env.N))
		}
		plain, err := aead.Open(nil, env.N, env.CT, encV2AAD(env.KID))
		if err != nil {
			return fmt.Errorf("codec: decrypt with key %q: %w", env.KID, err)
		}
		return c.inner.Unmarshal(plain, v)
	case encV1:
		// Legacy envelope (also the default write version): sealed with
		// nil AAD, kid outside authentication.
		key, ok := c.keys.Lookup(env.KID)
		if !ok {
			return fmt.Errorf("codec: unknown key id %q", env.KID)
		}
		aead, err := newAEAD(env.KID, key)
		if err != nil {
			return err
		}
		if len(env.N) != aead.NonceSize() {
			return fmt.Errorf("codec: decrypt with key %q: invalid nonce size %d", env.KID, len(env.N))
		}
		plain, err := aead.Open(nil, env.N, env.CT, nil)
		if err != nil {
			return fmt.Errorf("codec: decrypt with key %q: %w", env.KID, err)
		}
		return c.inner.Unmarshal(plain, v)
	default:
		return fmt.Errorf("codec: unsupported envelope version %d", env.Enc)
	}
}

// noteFallback records a plaintext fallback: internal counter, optional
// external counter, optional hook, and a Warn log.
func (c *encryptedCodec) noteFallback() {
	c.fallbacks.Add(1)
	if c.extCounter != nil {
		c.extCounter.Add(1)
	}
	if c.onFallback != nil {
		c.onFallback()
	}
	l := c.logger
	if l == nil {
		l = slog.Default()
	}
	l.Warn("codec: plaintext fallback for non-envelope payload")
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
