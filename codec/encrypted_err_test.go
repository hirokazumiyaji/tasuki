package codec_test

import (
	"errors"
	"testing"

	"github.com/hirokazumiyaji/tasuki/codec"
)

type failCodec struct{}

func (failCodec) Marshal(v any) ([]byte, error) { return nil, errors.New("marshal fail") }
func (failCodec) Unmarshal(data []byte, v any) error {
	return errors.New("unmarshal fail")
}

func TestEncrypted_InnerMarshalError(t *testing.T) {
	key := make([]byte, 32)
	kr, err := codec.StaticKeys("k1", map[string][]byte{"k1": key})
	if err != nil {
		t.Fatal(err)
	}
	c := codec.Encrypted(failCodec{}, kr)
	if _, err := c.Marshal("x"); err == nil {
		t.Fatal("want marshal error")
	}
}
