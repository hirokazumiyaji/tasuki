package codec_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/hirokazumiyaji/tasuki/codec"
)

func FuzzJSONRoundTrip(f *testing.F) {
	f.Add([]byte(`null`))
	f.Add([]byte(`{"a":1}`))
	f.Add([]byte(`[1,2,"x"]`))
	f.Add([]byte(`"hi"`))
	f.Fuzz(func(t *testing.T, data []byte) {
		c := codec.JSON()
		var v any
		if err := c.Unmarshal(data, &v); err != nil {
			return
		}
		out, err := c.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var v2 any
		if err := c.Unmarshal(out, &v2); err != nil {
			t.Fatal(err)
		}
		// Re-marshal both for stable compare of JSON values.
		a, _ := json.Marshal(v)
		b, _ := json.Marshal(v2)
		if !bytes.Equal(a, b) {
			t.Fatalf("round-trip mismatch: %s vs %s", a, b)
		}
	})
}
