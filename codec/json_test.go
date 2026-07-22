package codec_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/codec"
)

func TestJSONRoundTrip(t *testing.T) {
	c := codec.JSON()
	type payload struct {
		FireAt time.Time `json:"fire_at"`
	}
	in := payload{FireAt: time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)}
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out payload
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !out.FireAt.Equal(in.FireAt) {
		t.Fatalf("got %v want %v", out.FireAt, in.FireAt)
	}
}
