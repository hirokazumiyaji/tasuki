package tasuki

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestExpectedNextSeq(t *testing.T) {
	if got := expectedNextSeq(nil); got != 1 {
		t.Fatalf("empty=%d", got)
	}
	if got := expectedNextSeq([]journal.Event{{Seq: 3}, {Seq: 7}}); got != 8 {
		t.Fatalf("got %d", got)
	}
}
