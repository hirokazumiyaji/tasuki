package tasuki

import (
	"errors"
	"testing"
)

func TestNonRetryable_NilAndUnwrap(t *testing.T) {
	if NonRetryable(nil) != nil {
		t.Fatal("nil in → nil out")
	}
	base := errors.New("boom")
	err := NonRetryable(base)
	if !IsNonRetryable(err) {
		t.Fatal("want non-retryable")
	}
	if !errors.Is(err, base) {
		t.Fatal("unwrap")
	}
	if IsNonRetryable(base) {
		t.Fatal("plain error")
	}
}
