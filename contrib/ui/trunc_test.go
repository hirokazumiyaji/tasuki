package ui

import "testing"

func TestTruncStr(t *testing.T) {
	if got := truncStr("ab", 10); got != "ab" {
		t.Fatalf("%q", got)
	}
	if got := truncStr("abcdef", 3); got != "abc…" {
		t.Fatalf("%q", got)
	}
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'x'
	}
	got := truncStr(string(long), 0) // n<=0 → default 256
	if len(got) != 256+len("…") {
		t.Fatalf("len=%d", len(got))
	}
	if got[len(got)-len("…"):] != "…" {
		t.Fatalf("%q", got[len(got)-3:])
	}
}
