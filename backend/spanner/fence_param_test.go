package spanner

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestFencedStatementsEncodeAttemptAsInt64 is a regression test for the
// INT64 finding: fenced release/nack/renewal bind Go backend.Task.Attempt
// (int) against the Spanner attempt INT64 column. A native Go int param
// makes the Spanner client reject the DML, so the release/nack/renewal
// fails and the task stays hidden until lease expiry. All builders must
// encode attempt as int64.
func TestFencedStatementsEncodeAttemptAsInt64(t *testing.T) {
	claimed := backend.Task{ID: 7, WorkerID: "w1", Attempt: 3}
	now := time.Now().UTC()
	stmts := map[string]struct {
		params map[string]any
	}{
		"release": {fencedReleaseStatement(now, claimed).Params},
		"nack":    {fencedNackStatement(now, claimed, time.Second).Params},
		"renew":   {extendLeaseStatement(now, claimed).Params},
	}
	for name, s := range stmts {
		p, ok := s.params["attempt"]
		if name == "renew" {
			p, ok = s.params["a"]
		}
		if !ok {
			t.Fatalf("%s: missing attempt param", name)
		}
		if _, ok := p.(int64); !ok {
			t.Fatalf("%s: attempt param has Go type %T, want int64 (Spanner INT64 rejects Go int)", name, p)
		}
		if got := p.(int64); got != int64(claimed.Attempt) {
			t.Fatalf("%s: attempt param = %d, want %d", name, got, claimed.Attempt)
		}
	}
}

// TestFencedStatementsLegacyFallbackHasNoAttemptParam pins the zero-WorkerID
// fallback: legacy callers without a claim token still release/nack by ID
// with no attempt bind.
func TestFencedStatementsLegacyFallbackHasNoAttemptParam(t *testing.T) {
	legacy := backend.Task{ID: 9}
	now := time.Now().UTC()
	if _, ok := fencedReleaseStatement(now, legacy).Params["attempt"]; ok {
		t.Fatal("release fallback must not bind attempt")
	}
	if _, ok := fencedNackStatement(now, legacy, time.Second).Params["attempt"]; ok {
		t.Fatal("nack fallback must not bind attempt")
	}
}
