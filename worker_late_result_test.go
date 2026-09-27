package tasuki

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestResolveLocalResultDiscardsLateResults is a regression test for the
// local-timeout finding: a local activity returning at/after the deadline
// leaves both done and ctx.Done() ready, and select then picks
// nondeterministically, so a success delivered after expiry must be
// discarded (timeout) rather than journaled.
//
// The race itself is timing-nondeterministic and cannot be forced
// black-box ( whichever branch select evaluates first wins; runtime
// scheduling systematically favors the timer branch, and outcome rates in
// the race band overlap fully between fixed and unfixed code). The fix
// therefore routes both select branches through resolveLocalResult, whose
// timeout re-check is pinned deterministically here: expired-at-acceptance
// always yields the timeout error, even for a successful result.
func TestResolveLocalResultDiscardsLateResults(t *testing.T) {
	const timeout = 50 * time.Millisecond

	// An already-expired acceptance context with a successful result: the
	// both-branches-ready case. Must be discarded as a timeout.
	expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(10 * time.Millisecond) // let the deadline pass deterministically
	if expired.Err() == nil {
		t.Fatal("test setup: acceptance context should be expired")
	}
	out, err := resolveLocalResult("late", []byte("late-ok"), nil, time.Time{}, expired, context.Background(), timeout)
	if err == nil {
		t.Fatalf("expired success accepted (out=%q), want timeout error", out)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want deadline exceeded", err)
	}

	// Same, but the activity itself failed: still a timeout, never the
	// late failure payload.
	_, err = resolveLocalResult("late", nil, errors.New("boom"), time.Time{}, expired, context.Background(), timeout)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want deadline exceeded", err)
	}

	// Shutdown cancellation surfaces the turn error, not a local timeout.
	runCtx, stop := context.WithCancel(context.Background())
	stop()
	expired2, cancel2 := context.WithTimeout(runCtx, time.Nanosecond)
	defer cancel2()
	time.Sleep(10 * time.Millisecond)
	_, err = resolveLocalResult("x", []byte("v"), nil, time.Time{}, expired2, runCtx, timeout)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want turn cancellation", err)
	}

	// Live acceptance passes results and activity errors through.
	live, liveCancel := context.WithCancel(context.Background())
	defer liveCancel()
	if out, err := resolveLocalResult("ok", []byte("v"), nil, time.Time{}, live, context.Background(), timeout); err != nil || string(out) != "v" {
		t.Fatalf("live success: out=%q err=%v", out, err)
	}
	if _, err := resolveLocalResult("ok", nil, errors.New("boom"), time.Time{}, live, context.Background(), timeout); err == nil || err.Error() != "boom" {
		t.Fatalf("live activity error: err=%v", err)
	}
}
