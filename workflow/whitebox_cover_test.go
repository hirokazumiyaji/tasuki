package workflow

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestDeterminismPanic_ErrorUnwrap(t *testing.T) {
	inner := errors.New("det")
	d := determinismPanic{err: inner}
	if d.Error() != "det" {
		t.Fatal(d.Error())
	}
	if !errors.Is(d, inner) {
		t.Fatal("unwrap")
	}
	err, ok := AsDeterminismPanic(d)
	if !ok || !errors.Is(err, inner) {
		t.Fatalf("%v %v", err, ok)
	}
}

func TestFuture_errCanceled(t *testing.T) {
	f := newFuture[int](1)
	ctx := NewContext(nil, time.Time{})
	if err := f.errCanceled(ctx); err != nil {
		t.Fatal(err)
	}
	ctx = NewContext([]journal.Event{
		{Seq: 1, Type: journal.TypeCancelRequested},
	}, time.Time{})
	if !errors.Is(f.errCanceled(ctx), ErrCanceled) {
		t.Fatal("want ErrCanceled")
	}
}

func TestGetVersion_SkipsForeignMarker(t *testing.T) {
	other, _ := json.Marshal(versionPayload{ChangeID: "other", Version: 9})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeVersionMarker, Name: "other", Payload: other},
		{Seq: 3, Type: journal.TypeTimerCreated},
		{Seq: 4, Type: journal.TypeTimerFired, RefSeq: 3},
	}
	ctx := NewContext(events, time.Time{})
	// Foreign marker is skipped; timer ahead → return min without recording.
	if v := GetVersion(ctx, "mine", 1, 5); v != 1 {
		t.Fatalf("got %d want min=1", v)
	}
	if err := Sleep(ctx, 0); err != nil {
		t.Fatal(err)
	}
}
