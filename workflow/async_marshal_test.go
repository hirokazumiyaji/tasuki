package workflow_test

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// errFailCodec simulates an encryption codec with a missing key: every
// Marshal fails.
type errFailCodec struct{ err error }

func (c errFailCodec) Marshal(v any) ([]byte, error) { return nil, c.err }
func (c errFailCodec) Unmarshal(data []byte, v any) error {
	return errors.New("unreachable")
}

func TestExecuteAsync_MarshalError(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		// func values are not JSON-marshalable with the default codec.
		f := workflow.ExecuteAsync[func(), string](ctx, "a", func() {})
		if got := len(ctx.NewCommands()); got != 0 {
			t.Fatalf("no command must be recorded on marshal failure, got %d", got)
		}
		if f.Seq() != -1 {
			t.Fatalf("failed future Seq = %d, want -1", f.Seq())
		}
		idx, err := workflow.Await(ctx, f)
		if err != nil {
			t.Fatalf("Await err = %v", err)
		}
		if idx != 0 {
			t.Fatalf("Await idx = %d, want 0 (failed future is immediately ready)", idx)
		}
		_, err = f.Get(ctx)
		if err == nil || !strings.Contains(err.Error(), "unsupported type") {
			t.Fatalf("Get err = %v, want marshal error", err)
		}
		return nil, err
	})
	if res.Suspended {
		t.Fatal("must not suspend on marshal failure")
	}
	if len(res.NewCommands) != 0 {
		t.Fatalf("no command must be recorded, got %+v", res.NewCommands)
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "unsupported type") {
		t.Fatalf("Err = %v, want marshal error", res.Err)
	}
}

func TestExecuteAsync_MarshalError_CustomCodec(t *testing.T) {
	sentinel := errors.New("encryption key missing")
	ctx := workflow.NewContext([]journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
	}, time.Time{})
	ctx.SetCodec(errFailCodec{err: sentinel})
	f := workflow.ExecuteAsync[int, int](ctx, "a", 1)
	if got := len(ctx.NewCommands()); got != 0 {
		t.Fatalf("no command must be recorded, got %d", got)
	}
	if _, err := f.Get(ctx); !errors.Is(err, sentinel) {
		t.Fatalf("Get err = %v, want %v", err, sentinel)
	}
	if err := workflow.AwaitAll(ctx, f); !errors.Is(err, sentinel) {
		t.Fatalf("AwaitAll err = %v, want %v", err, sentinel)
	}
}

func TestExecuteAsync_SchedulePayloadMarshalError(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		// NaN cannot be encoded to JSON, forcing json.Marshal(sched) to fail.
		f := workflow.ExecuteAsync[int, int](ctx, "a", 1,
			workflow.WithRetry(workflow.RetryPolicy{BackoffCoefficient: math.NaN()}))
		if got := len(ctx.NewCommands()); got != 0 {
			t.Fatalf("no command must be recorded, got %d", got)
		}
		_, err := f.Get(ctx)
		if err == nil {
			t.Fatal("want schedule payload marshal error")
		}
		return nil, err
	})
	if res.Suspended {
		t.Fatal("must not suspend on schedule payload marshal failure")
	}
	if len(res.NewCommands) != 0 {
		t.Fatalf("got %+v", res.NewCommands)
	}
	if res.Err == nil {
		t.Fatal("want error")
	}
}

func TestExecuteChildAsync_MarshalError(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "parent"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		ctx.SetInfo(workflow.WorkflowInfo{InstanceID: "p1"})
		f := workflow.ExecuteChildAsync[func(), int](ctx, "child", func() {})
		if got := len(ctx.NewCommands()); got != 0 {
			t.Fatalf("no command must be recorded, got %d", got)
		}
		_, err := f.Get(ctx)
		if err == nil || !strings.Contains(err.Error(), "unsupported type") {
			t.Fatalf("Get err = %v, want marshal error", err)
		}
		return nil, err
	})
	if res.Suspended {
		t.Fatal("must not suspend on marshal failure")
	}
	if len(res.NewCommands) != 0 {
		t.Fatalf("got %+v", res.NewCommands)
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "unsupported type") {
		t.Fatalf("Err = %v", res.Err)
	}
}

func TestExecuteChild_MarshalError(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "parent"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		ctx.SetInfo(workflow.WorkflowInfo{InstanceID: "p1"})
		return workflow.ExecuteChild[func(), int](ctx, "child", func() {})
	})
	if res.Suspended {
		t.Fatal("must not suspend on marshal failure")
	}
	if len(res.NewCommands) != 0 {
		t.Fatalf("got %+v", res.NewCommands)
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "unsupported type") {
		t.Fatalf("Err = %v", res.Err)
	}
}

func TestAwaitAll_PropagatesMarshalError(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		bad := workflow.ExecuteAsync[func(), string](ctx, "a", func() {})
		return nil, workflow.AwaitAll(ctx, bad)
	})
	if res.Suspended {
		t.Fatal("AwaitAll must not suspend on marshal failure")
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "unsupported type") {
		t.Fatalf("Err = %v, want marshal error", res.Err)
	}
}
