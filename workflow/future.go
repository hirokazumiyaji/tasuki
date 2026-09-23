package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// Awaitable is the type-erased future used by Await.
type Awaitable interface {
	Seq() int64
	ready(ctx *Context) bool
	errCanceled(ctx *Context) error
	scheduleErr() error
}

// Future holds a pending or completed durable operation.
//
// If scheduling fails before a command is recorded (e.g. input Marshal
// fails), the Future carries err and no journal command is written. This
// matches the sync Execute path, which returns the Marshal error before
// recording anything. No command is recorded because serialization is a
// deterministic function of (input, codec): on replay the same call fails
// the same way, so there is nothing durable to match against. Recording a
// command with a placeholder input (e.g. "null") would instead schedule a
// real activity/child with the wrong input and hide codec or key errors.
type Future[O any] struct {
	seq int64
	err error
}

func (f *Future[O]) Seq() int64 { return f.seq }

// scheduleErr reports a scheduling-time failure (e.g. Marshal error) that
// prevented a journal command from being recorded.
func (f *Future[O]) scheduleErr() error { return f.err }

func (f *Future[O]) ready(ctx *Context) bool {
	if f.err != nil {
		return true
	}
	_, ok := ctx.awaitCompletion(f.seq)
	return ok
}

func (f *Future[O]) errCanceled(ctx *Context) error {
	if f.err != nil {
		return f.err
	}
	if ctx.canceled {
		return ErrCanceled
	}
	return nil
}

// Get blocks (via journal replay / suspend) until the future completes.
// A scheduling failure is returned immediately without suspending.
func (f *Future[O]) Get(ctx *Context) (O, error) {
	var zero O
	if f.err != nil {
		return zero, f.err
	}
	comp, ok := ctx.awaitCompletion(f.seq)
	if !ok {
		if ctx.canceled {
			return zero, ErrCanceled
		}
		ctx.suspend()
		return zero, nil
	}
	switch comp.Type {
	case journal.TypeActivityFailed, journal.TypeChildFailed:
		var msg string
		_ = json.Unmarshal(comp.Payload, &msg)
		if msg == "" {
			msg = "operation failed"
		}
		return zero, fmt.Errorf("%s", msg)
	case journal.TypeTimerFired:
		return zero, nil
	default:
		var out O
		if len(comp.Payload) == 0 {
			return zero, nil
		}
		if err := ctx.codec.Unmarshal(comp.Payload, &out); err != nil {
			return zero, err
		}
		return out, nil
	}
}

func newFuture[O any](seq int64) *Future[O] {
	return &Future[O]{seq: seq}
}

// newFailedFuture returns a Future that is immediately ready with err and
// has no journal command (Seq -1). Callers must not record a command for it.
func newFailedFuture[O any](err error) *Future[O] {
	return &Future[O]{seq: -1, err: err}
}
