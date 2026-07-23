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
}

// Future holds a pending or completed durable operation.
type Future[O any] struct {
	seq int64
}

func (f *Future[O]) Seq() int64 { return f.seq }

func (f *Future[O]) ready(ctx *Context) bool {
	_, ok := ctx.awaitCompletion(f.seq)
	return ok
}

func (f *Future[O]) errCanceled(ctx *Context) error {
	if ctx.canceled {
		return ErrCanceled
	}
	return nil
}

// Get blocks (via journal replay / suspend) until the future completes.
func (f *Future[O]) Get(ctx *Context) (O, error) {
	var zero O
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
		if err := json.Unmarshal(comp.Payload, &out); err != nil {
			return zero, err
		}
		return out, nil
	}
}

func newFuture[O any](seq int64) *Future[O] {
	return &Future[O]{seq: seq}
}
