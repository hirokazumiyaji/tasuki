package workflow

import (
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// ReceiveSignal waits for the next unmatched signal with the given name.
func ReceiveSignal[T any](ctx *Context, name string) (T, error) {
	var zero T
	ev, ok := ctx.takeSignal(name)
	if !ok {
		if ctx.canceled {
			return zero, ErrCanceled
		}
		ctx.suspend()
		return zero, nil
	}
	var out T
	if len(ev.Payload) > 0 {
		if err := ctx.codec.Unmarshal(ev.Payload, &out); err != nil {
			return zero, err
		}
	}
	return out, nil
}

// ReceiveSignalWithTimeout waits for a signal or times out via a durable timer.
func ReceiveSignalWithTimeout[T any](ctx *Context, name string, d time.Duration) (T, bool, error) {
	var zero T
	// Replay an already-recorded timer before checking signals, so command
	// matching stays aligned (like Sleep). Otherwise a timer recorded while
	// waiting that is skipped on replay leaves cmdIndex behind and the next
	// command fails with a determinism violation.
	var timer *Future[struct{}]
	if rec, ok := ctx.peekCommand(); ok && rec.Type == journal.TypeTimerCreated {
		timer = SleepAsync(ctx, d)
	} else {
		if ctx.canceled {
			return zero, false, ErrCanceled
		}
		// No recorded timer: if a signal is already available before scheduling,
		// still need stable history — only schedule when signal is not yet
		// present at this point in replay.
		if _, ok := ctx.peekSignal(name); ok {
			v, err := ReceiveSignal[T](ctx, name)
			return v, true, err
		}
		timer = SleepAsync(ctx, d)
	}
	if ctx.canceled {
		return zero, false, ErrCanceled
	}
	if _, ok := ctx.peekSignal(name); ok {
		v, err := ReceiveSignal[T](ctx, name)
		return v, true, err
	}
	if timer.ready(ctx) {
		_, err := timer.Get(ctx)
		return zero, false, err
	}
	ctx.suspend()
	return zero, false, nil
}
