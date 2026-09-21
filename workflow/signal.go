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
	// Replay an already-recorded timer created by this wait before checking
	// signals, so command matching stays aligned (like Sleep). Otherwise a
	// timer recorded while waiting that is skipped on replay leaves cmdIndex
	// behind and the next command fails with a determinism violation. A peeked
	// timer predated by an unconsumed signal belongs to a following wait and
	// is left for it (see below).
	var timer *Future[struct{}]
	if rec, ok := ctx.peekCommand(); ok && rec.Type == journal.TypeTimerCreated {
		// Only consume the peeked timer if this wait created it. When an
		// unconsumed signal predates the peeked timer (signal.Seq < timer.Seq),
		// the first execution took that signal without scheduling a timer, so
		// the peeked timer belongs to a following Sleep/timed wait and must be
		// left for it to consume.
		consume := true
		if sig, hasSignal := ctx.peekSignal(name); hasSignal && sig.Seq < rec.Seq {
			consume = false
		}
		if consume {
			timer = SleepAsync(ctx, d)
		}
	}
	if timer == nil {
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
