package workflow

import (
	"encoding/json"
	"time"
)

// ReceiveSignal waits for the next unmatched signal with the given name.
func ReceiveSignal[T any](ctx *Context, name string) (T, error) {
	var zero T
	if ctx.canceled {
		return zero, ErrCanceled
	}
	ev, ok := ctx.takeSignal(name)
	if !ok {
		ctx.suspend()
		return zero, nil
	}
	var out T
	if len(ev.Payload) > 0 {
		if err := json.Unmarshal(ev.Payload, &out); err != nil {
			return zero, err
		}
	}
	return out, nil
}

// ReceiveSignalWithTimeout waits for a signal or times out via a durable timer.
func ReceiveSignalWithTimeout[T any](ctx *Context, name string, d time.Duration) (T, bool, error) {
	var zero T
	if ctx.canceled {
		return zero, false, ErrCanceled
	}
	// Always schedule a timer command for a deterministic call sequence when waiting with timeout.
	// If a signal is already available before scheduling, still need stable history — only schedule
	// when signal is not yet present at this point in replay.
	if _, ok := ctx.peekSignal(name); ok {
		v, err := ReceiveSignal[T](ctx, name)
		return v, true, err
	}
	timer := SleepAsync(ctx, d)
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
