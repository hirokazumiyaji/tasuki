package workflow

import (
	"encoding/json"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// ExecuteAsync schedules an activity and returns a Future without waiting.
//
// If the input cannot be marshaled (or the schedule payload cannot be
// encoded), no journal command is recorded and the returned Future carries
// the error: Get (and AwaitAll) report it, and Await treats the future as
// immediately ready. This matches the sync Execute path, which returns the
// Marshal error before recording anything.
func ExecuteAsync[I, O any](ctx *Context, activityName string, in I, opts ...ExecuteOption) *Future[O] {
	var eo executeOptions
	for _, opt := range opts {
		opt(&eo)
	}
	input, err := ctx.codec.Marshal(in)
	if err != nil {
		return newFailedFuture[O](err)
	}
	sched := ActivitySchedule{Input: input}
	applyExecuteOptions(&sched, eo)
	payload, err := json.Marshal(sched)
	if err != nil {
		return newFailedFuture[O](err)
	}
	ev := ctx.recordOrReplay(journal.Command{
		Type: journal.TypeActivityScheduled,
		Name: activityName,
	}, payload)
	return newFuture[O](ev.Seq)
}

// SleepAsync schedules a durable timer and returns a Future.
func SleepAsync(ctx *Context, d time.Duration) *Future[struct{}] {
	payload, err := json.Marshal(timerPayload{FireAt: ctx.now.Add(d)})
	if err != nil {
		return newFailedFuture[struct{}](err)
	}
	ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeTimerCreated}, payload)
	return newFuture[struct{}](ev.Seq)
}

// Await returns the index of the first ready future, or suspends if none are ready.
func Await(ctx *Context, futures ...Awaitable) (int, error) {
	if len(futures) == 0 {
		return -1, nil
	}
	for i, f := range futures {
		if f.ready(ctx) {
			return i, nil
		}
	}
	if ctx.canceled {
		return -1, ErrCanceled
	}
	ctx.suspend()
	return -1, nil
}

// AwaitAll waits until all futures are ready, then returns the first Get error.
// Scheduling failures (e.g. Marshal errors stored in a Future) are returned
// as well, in argument order, before completion-payload errors.
func AwaitAll(ctx *Context, futures ...Awaitable) error {
	for _, f := range futures {
		if !f.ready(ctx) {
			if ctx.canceled {
				return ErrCanceled
			}
			ctx.suspend()
			return nil
		}
	}
	for _, f := range futures {
		if err := f.scheduleErr(); err != nil {
			return err
		}
		// Type-erase Get via checking completion payload errors.
		comp, ok := ctx.awaitCompletion(f.Seq())
		if !ok {
			continue
		}
		if comp.Type == journal.TypeActivityFailed || comp.Type == journal.TypeChildFailed {
			var msg string
			_ = json.Unmarshal(comp.Payload, &msg)
			if msg == "" {
				msg = "operation failed"
			}
			return jsonError(msg)
		}
	}
	return nil
}

type stringError string

func (e stringError) Error() string { return string(e) }

func jsonError(msg string) error { return stringError(msg) }
