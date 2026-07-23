package workflow

import (
	"encoding/json"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// ExecuteAsync schedules an activity and returns a Future without waiting.
func ExecuteAsync[I, O any](ctx *Context, activityName string, in I, opts ...ExecuteOption) *Future[O] {
	var eo executeOptions
	for _, opt := range opts {
		opt(&eo)
	}
	input, err := json.Marshal(in)
	if err != nil {
		// Schedule still needs a command for determinism; use empty input on marshal failure.
		input = []byte("null")
	}
	sched := ActivitySchedule{Input: input}
	if eo.retry != (RetryPolicy{}) {
		r := eo.retry.withDefaults()
		sched.Retry = &RetryPolicyJSON{
			InitialIntervalMs:  r.InitialInterval.Milliseconds(),
			BackoffCoefficient: r.BackoffCoefficient,
			MaxIntervalMs:      r.MaxInterval.Milliseconds(),
			MaxAttempts:        r.MaxAttempts,
		}
	}
	payload, _ := json.Marshal(sched)
	ev := ctx.recordOrReplay(journal.Command{
		Type: journal.TypeActivityScheduled,
		Name: activityName,
	}, payload)
	return newFuture[O](ev.Seq)
}

// SleepAsync schedules a durable timer and returns a Future.
func SleepAsync(ctx *Context, d time.Duration) *Future[struct{}] {
	payload, _ := json.Marshal(timerPayload{FireAt: ctx.now.Add(d)})
	ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeTimerCreated}, payload)
	return newFuture[struct{}](ev.Seq)
}

// Await returns the index of the first ready future, or suspends if none are ready.
func Await(ctx *Context, futures ...Awaitable) (int, error) {
	if ctx.canceled {
		return -1, ErrCanceled
	}
	if len(futures) == 0 {
		return -1, nil
	}
	for i, f := range futures {
		if f.ready(ctx) {
			return i, nil
		}
	}
	ctx.suspend()
	return -1, nil
}

// AwaitAll waits until all futures are ready, then returns the first Get error.
func AwaitAll(ctx *Context, futures ...Awaitable) error {
	if ctx.canceled {
		return ErrCanceled
	}
	for _, f := range futures {
		if !f.ready(ctx) {
			ctx.suspend()
			return nil
		}
	}
	for _, f := range futures {
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
