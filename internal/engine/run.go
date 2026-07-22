package engine

import (
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

type Result struct {
	Suspended   bool
	Result      any
	Err         error
	NewCommands []journal.Event
	Stuck       bool
}

func Run(events []journal.Event, fn func(*workflow.Context) (any, error)) Result {
	return RunAt(events, time.Time{}, fn)
}

func RunAt(events []journal.Event, now time.Time, fn func(*workflow.Context) (any, error)) Result {
	ctx := workflow.NewContext(events, now)
	done := make(chan struct{})
	var (
		result any
		fnErr  error
		stuck  error
	)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				if err, ok := workflow.AsDeterminismPanic(r); ok {
					stuck = err
				} else {
					stuck = fmt.Errorf("workflow panic: %v", r)
				}
			}
			close(done)
		}()
		result, fnErr = fn(ctx)
	}()

	<-done

	out := Result{NewCommands: ctx.NewCommands()}
	if stuck != nil {
		out.Stuck = true
		out.Err = stuck
		return out
	}
	if workflow.WasSuspended(ctx) {
		out.Suspended = true
		return out
	}
	out.Result = result
	out.Err = fnErr
	return out
}
