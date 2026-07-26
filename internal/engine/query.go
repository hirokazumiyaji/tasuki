package engine

import (
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// QueryResult is the outcome of a read-only workflow query replay.
type QueryResult struct {
	Payload []byte
	Err     error
	Stuck   bool
}

// RunQuery replays the workflow in query mode and invokes the named handler.
func RunQuery(events []journal.Event, now time.Time, queryName string, arg []byte, fn func(*workflow.Context) (any, error)) QueryResult {
	ctx := workflow.NewContext(events, now)
	ctx.SetQueryMode(true)
	done := make(chan struct{})
	var stuck error

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
		_, _ = fn(ctx)
	}()
	<-done

	if stuck != nil {
		return QueryResult{Stuck: true, Err: stuck}
	}
	payload, err := ctx.InvokeQuery(queryName, arg)
	return QueryResult{Payload: payload, Err: err}
}
