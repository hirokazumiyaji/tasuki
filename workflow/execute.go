package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// Execute schedules an activity by name and waits for its completion event.
// If the completion is not yet in the journal, the workflow goroutine suspends.
func Execute[I, O any](ctx *Context, activityName string, in I) (O, error) {
	var zero O
	payload, err := json.Marshal(in)
	if err != nil {
		return zero, err
	}
	ev := ctx.recordOrReplay(journal.Command{
		Type: journal.TypeActivityScheduled,
		Name: activityName,
	}, payload)

	comp, ok := ctx.awaitCompletion(ev.Seq)
	if !ok {
		ctx.suspend()
		return zero, nil
	}
	if comp.Type == journal.TypeActivityFailed {
		var msg string
		_ = json.Unmarshal(comp.Payload, &msg)
		if msg == "" {
			msg = "activity failed"
		}
		return zero, fmt.Errorf("%s", msg)
	}
	var out O
	if err := json.Unmarshal(comp.Payload, &out); err != nil {
		return zero, err
	}
	return out, nil
}
