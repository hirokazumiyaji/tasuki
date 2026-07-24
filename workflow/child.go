package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/journal"
)

type childPayload struct {
	ChildID string          `json:"child_id"`
	Name    string          `json:"name"`
	Input   json.RawMessage `json:"input"`
}

// ExecuteChild starts a child workflow and waits for its result.
func ExecuteChild[I, O any](ctx *Context, workflowName string, in I) (O, error) {
	return ExecuteChildAsync[I, O](ctx, workflowName, in).Get(ctx)
}

// ExecuteChildAsync starts a child workflow and returns a Future.
func ExecuteChildAsync[I, O any](ctx *Context, workflowName string, in I) *Future[O] {
	input, _ := ctx.codec.Marshal(in)
	childID := fmt.Sprintf("%s:%d", ctx.info.InstanceID, ctx.nextSeq)
	payload, _ := json.Marshal(childPayload{ChildID: childID, Name: workflowName, Input: input})
	ev := ctx.recordOrReplay(journal.Command{
		Type: journal.TypeChildScheduled,
		Name: workflowName,
	}, payload)
	return newFuture[O](ev.Seq)
}
