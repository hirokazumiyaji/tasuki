package tasuki

import (
	"context"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// Query replays a workflow instance in read-only mode and invokes a named query handler.
// The Worker must have the workflow registered. History and tasks are not modified.
func Query[I, O any](ctx context.Context, w *Worker, instanceID, name string, in I) (O, error) {
	var zero O
	state, err := w.backend.LoadWorkflow(ctx, instanceID)
	if err != nil {
		return zero, err
	}
	wf, err := w.reg.workflow(state.Instance.Name)
	if err != nil {
		return zero, err
	}

	next := state.NextSeq
	events := append([]journal.Event{}, state.Journal...)
	for _, item := range state.Inbox {
		ev := item.Event
		ev.Seq = next
		next++
		events = append(events, ev)
	}

	arg, err := w.reg.codec.Marshal(in)
	if err != nil {
		return zero, err
	}

	res := engine.RunQuery(events, state.Now, name, arg, func(wctx *workflow.Context) (any, error) {
		wctx.SetInfo(workflow.WorkflowInfo{
			InstanceID: state.Instance.ID,
			Name:       state.Instance.Name,
		})
		wctx.SetCodec(w.reg.codec)
		wctx.SetSearchAttributes(state.Instance.SearchAttributes)
		wctx.SetMemo(state.Instance.Memo)
		out, err := wf.fn(wctx, state.Instance.Input)
		if err != nil {
			return nil, err
		}
		return out, nil
	})
	if res.Err != nil {
		return zero, res.Err
	}
	var out O
	if err := w.reg.codec.Unmarshal(res.Payload, &out); err != nil {
		return zero, err
	}
	return out, nil
}
