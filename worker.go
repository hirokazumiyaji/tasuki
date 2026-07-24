package tasuki

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

type Worker struct {
	backend backend.Backend
	opts    WorkerOptions
	reg     *registry

	mu       sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
	inFlight map[int64]struct{}
}

func NewWorker(b backend.Backend, opts WorkerOptions) *Worker {
	opts = opts.withDefaults()
	return &Worker{
		backend:  b,
		opts:     opts,
		reg:      newRegistry(opts.Codec),
		inFlight: map[int64]struct{}{},
	}
}

func (w *Worker) Start(parent context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	w.cancel = cancel
	w.done = make(chan struct{})
	go w.loop(ctx)
}

// PollOnce runs one worker tick (timers, workflow tasks, activity tasks).
func (w *Worker) PollOnce(ctx context.Context) {
	w.tick(ctx)
}

func (w *Worker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	cancel := w.cancel
	done := w.done
	w.cancel = nil
	w.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	var waitErr error
	select {
	case <-done:
	case <-ctx.Done():
		waitErr = ctx.Err()
	}
	w.releaseInFlight(context.Background())
	return waitErr
}

func (w *Worker) track(taskID int64) {
	w.mu.Lock()
	w.inFlight[taskID] = struct{}{}
	w.mu.Unlock()
}

func (w *Worker) untrack(taskID int64) {
	w.mu.Lock()
	delete(w.inFlight, taskID)
	w.mu.Unlock()
}

func (w *Worker) releaseInFlight(ctx context.Context) {
	w.mu.Lock()
	ids := make([]int64, 0, len(w.inFlight))
	for id := range w.inFlight {
		ids = append(ids, id)
	}
	w.inFlight = map[int64]struct{}{}
	w.mu.Unlock()
	for _, id := range ids {
		_ = w.backend.ReleaseLease(ctx, id)
	}
}

func (w *Worker) loop(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.opts.PollInterval)
	defer ticker.Stop()

	var wake <-chan struct{}
	if n, ok := w.backend.(backend.TaskNotifier); ok {
		ch, err := n.Subscribe(ctx)
		if err != nil {
			w.opts.Logger.Warn("task notify subscribe failed", "err", err)
		} else {
			wake = ch
		}
	}

	for {
		w.tick(ctx)
		if wake == nil {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
	}
}

func (w *Worker) tick(ctx context.Context) {
	_, _ = w.backend.FireDueTimers(ctx, 100)
	_, _ = w.backend.ClaimDueSchedules(ctx, 100)

	wtasks, err := w.backend.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: w.opts.Queues, Limit: w.opts.ClaimLimit,
		Lease: w.opts.LeaseDuration, WorkerID: w.opts.WorkerID,
	})
	if err == nil {
		for _, t := range wtasks {
			w.opts.Metrics.AddWorkflowTask(ctx, 1)
			w.opts.Logger.Debug("workflow task", "instance_id", t.InstanceID, "task_id", t.ID)
			w.track(t.ID)
			_ = w.handleWorkflow(ctx, t)
			w.untrack(t.ID)
		}
	}

	atasks, err := w.backend.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: w.opts.Queues, Limit: w.opts.ClaimLimit,
		Lease: w.opts.LeaseDuration, WorkerID: w.opts.WorkerID,
	})
	if err == nil {
		for _, t := range atasks {
			w.opts.Metrics.AddActivityTask(ctx, 1)
			w.opts.Logger.Debug("activity task",
				"instance_id", t.InstanceID, "task_id", t.ID, "activity", t.Name, "attempt", t.Attempt)
			w.track(t.ID)
			_ = w.handleActivity(ctx, t)
			w.untrack(t.ID)
		}
	}
}

func (w *Worker) handleWorkflow(ctx context.Context, t backend.Task) error {
	state, err := w.backend.LoadWorkflow(ctx, t.InstanceID)
	if err != nil {
		return err
	}
	if state.Instance.Status != "running" {
		// Drop the task by committing empty? Just leave it — for M0 ignore.
		return nil
	}
	wf, err := w.reg.workflow(state.Instance.Name)
	if err != nil {
		return err
	}

	next := state.NextSeq
	drained := make([]int64, 0, len(state.Inbox))
	ingested := make([]journal.Event, 0, len(state.Inbox))
	inboxLimit := len(state.Inbox)
	if caps := w.backend.Capabilities(); caps.MaxAdvancementEffects > 0 {
		// Each drained inbox item costs ~2 TransactWrite actions (journal put + inbox delete).
		// Reserve headroom for instance CAS, task delete, ensure, timers/activities/children.
		budget := caps.MaxAdvancementEffects - 20
		if budget < 2 {
			budget = 2
		}
		inboxLimit = budget / 2
		if inboxLimit < 1 {
			inboxLimit = 1
		}
		if inboxLimit > len(state.Inbox) {
			inboxLimit = len(state.Inbox)
		}
	}
	for i, item := range state.Inbox {
		if i >= inboxLimit {
			break
		}
		ev := item.Event
		ev.Seq = next
		next++
		ingested = append(ingested, ev)
		drained = append(drained, item.ID)
	}
	events := append(append([]journal.Event{}, state.Journal...), ingested...)

	res := engine.RunAt(events, state.Now, func(wctx *workflow.Context) (any, error) {
		wctx.SetInfo(workflow.WorkflowInfo{
			InstanceID: state.Instance.ID,
			Name:       state.Instance.Name,
		})
		out, err := wf.fn(wctx, state.Instance.Input)
		if err != nil {
			return nil, err
		}
		return out, nil
	})

	adv := backend.Advancement{
		InstanceID:   t.InstanceID,
		TaskID:       t.ID,
		ExpectedSeq:  state.NextSeq,
		DrainedInbox: drained,
		NewEvents:    append([]journal.Event{}, ingested...),
	}

	if res.Stuck {
		adv.NewEvents = append(adv.NewEvents, res.NewCommands...)
		adv.Terminal = &backend.TerminalUpdate{
			Status:  "stuck",
			Failure: []byte(res.Err.Error()),
		}
		w.withParentNotify(&adv, state.Instance.ParentID, state.Instance.ParentSeq)
		w.opts.Logger.Warn("workflow stuck", "instance_id", t.InstanceID, "error", res.Err)
		w.opts.Metrics.AddTerminal(ctx, "stuck")
		return w.backend.CommitAdvancement(ctx, adv)
	}

	if res.Suspended {
		adv.NewEvents = append(adv.NewEvents, res.NewCommands...)
		w.attachEffects(&adv, state.Instance.Queue, res.NewCommands)
		w.withParentNotify(&adv, state.Instance.ParentID, state.Instance.ParentSeq)
	return w.backend.CommitAdvancement(ctx, adv)
	}

	// Completed (normal return or error return)
	adv.NewEvents = append(adv.NewEvents, res.NewCommands...)
	w.attachEffects(&adv, state.Instance.Queue, res.NewCommands)

	termSeq := state.NextSeq + int64(len(adv.NewEvents))
	if input, ok := workflow.AsContinueAsNew(res.Err); ok {
		adv.NewEvents = append(adv.NewEvents, journal.Event{
			Seq:     termSeq,
			Type:    journal.TypeContinuedAsNew,
			Payload: input,
		})
		adv.Terminal = &backend.TerminalUpdate{Status: "continued", Result: input}
		adv.Children = append(adv.Children, backend.NewInstance{
			ID:    fmt.Sprintf("%s~%d", state.Instance.ID, termSeq),
			Name:  state.Instance.Name,
			Queue: state.Instance.Queue,
			Input: input,
		})
		w.withParentNotify(&adv, state.Instance.ParentID, state.Instance.ParentSeq)
		return w.backend.CommitAdvancement(ctx, adv)
	}
	if res.Err != nil && errors.Is(res.Err, workflow.ErrCanceled) {
		adv.NewEvents = append(adv.NewEvents, journal.Event{
			Seq:  termSeq,
			Type: journal.TypeWorkflowCanceled,
		})
		adv.Terminal = &backend.TerminalUpdate{Status: "canceled"}
	} else if res.Err != nil {
		failPayload, _ := json.Marshal(res.Err.Error())
		adv.NewEvents = append(adv.NewEvents, journal.Event{
			Seq:     termSeq,
			Type:    journal.TypeWorkflowFailed,
			Payload: failPayload,
		})
		adv.Terminal = &backend.TerminalUpdate{
			Status:  "failed",
			Failure: failPayload,
		}
	} else {
		var resultBytes []byte
		switch v := res.Result.(type) {
		case []byte:
			resultBytes = v
		default:
			resultBytes, _ = json.Marshal(v)
		}
		adv.NewEvents = append(adv.NewEvents, journal.Event{
			Seq:     termSeq,
			Type:    journal.TypeWorkflowCompleted,
			Payload: resultBytes,
		})
		adv.Terminal = &backend.TerminalUpdate{
			Status: "completed",
			Result: resultBytes,
		}
	}
	w.withParentNotify(&adv, state.Instance.ParentID, state.Instance.ParentSeq)
	if adv.Terminal != nil {
		w.opts.Logger.Info("workflow terminal", "instance_id", t.InstanceID, "status", adv.Terminal.Status)
		w.opts.Metrics.AddTerminal(ctx, adv.Terminal.Status)
	}
	return w.backend.CommitAdvancement(ctx, adv)
}

func (w *Worker) withParentNotify(adv *backend.Advancement, parentID string, parentSeq int64) {
	if parentID == "" || adv.Terminal == nil {
		return
	}
	switch adv.Terminal.Status {
	case "completed":
		adv.ParentNotify = &journal.Event{Type: journal.TypeChildCompleted, RefSeq: parentSeq, Payload: adv.Terminal.Result}
	case "failed", "canceled", "terminated", "stuck":
		payload := adv.Terminal.Failure
		if len(payload) == 0 {
			payload, _ = json.Marshal(adv.Terminal.Status)
		}
		adv.ParentNotify = &journal.Event{Type: journal.TypeChildFailed, RefSeq: parentSeq, Payload: payload}
	}
}

func (w *Worker) attachEffects(adv *backend.Advancement, queue string, cmds []journal.Event) {
	for _, cmd := range cmds {
		switch cmd.Type {
		case journal.TypeActivityScheduled:
			input := append([]byte(nil), cmd.Payload...)
			retry := backend.RetryPolicy{}
			var sched workflow.ActivitySchedule
			if err := json.Unmarshal(cmd.Payload, &sched); err == nil && len(sched.Input) > 0 {
				input = append([]byte(nil), sched.Input...)
				if sched.Retry != nil {
					retry = backend.RetryPolicy{
						InitialInterval:    time.Duration(sched.Retry.InitialIntervalMs) * time.Millisecond,
						BackoffCoefficient: sched.Retry.BackoffCoefficient,
						MaxInterval:        time.Duration(sched.Retry.MaxIntervalMs) * time.Millisecond,
						MaxAttempts:        sched.Retry.MaxAttempts,
					}
				}
			}
			adv.ActivityTasks = append(adv.ActivityTasks, backend.NewTask{
				Kind:        "activity",
				Queue:       queue,
				InstanceID:  adv.InstanceID,
				Name:        cmd.Name,
				Seq:         cmd.Seq,
				Input:       input,
				MaxAttempts: retry.MaxAttempts,
				Retry:       retry,
			})
		case journal.TypeTimerCreated:
			var p struct {
				FireAt time.Time `json:"fire_at"`
			}
			_ = json.Unmarshal(cmd.Payload, &p)
			adv.Timers = append(adv.Timers, backend.NewTimer{Seq: cmd.Seq, FireAt: p.FireAt})
		case journal.TypeChildScheduled:
			var p struct {
				ChildID string          `json:"child_id"`
				Name    string          `json:"name"`
				Input   json.RawMessage `json:"input"`
			}
			_ = json.Unmarshal(cmd.Payload, &p)
			adv.Children = append(adv.Children, backend.NewInstance{
				ID: p.ChildID, Name: p.Name, Queue: queue, Input: p.Input,
				ParentID: adv.InstanceID, ParentSeq: cmd.Seq,
			})
		}
	}
}

func (w *Worker) handleActivity(ctx context.Context, t backend.Task) error {
	act, err := w.reg.activity(t.Name)
	if err != nil {
		return w.failActivity(ctx, t, err)
	}

	done := make(chan struct{})
	defer close(done)
	go w.extendLeaseLoop(ctx, t.ID, done)

	out, err := act.fn(ctx, t.Input)
	if err != nil {
		if IsNonRetryable(err) || (t.MaxAttempts > 0 && t.Attempt >= t.MaxAttempts) {
			return w.failActivity(ctx, t, err)
		}
		delay := workflow.RetryPolicy{
			InitialInterval:    t.Retry.InitialInterval,
			BackoffCoefficient: t.Retry.BackoffCoefficient,
			MaxInterval:        t.Retry.MaxInterval,
			MaxAttempts:        t.Retry.MaxAttempts,
		}.Backoff(t.Attempt)
		var now time.Time
		if st, loadErr := w.backend.LoadWorkflow(ctx, t.InstanceID); loadErr == nil {
			now = st.Now
		} else {
			now = time.Now().UTC()
		}
		w.opts.Logger.Info("activity retry",
			"instance_id", t.InstanceID, "activity", t.Name, "attempt", t.Attempt, "delay", delay)
		w.opts.Metrics.AddActivityRetry(ctx, 1)
		return w.backend.RetryActivity(ctx, t.ID, now.Add(delay))
	}
	return w.backend.CompleteActivity(ctx, t.ID, journal.Event{
		Type:    journal.TypeActivityCompleted,
		RefSeq:  t.Seq,
		Payload: out,
	})
}

func (w *Worker) failActivity(ctx context.Context, t backend.Task, err error) error {
	w.opts.Logger.Warn("activity failed", "instance_id", t.InstanceID, "activity", t.Name, "error", err)
	payload, _ := json.Marshal(err.Error())
	return w.backend.CompleteActivity(ctx, t.ID, journal.Event{
		Type:    journal.TypeActivityFailed,
		RefSeq:  t.Seq,
		Payload: payload,
	})
}

func (w *Worker) extendLeaseLoop(ctx context.Context, taskID int64, done <-chan struct{}) {
	d := w.opts.LeaseDuration / 2
	if d <= 0 {
		return
	}
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = w.backend.ExtendLease(ctx, taskID, w.opts.LeaseDuration)
		}
	}
}
