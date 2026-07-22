package tasuki

import (
	"context"
	"encoding/json"
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
	for {
		w.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) tick(ctx context.Context) {
	_, _ = w.backend.FireDueTimers(ctx, 100)

	wtasks, err := w.backend.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: w.opts.Queues, Limit: 10,
		Lease: w.opts.LeaseDuration, WorkerID: w.opts.WorkerID,
	})
	if err == nil {
		for _, t := range wtasks {
			w.track(t.ID)
			_ = w.handleWorkflow(ctx, t)
			w.untrack(t.ID)
		}
	}

	atasks, err := w.backend.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: w.opts.Queues, Limit: 10,
		Lease: w.opts.LeaseDuration, WorkerID: w.opts.WorkerID,
	})
	if err == nil {
		for _, t := range atasks {
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
	for _, item := range state.Inbox {
		ev := item.Event
		ev.Seq = next
		next++
		ingested = append(ingested, ev)
		drained = append(drained, item.ID)
	}
	events := append(append([]journal.Event{}, state.Journal...), ingested...)

	res := engine.RunAt(events, state.Now, func(wctx *workflow.Context) (any, error) {
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
		return w.backend.CommitAdvancement(ctx, adv)
	}

	if res.Suspended {
		adv.NewEvents = append(adv.NewEvents, res.NewCommands...)
		w.attachEffects(&adv, state.Instance.Queue, res.NewCommands)
		return w.backend.CommitAdvancement(ctx, adv)
	}

	// Completed (normal return or error return)
	adv.NewEvents = append(adv.NewEvents, res.NewCommands...)
	w.attachEffects(&adv, state.Instance.Queue, res.NewCommands)

	termSeq := state.NextSeq + int64(len(adv.NewEvents))
	if res.Err != nil {
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
	return w.backend.CommitAdvancement(ctx, adv)
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
		return w.backend.RetryActivity(ctx, t.ID, now.Add(delay))
	}
	return w.backend.CompleteActivity(ctx, t.ID, journal.Event{
		Type:    journal.TypeActivityCompleted,
		RefSeq:  t.Seq,
		Payload: out,
	})
}

func (w *Worker) failActivity(ctx context.Context, t backend.Task, err error) error {
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
