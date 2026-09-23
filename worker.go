package tasuki

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hirokazumiyaji/tasuki/activity"
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
	inFlight map[int64]backend.Task

	stickyMu sync.Mutex
	sticky   map[string]stickyEntry

	instMu   sync.Mutex
	instLock map[string]*workflowActor

	// Persistent execution slots bound concurrent workflow/activity handlers
	// across ticks so a blocked activity cannot stall timers or workflows.
	wfSem  chan struct{}
	actSem chan struct{}

	// actWg tracks detached activity goroutines so Shutdown can wait for
	// them within its grace period instead of releasing their leases early
	// (which would let peers duplicate the execution).
	actWg    sync.WaitGroup
	actMu    sync.Mutex
	stopping bool

	recoverMu   sync.Mutex
	lastRecover time.Time

	backlogMu   sync.Mutex
	lastBacklog time.Time
}

func NewWorker(b backend.Backend, opts WorkerOptions) *Worker {
	opts = opts.withDefaults()
	return &Worker{
		backend:  b,
		opts:     opts,
		reg:      newRegistry(opts.Codec),
		inFlight: map[int64]backend.Task{},
		sticky:   map[string]stickyEntry{},
		instLock: map[string]*workflowActor{},
		wfSem:    make(chan struct{}, opts.WorkflowConcurrency),
		actSem:   make(chan struct{}, opts.ActivityConcurrency),
	}
}

func (w *Worker) Start(parent context.Context) {
	if err := w.StartWithError(parent); err != nil {
		w.opts.Logger.Error("tasuki: worker start failed", "error", err)
	}
}

// StartWithError starts the worker's background polling loop and reports
// startup failures to the caller.
//
// It returns an error when schema validation fails (see ValidateSchema and
// WorkerOptions.DisableSchemaValidation) or when the worker is already
// running (ErrWorkerAlreadyRunning). On error the worker is not started;
// check Running to gate health checks or traffic.
//
// StartWithError starts polling asynchronously and returns immediately once
// the loop is launched (it does not wait for tasks to complete).
func (w *Worker) StartWithError(parent context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		return ErrWorkerAlreadyRunning
	}
	if !w.opts.DisableSchemaValidation {
		if err := ValidateSchema(parent, w.backend); err != nil {
			return fmt.Errorf("tasuki: schema validation failed: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	w.cancel = cancel
	w.done = done
	w.actMu.Lock()
	w.stopping = false
	w.actMu.Unlock()
	go w.loop(ctx, done)
	return nil
}

// Running reports whether the worker's background polling loop is started.
// It returns false when Start has never succeeded, when schema validation
// refused the start, after Shutdown, or after the parent context is canceled
// and the loop has exited; use it for health checks.
func (w *Worker) Running() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cancel != nil
}

// ValidateSchema checks that the backend's store schema is ready for use.
// It is a no-op for backends that do not implement backend.SchemaValidator.
// Workers run it automatically at Start; call it explicitly to gate your own
// startup sequence (e.g. before accepting traffic).
func ValidateSchema(ctx context.Context, b backend.Backend) error {
	if v, ok := b.(backend.SchemaValidator); ok {
		return v.ValidateSchema(ctx)
	}
	return nil
}

// PollOnce runs one worker tick (timers, workflow tasks, activity tasks).
// Unlike the background loop (which never blocks on long activities),
// PollOnce waits for activities claimed in this tick so single-threaded
// test environments observe synchronous progress.
func (w *Worker) PollOnce(ctx context.Context) {
	w.tickSync(ctx)
}

func (w *Worker) tickSync(ctx context.Context) {
	if n, err := w.backend.FireDueTimers(ctx, 100); err != nil {
		w.recordStoreError(ctx, "fire_timers", err)
	} else if n > 0 {
		w.opts.Logger.Debug("fired timers", "n", n)
	}
	if _, err := w.backend.ClaimDueSchedules(ctx, 100); err != nil {
		w.recordStoreError(ctx, "claim_schedules", err)
	}
	w.sampleBacklog(ctx)
	w.recoverOrphanedTasks(ctx)
	w.tickWorkflows(ctx)
	// Synchronous activities for PollOnce/test determinism.
	w.tickActivitiesSync(ctx)
}

func (w *Worker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	cancel := w.cancel
	done := w.done
	w.cancel = nil
	w.done = nil
	w.mu.Unlock()
	if cancel == nil && done == nil {
		return nil
	}
	if cancel != nil {
		cancel()
	}
	var waitErr error
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			waitErr = ctx.Err()
		}
	}
	// Mark stopping so no new detached activities start, then wait for
	// in-flight activities within the remaining grace period. Only
	// activities still running after the grace get their leases released
	// (peers reclaim them after lease expiry or via the release below).
	w.actMu.Lock()
	w.stopping = true
	w.actMu.Unlock()
	waitForWaitGroup(&w.actWg, ctx)
	// Bound lease release: the store may hang, but Shutdown must return
	// within a predictable budget. Unreleased leases expire via lease timeout
	// and are reclaimed by other workers.
	releaseTimeout := w.opts.ShutdownReleaseTimeout
	if releaseTimeout <= 0 {
		releaseTimeout = 5 * time.Second
	}
	relCtx, relCancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer relCancel()
	w.releaseInFlight(relCtx)
	return waitErr
}

// waitForWaitGroup blocks until wg drains or ctx ends.
func waitForWaitGroup(wg *sync.WaitGroup, ctx context.Context) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// trackActivity registers one detached activity for Shutdown-aware waiting.
// It returns a done func that must be called when the activity finishes.
// When the worker is stopping it returns ok=false and the caller must
// release the task lease instead of running it, so Shutdown never waits on
// work claimed after the stop began.
func (w *Worker) trackActivity() (done func(), ok bool) {
	w.actMu.Lock()
	defer w.actMu.Unlock()
	if w.stopping {
		return nil, false
	}
	w.actWg.Add(1)
	return w.actWg.Done, true
}

func (w *Worker) track(t backend.Task) {
	w.mu.Lock()
	w.inFlight[t.ID] = t
	w.mu.Unlock()
}

func (w *Worker) untrack(taskID int64) {
	w.mu.Lock()
	delete(w.inFlight, taskID)
	w.mu.Unlock()
}

// claimWorkflowRelease atomically removes taskID from the in-flight set,
// reporting whether this caller still owns the lease and may release it.
// Shutdown's releaseInFlight and the workflow abandon paths below both
// funnel through in-flight ownership so only one of them releases a given
// lease: after a shutdown-timeout release, a peer may have re-claimed the
// task, and backends match the lease by task ID/key alone, so a second
// (late-cancel) release by the old turn would clear the peer's fresh lease
// and let a third worker execute concurrently with the peer.
func (w *Worker) claimWorkflowRelease(taskID int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.inFlight[taskID]; !ok {
		return false
	}
	delete(w.inFlight, taskID)
	return true
}

func (w *Worker) releaseInFlight(ctx context.Context) {
	w.mu.Lock()
	tasks := make([]backend.Task, 0, len(w.inFlight))
	for _, t := range w.inFlight {
		tasks = append(tasks, t)
	}
	w.inFlight = map[int64]backend.Task{}
	w.mu.Unlock()
	for _, t := range tasks {
		select {
		case <-ctx.Done():
			w.opts.Logger.Warn("shutdown lease release timed out",
				"released", 0, "remaining", len(tasks), "error", ctx.Err())
			w.opts.Metrics.AddStoreError(context.Background(), "release_lease")
			return
		default:
		}
		// Kind-routed: workflow tasks live under WF#<instanceID> on
		// DynamoDB/Firestore, so the full task identity (not just the
		// numeric ID) addresses the lease.
		if err := w.backend.ReleaseLease(ctx, t); err != nil {
			// A fenced release reports ErrNotFound when the lease moved
			// on (peer reclaim or successor turn): the lease is already
			// released, not a failure.
			if errors.Is(err, backend.ErrNotFound) {
				w.opts.Logger.Debug("shutdown lease already released",
					"task_id", t.ID)
				continue
			}
			w.opts.Logger.Warn("shutdown lease release failed",
				"task_id", t.ID, "error", err)
			w.opts.Metrics.AddStoreError(context.Background(), "release_lease")
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (w *Worker) loop(ctx context.Context, done chan struct{}) {
	// Clear running state when the polling loop exits (e.g. parent context
	// canceled without Shutdown) so Running stops reporting true and a
	// subsequent StartWithError can start a fresh loop. Only clear when this
	// loop is still current to avoid a stale loop clearing a restart.
	defer func() {
		w.mu.Lock()
		if w.done == done {
			w.cancel = nil
		}
		w.mu.Unlock()
		close(done)
	}()
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

func (w *Worker) sampleBacklog(ctx context.Context) {
	if w.opts.Metrics == nil {
		return
	}
	interval := w.opts.BacklogSampleInterval
	if interval < 0 {
		// Negative disables backlog sampling (avoids COUNT queries entirely).
		return
	}
	if interval == 0 {
		// Workers built without withDefaults (e.g. &Worker{} in tests)
		// fall back to the documented default.
		interval = 10 * time.Second
	}
	// Throttle COUNT queries: at most once per interval. Crash gaps and
	// queue depth change slowly, so per-tick sampling (PollInterval default
	// 1s, plus NOTIFY wakes) would hammer the store with 2x
	// CountClaimableTasks per tick for no extra signal.
	w.backlogMu.Lock()
	since := time.Since(w.lastBacklog)
	if since < interval && !w.lastBacklog.IsZero() {
		w.backlogMu.Unlock()
		return
	}
	w.lastBacklog = time.Now()
	w.backlogMu.Unlock()
	for _, kind := range []string{"workflow", "activity"} {
		counts, err := w.backend.CountClaimableTasks(ctx, kind, w.opts.Queues)
		if err != nil {
			w.opts.Logger.Debug("backlog count failed", "kind", kind, "err", err)
			continue
		}
		for _, q := range w.opts.Queues {
			w.opts.Metrics.RecordBacklog(ctx, kind, q, counts[q])
		}
	}
}

func (w *Worker) tick(ctx context.Context) {
	if n, err := w.backend.FireDueTimers(ctx, 100); err != nil {
		w.recordStoreError(ctx, "fire_timers", err)
	} else if n > 0 {
		w.opts.Logger.Debug("fired timers", "n", n)
	}
	if _, err := w.backend.ClaimDueSchedules(ctx, 100); err != nil {
		w.recordStoreError(ctx, "claim_schedules", err)
	}
	w.sampleBacklog(ctx)
	// Best-effort recovery for inbox→task gaps (crash between commit and
	// ensure). Backends without support are skipped.
	w.recoverOrphanedTasks(ctx)

	w.tickWorkflows(ctx)
	w.tickActivities(ctx)
}

func (w *Worker) availableSlots(sem chan struct{}) int {
	return cap(sem) - len(sem)
}

func (w *Worker) tickWorkflows(ctx context.Context) {
	if ctx.Err() != nil {
		// Worker lifecycle ended (Shutdown): never start new turns on a
		// canceled tick. Without this, the loop's wake/ticker select can
		// win over ctx.Done and re-claim a just-released turn for another
		// round of work after shutdown began.
		return
	}
	avail := w.availableSlots(w.wfSem)
	if avail <= 0 {
		return
	}
	limit := w.opts.ClaimLimit
	if limit <= 0 || limit > avail {
		limit = avail
	}
	wtasks, err := w.backend.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: w.opts.Queues, Limit: limit,
		Lease: w.opts.LeaseDuration, WorkerID: w.opts.WorkerID,
		MaxPerInstance: w.opts.MaxPerInstance,
	})
	if err != nil {
		w.recordStoreError(ctx, "claim_workflow", err)
		return
	}
	if len(wtasks) == 0 {
		return
	}
	var wg sync.WaitGroup
	var pendingMu sync.Mutex
	var pending []pendingWorkflowCommit
	// leaseDones stays open until the batch commit finishes for tasks awaiting
	// commit: a task is only deleted by flushWorkflowCommits after every
	// sibling turn completes, so stopping renewal at handleWorkflow return
	// would let a finished task's lease expire while a slow sibling still
	// runs (peer reclaim + duplicate execution). Tasks with no pending
	// commit (nacked, abandoned, error, or no-op) stop renewal immediately:
	// extending a nacked task would overwrite NackTask's visible_at
	// (IncompatibleRetryDelay) with the lease duration and delay a
	// compatible worker. Renewal also stops early via ctx on shutdown.
	// Each stop func closes exactly once (early per-task stop + final
	// stop after the flush), so record one per task.
	var renewalStops []func()
	for _, t := range wtasks {
		// Reserve a slot before dispatch so Claim never over-subscribes and
		// lease extension starts without semaphore wait.
		select {
		case w.wfSem <- struct{}{}:
		default:
			// No slot: make the task visible again promptly for peers.
			// Kind-routed so workflow tasks address WF#<instanceID>.
			_ = w.backend.ReleaseLease(ctx, t)
			continue
		}
		leaseDone := make(chan struct{})
		var leaseOnce sync.Once
		var leaseWg sync.WaitGroup
		leaseWg.Add(1)
		// stopRenewal closes the renewal loop and WAITS for it to exit:
		// closing leaseDone alone does not join extendLeaseLoop, which
		// may be inside an ExtendLease call that would land after a
		// NackTask below and overwrite its visible_at. Every store op
		// after a stop therefore sees no renewal in flight.
		stopRenewal := func() {
			leaseOnce.Do(func() { close(leaseDone) })
			leaseWg.Wait()
		}
		renewalStops = append(renewalStops, stopRenewal)
		wg.Add(1)
		go func(t backend.Task, leaseDone chan struct{}, stopRenewal func()) {
			defer wg.Done()
			defer func() { <-w.wfSem }()
			actor := w.actorFor(t.InstanceID)
			actor.dispatch(func() {
				w.opts.Metrics.AddWorkflowTask(ctx, 1)
				w.opts.Logger.Debug("workflow task", "instance_id", t.InstanceID, "task_id", t.ID)
				w.track(t)
				// Extend the workflow task lease while the turn runs so long
				// replays and local activities cannot lose the lease to a
				// peer (which would duplicate the execution).
				go func() {
					defer leaseWg.Done()
					w.extendLeaseLoop(ctx, t, leaseDone)
				}()
				p, herr := w.handleWorkflow(ctx, t, stopRenewal)
				if herr != nil {
					// No commit follows: stop renewal before handling the
					// error so ExtendLease cannot race a lease release
					// below or overwrite a nack's visible_at. The abandon
					// path below funnels its release through in-flight
					// ownership (see claimWorkflowRelease) instead of
					// untracking here, so a Shutdown releaseInFlight that
					// already released this turn cannot be followed by a
					// second release.
					stopRenewal()
					if errors.Is(herr, errTurnAbandoned) ||
						(ctx.Err() != nil && isCancellationError(herr)) {
						// Worker lifecycle ended mid-turn: abandon the turn
						// and release the lease promptly so a peer retries
						// instead of committing shutdown as a failure.
						// Kind-routed so workflow tasks address
						// WF#<instanceID>, not a missing ACT# key. Only the
						// in-flight owner releases: Shutdown's
						// releaseInFlight may have already released (and a
						// peer re-claimed) this lease, and backends match the
						// lease by ID/key alone, so a second release would
						// clear the peer's lease and enable concurrent
						// execution by a third worker.
						w.opts.Logger.Debug("workflow turn abandoned on shutdown",
							"instance_id", t.InstanceID, "task_id", t.ID)
						if w.claimWorkflowRelease(t.ID) {
							w.releaseWorkflowLease(t)
						} else {
							w.opts.Logger.Debug("skipping workflow lease release; not in-flight owner",
								"instance_id", t.InstanceID, "task_id", t.ID)
						}
						return
					}
					w.untrack(t.ID)
					w.recordStoreError(ctx, "commit_workflow", herr,
						"instance_id", t.InstanceID, "task_id", t.ID)
					w.opts.Logger.Debug("workflow task error", "instance_id", t.InstanceID, "err", herr)
					return
				}
				if p == nil {
					// Nacked (renewal already stopped and joined inside
					// handleWorkflow before the NackTask call) or no-op:
					// nothing awaits commit, so untrack and stop renewal
					// now instead of renewing during the wait for slower
					// siblings. The nack-path stop above makes this a no-op
					// there.
					w.untrack(t.ID)
					stopRenewal()
					return
				}
				// A commit follows in flushWorkflowCommits: stay tracked
				// until it succeeds. Untracking here would hide a leased,
				// uncommitted task from releaseInFlight, so a shutdown in
				// the window leaves peers waiting for lease expiry (and a
				// canceled flush is rejected by context-aware backends).
				p.task = t
				pendingMu.Lock()
				pending = append(pending, *p)
				pendingMu.Unlock()
			})
		}(t, leaseDone, stopRenewal)
	}
	wg.Wait()
	// Pending tasks stayed tracked through the flush (see above): successes
	// are untracked inside flushWorkflowCommits. Commits that failed under
	// a canceled tick context are released explicitly for a prompt peer
	// retry instead of waiting for lease expiry (a canceled flush is
	// rejected before touching the store, so the lease is still ours
	// unless Shutdown's releaseInFlight already released it — the release
	// below is ownership-gated for that race). Live-context failures
	// (conflict/transient) are untracked without release: the task may be
	// superseded, and the lease expires naturally.
	failed := w.flushWorkflowCommits(ctx, pending)
	if len(failed) > 0 {
		if ctx.Err() != nil {
			for _, p := range failed {
				// Only the in-flight owner releases (see
				// claimWorkflowRelease): Shutdown's releaseInFlight may
				// have released this pending task mid-flush and a peer
				// may have re-claimed it, and backends match the lease
				// by ID/key alone, so an unconditional release would
				// clear the peer's lease.
				if w.claimWorkflowRelease(p.adv.TaskID) {
					w.releaseWorkflowLease(p.task)
				} else {
					w.opts.Logger.Debug("skipping workflow lease release; not in-flight owner",
						"instance_id", p.task.InstanceID, "task_id", p.adv.TaskID)
				}
			}
		} else {
			for _, p := range failed {
				w.untrack(p.adv.TaskID)
			}
		}
	}
	for _, stop := range renewalStops {
		stop()
	}
	w.evictIdleInstanceLocks(time.Now())
	w.evictIdleSticky(time.Now())
}

func (w *Worker) tickActivities(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	avail := w.availableSlots(w.actSem)
	if avail <= 0 {
		return
	}
	limit := w.opts.ClaimLimit
	if limit <= 0 || limit > avail {
		limit = avail
	}
	atasks, err := w.backend.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: w.opts.Queues, Limit: limit,
		Lease: w.opts.LeaseDuration, WorkerID: w.opts.WorkerID,
		MaxPerInstance: w.opts.MaxPerInstance,
	})
	if err != nil {
		w.recordStoreError(ctx, "claim_activity", err)
		return
	}
	for _, t := range atasks {
		select {
		case w.actSem <- struct{}{}:
		default:
			_ = w.backend.ReleaseLease(ctx, t)
			continue
		}
		// Lease extension starts in the handler goroutine immediately,
		// before any semaphore wait (slot already reserved), so long
		// activities do not lose their lease while queued.
		w.opts.Metrics.AddActivityTask(ctx, 1)
		w.opts.Logger.Debug("activity task",
			"instance_id", t.InstanceID, "task_id", t.ID, "activity", t.Name, "attempt", t.Attempt)
		w.track(t)
		done, ok := w.trackActivity()
		if !ok {
			w.untrack(t.ID)
			<-w.actSem
			_ = w.backend.ReleaseLease(ctx, t)
			continue
		}
		go func(t backend.Task) {
			defer done()
			defer func() { <-w.actSem }()
			defer w.untrack(t.ID)
			if herr := w.handleActivity(ctx, t); herr != nil {
				w.opts.Logger.Debug("activity task error",
					"instance_id", t.InstanceID, "task_id", t.ID, "err", herr)
			}
		}(t)
	}
	// Do not wait: long activities must not block the next tick's timers
	// or workflow progress. Concurrency stays bounded by actSem and Lease
	// expiry reclaims tasks from crashed workers.
}

// tickActivitiesSync is the PollOnce path: claim and run activities to
// completion before returning for deterministic single-threaded tests.
func (w *Worker) tickActivitiesSync(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	avail := w.availableSlots(w.actSem)
	if avail <= 0 {
		return
	}
	limit := w.opts.ClaimLimit
	if limit <= 0 || limit > avail {
		limit = avail
	}
	atasks, err := w.backend.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: w.opts.Queues, Limit: limit,
		Lease: w.opts.LeaseDuration, WorkerID: w.opts.WorkerID,
		MaxPerInstance: w.opts.MaxPerInstance,
	})
	if err != nil {
		w.recordStoreError(ctx, "claim_activity", err)
		return
	}
	var wg sync.WaitGroup
	for _, t := range atasks {
		select {
		case w.actSem <- struct{}{}:
		default:
			_ = w.backend.ReleaseLease(ctx, t)
			continue
		}
		wg.Add(1)
		w.opts.Metrics.AddActivityTask(ctx, 1)
		w.opts.Logger.Debug("activity task",
			"instance_id", t.InstanceID, "task_id", t.ID, "activity", t.Name, "attempt", t.Attempt)
		w.track(t)
		done, global := w.trackActivity()
		go func(t backend.Task) {
			defer wg.Done()
			if global {
				defer done()
			}
			defer func() { <-w.actSem }()
			defer w.untrack(t.ID)
			if herr := w.handleActivity(ctx, t); herr != nil {
				w.opts.Logger.Debug("activity task error",
					"instance_id", t.InstanceID, "task_id", t.ID, "err", herr)
			}
		}(t)
	}
	wg.Wait()
}

// errTurnAbandoned marks a workflow turn canceled by the worker lifecycle
// (Shutdown) whose error derives from that cancellation. The turn must be
// abandoned — lease released, nothing committed — instead of persisting the
// shutdown as a terminal workflow failure.
var errTurnAbandoned = errors.New("tasuki: workflow turn abandoned on shutdown")

// isCancellationError reports context lifecycle errors. workflow.ErrCanceled
// (user-requested cancellation) is deliberately excluded: it is a legitimate
// terminal outcome, unlike Shutdown-induced context.Canceled.
func isCancellationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// releaseWorkflowLease releases a task lease with a detached context so the
// release survives worker shutdown (the tick context is already canceled).
// It takes the full task identity and routes by kind: workflow tasks live
// under WF#<instanceID> on DynamoDB/Firestore, so a numeric ID alone would
// address a missing ACT# key (ErrNotFound) and stall peers until expiry.
func (w *Worker) releaseWorkflowLease(t backend.Task) {
	timeout := w.opts.ShutdownReleaseTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	relCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := w.backend.ReleaseLease(relCtx, t); err != nil {
		// A fenced release reports ErrNotFound when the lease moved on
		// (peer reclaim or successor turn): the lease is already
		// released, not a failure.
		if errors.Is(err, backend.ErrNotFound) {
			w.opts.Logger.Debug("workflow lease already released",
				"instance_id", t.InstanceID, "task_id", t.ID)
			return
		}
		w.recordStoreError(context.Background(), "release_lease", err, "task_id", t.ID)
	}
}

func (w *Worker) handleWorkflow(ctx context.Context, t backend.Task, stopRenewal func()) (*pendingWorkflowCommit, error) {
	if ctx.Err() != nil {
		// Worker lifecycle ended before the turn started: abandon so the
		// lease is released for a peer instead of committing shutdown-driven
		// work (including nacks) on a canceled context.
		return nil, errTurnAbandoned
	}
	state, err := w.loadWorkflowState(ctx, t.InstanceID)
	if err != nil {
		return nil, err
	}
	if state.Instance.Status != "running" {
		// Drop the task by committing empty? Just leave it — for M0 ignore.
		return nil, nil
	}
	wf, err := w.reg.workflow(state.Instance.Name)
	if err != nil {
		if errors.Is(err, ErrWorkflowNotRegistered) {
			// Stop (and join) lease renewal BEFORE nacking: the loop
			// may be inside ExtendLease, which would otherwise land
			// after NackTask and overwrite its visible_at with the
			// lease duration. stopRenewal is once-guarded, so the
			// caller's post-return stop is a no-op.
			if stopRenewal != nil {
				stopRenewal()
			}
			return nil, w.nackIncompatible(ctx, t, "unregistered_workflow", err)
		}
		return nil, err
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

	if th := w.opts.JournalWarnThreshold; th > 0 {
		if n := len(state.Journal); n >= th {
			w.opts.Logger.Warn("journal size warning",
				"instance_id", t.InstanceID,
				"workflow", state.Instance.Name,
				"journal_events", n,
				"threshold", th,
			)
			w.opts.Metrics.AddJournalWarning(ctx, 1)
		}
	}

	res := engine.RunAt(events, state.Now, func(wctx *workflow.Context) (any, error) {
		wctx.SetInfo(workflow.WorkflowInfo{
			InstanceID: state.Instance.ID,
			Name:       state.Instance.Name,
		})
		wctx.SetCodec(w.reg.codec)
		wctx.SetSearchAttributes(state.Instance.SearchAttributes)
		wctx.SetMemo(state.Instance.Memo)
		w.attachLocalActivityRunner(wctx, ctx)
		out, err := wf.fn(wctx, state.Instance.Input)
		if err != nil {
			return nil, err
		}
		return out, nil
	})

	if !res.Stuck && res.WorkflowContext() != nil {
		for {
			before := len(res.WorkflowContext().NewCommands())
			ures := engine.ContinueUpdates(res.WorkflowContext())
			res.NewCommands = res.WorkflowContext().NewCommands()
			if ures.Stuck {
				res.Stuck = true
				res.Err = ures.Err
				break
			}
			if ures.Suspended {
				res.Suspended = true
				break
			}
			if len(res.WorkflowContext().NewCommands()) == before {
				break
			}
		}
	}

	if ctx.Err() != nil {
		// Worker lifecycle ended mid-turn: abandon regardless of how
		// workflow code handled the cancellation. A local activity may
		// have observed ctx.Done but returned nil or a domain error, or
		// the workflow may have caught the error and suspended — either
		// way the turn must not advance. Backends such as memory ignore
		// the canceled commit context, so any advancement built below
		// would persist shutdown as a completion, failure, or commands.
		return nil, errTurnAbandoned
	}

	adv := backend.Advancement{
		InstanceID:   t.InstanceID,
		TaskID:       t.ID,
		ExpectedSeq:  state.NextSeq,
		DrainedInbox: drained,
		NewEvents:    append([]journal.Event{}, ingested...),
	}

	pending := func() *pendingWorkflowCommit {
		return &pendingWorkflowCommit{
			instanceID:  t.InstanceID,
			baseJournal: state.Journal,
			adv:         adv,
		}
	}

	if res.Stuck {
		if errors.Is(res.Err, journal.ErrDeterminismViolation) {
			// Join renewal before nacking (see above): an in-flight
			// ExtendLease must not overwrite the nack's visible_at.
			if stopRenewal != nil {
				stopRenewal()
			}
			return nil, w.nackIncompatible(ctx, t, "determinism", res.Err)
		}
		adv.NewEvents = append(adv.NewEvents, res.NewCommands...)
		adv.Terminal = &backend.TerminalUpdate{
			Status:  "stuck",
			Failure: []byte(res.Err.Error()),
		}
		w.withParentNotify(&adv, state.Instance.ParentID, state.Instance.ParentSeq)
		if err := w.checkTerminalBudget(&adv); err != nil {
			return nil, err
		}
		w.opts.Logger.Warn("workflow stuck", "instance_id", t.InstanceID, "error", res.Err)
		w.opts.Metrics.AddTerminal(ctx, "stuck")
		p := pending()
		p.adv = adv
		return p, nil
	}

	if res.Suspended {
		adv.NewEvents = append(adv.NewEvents, res.NewCommands...)
		w.attachEffects(&adv, state.Instance.Queue, res.NewCommands)
		w.withParentNotify(&adv, state.Instance.ParentID, state.Instance.ParentSeq)
		if err := w.fitAdvancementToBudget(&adv, res.NewCommands, state.Instance.Queue); err != nil {
			return nil, err
		}
		p := pending()
		p.adv = adv
		return p, nil
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
			// Inherit the parent chain so the final run still notifies
			// the original parent exactly once.
			ParentID:  state.Instance.ParentID,
			ParentSeq: state.Instance.ParentSeq,
		})
		w.withParentNotify(&adv, state.Instance.ParentID, state.Instance.ParentSeq)
		if err := w.checkTerminalBudget(&adv); err != nil {
			return nil, err
		}
		p := pending()
		p.adv = adv
		return p, nil
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
		if err := w.checkTerminalBudget(&adv); err != nil {
			return nil, err
		}
		w.opts.Logger.Info("workflow terminal", "instance_id", t.InstanceID, "status", adv.Terminal.Status)
		w.opts.Metrics.AddTerminal(ctx, adv.Terminal.Status)
	}
	p := pending()
	p.adv = adv
	return p, nil
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
			var startToClose time.Duration
			var sched workflow.ActivitySchedule
			if err := json.Unmarshal(cmd.Payload, &sched); err == nil {
				if len(sched.Input) > 0 {
					input = append([]byte(nil), sched.Input...)
				}
				if sched.Retry != nil {
					retry = backend.RetryPolicy{
						InitialInterval:    time.Duration(sched.Retry.InitialIntervalMs) * time.Millisecond,
						BackoffCoefficient: sched.Retry.BackoffCoefficient,
						MaxInterval:        time.Duration(sched.Retry.MaxIntervalMs) * time.Millisecond,
						MaxAttempts:        sched.Retry.MaxAttempts,
					}
				}
				if sched.StartToCloseTimeoutMs > 0 {
					startToClose = time.Duration(sched.StartToCloseTimeoutMs) * time.Millisecond
				}
			}
			adv.ActivityTasks = append(adv.ActivityTasks, backend.NewTask{
				Kind:                "activity",
				Queue:               queue,
				InstanceID:          adv.InstanceID,
				Name:                cmd.Name,
				Seq:                 cmd.Seq,
				Input:               input,
				MaxAttempts:         retry.MaxAttempts,
				Retry:               retry,
				StartToCloseTimeout: startToClose,
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

// advancementOps estimates DynamoDB-style TransactWriteItems operations for an
// advancement: 1 instance CAS + N journal puts + activity/timer puts +
// inbox deletes + 3 per child + parent inbox + 1 task delete.
// It mirrors backend/dynamodb buildAdvancementItems counting so workers can
// stay within backend.Capabilities.MaxAdvancementEffects atomically.
func advancementOps(adv *backend.Advancement) int {
	n := 2 + len(adv.NewEvents) + len(adv.ActivityTasks) + len(adv.Timers) + len(adv.DrainedInbox) + 3*len(adv.Children)
	if adv.ParentNotify != nil {
		n++
	}
	return n
}

func advancementBudget(b backend.Backend) int {
	if caps := b.Capabilities(); caps.MaxAdvancementEffects > 0 {
		return caps.MaxAdvancementEffects
	}
	return 100
}

// checkTerminalBudget validates a terminal advancement against the backend
// transaction budget. Terminal turns cannot be split across ticks, so an
// oversized single terminal advancement is a diagnostic error (the backend
// would reject the transaction anyway).
func (w *Worker) checkTerminalBudget(adv *backend.Advancement) error {
	if ops, budget := advancementOps(adv), advancementBudget(w.backend); ops > budget {
		return fmt.Errorf("tasuki: terminal advancement needs %d ops, budget %d: split fanout across ticks or child workflows (see docs/09-limits.md)",
			ops, budget)
	}
	return nil
}

// fitAdvancementToBudget truncates suspended advancements to the backend
// transaction budget while keeping journal and tasks consistent (prefix).
// Truncated work is re-emitted on replay, so no duplication occurs; the
// advancement is flagged EnsureWorkflowTask for an immediate follow-up tick.
// Terminal advancements are never truncated: oversized single operations
// return a diagnostic error.
func (w *Worker) fitAdvancementToBudget(adv *backend.Advancement, commands []journal.Event, queue string) error {
	budget := advancementBudget(w.backend)
	if advancementOps(adv) <= budget {
		return nil
	}
	if adv.Terminal != nil {
		return fmt.Errorf("tasuki: advancement needs %d ops, budget %d: single terminal advancement does not fit (reduce fanout per tick)",
			advancementOps(adv), budget)
	}
	ingestedLen := len(adv.DrainedInbox)
	if ingestedLen > len(adv.NewEvents) {
		ingestedLen = len(adv.NewEvents)
	}
	ingested := append([]journal.Event(nil), adv.NewEvents[:ingestedLen]...)
	// Base cost with zero new commands.
	base := 2 + len(ingested) + len(adv.DrainedInbox)
	if adv.ParentNotify != nil {
		base++
	}
	if base >= budget {
		return fmt.Errorf("tasuki: inbox drain alone needs %d ops, budget %d: reduce MaxPerInstance/inbox batch",
			base, budget)
	}
	kept := 0
	// Incremental cost per command: 1 journal + task/timer/child extras.
	for i, cmd := range commands {
		extra := 1
		switch cmd.Type {
		case journal.TypeActivityScheduled:
			extra = 2 // journal + activity task
		case journal.TypeTimerCreated:
			extra = 2 // journal + timer
		case journal.TypeChildScheduled:
			extra = 4 // journal + instance/journal/task
		}
		_ = i
		if base+extra > budget {
			break
		}
		base += extra
		kept++
	}
	if kept == 0 {
		return fmt.Errorf("tasuki: single command needs %d ops, budget %d", base+1, budget)
	}
	if kept >= len(commands) {
		return nil
	}
	// Rebuild prefix consistently.
	adv.NewEvents = append(append([]journal.Event(nil), ingested...), commands[:kept]...)
	adv.ActivityTasks = nil
	adv.Timers = nil
	// Children from truncated commands only; preserve pre-existing children
	// that came from elsewhere (none for suspended, but be safe: children
	// derived from commands are rebuilt, others kept).
	// Suspended advancements have no prior children, so rebuild fully.
	adv.Children = nil
	w.attachEffects(adv, queue, commands[:kept])
	adv.EnsureWorkflowTask = true
	w.opts.Logger.Info("truncated fanout advancement to budget",
		"instance_id", adv.InstanceID, "kept_commands", kept, "total_commands", len(commands), "budget", budget)
	return nil
}

func (w *Worker) handleActivity(ctx context.Context, t backend.Task) error {
	act, err := w.reg.activity(t.Name)
	if err != nil {
		if errors.Is(err, ErrActivityNotRegistered) {
			return w.nackIncompatible(ctx, t, "unregistered_activity", err)
		}
		return w.failActivity(ctx, t, err)
	}

	done := make(chan struct{})
	defer close(done)
	go w.extendLeaseLoop(ctx, t, done)

	attempt := t.Attempt
	if attempt < 1 {
		attempt = 1
	}
	runCtx := ctx
	var cancel context.CancelFunc
	if t.StartToCloseTimeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, t.StartToCloseTimeout)
		defer cancel()
	}
	actCtx := activity.WithEnv(runCtx, &activity.Env{
		Info: activity.Info{
			InstanceID:     t.InstanceID,
			ActivityName:   t.Name,
			Attempt:        attempt,
			TaskID:         t.ID,
			IdempotencyKey: fmt.Sprintf("%s/%d", t.InstanceID, t.Seq),
		},
		Codec:   w.opts.Codec,
		Details: append([]byte(nil), t.HeartbeatDetails...),
		Record: func(ctx context.Context, details []byte) error {
			return w.backend.RecordHeartbeat(ctx, t.ID, w.opts.LeaseDuration, details)
		},
	})

	out, err := w.invokeActivity(actCtx, act.fn, t.Input)
	if runCtx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) {
		err = fmt.Errorf("activity start-to-close timeout")
	}
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
		w.opts.Logger.Info("activity retry",
			"instance_id", t.InstanceID, "activity", t.Name, "attempt", t.Attempt, "delay", delay)
		w.opts.Metrics.AddActivityRetry(ctx, 1)
		if rerr := w.backend.RetryActivity(ctx, t.ID, delay); rerr != nil {
			w.recordStoreError(ctx, "retry_activity", rerr, "instance_id", t.InstanceID, "activity", t.Name)
			return rerr
		}
		return nil
	}
	if cerr := w.backend.CompleteActivity(ctx, t.ID, journal.Event{
		Type:    journal.TypeActivityCompleted,
		RefSeq:  t.Seq,
		Payload: out,
	}); cerr != nil {
		w.recordStoreError(ctx, "complete_activity", cerr, "instance_id", t.InstanceID, "activity", t.Name)
		return cerr
	}
	return nil
}

// invokeActivity runs a user activity function, converting panics into
// retryable errors so one bad activity cannot crash the worker process.
func (w *Worker) invokeActivity(ctx context.Context, fn func(context.Context, []byte) ([]byte, error), input []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			w.opts.Logger.Warn("activity panic recovered",
				"panic", fmt.Sprint(r))
			err = fmt.Errorf("activity panic: %v", r)
		}
	}()
	return fn(ctx, input)
}

func (w *Worker) failActivity(ctx context.Context, t backend.Task, err error) error {
	w.opts.Logger.Warn("activity failed", "instance_id", t.InstanceID, "activity", t.Name, "error", err)
	payload, _ := json.Marshal(err.Error())
	if cerr := w.backend.CompleteActivity(ctx, t.ID, journal.Event{
		Type:    journal.TypeActivityFailed,
		RefSeq:  t.Seq,
		Payload: payload,
	}); cerr != nil {
		w.recordStoreError(ctx, "complete_activity", cerr, "instance_id", t.InstanceID, "activity", t.Name)
		return cerr
	}
	return nil
}

// recordStoreError logs a store failure with its operation name and counts it.
// Expected contention (ErrConflict/ErrSuperseded) and shutdown cancellations
// are debug-level and uncounted; genuine failures are warn-level with a bounded
// op label.
func (w *Worker) recordStoreError(ctx context.Context, op string, err error, attrs ...any) {
	if err == nil {
		return
	}
	if errors.Is(err, backend.ErrConflict) || errors.Is(err, backend.ErrSuperseded) {
		w.opts.Logger.Debug("store contention", append([]any{"op", op, "err", err}, attrs...)...)
		return
	}
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		w.opts.Logger.Debug("store op canceled on shutdown", append([]any{"op", op}, attrs...)...)
		return
	}
	w.opts.Logger.Warn("store operation failed", append([]any{"op", op, "err", err}, attrs...)...)
	w.opts.Metrics.AddStoreError(ctx, op)
}

func (w *Worker) attachLocalActivityRunner(wctx *workflow.Context, runCtx context.Context) {
	wctx.SetLocalActivityRunner(func(name string, input []byte) (out []byte, err error) {
		defer func() {
			if r := recover(); r != nil {
				w.opts.Logger.Warn("local activity panic recovered",
					"activity", name, "panic", fmt.Sprint(r))
				out = nil
				err = fmt.Errorf("activity panic: %v", r)
			}
		}()
		act, err := w.reg.activity(name)
		if err != nil {
			return nil, err
		}
		// Run with the worker turn's context so Shutdown cancels a running
		// local activity.
		ctx := runCtx
		if ctx == nil {
			ctx = context.Background()
		}
		timeout := w.opts.LocalActivityTimeout
		if timeout <= 0 {
			return act.fn(ctx, input)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		// Enforce the timeout outside the call: an activity that ignores
		// cancellation (or blocks in a non-context-aware operation) cannot
		// hold the turn, lease renewal, and Shutdown hostage. The late
		// result is discarded; the underlying call keeps running until it
		// returns, so local activities should still respect ctx to avoid
		// wasted work after a timeout.
		done := make(chan callResult, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					w.opts.Logger.Warn("local activity panic recovered",
						"activity", name, "panic", fmt.Sprint(r))
					done <- callResult{nil, fmt.Errorf("activity panic: %v", r), ctx.Err() == nil}
				}
			}()
			o, e := act.fn(ctx, input)
			select {
			case done <- callResult{o, e, ctx.Err() == nil}:
			default: // turn already moved on (timeout/shutdown); drop late result
			}
		}()
		select {
		case r := <-done:
			return acceptLocalResult(name, r, ctx, runCtx, timeout)
		case <-ctx.Done():
			// The deadline may have fired while an on-time result sat
			// buffered: drain it first so a completed-before-expiry
			// result is preserved instead of reported as a timeout.
			select {
			case r := <-done:
				return acceptLocalResult(name, r, ctx, runCtx, timeout)
			default:
				return resolveLocalResult(name, nil, nil, ctx, runCtx, timeout)
			}
		}
	})
}

// acceptLocalResult maps a delivered local activity result to its outcome,
// preferring production-time knowledge over consumption-time state. A
// result that completed before the local deadline fired (onTime) is
// returned as-is even if the deadline fired before consumption; late
// results go through the timeout/cancel mapping in resolveLocalResult so a
// late success is never journaled after expiry.
func acceptLocalResult(name string, r callResult, ctx, runCtx context.Context, timeout time.Duration) ([]byte, error) {
	if r.onTime {
		return r.out, r.err
	}
	return resolveLocalResult(name, r.out, r.err, ctx, runCtx, timeout)
}

// callResult carries one local activity call outcome with its
// production-time completion flag (see attachLocalActivityRunner).
type callResult struct {
	out    []byte
	err    error
	onTime bool
}

// localResultTimeoutError builds the deadline error for one ExecuteLocal call.
func localResultTimeoutError(name string, timeout time.Duration) error {
	return fmt.Errorf("local activity %q timeout after %s: %w",
		name, timeout, context.DeadlineExceeded)
}

// resolveLocalResult maps a delivered local activity result to its outcome
// under the timeout/cancel state observed at acceptance. It is the single
// decision point for both select branches above: when a result arrives
// at/after the deadline, both done and ctx.Done() are ready and select
// picks nondeterministically, so accepting the result must re-check the
// timeout — a late success is discarded and reported as a timeout instead
// of being journaled after expiry. Shutdown cancellations surface the turn
// context error so the turn is abandoned rather than recorded as a local
// timeout.
func resolveLocalResult(name string, out []byte, actErr error, ctx, runCtx context.Context, timeout time.Duration) ([]byte, error) {
	if ctx.Err() != nil {
		if runCtx != nil && runCtx.Err() != nil {
			return nil, runCtx.Err()
		}
		return nil, localResultTimeoutError(name, timeout)
	}
	return out, actErr
}

func (w *Worker) nackIncompatible(ctx context.Context, t backend.Task, reason string, cause error) error {
	delay := w.opts.IncompatibleRetryDelay
	w.opts.Logger.Warn("incompatible worker nack",
		"instance_id", t.InstanceID,
		"task_id", t.ID,
		"reason", reason,
		"error", cause,
		"delay", delay,
	)
	w.opts.Metrics.AddIncompatibleNack(ctx, reason)
	return w.backend.NackTask(ctx, t, delay)
}

func (w *Worker) extendLeaseLoop(ctx context.Context, t backend.Task, done <-chan struct{}) {
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
			if err := w.backend.ExtendLease(ctx, t, w.opts.LeaseDuration); err != nil {
				w.recordStoreError(ctx, "extend_lease", err, "task_id", t.ID)
			}
		}
	}
}

// TaskRecoverer is implemented by backends that can re-create workflow tasks
// for committed inbox events orphaned by a crash between commit and ensure.
// Workers call it best-effort each tick; missing support is skipped.
type TaskRecoverer interface {
	RecoverOrphanedWorkflowTasks(ctx context.Context) (int, error)
}

func (w *Worker) recoverOrphanedTasks(ctx context.Context) {
	r, ok := w.backend.(TaskRecoverer)
	if !ok {
		return
	}
	// Throttle full scans: at most once per 5s. Crash gaps are recovered
	// within seconds without scanning on every poll tick.
	w.recoverMu.Lock()
	since := time.Since(w.lastRecover)
	if since < 5*time.Second && !w.lastRecover.IsZero() {
		w.recoverMu.Unlock()
		return
	}
	w.lastRecover = time.Now()
	w.recoverMu.Unlock()
	n, err := r.RecoverOrphanedWorkflowTasks(ctx)
	if err != nil {
		w.recordStoreError(ctx, "recover_tasks", err)
		return
	}
	if n > 0 {
		w.opts.Logger.Info("recovered orphaned workflow tasks", "n", n)
	}
}
