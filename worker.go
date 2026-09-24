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

	// wfClaim records the local wall-clock claim time of each workflow
	// task claimed by tickWorkflows. NackTask is fenced on the claim token
	// (worker + attempt, like ReleaseLease), so a stale delayed nack in
	// requeueWorkflowTask is rejected by the backend without touching a
	// peer's fresh lease. The local lease-expiry estimate (claim time +
	// LeaseDuration) stays as a fast path: once it has passed, a peer may
	// have reclaimed the task, so the stale worker skips the nack call
	// entirely and expiry reclaims naturally.
	wfClaimMu sync.Mutex
	wfClaim   map[int64]time.Time

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
		wfClaim:  map[int64]time.Time{},
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
	if w.opts.MaxPerInstance > 0 && !w.backend.Capabilities().FairDispatch {
		w.opts.Logger.Warn("tasuki: MaxPerInstance is set but the backend ignores it (no fair dispatch support); claims fall back to FIFO",
			"max_per_instance", w.opts.MaxPerInstance)
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

// untrackWorkflow removes t from the in-flight set only when the entry
// still carries t's claim generation (worker + attempt, as
// ownsWorkflowCommit and claimWorkflowRelease check). A shutdown-timeout
// restart lets the new generation track() a new attempt under the same
// task ID while the old turn is still blocked (e.g. on the instance
// actor); when the old backend call returns a domain/store error (not a
// cancellation) the failed-turn branch must not delete the NEW entry by
// ID — that would make ownsWorkflowCommit fail for the live turn, skip
// its commit, and risk repeating side effects post-expiry. On a
// generation mismatch the new entry is left intact and no local cleanup
// runs for the stale turn. Untracked IDs are ignored.
func (w *Worker) untrackWorkflow(t backend.Task) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.inFlight[t.ID]
	if !ok {
		return
	}
	if e.WorkerID != t.WorkerID || e.Attempt != t.Attempt {
		w.opts.Logger.Debug("skipping workflow untrack; entry re-tracked by a newer attempt",
			"instance_id", t.InstanceID, "task_id", t.ID)
		return
	}
	delete(w.inFlight, t.ID)
}

// untrackPending drops a pending commit's in-flight entry after disposal,
// generation-gated on the pending task when present (see untrackWorkflow)
// so a stale turn never deletes its successor's entry. Legacy/test
// pendings without a task fall back to the adv-derived ID.
func (w *Worker) untrackPending(p pendingWorkflowCommit) {
	if p.task.ID != 0 || p.task.InstanceID != "" {
		w.untrackWorkflow(p.task)
		return
	}
	w.untrack(p.adv.TaskID)
}

// claimWorkflowRelease atomically removes t from the in-flight set,
// reporting whether this caller still owns the lease and may release it.
// Shutdown's releaseInFlight and the workflow abandon paths below both
// funnel through in-flight ownership so only one of them releases a given
// lease: after a shutdown-timeout release, a peer may have re-claimed the
// task, and backends match the lease by task ID/key alone, so a second
// (late-cancel) release by the old turn would clear the peer's fresh lease
// and let a third worker execute concurrently with the peer.
//
// Presence alone is not ownership: the entry must still carry this turn's
// claim generation (worker + attempt, as ownsWorkflowCommit checks). A
// restart after a shutdown timeout lets the new generation track() a new
// attempt under the same task ID, REPLACING the old turn's entry; the old
// turn's late release must not delete the new entry (which would make
// ownsWorkflowCommit fail for the live turn and leave its lease untracked
// until expiry) — and must not release the new lease. A generation
// mismatch therefore reports false without touching the entry: the newer
// attempt owns it now.
func (w *Worker) claimWorkflowRelease(t backend.Task) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.inFlight[t.ID]
	if !ok {
		return false
	}
	if e.WorkerID != t.WorkerID || e.Attempt != t.Attempt {
		w.opts.Logger.Debug("skipping workflow lease release; entry re-tracked by a newer attempt",
			"instance_id", t.InstanceID, "task_id", t.ID)
		return false
	}
	delete(w.inFlight, t.ID)
	return true
}

// ownsWorkflowCommit reports whether the claimed task generation t is still
// tracked in the in-flight set, i.e. whether a pending advancement commit
// for it may proceed. A shutdown-timeout releaseInFlight may have released
// a finished pending turn (removing it here) while a peer re-claimed the
// task; flushing the stale advancement afterwards would commit (and delete)
// the peer's active task after duplicate execution, since backends validate
// the advancement by task ID alone. Generation fencing (worker + attempt)
// additionally covers an entry re-tracked by a newer claim of the same
// task ID, whose lease must never be disturbed by the old turn.
func (w *Worker) ownsWorkflowCommit(t backend.Task) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.inFlight[t.ID]
	if !ok {
		return false
	}
	return e.WorkerID == t.WorkerID && e.Attempt == t.Attempt
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
		// numeric ID) addresses the lease. Fenced by the tracked claim
		// token: if the task was reclaimed by a peer while shutting
		// down, the backend reports ErrNotFound and the fresh lease is
		// left intact. A fenced release reports ErrNotFound when the
		// lease moved on (peer reclaim or successor turn): the lease is
		// already released, not a failure.
		if err := w.backend.ReleaseLease(ctx, t); err != nil {
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
	// Conservative lease base (round-23 P2a): backends stamp visible_at
	// during the claim transaction, so a slow ClaimTasks (scan-heavy
	// stores) returns claims whose store lease already started. Measuring
	// the retry deadline from loop entry (after the call returns) extends
	// the window past the actual lease by the call latency; a peer then
	// reclaims mid-turn while this worker still runs side effects.
	// Capturing the instant BEFORE the call can only bound the window
	// early, never late.
	claimStart := time.Now()
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
	// Record local claim times so the delayed nack can be fenced against
	// a reclaim race (see requeueWorkflowTask). Cleared after the flush
	// below; entries are wall-clock only, never store time. Stamped from
	// the pre-claim instant above for the same conservatism.
	for _, t := range wtasks {
		w.trackWfClaimAt(t.ID, claimStart)
	}
	defer w.clearWfClaims(wtasks)
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
			// Kind-routed so workflow tasks address WF#<instanceID>,
			// fenced on the just-claimed token so this only releases our
			// own claim.
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
		// after a stop therefore sees no renewal in flight. The join is
		// bounded by the lease scale (round-26 P1): every renewal store
		// call ends at the abandonment deadline (see renewOnceBounded)
		// and every sleep selects on done, so the loop always exits by
		// then; the explicit cap only guards against a future blocking
		// addition. Giving up still proceeds to the release/nack while
		// an orphaned context-ignoring call may land later (the same
		// documented residual as a pre-existing blocked renewal).
		stopRenewal := func() {
			leaseOnce.Do(func() { close(leaseDone) })
			joinDone := make(chan struct{})
			go func() {
				defer close(joinDone)
				leaseWg.Wait()
			}()
			bound := w.opts.LeaseDuration
			if bound <= 0 {
				bound = 30 * time.Second
			}
			timer := time.NewTimer(bound)
			defer timer.Stop()
			select {
			case <-joinDone:
			case <-timer.C:
				w.opts.Logger.Warn("renewal loop did not exit in lease bound; proceeding without the join",
					"task_id", t.ID)
			}
		}
		renewalStops = append(renewalStops, stopRenewal)
		wg.Add(1)
		go func(t backend.Task, leaseDone chan struct{}, stopRenewal func(), claimBase time.Time) {
			defer wg.Done()
			defer func() { <-w.wfSem }()
			// Track the claim and start renewal BEFORE waiting on the
			// per-instance actor (round-11 P1): a turn queued behind
			// a long turn on the same instance otherwise holds no
			// renewal while queued, so its lease expires mid-queue
			// and a peer reclaims it — concurrent execution of the
			// same turn, including local side effects. Renewal stops
			// via stopRenewal on every path below (early per-task
			// stops inside dispatch plus the final stop after the
			// flush), and the abandon path stops it when dispatch
			// never runs.
			w.track(t)
			// Per-turn execution context (round-21 P2a): proven lease loss
			// (ExtendLease ErrNotFound — the lease moved on, so a peer owns
			// the turn now) cancels the turn, so handleWorkflow and the
			// local activities running under it abandon instead of
			// executing side effects concurrently with the peer. The
			// parent is the tick ctx, so Shutdown still cancels every
			// turn; the deferred cancel avoids leaking the context chain
			// on paths that return early. Renewal signals the loss (see
			// extendLeaseLoop); the turn observes it through the
			// pre-invoke and post-call runCtx checks in
			// attachLocalActivityRunner and the entry/exit ctx checks in
			// handleWorkflow. Store calls outside the turn itself
			// (metrics, requeue, release) keep the tick ctx below so a
			// loss-canceled turn still issues them on a live context and
			// the backend fence — not ctx state — decides their fate.
			turnCtx, cancelTurn := context.WithCancel(ctx)
			defer cancelTurn()
			go func() {
				defer leaseWg.Done()
				// NOTE: the loop keeps the tick ctx, not turnCtx: a
				// finished turn stays tracked (and renewed) until the
				// batch flush, long after its dispatch closure — and
				// this goroutine's deferred cancelTurn — ran. Binding
				// the loop to turnCtx would stop renewal at
				// handleWorkflow return and let a finished task's lease
				// expire while a slow sibling still runs (peer reclaim
				// + duplicate execution). cancelTurn travels only as
				// the loss callback; the loop returns right after
				// signaling, so it never observes its own signal.
				// claimBase bounds the retry window below (see
				// extendLeaseLoop): the renewal deadline derives
				// from the actual claim, not from loop entry.
				w.extendLeaseLoop(ctx, t, leaseDone, cancelTurn, claimBase)
			}()
			// The actor wait observes the per-turn context, not just the
			// tick context (round-26 P2): a renewal failure or
			// ErrNotFound cancels turnCtx (see extendLeaseLoop), and
			// a turn still queued on the instance actor behind an
			// old context-ignoring turn must abandon acquisition
			// instead of blocking forever on the live tick ctx —
			// holding its workflow slot and stalling wg.Wait (and
			// the flush of other turns) behind a lease it already
			// lost. Shutdown still cancels every turn through the
			// tick-ctx parent, preserving the existing abandon path
			// below; the release there stays ownership-gated so a
			// concurrent releaseInFlight can never be followed by a
			// second release.
			if !w.dispatchWorkflow(turnCtx, t.InstanceID, func() {
				w.opts.Metrics.AddWorkflowTask(ctx, 1)
				w.opts.Logger.Debug("workflow task", "instance_id", t.InstanceID, "task_id", t.ID)
				p, herr := w.handleWorkflow(turnCtx, t, stopRenewal)
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
					// A canceled tick abandons the turn regardless of
					// herr (round-19 P2b): Shutdown racing an early
					// non-cancel error (loadWorkflowState, registry
					// lookup) used to miss the abandon path (herr is
					// not a context error) and fall through to
					// untrack + nack/release with the canceled tick
					// ctx — which a context-aware backend rejects —
					// leaving the task untracked so neither this
					// path nor Shutdown's releaseInFlight can release
					// it, and failover waits for lease expiry. Check
					// ctx.Err() first: any canceled tick abandons
					// with an ownership-gated detached release
					// (see claimWorkflowRelease) instead. A
					// lease-loss-canceled turn (round-21 P2a) abandons the
					// same way even when the tick itself is live: the lease
					// belongs to a peer now, so requeueing would only issue
					// fenced no-ops while delaying the peer retry.
					if errors.Is(herr, errTurnAbandoned) ||
						ctx.Err() != nil || turnCtx.Err() != nil {
						// Worker lifecycle ended mid-turn, or the lease was
						// lost to a peer mid-turn: abandon the turn
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
						w.opts.Logger.Debug("workflow turn abandoned; lease released for peer retry",
							"instance_id", t.InstanceID, "task_id", t.ID)
						if w.claimWorkflowRelease(t) {
							w.releaseWorkflowLease(t)
						} else {
							w.opts.Logger.Debug("skipping workflow lease release; not in-flight owner",
								"instance_id", t.InstanceID, "task_id", t.ID)
						}
						return
					}
					w.recordStoreError(ctx, "commit_workflow", herr,
						"instance_id", t.InstanceID, "task_id", t.ID)
					w.opts.Logger.Debug("workflow task error", "instance_id", t.InstanceID, "err", herr)
					if errors.Is(herr, backend.ErrConflict) || errors.Is(herr, backend.ErrSuperseded) || errors.Is(herr, backend.ErrNotFound) {
						w.dropSticky(t.InstanceID)
					}
					// Contention releases immediately for fast replay;
					// anything else backs off via delayed nack so a
					// persistently failing task does not spin the poll
					// loop (see requeueWorkflowTask). The turn stays
					// tracked through the requeue (round-20 P2): the tick
					// was live at the check above, but Shutdown may cancel
					// it before or during the requeue, and a
					// context-aware backend then rejects the requeue's
					// release/nack on the canceled ctx. Untracking first
					// would leave the task untracked so neither the
					// rejected requeue nor Shutdown's releaseInFlight can
					// release it, hiding the renewed lease until expiry.
					// Tracked, a concurrent releaseInFlight still finds
					// and releases it (a second fenced requeue op then
					// reports ErrNotFound, which requeue tolerates); if it
					// already passed, the post-requeue check below falls
					// back to the detached abandonment release.
					w.requeueWorkflowTask(ctx, t, herr)
					if ctx.Err() != nil {
						// Tick canceled during the requeue: the requeue's
						// release/nack on the canceled ctx may have been
						// rejected, leaving the lease held with nobody
						// else guaranteed to release it. Abandon via the
						// detached-context release instead (live detached
						// ctx, ownership-gated like the abandon path: a
						// concurrent releaseInFlight that already released
						// makes the claim fail and nothing is double
						// released).
						if w.claimWorkflowRelease(t) {
							w.releaseWorkflowLease(t)
						} else {
							w.opts.Logger.Debug("skipping workflow lease release; not in-flight owner",
								"instance_id", t.InstanceID, "task_id", t.ID)
						}
						return
					}
					w.untrackWorkflow(t)
					return
				}
				if p == nil {
					// Nacked (renewal already stopped and joined inside
					// handleWorkflow before the NackTask call) or no-op:
					// nothing awaits commit, so untrack and stop renewal
					// now instead of renewing during the wait for slower
					// siblings. The nack-path stop above makes this a no-op
					// there. Generation-gated like the failed-turn path:
					// a stale turn must not delete its successor's entry.
					w.untrackWorkflow(t)
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
			}) {
				// Dispatch abandoned (ctx canceled while queued on the
				// actor): the turn never ran, so stop its renewal first
				// — no ExtendLease may land after the release below —
				// and release the lease for a prompt peer retry.
				// Ownership-gated like every other release path:
				// Shutdown's releaseInFlight may have released already
				// (and a peer re-claimed), and backends match the lease
				// by ID/key alone, so an unconditional release would
				// clear the peer's lease.
				stopRenewal()
				if w.claimWorkflowRelease(t) {
					w.releaseWorkflowLease(t)
				} else {
					w.opts.Logger.Debug("skipping workflow lease release; not in-flight owner",
						"instance_id", t.InstanceID, "task_id", t.ID)
				}
			}
		}(t, leaseDone, stopRenewal, claimStart)
	}
	wg.Wait()
	// A canceled tick must not flush: a turn may have completed (pending)
	// while wg.Wait() waited for a slower sibling, and Shutdown cancels
	// the tick context before this flush. Context-aware backends reject
	// the canceled flush, but ctx-insensitive ones (memory and similar)
	// still persist the advancement — though documented behavior says
	// canceled turns are abandoned and released for a peer retry.
	// Dispose instead: ownership-gated, kind-routed releases (see
	// claimWorkflowRelease) so a Shutdown releaseInFlight that already
	// released (and a peer re-claimed) lease is never cleared twice.
	// The check below races a cancel landing between it and the first
	// store call; flushWorkflowCommits re-checks cancellation (and
	// ownership) immediately before EACH store call (round-18 P1), so
	// the remaining window is only the backend call itself — plus the
	// documented residual of a cancel landing mid-call on a
	// context-insensitive backend, whose fenced commit still cannot
	// disturb a peer's reclaimed lease (see flushWorkflowCommits).
	if ctx.Err() != nil {
		// Dispose under one shared release budget (see
		// releaseWorkflowLeases): N pendings released serially with a
		// fresh timeout each cost up to N×timeout with a blocking
		// backend. Ownership is still claimed per task so a Shutdown
		// releaseInFlight that already released (and a peer re-claimed)
		// lease is never cleared twice; claimed entries stay claimed
		// even when the shared budget runs out early.
		var toRelease []backend.Task
		for _, p := range pending {
			if w.claimWorkflowRelease(p.task) {
				toRelease = append(toRelease, p.task)
			} else {
				w.opts.Logger.Debug("skipping workflow lease release; not in-flight owner",
					"instance_id", p.task.InstanceID, "task_id", p.adv.TaskID)
			}
		}
		w.releaseWorkflowLeases(toRelease)
	} else {
		// Live tick: flush. Pending tasks stayed tracked through the
		// flush (see above): successes are untracked inside
		// flushWorkflowCommits. Commits that failed under a tick
		// canceled mid-flush are released explicitly for a prompt peer
		// retry instead of waiting for lease expiry (a flush whose
		// per-call cancellation gate fires before a store call never
		// touches the store, so the lease is still ours unless
		// Shutdown's releaseInFlight already released it — the release
		// below is ownership-gated for that race).
		// Live-context failures (conflict/transient) are untracked
		// without release: the task may be superseded, and the lease
		// expires naturally.
		failed := w.flushWorkflowCommits(ctx, pending)
		if len(failed) > 0 {
			if ctx.Err() != nil {
				// Same shared budget as the canceled-tick dispose above:
				// a mid-flush cancel with several failed pendings must
				// not cost one fresh timeout per task.
				var toRelease []backend.Task
				for _, p := range failed {
					// Only the in-flight owner releases (see
					// claimWorkflowRelease): Shutdown's releaseInFlight may
					// have released this pending task mid-flush and a peer
					// may have re-claimed it, and backends match the lease
					// by ID/key alone, so an unconditional release would
					// clear the peer's lease.
					if w.claimWorkflowRelease(p.task) {
						toRelease = append(toRelease, p.task)
					} else {
						w.opts.Logger.Debug("skipping workflow lease release; not in-flight owner",
							"instance_id", p.task.InstanceID, "task_id", p.adv.TaskID)
					}
				}
				w.releaseWorkflowLeases(toRelease)
			} else {
				for _, p := range failed {
					w.untrackPending(p)
				}
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
	// Conservative lease base (round-23 P2a, see tickWorkflows): the
	// renewal deadline derives from the actual claim, not handler start.
	claimStart := time.Now()
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
		go func(t backend.Task, claimBase time.Time) {
			defer done()
			defer func() { <-w.actSem }()
			defer w.untrack(t.ID)
			if herr := w.handleActivity(ctx, t, claimBase); herr != nil {
				w.opts.Logger.Debug("activity task error",
					"instance_id", t.InstanceID, "task_id", t.ID, "err", herr)
			}
		}(t, claimStart)
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
	// Conservative lease base (round-23 P2a, see tickWorkflows).
	claimStart := time.Now()
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
		go func(t backend.Task, claimBase time.Time) {
			defer wg.Done()
			if global {
				defer done()
			}
			defer func() { <-w.actSem }()
			defer w.untrack(t.ID)
			if herr := w.handleActivity(ctx, t, claimBase); herr != nil {
				w.opts.Logger.Debug("activity task error",
					"instance_id", t.InstanceID, "task_id", t.ID, "err", herr)
			}
		}(t, claimStart)
	}
	wg.Wait()
}

// errTurnAbandoned marks a workflow turn canceled by the worker lifecycle
// (Shutdown) whose error derives from that cancellation. The turn must be
// abandoned — lease released, nothing committed — instead of persisting the
// shutdown as a terminal workflow failure.
var errTurnAbandoned = errors.New("tasuki: workflow turn abandoned on shutdown")

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

// releaseWorkflowLeases releases a batch of workflow-task leases under one
// shared ShutdownReleaseTimeout deadline (round-17 P2). Each
// releaseWorkflowLease call mints a fresh timeout, so releasing N
// completed pendings from a shutdown-canceled tick serially costs up to
// N×timeout (with a blocking backend) even though the option documents a
// single bound. Sharing one context bounds the whole batch to ~1×timeout:
// the first blocked release consumes the budget and the rest fail fast,
// and unreleased leases expire naturally for peer reclaim. Ownership must
// already be claimed (see claimWorkflowRelease) before calling: entries
// removed from the in-flight set stay removed even when the shared
// budget runs out before their ReleaseLease is attempted.
func (w *Worker) releaseWorkflowLeases(tasks []backend.Task) {
	if len(tasks) == 0 {
		return
	}
	timeout := w.opts.ShutdownReleaseTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	relCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, t := range tasks {
		select {
		case <-relCtx.Done():
			w.opts.Logger.Warn("shutdown lease release timed out",
				"remaining", len(tasks), "error", relCtx.Err())
			w.opts.Metrics.AddStoreError(context.Background(), "release_lease")
			return
		default:
		}
		if err := w.backend.ReleaseLease(relCtx, t); err != nil {
			// A fenced release reports ErrNotFound when the lease moved on
			// (peer reclaim or successor turn): the lease is already
			// released, not a failure.
			if errors.Is(err, backend.ErrNotFound) {
				w.opts.Logger.Debug("workflow lease already released",
					"instance_id", t.InstanceID, "task_id", t.ID)
				continue
			}
			w.recordStoreError(context.Background(), "release_lease", err, "task_id", t.ID)
			select {
			case <-relCtx.Done():
				return
			default:
			}
		}
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
		// Fence the commit to the claimed generation (see Advancement):
		// a slow/non-context-aware CommitAdvancement starting after the
		// in-memory preflight could otherwise validate ID/sequence only
		// and delete a peer's reclaimed task after a Shutdown release.
		WorkerID: t.WorkerID,
		Attempt:  t.Attempt,
	}

	pending := func() *pendingWorkflowCommit {
		return &pendingWorkflowCommit{
			instanceID:  t.InstanceID,
			baseJournal: state.Journal,
			adv:         adv,
			task:        t,
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

func (w *Worker) handleActivity(ctx context.Context, t backend.Task, claimBase time.Time) error {
	act, err := w.reg.activity(t.Name)
	if err != nil {
		if errors.Is(err, ErrActivityNotRegistered) {
			return w.nackIncompatible(ctx, t, "unregistered_activity", err)
		}
		return w.failActivity(ctx, t, err)
	}

	done := make(chan struct{})
	defer close(done)
	// claimBase bounds the retry window below (see extendLeaseLoop): the
	// renewal deadline derives from the actual claim, not handler start.
	go w.extendLeaseLoop(ctx, t, done, nil, claimBase)

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
				// Worker cancellation wins over even a panic result,
				// unifying with acceptLocalResult's prioritization
				// (round-16 P2): the turn is abandoned either way.
				if runCtx != nil && runCtx.Err() != nil {
					out = nil
					err = runCtx.Err()
				}
			}
		}()
		act, err := w.reg.activity(name)
		if err != nil {
			return nil, err
		}
		// Reject an already-canceled runCtx BEFORE invoking in both
		// branches below (round-19 P2a): a workflow that catches
		// cancellation from one local activity and calls ExecuteLocal
		// again passes the same canceled runCtx, and invoking act.fn
		// (or launching the timeout goroutine that invokes it) would
		// run side effects post-shutdown. The post-call checks alone
		// cannot prevent the invocation. Return the cancellation
		// without invoking fn or launching a goroutine.
		if runCtx != nil && runCtx.Err() != nil {
			return nil, runCtx.Err()
		}
		// Run with the worker turn's context so Shutdown cancels a running
		// local activity.
		ctx := runCtx
		if ctx == nil {
			ctx = context.Background()
		}
		timeout := w.opts.LocalActivityTimeout
		if timeout <= 0 {
			// No timeout: the call above runs inline, but worker
			// cancellation (runCtx: Shutdown or Start-parent cancel)
			// still wins over a returned value or domain error
			// (round-16 P2). Without this check an activity that
			// observes the cancellation yet returns a value is
			// accepted, and the workflow keeps invoking further
			// local activities — with side effects — until wf.fn
			// returns, even though handleWorkflow abandons the turn
			// afterwards anyway. This unifies the timeout-disabled
			// branch with acceptLocalResult's prioritization.
			out, err := act.fn(ctx, input)
			if runCtx != nil && runCtx.Err() != nil {
				return nil, runCtx.Err()
			}
			return out, err
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
					done <- callResult{nil, fmt.Errorf("activity panic: %v", r), time.Now()}
				}
			}()
			o, e := act.fn(ctx, input)
			// Capture the completion instant when the result is
			// produced (see acceptLocalResult): sampling ctx.Err()
			// here instead arbitrates completion vs expiry on
			// consumption timing, marking a return-just-before-deadline
			// late when the timer fires first, or accepting a
			// return-after-deadline when the timer has not fired yet.
			completed := time.Now()
			select {
			case done <- callResult{o, e, completed}:
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
				return resolveLocalResult(name, nil, nil, time.Time{}, ctx, runCtx, timeout)
			}
		}
	})
}

// acceptLocalResult maps a delivered local activity result to its outcome,
// preferring production-time knowledge over consumption-time state. A
// result completed before the local deadline fired is returned as-is even
// if the deadline fired before consumption; late results go through the
// timeout/cancel mapping in resolveLocalResult so a late success is never
// journaled after expiry.
//
// Worker cancellation (runCtx: Shutdown or Start-parent cancel) wins over
// even an on-time result. A local activity that observes the cancellation
// but still returns a value before the deadline would otherwise be
// accepted via its on-time timestamp, and the workflow would keep
// executing (with side effects) until wf.fn returns — work whose
// advancement is abandoned anyway. Returning the cancellation surfaces
// the shutdown so the turn is abandoned promptly instead.
func acceptLocalResult(name string, r callResult, ctx, runCtx context.Context, timeout time.Duration) ([]byte, error) {
	if runCtx != nil && runCtx.Err() != nil {
		return nil, runCtx.Err()
	}
	if localCompletedOnTime(r.completed, ctx) {
		return r.out, r.err
	}
	return resolveLocalResult(name, r.out, r.err, r.completed, ctx, runCtx, timeout)
}

// localCompletedOnTime arbitrates completion vs expiry on timestamps
// rather than on ctx state sampled at consumption: completed is the
// instant the result was produced, compared against the call's deadline.
// A result produced at or before the deadline is on time even if the
// timer fired before the turn consumed it; a result produced after the
// deadline is late even if the timer has not fired yet. Without a
// deadline (defensive; the timeout path always sets one) it falls back
// to consumption-time state, as does a missing timestamp.
func localCompletedOnTime(completed time.Time, ctx context.Context) bool {
	if completed.IsZero() {
		return ctx.Err() == nil
	}
	dl, ok := ctx.Deadline()
	if !ok {
		return ctx.Err() == nil
	}
	return !completed.After(dl)
}

// callResult carries one local activity call outcome with its
// production-time completion instant (see attachLocalActivityRunner).
type callResult struct {
	out       []byte
	err       error
	completed time.Time
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
//
// completed is the production-time completion instant: when the deadline
// has passed but the runtime timer has not fired yet (consumption won the
// race, ctx still live), arbitration still uses the timestamp, so a
// post-deadline result is reported as a timeout rather than wrongly
// accepted. A zero completed disables the timestamp check and preserves
// the pure consumption-time mapping.
func resolveLocalResult(name string, out []byte, actErr error, completed time.Time, ctx, runCtx context.Context, timeout time.Duration) ([]byte, error) {
	if ctx.Err() != nil {
		if runCtx != nil && runCtx.Err() != nil {
			return nil, runCtx.Err()
		}
		return nil, localResultTimeoutError(name, timeout)
	}
	if dl, ok := ctx.Deadline(); ok && !completed.IsZero() && completed.After(dl) {
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

// renewCallResult carries one bounded ExtendLease attempt's store outcome
// from the issuing goroutine to the waiting renewal loop.
type renewCallResult struct {
	err error
}

// renewSettleGrace bounds how long renewOnceBounded waits past the
// abandonment deadline for an in-window call that settles in the same
// instant the deadline fires (goroutine scheduling). A fast backend's
// verdict then arrives microseconds later; discarding it would abandon a
// success the pre-fix code accepted (round-22 in-window recovery) and make
// boundary attempts flaky. The grace stays far below the abandonment
// margin floor (25ms, see renewAbandonMargin), so the loss signal still
// precedes any peer reclaim at expiry by at least half the margin.
const renewSettleGrace = 10 * time.Millisecond

// renewOnceBounded issues one ExtendLease for t and waits for it
// abandonably (round-26 P1). The store call runs with a context ending at
// deadline — the abandonment deadline lastSuccess+lease-margin — instead of
// the long-lived tick context, so a blocked call cannot hold the renewal
// loop past the point where the turn must be abandoned: the loop would
// otherwise keep the turn's side effects running after a peer reclaims at
// expiry, and stopRenewal would hang behind the call.
//
// It reports settled=true with the call's result (at is the conservative
// pre-call instant, mirroring the round-24 P2b stamping) when the call
// returns first. It reports settled=false when done or the tick context
// ends first (quiet stop — Shutdown owns the abandonment there) or when
// the deadline passes while the call is still blocked (expired=true — the
// caller signals lease loss so the turn cancels before a peer reclaims).
// A call settling within renewSettleGrace past the deadline still reports
// settled: it was issued in-window and its success extends our own live
// lease (expiry sits a full margin later), so accepting it preserves the
// round-22 short-lease recovery instead of abandoning on scheduling noise.
// The orphaned call settles on its own (buffered channel, no waiter
// leak): a context-aware backend aborts on the canceled call context,
// while a context-ignoring one landing later is the same documented
// residual as a pre-existing blocked renewal.
func (w *Worker) renewOnceBounded(ctx context.Context, done <-chan struct{}, t backend.Task, lease time.Duration, deadline time.Time, margin time.Duration) (at time.Time, err error, settled bool, expired bool) {
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	resCh := make(chan renewCallResult, 1)
	at = time.Now()
	go func() {
		resCh <- renewCallResult{err: w.backend.ExtendLease(callCtx, t, lease)}
	}()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-done:
		return time.Time{}, nil, false, false
	case <-ctx.Done():
		return time.Time{}, nil, false, false
	case <-timer.C:
		// Boundary tie-break (see renewSettleGrace): prefer a
		// verdict that lands with the deadline over abandoning on
		// scheduling noise. Past the grace the call is genuinely
		// stuck and the caller signals loss.
		grace := margin / 2
		if grace > renewSettleGrace {
			grace = renewSettleGrace
		}
		if grace < 0 {
			grace = 0
		}
		gtimer := time.NewTimer(grace)
		defer gtimer.Stop()
		select {
		case <-done:
			return time.Time{}, nil, false, false
		case <-ctx.Done():
			return time.Time{}, nil, false, false
		case res := <-resCh:
			return at, res.err, true, false
		case <-gtimer.C:
			return time.Time{}, nil, false, true
		}
	case res := <-resCh:
		return at, res.err, true, false
	}
}

func (w *Worker) extendLeaseLoop(ctx context.Context, t backend.Task, done <-chan struct{}, onLeaseLost func(), claimBase time.Time) {
	lease := w.opts.LeaseDuration
	d := lease / 2
	if d <= 0 {
		return
	}
	// lastSuccess is the wall-clock instant of the last renewal that moved
	// the store lease forward. It bounds the retry window below (round-21
	// P2b). It approximates the store clock, which production backends
	// keep aligned with wall time; tests align them manually when they
	// move the store clock.
	//
	// The base is the actual claim (round-23 P2a), not loop entry: the
	// loop starts after ClaimTasks returns plus dispatch queueing, and a
	// slow ClaimTasks (scan-heavy stores stamp visible_at during the
	// call) would otherwise stretch the retry deadline past the store
	// lease by that latency. lastSuccess bounds conservatively from the
	// earlier of the pre-claim wall instant and the claim's own
	// VisibleAt-derived start (see leaseBaseFromClaim).
	//
	// Every success stamps the pre-call instant (round-24 P2b), not the
	// response time: a successful ExtendLease delayed in flight was
	// already aging the store lease while blocked, so measuring from the
	// response stretches the following retry window past the actual
	// store expiry by the response latency — a later failure then keeps
	// the turn alive while a peer reclaims mid-side-effects. Stamping
	// before the call can only bound the window early, never late (same
	// conservatism as refreshLeaseAt/trackAt).
	lastSuccess := leaseBaseFromClaim(t, claimBase, lease)
	// The first renewal is scheduled from the claim timestamp (round-24
	// P2a), not from loop entry: a plain half-lease ticker started AFTER
	// ClaimTasks returns waits a full half-lease more, so a claim that
	// consumed more than half the lease lets the original lease expire —
	// and a peer reclaim — while the turn still waits for its first
	// renewal. When the claim was slow, the first attempt fires almost
	// immediately (the remaining lease minus the abandon margin, floored
	// at zero); when it was fast this equals the regular half-lease tick.
	// Later ticks keep the half-lease period.
	margin := renewAbandonMargin(lease)
	firstDelay := d
	if remaining := time.Until(lastSuccess.Add(lease - margin)); remaining < firstDelay {
		firstDelay = remaining
		if firstDelay < 0 {
			firstDelay = 0
		}
	}
	firstTimer := time.NewTimer(firstDelay)
	defer firstTimer.Stop()
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	first := true
	for {
		tick := ticker.C
		if first {
			tick = firstTimer.C
		}
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-tick:
			if first {
				first = false
			}
			// Bound the store call to the remaining lease window
			// (round-26 P1): a blocked ExtendLease must not hold
			// the loop past the abandonment deadline — the turn
			// would keep running side effects after a peer
			// reclaims at expiry, and stopRenewal would hang
			// behind the call.
			deadline := lastSuccess.Add(lease - margin)
			at, rerr, settled, expired := w.renewOnceBounded(ctx, done, t, lease, deadline, margin)
			if expired {
				// The call was still blocked at the abandonment
				// deadline: the lease may expire (or already
				// have expired) while it is in flight, so
				// signal loss now — the turn cancels and
				// abandons instead of executing beside the peer
				// that reclaims at expiry.
				w.signalLeaseLost(onLeaseLost, t)
				return
			}
			if !settled {
				return
			}
			if rerr != nil {
				// A fenced renewal reports ErrNotFound when the lease
				// moved on (task completed/deleted, or a peer reclaim
				// after a nack or an expired lease advanced the
				// generation): the lease is no longer ours, so signal
				// the loss (the turn cancels and abandons instead of
				// running side effects concurrently with the peer)
				// and stop renewing quietly instead of warning and
				// retrying a renewal the backend will keep rejecting.
				if errors.Is(rerr, backend.ErrNotFound) {
					w.opts.Logger.Debug("lease moved on; stopping renewal",
						"task_id", t.ID)
					w.signalLeaseLost(onLeaseLost, t)
					return
				}
				if ctx.Err() != nil {
					return
				}
				w.recordStoreError(ctx, "extend_lease", rerr, "task_id", t.ID)
				// A transient failure must not idle until the next
				// half-lease tick: that tick lands at the original
				// deadline, so a peer reclaims first and executes
				// concurrently after a single blip. Retry promptly
				// inside the remaining lease window instead (round-21
				// P2b); the turn is abandoned before expiry when the
				// lease proves unrestorable.
				if at, renewed := w.retryRenewal(ctx, t, done, lease, lastSuccess, onLeaseLost); renewed {
					lastSuccess = at
					// The store lease moved forward: the local
					// claim-time estimate used by the delayed nack
					// (see requeueWorkflowTask) must move with it
					// (see refreshWfClaim).
					w.refreshWfClaim(t.ID)
					continue
				}
				return
			}
			lastSuccess = at
			// The store lease moved forward: the local
			// claim-time estimate used by the delayed nack
			// (see requeueWorkflowTask) must move with it
			// (see refreshWfClaim).
			w.refreshWfClaim(t.ID)
		}
	}
}

// leaseBaseFromClaim derives the conservative renewal baseline for a
// claimed task (round-23 P2a). The pre-claim wall instant is always a
// valid lower bound for the store-visible lease start; when the claim
// itself carries a VisibleAt-derived start that is EARLIER (wall clock
// jumped forward, or the caller passed a post-claim instant), the
// earlier instant wins so the retry deadline never stretches past the
// actual store lease. A zero claimBase falls back to now (older/test
// call sites that never captured the pre-claim instant).
func leaseBaseFromClaim(t backend.Task, claimBase time.Time, lease time.Duration) time.Time {
	if claimBase.IsZero() {
		claimBase = time.Now()
	}
	if lease > 0 && !t.VisibleAt.IsZero() {
		if storeStart := t.VisibleAt.Add(-lease); storeStart.Before(claimBase) {
			return storeStart
		}
	}
	return claimBase
}

// renewAbandonMargin bounds how close to lease expiry retryRenewal may keep
// trying: once the remaining lease drops below the margin, the turn is
// abandoned (its context canceled via onLeaseLost) instead of racing a peer
// reclaim at expiry. A quarter of the lease, clamped to [25ms, 2s] so short
// test leases still retry while long production leases abandon with seconds
// to spare.
//
// The margin must stay strictly below half the lease: the first
// post-failure check runs about half a lease after the last success (the
// half-lease tick), so a margin at or above half the lease abandons
// immediately on one transient failure instead of retrying in-window —
// with the old 200ms floor, any lease at or below 400ms abandoned without
// a single retry, causing needless replay and repeated side effects. The
// cap below keeps at least one retry attempt possible for every positive
// lease (the loop attempts once before re-checking the deadline); for
// degenerate non-positive leases the margin bottoms out at zero.
func renewAbandonMargin(lease time.Duration) time.Duration {
	m := lease / 4
	if m < 25*time.Millisecond {
		m = 25 * time.Millisecond
	}
	if m > 2*time.Second {
		m = 2 * time.Second
	}
	if ceiling := lease/2 - time.Millisecond; m > ceiling {
		m = ceiling
	}
	if m < 0 {
		m = 0
	}
	return m
}

// retryRenewal retries a failed lease renewal with bounded backoff while
// lease time remains (round-21 P2b). lastSuccess is the last instant known
// to have a live store lease (see extendLeaseLoop). It reports the renewal
// instant and true when a retry succeeded — the caller refreshes its local
// estimates and resumes the ticker. It reports false when the turn must
// stop renewing: done or ctx ended (return quietly — Shutdown owns the
// abandonment), the lease proved lost (ErrNotFound: the loss is signaled so
// the turn cancels promptly instead of running on beside the peer), or the
// remaining lease ran below the abandon margin with no success (the loss is
// signaled so the turn is canceled BEFORE a peer reclaims at expiry and
// executes concurrently with this turn's remaining side effects).
//
// The backoff (100ms doubling to 1s) is prompt next to half-lease ticks
// (seconds to minutes) yet bounded per attempt; every sleep selects on done
// and ctx so stopRenewal never hangs behind a retry.
func (w *Worker) retryRenewal(ctx context.Context, t backend.Task, done <-chan struct{}, lease time.Duration, lastSuccess time.Time, onLeaseLost func()) (time.Time, bool) {
	backoff := 100 * time.Millisecond
	const maxBackoff = time.Second
	margin := renewAbandonMargin(lease)
	for {
		select {
		case <-done:
			return lastSuccess, false
		case <-ctx.Done():
			return lastSuccess, false
		default:
		}
		// Cap every sleep at the abandonment deadline (round-23 P2b):
		// an uncapped backoff overshoots lastSuccess+lease-margin, so
		// the turn keeps running side effects past the abandonment
		// point — and, for short leases, past the store expiry itself
		// (a 100ms lease fails its first renewal at ~50ms and would
		// sleep 100ms while a peer reclaims at 100ms). Waking at the
		// deadline routes back through the abandonment check above,
		// which signals the loss before a peer can reclaim.
		remaining := time.Until(lastSuccess.Add(lease - margin))
		if remaining <= 0 {
			w.opts.Logger.Warn("lease renewal unrestorable; abandoning turn before expiry",
				"task_id", t.ID)
			w.recordStoreError(ctx, "extend_lease", errors.New("tasuki: lease renewal unrestorable"), "task_id", t.ID)
			w.signalLeaseLost(onLeaseLost, t)
			return lastSuccess, false
		}
		sleep := backoff
		if sleep > remaining {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-done:
			timer.Stop()
			return lastSuccess, false
		case <-ctx.Done():
			timer.Stop()
			return lastSuccess, false
		case <-timer.C:
		}
		// Conservative renewal instant (round-24 P2b, mirroring the
		// claimBase pattern from round-23): the store lease was already
		// aging while this call was in flight, so the retry window
		// bounds from the pre-call instant, never the delayed response.
		// Bound each attempt to the remaining lease window (round-26
		// P1): a blocked attempt must not hold the retry past the
		// abandonment deadline (same reasoning as the ticker path in
		// extendLeaseLoop above).
		deadline := lastSuccess.Add(lease - margin)
		at, rerr, settled, expired := w.renewOnceBounded(ctx, done, t, lease, deadline, margin)
		if expired {
			w.signalLeaseLost(onLeaseLost, t)
			return lastSuccess, false
		}
		if !settled {
			return lastSuccess, false
		}
		if rerr == nil {
			return at, true
		} else if errors.Is(rerr, backend.ErrNotFound) {
			w.opts.Logger.Debug("lease moved on; stopping renewal",
				"task_id", t.ID)
			w.signalLeaseLost(onLeaseLost, t)
			return lastSuccess, false
		} else if ctx.Err() != nil {
			return lastSuccess, false
		} else {
			w.opts.Logger.Debug("lease renewal retry failed",
				"task_id", t.ID, "err", rerr)
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// signalLeaseLost propagates proven ownership loss to the turn: the
// workflow turn's context is canceled so handleWorkflow and its local
// activities abandon instead of executing side effects concurrently with
// the peer that reclaimed the lease (round-21 P2a). A nil callback (regular
// activity turns, which carry no cancellable turn context) keeps the
// previous behavior of stopping renewal quietly.
func (w *Worker) signalLeaseLost(onLeaseLost func(), t backend.Task) {
	if onLeaseLost == nil {
		return
	}
	w.opts.Logger.Debug("signaling lease loss; abandoning turn",
		"task_id", t.ID, "instance_id", t.InstanceID)
	onLeaseLost()
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
