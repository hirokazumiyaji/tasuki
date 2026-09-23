package tasuki

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	// inFlight tracks claimed task IDs with their local lease-expiry
	// estimate (claim time + LeaseDuration, refreshed on each successful
	// renewal). Backend leases are keyed by task ID alone with no
	// ownership fencing, so the mere presence of an entry is NOT proof
	// this worker still owns the lease: once the local expiry passes the
	// lease may have been reclaimed by a peer and releasing by ID would
	// clear the peer's fresh lease. Release paths must honor the expiry;
	// result commits additionally transfer ownership out of this map
	// before touching the store (see claimCommitOwnership).
	inFlight map[int64]time.Time

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
	// execCtx is the execution context for detached activities. It stays
	// valid during Shutdown's grace period (unlike the poll loop ctx which
	// is canceled immediately) so activities finishing within the grace can
	// still commit their results. It is canceled after the grace expires to
	// abort stragglers. Guarded by actMu.
	execCtx    context.Context
	execCancel context.CancelFunc

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
		inFlight: map[int64]time.Time{},
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
	// Execution observes the Start parent (so parent cancel still aborts
	// activities and lease renewal) but uses its own cancel separate from
	// the poll loop ctx, so Shutdown's immediate loop cancellation does not
	// abort activities still within their grace.
	w.execCtx, w.execCancel = context.WithCancel(parent)
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
	// Stop new detached activities first so the grace period only covers
	// work already in flight. The poll loop ctx is canceled immediately to
	// stop new claims, but activity execution uses execCtx which stays valid
	// until the grace below expires. This keeps within-grace completions
	// committable: result commits must not use the canceled loop ctx
	// (pgx Begin on a canceled ctx fails and the result would be lost until
	// lease expiry).
	w.actMu.Lock()
	w.stopping = true
	execCancel := w.execCancel
	w.execCancel = nil
	w.actMu.Unlock()
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
	// Wait for in-flight activities within the remaining grace period. Only
	// activities still running after the grace get their leases released
	// (peers reclaim them after lease expiry or via the release below).
	waitForWaitGroup(&w.actWg, ctx)
	// Grace is over: abort stragglers. Activities observe execCtx cancellation
	// and release (not retry/fail) so Shutdown never consumes an attempt.
	// Commits that already started use a detached context (see commitContext)
	// and are covered by the actWg wait above. If this wait expires first,
	// the release below still cannot hand a committing task to a peer:
	// result commits transfer ownership out of the in-flight set before
	// touching the store (see claimCommitOwnership).
	if execCancel != nil {
		execCancel()
	}
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

// execContext returns the execution context for detached activities.
// It outlives the poll loop ctx across Shutdown's grace period so
// within-grace completions remain committable, but it still observes the
// Start parent cancellation so parent cancel aborts activities and lease
// renewal.
func (w *Worker) execContext(fallback context.Context) context.Context {
	w.actMu.Lock()
	defer w.actMu.Unlock()
	if w.execCtx != nil {
		return w.execCtx
	}
	return fallback
}

// defaultCommitTimeout bounds detached result commits during normal
// operation. It is independent of ShutdownReleaseTimeout (which only bounds
// shutdown lease cleanup) so a short shutdown-only value cannot cancel
// ordinary commits.
const defaultCommitTimeout = 30 * time.Second

// commitContext returns a store-commit context detached from execution
// cancellation with a bounded timeout. Result commits (Complete/Retry/fail)
// must never use the canceled loop/execution ctx: pgx Begin on a canceled
// ctx fails and a within-grace result would be lost until lease expiry
// (and untracked, so not even released). Late commits after the grace are
// suppressed by the caller checking execution ctx cancellation first.
// Call it immediately before each result operation: creating it at handler
// entry lets the timeout expire during long activity execution.
//
// The bound is CommitTimeout during normal operation and
// ShutdownReleaseTimeout once Shutdown has begun (when commits racing
// shutdown must fit the shutdown budget so Shutdown stays predictable).
func (w *Worker) commitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := w.opts.CommitTimeout
	if timeout <= 0 {
		timeout = defaultCommitTimeout
	}
	w.actMu.Lock()
	stopping := w.stopping
	w.actMu.Unlock()
	if stopping {
		timeout = w.opts.ShutdownReleaseTimeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func (w *Worker) track(taskID int64) {
	w.mu.Lock()
	w.inFlight[taskID] = time.Now().Add(w.leaseDuration())
	w.mu.Unlock()
}

func (w *Worker) untrack(taskID int64) {
	w.mu.Lock()
	delete(w.inFlight, taskID)
	w.mu.Unlock()
}

// leaseDuration returns the configured lease, defaulted so a missing value
// never yields an immediately-expired in-flight entry.
func (w *Worker) leaseDuration() time.Duration {
	if w.opts.LeaseDuration > 0 {
		return w.opts.LeaseDuration
	}
	return 30 * time.Second
}

// refreshLease pushes a task's local lease expiry forward after a successful
// renewal (ExtendLease/RecordHeartbeat). Unknown IDs are ignored: the task
// already transferred to a commit or was released.
func (w *Worker) refreshLease(taskID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.inFlight[taskID]; ok {
		w.inFlight[taskID] = time.Now().Add(w.leaseDuration())
	}
}

// claimCommitOwnership transfers taskID out of the in-flight set so a
// concurrent Shutdown releaseInFlight cannot hand the task to a peer while
// this worker's detached result commit is still running. It reports whether
// this caller owned the task: false means Shutdown already released it (a
// peer may own it now) and the caller must skip its backend commit, since a
// task-ID-only Complete/Retry/fail would delete or reschedule the peer-owned
// task and produce concurrent or stale execution.
//
// Unlike claimReleaseOwnership this is not gated on the local lease expiry:
// discarding a result that may still be valid is worse than attempting the
// commit, and these paths require a live execution context with active
// renewal. Callers whose commit then fails leave the task untracked, so the
// lease expires naturally instead of being released promptly.
func (w *Worker) claimCommitOwnership(taskID int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.inFlight[taskID]; !ok {
		return false
	}
	delete(w.inFlight, taskID)
	return true
}

// beginResultCommit transfers commit ownership (see claimCommitOwnership),
// reporting whether the backend result commit may proceed. False means
// Shutdown already released the lease to a peer: the caller must skip its
// task-ID-only store op and return (ctx.Err() preserves shutdown/cancel
// visibility for logging) to avoid clobbering the new owner.
func (w *Worker) beginResultCommit(taskID int64) bool {
	if w.claimCommitOwnership(taskID) {
		return true
	}
	w.opts.Logger.Debug("skipping stale activity commit; lease already released",
		"task_id", taskID)
	return false
}

// beginDetachedCommit transfers commit ownership (see beginResultCommit)
// and marks a detached result commit as in flight so lease renewal stays
// alive until the commit finishes (see extendLeaseLoop). Call it immediately
// before creating the detached commit context: a parent cancel or
// shutdown-grace expiry racing the commit must not stop renewal mid-commit,
// or a commit longer than the remaining lease races a peer reclaim and the
// task-ID-only store op clobbers the peer's task.
func (w *Worker) beginDetachedCommit(taskID int64, committing *atomic.Bool) bool {
	if !w.beginResultCommit(taskID) {
		return false
	}
	committing.Store(true)
	return true
}

// claimReleaseOwnership atomically removes taskID from the in-flight set,
// reporting whether this caller may release the lease. Shutdown's
// releaseInFlight and the handler's shutdown-release path both funnel through
// in-flight ownership so only one of them releases a given lease: an activity
// that ignores cancellation and returns after Shutdown already released (and
// a peer re-claimed) its lease must not ReleaseLease again, since backend
// leases are keyed by task ID alone and a second release would clear the
// peer's fresh lease and enable duplicate execution.
//
// Presence alone is not ownership: when the Start parent is canceled without
// Shutdown, renewal stops but a cancel-ignoring activity can keep running
// past LeaseDuration, letting a peer reclaim the task. A locally expired
// lease therefore reports false (after still removing the entry) so the
// caller does not clear a potentially peer-owned lease; the task is already
// reclaimable via expiry.
func (w *Worker) claimReleaseOwnership(taskID int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	expiry, ok := w.inFlight[taskID]
	if !ok {
		return false
	}
	delete(w.inFlight, taskID)
	if time.Now().After(expiry) {
		w.opts.Logger.Debug("skipping lease release; local lease expired",
			"task_id", taskID)
		return false
	}
	return true
}

// releaseContext returns a live store-call context for best-effort lease
// releases on tick paths, detached from poll/execution cancellation and
// bounded by ShutdownReleaseTimeout. Tasks rejected by trackActivity after
// a concurrent Shutdown starts are already untracked (so releaseInFlight
// cannot cover them) while the poll ctx may already be canceled, which
// context-aware stores reject; a detached context keeps the release
// effective so the task becomes claimable immediately instead of waiting
// out the full lease.
func (w *Worker) releaseContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := w.opts.ShutdownReleaseTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func (w *Worker) releaseInFlight(ctx context.Context) {
	now := time.Now()
	w.mu.Lock()
	ids := make([]int64, 0, len(w.inFlight))
	for id, expiry := range w.inFlight {
		if now.After(expiry) {
			// Lease already expired locally: a peer may have reclaimed it,
			// and releasing by task ID alone would clear the peer's fresh
			// lease. Drop without releasing; expiry already makes it
			// claimable.
			w.opts.Logger.Debug("shutdown lease release skipped; local lease expired",
				"task_id", id)
			delete(w.inFlight, id)
			continue
		}
		// Tasks with a result commit underway are absent: handleActivity
		// transfers ownership out via claimCommitOwnership before touching
		// the store, so they are never released from under their commit.
		ids = append(ids, id)
		delete(w.inFlight, id)
	}
	w.mu.Unlock()
	for _, id := range ids {
		select {
		case <-ctx.Done():
			w.opts.Logger.Warn("shutdown lease release timed out",
				"released", 0, "remaining", len(ids), "error", ctx.Err())
			w.opts.Metrics.AddStoreError(context.Background(), "release_lease")
			return
		default:
		}
		if err := w.backend.ReleaseLease(ctx, id); err != nil {
			w.opts.Logger.Warn("shutdown lease release failed",
				"task_id", id, "error", err)
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
		// Do not start new claim work once the loop context is canceled
		// (Shutdown or parent cancel). Without this guard a tick that
		// finishes concurrently with cancellation is followed by another
		// full tick: with a canceled poll ctx the claim is rejected and
		// immediately released, then re-claimed and re-released by the
		// extra tick (the ticker and task-notifier branches stay ready
		// and can win the select below over Done). The in-flight tick
		// still completes so its rejection release uses a live context.
		select {
		case <-ctx.Done():
			return
		default:
		}
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
	for _, t := range wtasks {
		// Reserve a slot before dispatch so Claim never over-subscribes and
		// lease extension starts without semaphore wait.
		select {
		case w.wfSem <- struct{}{}:
		default:
			// No slot: make the task visible again promptly for peers.
			// Detached: the poll ctx may be canceled by a concurrent
			// Shutdown, and the task was never tracked (releaseInFlight
			// cannot cover it).
			relCtx, relCancel := w.releaseContext(ctx)
			_ = w.backend.ReleaseLease(relCtx, t.ID)
			relCancel()
			continue
		}
		wg.Add(1)
		go func(t backend.Task) {
			defer wg.Done()
			defer func() { <-w.wfSem }()
			// Lease extension starts immediately after claim (extendLoop
			// runs inside handleActivity; workflows are short so no
			// extension needed here).
			actor := w.actorFor(t.InstanceID)
			actor.dispatch(func() {
				w.opts.Metrics.AddWorkflowTask(ctx, 1)
				w.opts.Logger.Debug("workflow task", "instance_id", t.InstanceID, "task_id", t.ID)
				w.track(t.ID)
				p, herr := w.handleWorkflow(ctx, t)
				w.untrack(t.ID)
				if herr != nil {
					w.recordStoreError(ctx, "commit_workflow", herr,
						"instance_id", t.InstanceID, "task_id", t.ID)
					w.opts.Logger.Debug("workflow task error", "instance_id", t.InstanceID, "err", herr)
					return
				}
				if p != nil {
					pendingMu.Lock()
					pending = append(pending, *p)
					pendingMu.Unlock()
				}
			})
		}(t)
	}
	wg.Wait()
	// Flush with a detached commit ctx so within-grace workflow results are
	// not lost when the poll loop ctx was canceled by Shutdown.
	commitCtx, commitCancel := w.commitContext(ctx)
	w.flushWorkflowCommits(commitCtx, pending)
	commitCancel()
	w.evictIdleInstanceLocks(time.Now())
	w.evictIdleSticky(time.Now())
}

func (w *Worker) tickActivities(ctx context.Context) {
	avail := w.availableSlots(w.actSem)
	if avail <= 0 {
		return
	}
	limit := w.opts.ClaimLimit
	if limit <= 0 || limit > avail {
		limit = avail
	}
	// Capture the execution context before claiming: it identifies this
	// run's generation. If Shutdown's grace expires and the worker restarts
	// before a dispatched goroutine runs, the goroutine must use the
	// captured (canceled) context and abort instead of fetching the new
	// run's live context and executing an already-released task (whose
	// side effects fencing cannot undo).
	execCtx := w.execContext(ctx)
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
			// Detached (see releaseContext): the poll ctx may be canceled
			// by a concurrent Shutdown and the task was never tracked.
			relCtx, relCancel := w.releaseContext(ctx)
			_ = w.backend.ReleaseLease(relCtx, t.ID)
			relCancel()
			continue
		}
		// Lease extension starts in the handler goroutine immediately,
		// before any semaphore wait (slot already reserved), so long
		// activities do not lose their lease while queued.
		w.opts.Metrics.AddActivityTask(ctx, 1)
		w.opts.Logger.Debug("activity task",
			"instance_id", t.InstanceID, "task_id", t.ID, "activity", t.Name, "attempt", t.Attempt)
		w.track(t.ID)
		done, ok := w.trackActivity()
		if !ok {
			w.untrack(t.ID)
			<-w.actSem
			// Detached: Shutdown already canceled the poll ctx and this ID
			// is untracked, so releaseInFlight cannot cover it; a canceled
			// ctx would make context-aware stores reject the release and
			// stall the task until lease expiry.
			relCtx, relCancel := w.releaseContext(ctx)
			_ = w.backend.ReleaseLease(relCtx, t.ID)
			relCancel()
			continue
		}
		go func(t backend.Task, execCtx context.Context) {
			defer done()
			defer func() { <-w.actSem }()
			defer w.untrack(t.ID)
			// Execution outlives the poll loop ctx across Shutdown grace so
			// within-grace completions can still commit (see Shutdown).
			// execCtx is the generation captured before dispatch: a stale
			// claim from a previous run aborts via the canceled context
			// instead of running under the new run.
			if herr := w.handleActivity(execCtx, t); herr != nil {
				w.opts.Logger.Debug("activity task error",
					"instance_id", t.InstanceID, "task_id", t.ID, "err", herr)
			}
		}(t, execCtx)
	}
	// Do not wait: long activities must not block the next tick's timers
	// or workflow progress. Concurrency stays bounded by actSem and Lease
	// expiry reclaims tasks from crashed workers.
}

// tickActivitiesSync is the PollOnce path: claim and run activities to
// completion before returning for deterministic single-threaded tests.
func (w *Worker) tickActivitiesSync(ctx context.Context) {
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
			// Detached (see releaseContext): the poll ctx may be canceled
			// by a concurrent Shutdown and the task was never tracked.
			relCtx, relCancel := w.releaseContext(ctx)
			_ = w.backend.ReleaseLease(relCtx, t.ID)
			relCancel()
			continue
		}
		wg.Add(1)
		w.opts.Metrics.AddActivityTask(ctx, 1)
		w.opts.Logger.Debug("activity task",
			"instance_id", t.InstanceID, "task_id", t.ID, "activity", t.Name, "attempt", t.Attempt)
		w.track(t.ID)
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

func (w *Worker) handleWorkflow(ctx context.Context, t backend.Task) (*pendingWorkflowCommit, error) {
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
		w.attachLocalActivityRunner(wctx)
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
	// Result commits use a bounded detached context created immediately
	// before each result operation. Shutdown cancels the poll loop ctx
	// immediately and the execution ctx after its grace; committing with
	// either canceled ctx (e.g. pgx Begin) fails and a within-grace result
	// would be lost until lease expiry (and untracked, so not even released
	// for peers). The timeout must start after activity execution: creating
	// it here at handler entry would let the default 5s budget expire during
	// a long activity and reject Complete/Retry/fail with an expired ctx.

	// Grace already expired before we started: don't execute, release for a peer.
	if ctx.Err() != nil {
		// Only the in-flight owner releases: Shutdown's releaseInFlight
		// may have already released (and a peer re-claimed) this lease.
		if w.claimReleaseOwnership(t.ID) {
			commitCtx, commitCancel := w.commitContext(ctx)
			_ = w.backend.ReleaseLease(commitCtx, t.ID)
			commitCancel()
		}
		return ctx.Err()
	}

	// committing marks a detached result commit as in flight so lease
	// renewal stays alive until the commit finishes (see
	// beginDetachedCommit and extendLeaseLoop). done still closes at
	// handler return, which is after every commit path below. Renewal
	// starts before the registry lookup so the unregistered-activity
	// early result paths below are covered too: their detached nack
	// would otherwise run without renewal.
	var committing atomic.Bool
	done := make(chan struct{})
	defer close(done)
	// renewDone closes when the renewal loop exits so the shutdown-release
	// path below can JOIN it before releasing (see below): joining
	// guarantees no ExtendLease is in flight that could land after the
	// ReleaseLease and re-hide the task for a full lease (or modify a
	// peer's fresh lease — backend lease ops are keyed by task ID alone).
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		w.extendLeaseLoop(ctx, t.ID, done, &committing)
	}()

	act, err := w.reg.activity(t.Name)
	if err != nil {
		if errors.Is(err, ErrActivityNotRegistered) {
			if !w.beginDetachedCommit(t.ID, &committing) {
				return ctx.Err()
			}
			commitCtx, commitCancel := w.commitContext(ctx)
			rerr := w.nackIncompatible(commitCtx, t, "unregistered_activity", err)
			commitCancel()
			return rerr
		}
		if !w.beginDetachedCommit(t.ID, &committing) {
			return ctx.Err()
		}
		commitCtx, commitCancel := w.commitContext(ctx)
		rerr := w.failActivity(commitCtx, t, err)
		commitCancel()
		return rerr
	}

	// Execution setup: renewal already runs (see above), so every commit
	// path below stays covered.
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
			err := w.backend.RecordHeartbeat(ctx, t.ID, w.opts.LeaseDuration, details)
			if err == nil {
				w.refreshLease(t.ID)
			}
			return err
		},
	})

	out, err := w.invokeActivity(actCtx, act.fn, t.Input)
	// Enter detached-commit renewal mode BEFORE checking for
	// parent-cancel/shutdown-expiry: a cancel landing between the check
	// and committing.Store(true) would let extendLeaseLoop exit while a
	// detached commit below starts without renewal (peer reclaim, then
	// the task-ID-only store op clobbers the peer's task). With the flag
	// set first, either renewal stays alive through the commit or the
	// shutdown check below routes to release — never a commit without
	// renewal. A set flag on the release path only keeps renewal alive
	// until done closes at return.
	committing.Store(true)
	// Shutdown (grace expired) aborted the execution: never consume an
	// attempt or record a timeout failure for a Shutdown-caused cancel.
	// Release with a fresh detached commit ctx so a peer retries promptly.
	if ctx.Err() != nil {
		// Leave detached-commit renewal mode and JOIN the renewal loop
		// before releasing. The flag was set first so a cancel racing the
		// commit entry keeps renewal alive through the commit (see above);
		// on this release path no commit follows, so renewal must stop
		// first: otherwise the loop — already inside renewUntilDone or
		// about to enter it on ctx.Done — issues a detached ExtendLease
		// that lands after the ReleaseLease below, re-hiding the task for
		// a full lease or modifying a peer's fresh lease. The join waits
		// for the loop goroutine to return, and every ExtendLease it
		// issued completes before that return, so none can land after the
		// joined release. Only the in-flight owner releases (see
		// claimReleaseOwnership): releasing a lease Shutdown already
		// handed to a peer would clear the peer's lease and enable
		// duplicate execution.
		committing.Store(false)
		<-renewDone
		if w.claimReleaseOwnership(t.ID) {
			commitCtx, commitCancel := w.commitContext(ctx)
			_ = w.backend.ReleaseLease(commitCtx, t.ID)
			commitCancel()
		}
		return ctx.Err()
	}
	if runCtx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) {
		err = fmt.Errorf("activity start-to-close timeout")
	}
	if err != nil {
		if IsNonRetryable(err) || (t.MaxAttempts > 0 && t.Attempt >= t.MaxAttempts) {
			if !w.beginDetachedCommit(t.ID, &committing) {
				return ctx.Err()
			}
			commitCtx, commitCancel := w.commitContext(ctx)
			rerr := w.failActivity(commitCtx, t, err)
			commitCancel()
			return rerr
		}
		delay := workflow.RetryPolicy{
			InitialInterval:    t.Retry.InitialInterval,
			BackoffCoefficient: t.Retry.BackoffCoefficient,
			MaxInterval:        t.Retry.MaxInterval,
			MaxAttempts:        t.Retry.MaxAttempts,
		}.Backoff(t.Attempt)
		w.opts.Logger.Info("activity retry",
			"instance_id", t.InstanceID, "activity", t.Name, "attempt", t.Attempt, "delay", delay)
		if !w.beginDetachedCommit(t.ID, &committing) {
			return ctx.Err()
		}
		commitCtx, commitCancel := w.commitContext(ctx)
		w.opts.Metrics.AddActivityRetry(commitCtx, 1)
		if rerr := w.backend.RetryActivity(commitCtx, t.ID, delay); rerr != nil {
			w.recordStoreError(commitCtx, "retry_activity", rerr, "instance_id", t.InstanceID, "activity", t.Name)
			commitCancel()
			return rerr
		}
		commitCancel()
		return nil
	}
	if !w.beginDetachedCommit(t.ID, &committing) {
		return ctx.Err()
	}
	commitCtx, commitCancel := w.commitContext(ctx)
	if cerr := w.backend.CompleteActivity(commitCtx, t.ID, journal.Event{
		Type:    journal.TypeActivityCompleted,
		RefSeq:  t.Seq,
		Payload: out,
	}); cerr != nil {
		w.recordStoreError(commitCtx, "complete_activity", cerr, "instance_id", t.InstanceID, "activity", t.Name)
		commitCancel()
		return cerr
	}
	commitCancel()
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

func (w *Worker) attachLocalActivityRunner(wctx *workflow.Context) {
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
		return act.fn(context.Background(), input)
	})
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

func (w *Worker) extendLeaseLoop(ctx context.Context, taskID int64, done <-chan struct{}, committing *atomic.Bool) {
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
			if committing == nil || !committing.Load() {
				return
			}
			// A detached result commit is in flight (see
			// beginDetachedCommit): its commit context outlives this
			// execution context, so keep renewing until the commit
			// finishes (done closes when handleActivity returns).
			// Exiting here would let a commit longer than the remaining
			// lease race a peer reclaim, and the task-ID-only commit
			// would then clobber the peer's task.
			w.renewUntilDone(ctx, taskID, done, ticker, committing)
			return
		case <-ticker.C:
			if err := w.backend.ExtendLease(ctx, taskID, w.opts.LeaseDuration); err != nil {
				w.recordStoreError(ctx, "extend_lease", err, "task_id", taskID)
			} else {
				w.refreshLease(taskID)
			}
		}
	}
}

// renewUntilDone keeps extending taskID with a detached context until done
// closes. It serves a detached result commit that outlives its execution
// context (parent cancel or shutdown-grace expiry mid-commit). Each renewal
// is bounded by the lease duration and the commit itself is bounded by its
// commit context, so this loop always terminates when the commit returns.
//
// committing gates detached mode: the shutdown-release path in
// handleActivity unsets it and joins the renewal loop before releasing, so
// every iteration re-checks it and exits promptly once the release path
// leaves detached mode. Without these checks the loop would only exit on
// done (closed at handler return, i.e. after the release) and a detached
// renewal could land after the ReleaseLease.
func (w *Worker) renewUntilDone(ctx context.Context, taskID int64, done <-chan struct{}, ticker *time.Ticker, committing *atomic.Bool) {
	// Renew immediately on entering detached mode instead of waiting for
	// the next tick: the transition can coincide with a renewal tick that
	// used the now-canceled ctx and failed, and the next tick is a
	// half-lease away (== the original lease expiry for the first
	// renewal). Waiting would let a peer reclaim the still-committing
	// task mid-commit and the task-ID-only commit would then clobber the
	// peer's task.
	select {
	case <-done:
		return
	default:
	}
	if committing == nil || !committing.Load() {
		return
	}
	w.renewOnceDetached(ctx, taskID)
	for {
		if committing == nil || !committing.Load() {
			return
		}
		select {
		case <-done:
			return
		case <-ticker.C:
			select {
			case <-done:
				return
			default:
			}
			if committing == nil || !committing.Load() {
				return
			}
			w.renewOnceDetached(ctx, taskID)
		}
	}
}

// renewOnceDetached extends taskID once with a context detached from
// execution cancellation, bounded by the lease duration.
func (w *Worker) renewOnceDetached(ctx context.Context, taskID int64) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.leaseDuration())
	defer cancel()
	if err := w.backend.ExtendLease(rctx, taskID, w.opts.LeaseDuration); err != nil {
		w.recordStoreError(rctx, "extend_lease", err, "task_id", taskID)
	} else {
		w.refreshLease(taskID)
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
