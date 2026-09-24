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
	// shuttingDown gates restarts across Shutdown: it is set under mu at
	// Shutdown entry (alongside clearing cancel/done) and cleared under mu
	// at Shutdown return, so a StartWithError racing the shutdown window
	// (after the mu clear but before Shutdown's actMu capture) observes it
	// and fails with ErrWorkerShuttingDown instead of installing a new
	// execCancel that Shutdown then captures and cancels at grace expiry
	// (leaving the old execCtx live). Guarded by mu.
	shuttingDown bool
	// inFlight tracks claimed task IDs with their local lease-expiry
	// estimate (claim time + LeaseDuration, refreshed on each successful
	// renewal) plus the claimed task itself. The local expiry is a fast
	// path: once it passes, the lease may have been reclaimed by a peer
	// and local release paths must not fire (see claimReleaseOwnership).
	// The stored task additionally carries the claim token (worker +
	// attempt) so backend ReleaseLease calls stay fenced even when the
	// local estimate has not yet expired. Result commits transfer
	// ownership out of this map before touching the store (see
	// claimCommitOwnership).
	//
	// Entries are stamped with the claiming invocation's token (start
	// epoch + per-claim sequence): a Start-parent cancel followed by a
	// worker restart lets a cancellation-ignoring invocation outlive its
	// lease while the new generation reclaims and tracks the same task
	// ID. The stale invocation must not treat the new entry as its own
	// (delete it and release the new lease, enabling concurrent
	// execution). Every removal honors the token and ignores entries
	// stamped by another invocation.
	inFlight map[int64]inFlightEntry
	// epoch identifies this worker's Start generation. It is incremented
	// on every StartWithError under mu and stamped into each in-flight
	// entry, so a claim from a previous run never matches an entry
	// tracked by the current run (see claimToken).
	epoch uint64
	// claimSeq sources per-claim sequence numbers stamped into each
	// in-flight entry. It is monotonic across restarts, so two
	// invocations of the same generation claiming the same task ID (a
	// reclaim after local expiry with spare concurrency) still hold
	// distinct tokens.
	claimSeq uint64

	// wfClaim records the local wall-clock claim time of each workflow
	// task claimed by tickWorkflows. NackTask is fenced on the claim token
	// (worker + attempt, like ReleaseLease), so a stale delayed nack in
	// requeueWorkflowTask is rejected by the backend without touching a
	// peer's fresh lease. The local lease-expiry estimate (claim time +
	// LeaseDuration) stays as a fast path: once it has passed, a peer may
	// have reclaimed the task, so the stale worker skips the nack call
	// entirely and expiry reclaims naturally. Entries survive untrack
	// (cleared after the tick flush) so the post-commit requeue path can
	// still gate on them.
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
	// execCtx is the execution context for detached activities. It stays
	// valid during Shutdown's grace period (unlike the poll loop ctx which
	// is canceled immediately) so activities finishing within the grace can
	// still commit their results. It is canceled after the grace expires to
	// abort stragglers. Guarded by actMu.
	execCtx    context.Context
	execCancel context.CancelFunc

	// renewMu guards the ordinary-renewal barrier below. A sync.WaitGroup
	// cannot serve as this barrier: a ticker paused before Add while
	// another renewal keeps the counter nonzero lets Shutdown's Wait
	// observe a zero counter first, and the paused ticker's Add then
	// runs concurrently with Wait → panic("sync: WaitGroup misuse"),
	// crashing Shutdown (round-14 P1). The count + stop flag + idle
	// channel + generation are all guarded by this one mutex instead,
	// so registration and the stop-check are atomic and no Add-during-Wait
	// exists by construction.
	renewMu sync.Mutex
	// renewEpoch is the Start generation owning the barrier. It is set
	// to w.epoch on every Start under the same renewMu hold that resets
	// renewStopped, and checked atomically with the stop flag on every
	// admission (see renewTryEnter). A ticker from a previous generation
	// paused between ownsFresh and renewTryEnter across grace expiry +
	// restart must not observe the new generation's reset stop flag and
	// issue an ID-only ExtendLease against the reclaimed task (memory
	// ignores the canceled ctx and extends the new owner's lease):
	// its token epoch no longer matches, so admission is rejected even
	// though the flag is clear for the new generation (round-15 P1).
	// Guarded by renewMu.
	renewEpoch uint64
	// renewInflight counts in-flight ORDINARY lease-renewal store calls
	// (the ticker path in extendLeaseLoop while no detached commit owns
	// the task). Shutdown joins it before releasing leases (see
	// shutdownRenewalJoin): a renewal already issued when the grace
	// expires completes despite the execution-context cancel (backends
	// may ignore cancellation), and without the join it lands after the
	// ReleaseLease — re-hiding the released task for a full lease or
	// extending a peer's fresh lease through the ID-only ExtendLease.
	// Detached-commit renewals (see renewOnceDetached) are not counted:
	// their entries transferred out of inFlight at commit entry, so no
	// shutdown release can land on them. Guarded by renewMu.
	renewInflight int
	// renewStopped, once set at Shutdown grace expiry, stops new
	// ordinary renewals: the ticker path registers and checks this flag
	// atomically with its generation (see renewTryEnter), so a
	// registration after Shutdown's stop always observes the flag and
	// issues nothing. Reset on every Start for the new generation only;
	// stale generations are rejected by the epoch check even though the
	// flag is clear again (round-15 P1). Detached-commit cover renewals
	// ignore it (their commit needs the cover and their entries are out
	// of the release set). Guarded by renewMu.
	renewStopped bool
	// renewIdle is closed when renewInflight drops to zero and replaced
	// with a fresh open channel on the 0→1 transition (see
	// renewTryEnter/renewExit). Shutdown snapshots it under renewMu
	// after setting renewStopped and waits on the snapshot (see
	// shutdownRenewalJoin). Nil only on zero-value Workers never built
	// by NewWorker; the helpers treat nil as already drained.
	// Guarded by renewMu.
	renewIdle chan struct{}
	// renewPerTask tracks admitted ordinary renewals per task ID so a
	// row-preserving result commit joins only its own task's raced
	// renewal (see joinOrdinaryRenewalsForCommit). The global
	// renewInflight/renewIdle barrier above stays for Shutdown, which
	// must join every task; the commit path must not wait on
	// unrelated tasks (round-17 P1): an unrelated slow/stuck admitted
	// ExtendLease — or an old-generation renewal surviving a bounded
	// shutdown — would otherwise keep the global count nonzero (and
	// new unrelated renewals prolong it) while the commit holds its
	// activity slot with no result write. Entries are created on the
	// 0→1 transition and deleted on drain to avoid leaking one entry
	// per historical task ID. Guarded by renewMu.
	renewPerTask map[int64]*renewTaskEntry
	// coverInflight tracks admitted detached-cover renewals per task ID
	// so a row-preserving result commit joins its own task's in-flight
	// cover before writing (round-20 P1b, see
	// joinCoverRenewalsForCommit): without it a cover ExtendLease
	// blocked in a context-ignoring backend across the write lands
	// after it and overwrites what it wrote. Entries are created on
	// the 0→1 transition and deleted on drain, like renewPerTask.
	// Guarded by detMu (cover renewals register under detMu, never mu).
	coverInflight map[int64]*renewTaskEntry
	// detMu guards detGuard. Lock order with mu is mu-then-detMu, taken
	// together only in beginDetachedCommit; renewOnceDetached,
	// guardedDetachedCommit, and dropDetachedGuard take detMu alone and
	// never nest mu inside it. Cancellation of a commit's cover
	// renewals (see detachedGuard.coverCancel) never fires while
	// holding detMu.
	detMu sync.Mutex
	// detGuard records, for each task with a detached result commit in
	// flight, the worker-side lease-continuity deadline (see
	// detachedGuard). Created atomically with the commit transfer in
	// beginDetachedCommit, dropped at handler return.
	detGuard map[int64]detachedGuard

	recoverMu   sync.Mutex
	lastRecover time.Time

	backlogMu   sync.Mutex
	lastBacklog time.Time
}

// renewTaskEntry is one task's share of the ordinary-renewal barrier:
// the count of admitted but not yet returned ExtendLease calls for a
// single task ID, plus the channel closed on drain that that task's
// committer waits on (see joinOrdinaryRenewalsForCommit).
type renewTaskEntry struct {
	count int
	idle  chan struct{}
}

func NewWorker(b backend.Backend, opts WorkerOptions) *Worker {
	opts = opts.withDefaults()
	idle := make(chan struct{})
	close(idle)
	return &Worker{
		backend:       b,
		opts:          opts,
		reg:           newRegistry(opts.Codec),
		inFlight:      map[int64]inFlightEntry{},
		detGuard:      map[int64]detachedGuard{},
		wfClaim:       map[int64]time.Time{},
		sticky:        map[string]stickyEntry{},
		instLock:      map[string]*workflowActor{},
		wfSem:         make(chan struct{}, opts.WorkflowConcurrency),
		actSem:        make(chan struct{}, opts.ActivityConcurrency),
		renewIdle:     idle,
		renewPerTask:  map[int64]*renewTaskEntry{},
		coverInflight: map[int64]*renewTaskEntry{},
	}
}

// inFlightEntry is one tracked claim: the local lease-expiry estimate, the
// claiming invocation's token (see claimToken), and the claimed task itself
// (carrying the worker + attempt claim token for fenced backend releases).
type inFlightEntry struct {
	expiry time.Time
	epoch  uint64
	seq    uint64
	task   backend.Task
}

// claimToken identifies one claim invocation: the worker Start generation
// (epoch) plus a per-claim sequence number. In-flight removals (untrack,
// commit/release ownership, lease refresh) honor the token and ignore
// entries stamped by another invocation, so a stale invocation from a
// previous run — or an earlier claim of the same run that lost its lease
// to a reclaim — cannot delete or release a new generation's entry for
// the same task ID.
type claimToken struct {
	epoch uint64
	seq   uint64
}

// detachedGuard is the worker-side lease-continuity record for one
// detached result commit. No backend offers a conditional (claim-token
// fenced) ExtendLease — every ExtendLease and every result op
// (Complete/Retry/fail) addresses the task by ID alone — so a renewal
// that SUCCEEDS after the lease moved on (expiry + peer reclaim +
// earlier failed renewals) extends the PEER's lease, and the stale
// commit that follows modifies the peer's task. Error handling alone
// cannot catch that: success is the dangerous case.
//
// The guard closes it worker-side. It is seeded at commit entry with
// the entry's local lease-expiry estimate and refreshed on every
// successful detached renewal (see renewOnceDetached), chaining the
// deadline forward while renewals succeed continuously. A success whose
// call started after the deadline means the backend lease may have
// expired and been reclaimed in the gap — this call just extended the
// peer's lease — so the renewal reports lease loss and the commit
// aborts instead of touching the store (success-without-ownership is
// treated as lost, never committed). The local estimate is
// conservative-early (measured from before each store call, like
// trackAt/refreshLeaseAt), so a live lease always verifies: only a
// genuine renewal gap trips the guard.
type detachedGuard struct {
	epoch    uint64
	seq      uint64
	deadline time.Time
	// cancel aborts the in-flight result store op when a detached
	// renewal reports lease loss mid-commit (see cancelDetachedCommit):
	// the commit runs on an independent detached context, so without
	// the cancel a loss observed during the call would still let the
	// ID-only Complete/Retry touch a peer's reclaimed task
	// (round-10 P1b). Stashed by the commit path while its store op
	// runs, cleared after; nil when no commit is active.
	cancel context.CancelFunc
	// coverCtx bounds this commit's cover renewals; coverCancel stops
	// them (round-11 P1b). It is canceled when the result store op
	// completes (see guardedDetachedCommit) so a renewal still blocked
	// in the backend cannot land after the result write and overwrite
	// it — a context-aware backend drops the write — and when the
	// guard is tripped or dropped, so no renewal outlives the commit.
	// Renewals derive their store-call context from it; a loss
	// observed through its cancellation exits quietly (the commit is
	// already over) instead of recording a store error.
	coverCtx    context.Context
	coverCancel context.CancelFunc
	// writing holds cover renewals while a row-preserving result store
	// op runs (round-20 P1b, see guardedDetachedCommit): a cover
	// ExtendLease already blocked in a context-ignoring backend when
	// the write completes ignores the post-write cover cancel and
	// lands after it, overwriting what it wrote (the retry delay, the
	// nack's visible_at) — and the bounded post-commit join
	// (joinCommitStop) gives up instead of ordering it. Set before the
	// pre-write cover join and cleared with the guard after the op;
	// renewOnceDetached skips issuing (without tripping) while set, so
	// no cover renewal can overlap the write.
	writing bool
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
	if w.shuttingDown {
		return ErrWorkerShuttingDown
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
	// A new Start generation: stamp subsequent claims with a fresh epoch
	// so invocations from a previous run (still holding cancellation-
	// ignoring activities) never match entries tracked by this run.
	w.epoch++
	w.actMu.Lock()
	w.stopping = false
	w.renewMu.Lock()
	// Re-arm admissions for the NEW generation only: the epoch moves
	// with the stop reset under the same hold, so a stale ticker paused
	// across grace expiry + restart never observes a clear flag for its
	// own generation — its renewTryEnter epoch check rejects it even
	// though the flag is clear for the new generation (round-15 P1).
	// The stop state a stale generation could observe is therefore never
	// reset; only the new generation's barrier state is.
	w.renewEpoch = w.epoch
	w.renewStopped = false
	// Keep the in-flight count and idle channel as-is: a previous
	// Shutdown that timed out on its release budget may still have
	// renewals outstanding, and they still own the open idle channel
	// they will close on exit.
	w.renewMu.Unlock()
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
	if w.shuttingDown {
		// A concurrent Shutdown is already in progress; the in-flight
		// call owns the grace, renewal join, and lease release.
		w.mu.Unlock()
		return nil
	}
	cancel := w.cancel
	done := w.done
	w.cancel = nil
	w.done = nil
	if cancel == nil && done == nil {
		w.mu.Unlock()
		return nil
	}
	w.shuttingDown = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.shuttingDown = false
		w.mu.Unlock()
	}()
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
	// Join ordinary lease renewal before releasing (round-9 P1a): a
	// renewal already issued when the grace expired completes despite the
	// cancel above and would otherwise land after the ReleaseLease below
	// (see shutdownRenewalJoin).
	w.shutdownRenewalJoin(relCtx)
	w.releaseInFlight(relCtx)
	return waitErr
}

// shutdownRenewalJoin stops new ordinary lease renewals and waits for
// in-flight renewal store calls to settle before Shutdown releases
// leases. Grace expiry while extendLeaseLoop is inside ExtendLease
// leaves a call that completes despite the execution-context cancel;
// releasing first lets that renewal land after the ReleaseLease,
// re-hiding the released task for a full lease (or extending a peer's
// fresh lease through the ID-only ExtendLease). The join guarantees
// every such call completed before any release is issued.
//
// Ordering with the ticker path: registration, the generation check,
// and the stop-check happen atomically under renewMu (see
// renewTryEnter), while Shutdown sets renewStopped and snapshots the
// idle channel under the same renewMu hold. A registration before the
// stop is counted and its idle channel waited for; a registration after
// always observes the flag and issues nothing — so no renewal can slip
// past the join in either direction, and no Add-during-Wait can panic
// by construction (round-14 P1). A registration from a previous Start
// generation is additionally rejected by the epoch check even when the
// flag is clear again after a restart (round-15 P1): the reset only
// re-arms the new generation. The wait is bounded by ctx (the release
// budget): on timeout the release below is skipped and leases expire
// naturally, which is safe but slower.
//
// Only ordinary renewals participate: detached-commit cover renewals
// belong to entries already transferred out of inFlight, so the release
// below cannot land on them, and their commits need the cover.
func (w *Worker) shutdownRenewalJoin(ctx context.Context) {
	w.renewMu.Lock()
	w.renewStopped = true
	idle := w.renewIdle
	if w.renewInflight == 0 || idle == nil {
		w.renewMu.Unlock()
		return
	}
	w.renewMu.Unlock()
	select {
	case <-idle:
	case <-ctx.Done():
	}
}

// renewTryEnter registers one ordinary renewal for taskID with the
// shutdown join and the per-task commit join. Registration, the
// generation check, and the stop-check are atomic
// under renewMu: false means Shutdown already passed grace expiry, or
// the caller's claim belongs to a previous Start generation, and the
// caller must issue nothing; true means the caller holds one in-flight
// slot (globally and for taskID) and must call renewExit(taskID) once
// its ExtendLease call returns. On the global 0→1 transition a fresh
// idle channel is installed for Shutdown to wait on, and on the
// per-task 0→1 transition a fresh per-task channel is installed for
// that task's committer to wait on (see
// joinOrdinaryRenewalsForCommit). The token's epoch is compared against the barrier's Start
// generation (see renewEpoch): a stale ticker paused between ownsFresh
// and admission across grace expiry + restart is rejected even though
// the stop flag was reset for the new generation, so its ID-only
// ExtendLease can never land on the reclaimed task (round-15 P1).
func (w *Worker) renewTryEnter(taskID int64, tok claimToken) bool {
	w.renewMu.Lock()
	defer w.renewMu.Unlock()
	if w.renewStopped {
		return false
	}
	if tok.epoch != w.renewEpoch {
		return false
	}
	if w.renewInflight == 0 {
		w.renewIdle = make(chan struct{})
	}
	w.renewInflight++
	if w.renewPerTask == nil {
		w.renewPerTask = map[int64]*renewTaskEntry{}
	}
	e := w.renewPerTask[taskID]
	if e == nil {
		e = &renewTaskEntry{idle: make(chan struct{})}
		w.renewPerTask[taskID] = e
	}
	e.count++
	return true
}

// renewExit releases one slot claimed by renewTryEnter(taskID), closing
// the global idle channel when the last renewal drains so a waiting
// shutdownRenewalJoin wakes, and closing + deleting the task's per-task
// entry when that task's last renewal drains so a waiting
// joinOrdinaryRenewalsForCommit wakes. A nil idle (zero-value Worker)
// is never closed; a missing per-task entry (zero-value Worker, or an
// exit for a task with nothing in flight) is a no-op.
func (w *Worker) renewExit(taskID int64) {
	w.renewMu.Lock()
	defer w.renewMu.Unlock()
	if w.renewInflight > 0 {
		w.renewInflight--
		if w.renewInflight == 0 && w.renewIdle != nil {
			close(w.renewIdle)
		}
	}
	e := w.renewPerTask[taskID]
	if e == nil || e.count <= 0 {
		return
	}
	e.count--
	if e.count == 0 {
		close(e.idle)
		delete(w.renewPerTask, taskID)
	}
}

// commitJoinCap bounds the per-task ordinary-renewal join below. The
// commit's own context already bounds it (CommitTimeout normally,
// ShutdownReleaseTimeout once Shutdown began), but with a long commit
// timeout a same-task stuck renewal would otherwise hold the activity
// slot for the whole timeout with no result write. The cap keeps that
// hold predictable; a renewal settling within the cap still orders
// correctly, and a join that gives up aborts the commit (errLeaseLost)
// instead of running a write the stuck renewal could then overwrite.
// The same cap also bounds the deferred teardown (see joinCommitStop):
// the pre-commit abort does not unstick a context-ignoring backend
// call, so the post-commit join must give up on its own or the slot
// stays held despite this cap.
const commitJoinCap = 5 * time.Second

// joinOrdinaryRenewalsForCommit waits for admitted ordinary renewals
// for taskID — and only taskID — to settle before a row-preserving
// result write (round-16 P1, scoped per task in round-17 P1). An
// ordinary ticker renewal admitted via renewTryEnter just before
// beginDetachedCommit flips committing uses the execution context —
// not the commit's cover context — so the post-commit cover cancel
// cannot stop it: it stays blocked while the sync pre-commit renewal
// succeeds and RetryActivity (or a nack) completes, then lands after
// the result write and overwrites what it wrote (the retry delay, the
// nack's visible_at). The deferred joinCommitStop only joins after the
// write, which is too late.
//
// Waiting here orders the write after the admitted renewal: the renewal
// lands first and the result overwrites it. New ordinary renewals for
// this task cannot start during the wait — the ticker takes the
// detached branch once committing is set — so the snapshot covers the
// raced call. Renewals for OTHER task IDs are irrelevant to this
// task's ID-only write and are never waited on: the pre-fix global
// join parked the commit until every task's renewals drained, so one
// unrelated slow/stuck admitted ExtendLease (or an old-generation
// renewal surviving a bounded shutdown, or a stream of new unrelated
// renewals) held this commit's activity slot indefinitely with no
// result write and no bound from the commit context. Unlike
// shutdownRenewalJoin this sets no stop flag: ordinary renewal stays
// armed for every task that never commits. Callers re-gate the
// detached guard after the wait (the guard may have tripped and the
// continuity deadline may have passed while waiting) and run the
// store op only when the gate still holds. detMu is never held across
// the wait, so cover renewals keep the commit covered meanwhile.
//
// The wait is bounded by ctx (the commit's store-call context) and
// commitJoinCap: it reports false when either fires, and the caller
// aborts the commit with errLeaseLost instead of running a write a
// still-blocked renewal could overwrite. A nil ctx waits up to the
// cap.
func (w *Worker) joinOrdinaryRenewalsForCommit(taskID int64, ctx context.Context) bool {
	w.renewMu.Lock()
	e := w.renewPerTask[taskID]
	var idle chan struct{}
	var n int
	if e != nil {
		n = e.count
		idle = e.idle
	}
	w.renewMu.Unlock()
	if n == 0 || idle == nil {
		return true
	}
	timer := time.NewTimer(commitJoinCap)
	defer timer.Stop()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-idle:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

// joinCoverRenewalsForCommit waits for admitted detached-cover renewals
// for taskID — and only taskID — to settle before a row-preserving
// result write (round-20 P1b). Cover renewals run on the commit's cover
// context, so the post-write cover cancel cannot stop one already
// blocked in a context-ignoring backend: it lands after the write and
// overwrites what it wrote (the retry delay, the nack's visible_at),
// and the bounded post-commit join (joinCommitStop) gives up instead of
// ordering it. Waiting here orders the write after the admitted cover:
// the renewal lands first and the result overwrites it.
//
// The caller sets the guard's writing hold BEFORE this join (see
// guardedDetachedCommit), so the snapshot covers the raced call: a
// renewal registering after the hold skips issuing, and one registered
// before it is counted here. detMu is never held across the wait, so
// cover keeps the commit covered meanwhile.
//
// The wait is bounded by ctx (the commit's store-call context) and
// commitJoinCap: it reports false when either fires, and the caller
// aborts the commit with errLeaseLost instead of running a write a
// still-blocked renewal could overwrite. A missing or superseded guard
// also reports false (the commit must abort anyway). A nil ctx waits up
// to the cap.
func (w *Worker) joinCoverRenewalsForCommit(taskID int64, tok claimToken, ctx context.Context) bool {
	w.detMu.Lock()
	g, ok := w.detGuard[taskID]
	if !ok || g.epoch != tok.epoch || g.seq != tok.seq {
		w.detMu.Unlock()
		return false
	}
	e := w.coverInflight[taskID]
	var idle chan struct{}
	var n int
	if e != nil {
		n = e.count
		idle = e.idle
	}
	w.detMu.Unlock()
	if n == 0 || idle == nil {
		return true
	}
	timer := time.NewTimer(commitJoinCap)
	defer timer.Stop()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-idle:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

// coverRenewExit releases one cover slot claimed in renewOnceDetached,
// closing + deleting the task's entry when that task's last cover
// drains so a waiting joinCoverRenewalsForCommit wakes. A missing entry
// (guard dropped with no cover in flight) is a no-op.
func (w *Worker) coverRenewExit(taskID int64) {
	w.detMu.Lock()
	defer w.detMu.Unlock()
	e := w.coverInflight[taskID]
	if e == nil || e.count <= 0 {
		return
	}
	e.count--
	if e.count == 0 {
		close(e.idle)
		delete(w.coverInflight, taskID)
	}
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

func (w *Worker) track(taskID int64) claimToken {
	return w.trackAt(taskID, time.Now())
}

// trackAt registers taskID with a lease expiry measured from at, the
// instant BEFORE the claim store call that produced the task. Backends
// stamp the visible lease during the claim transaction, so measuring from
// after the call returns extends the local estimate past the actual lease
// by the call latency; a peer reclaiming inside that gap would still look
// invalid locally, and a subsequent owner-gated release would clear the
// peer's fresh lease. Measuring from before the call can only expire the
// local estimate early (the task then becomes reclaimable via expiry),
// never late.
func (w *Worker) trackAt(taskID int64, at time.Time) claimToken {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.claimSeq++
	tok := claimToken{epoch: w.epoch, seq: w.claimSeq}
	w.inFlight[taskID] = inFlightEntry{expiry: at.Add(w.leaseDuration()), epoch: tok.epoch, seq: tok.seq}
	return tok
}

// trackTaskAt is trackAt for production claim sites: it additionally stamps
// the claimed task into the entry so shutdown and no-slot release paths can
// issue fenced ReleaseLease calls carrying the claim token (worker +
// attempt). trackAt (without the task) stays for tests and paths where only
// the lease-expiry estimate matters; unfenced Task{ID} fallbacks still
// release correctly since backend fencing predicates skip empty fields.
func (w *Worker) trackTaskAt(t backend.Task, at time.Time) claimToken {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.claimSeq++
	tok := claimToken{epoch: w.epoch, seq: w.claimSeq}
	w.inFlight[t.ID] = inFlightEntry{expiry: at.Add(w.leaseDuration()), epoch: tok.epoch, seq: tok.seq, task: t}
	return tok
}

func (w *Worker) untrack(taskID int64, tok claimToken) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e, ok := w.inFlight[taskID]; ok && e.epoch == tok.epoch && e.seq == tok.seq {
		delete(w.inFlight, taskID)
	}
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
// renewal (ExtendLease/RecordHeartbeat). Unknown IDs — and entries stamped
// by another invocation — are ignored: the task already transferred to a
// commit, was released, or was reclaimed and re-tracked by a new
// generation, whose own renewal maintains its expiry.
func (w *Worker) refreshLease(taskID int64, tok claimToken) {
	w.refreshLeaseAt(taskID, tok, time.Now())
}

// refreshLeaseAt pushes a task's local lease expiry forward after a
// successful renewal (ExtendLease/RecordHeartbeat), measuring from at, the
// instant BEFORE the renewal store call. Like trackAt, this keeps the
// local estimate conservative: measuring from after the call would extend
// it past the backend-stamped lease by the call latency. Unknown IDs — and
// entries stamped by another invocation — are ignored: the task already
// transferred to a commit, was released, or was reclaimed and re-tracked
// by a new generation, whose own renewal maintains its expiry.
//
// The update is monotonic (max-assignment): heartbeat and periodic renewal
// overlap, both stamp their start pre-call, and out-of-order returns would
// otherwise let the older start overwrite a newer deadline — ownsFresh
// then stops early (or the detached guard rejects a valid result) while
// the backend lease is still live, delaying re-execution and risking
// duplicate side effects. Only a LATER expiry replaces the current one.
func (w *Worker) refreshLeaseAt(taskID int64, tok claimToken, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e, ok := w.inFlight[taskID]; ok && e.epoch == tok.epoch && e.seq == tok.seq {
		if newExpiry := at.Add(w.leaseDuration()); newExpiry.After(e.expiry) {
			w.inFlight[taskID] = inFlightEntry{expiry: newExpiry, epoch: e.epoch, seq: e.seq, task: e.task}
		}
	}
}

// owns reports whether tok still stamps the in-flight entry for taskID: a
// stale invocation whose entry was overwritten by a reclaim (same task ID,
// new generation or new claim) must stop renewing and must not remove or
// release the entry.
func (w *Worker) owns(taskID int64, tok claimToken) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.inFlight[taskID]
	return ok && e.epoch == tok.epoch && e.seq == tok.seq
}

// ownsFresh reports whether tok still stamps the in-flight entry for
// taskID AND the local lease estimate has not yet expired. The ordinary
// renewal path must gate on both: past local expiry a peer may have
// reclaimed the task, and an ID-only ExtendLease would then extend the
// peer's lease — hiding the peer's task for a full lease while masking
// our own ownership loss. An unfresh lease is left for natural expiry
// reclaim instead of being renewed (round-9 P1b).
func (w *Worker) ownsFresh(taskID int64, tok claimToken) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.inFlight[taskID]
	if !ok || e.epoch != tok.epoch || e.seq != tok.seq {
		return false
	}
	return time.Now().Before(e.expiry)
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
//
// The token gates the transfer: a stale invocation (previous Start
// generation, or an earlier claim that lost its lease to a reclaim) whose
// task ID was re-tracked by a new invocation must not steal the new
// entry — that would untrack the new generation's claim and let a later
// release hand its task to a peer while it still runs.
func (w *Worker) claimCommitOwnership(taskID int64, tok claimToken) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.inFlight[taskID]
	if !ok || e.epoch != tok.epoch || e.seq != tok.seq {
		return false
	}
	delete(w.inFlight, taskID)
	return true
}

// beginDetachedCommit atomically verifies commit ownership, enters
// detached-commit renewal mode, and transfers taskID out of the in-flight
// set (see claimCommitOwnership), reporting whether the backend result
// commit may proceed. False means Shutdown already released the lease to a
// peer: the caller must skip its task-ID-only store op and return
// (ctx.Err() preserves shutdown/cancel visibility for logging) to avoid
// clobbering the new owner. Call it immediately before creating the
// detached commit context.
//
// Ownership, detached-mode entry, and the transfer happen under a single
// mutex hold, in that order. The renewal loop exits only when it observes
// neither ownership (see owns) nor detached mode, and releaseInFlight
// collects under this same mutex, so exactly one side wins: either the
// release wins — the entry is still present and the flag was never set, so
// no detached renewal can land after the release — or this transfer wins —
// the entry is gone, so no release can land during the commit. Entering
// detached mode only after the transfer (or setting the flag before
// checking ownership) admits a cancel/renewal-tick in the gap where the
// loop exits while a detached commit still runs; the commit then outlives
// the lease, a peer reclaims, and the task-ID-only store op modifies the
// peer's task. A parent cancel or shutdown-grace expiry racing the commit
// must not stop renewal mid-commit either: with the flag set first, either
// renewal stays alive through the commit or the shutdown check routes to
// release — never a commit without renewal.
func (w *Worker) beginDetachedCommit(taskID int64, tok claimToken, committing *atomic.Bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.inFlight[taskID]
	if !ok || e.epoch != tok.epoch || e.seq != tok.seq {
		w.opts.Logger.Debug("skipping stale activity commit; lease already released",
			"task_id", taskID)
		return false
	}
	// Seed the detached-renewal continuity guard (see detachedGuard)
	// from the entry's local lease-expiry estimate. detMu nests inside
	// mu here — the only place both are held — while guard readers take
	// detMu alone, so the order never inverts. The guard is stored BEFORE
	// detached mode is entered below: a renewal loop that observes the
	// flag is guaranteed to observe the guard, so it can never mistake a
	// covered commit for an unowned task and skip its renewal.
	w.detMu.Lock()
	defer w.detMu.Unlock()
	if w.detGuard == nil {
		w.detGuard = map[int64]detachedGuard{}
	}
	// Cover renewals for this commit live on an independent cover
	// context (see detachedGuard): it outlives execution cancellation
	// like the commit context, but the commit cancels it the moment
	// its store op completes so no renewal lands after the result
	// write (round-11 P1b). The deferred guard drop at handler return
	// cancels it on every path that never commits.
	coverCtx, coverCancel := context.WithCancel(context.Background())
	w.detGuard[taskID] = detachedGuard{epoch: tok.epoch, seq: tok.seq, deadline: e.expiry, coverCtx: coverCtx, coverCancel: coverCancel}
	committing.Store(true)
	delete(w.inFlight, taskID)
	return true
}

// dropDetachedGuard removes taskID's continuity guard when the token
// matches: the detached commit finished (committed, aborted, or
// released) and no further renewal may extend the lease. A stale
// invocation must not drop a newer generation's guard for the same
// task ID. Called at handler return; a renewal loop still in flight
// past that point finds no guard and — with the entry gone too —
// reports lease loss instead of extending a possibly-moved-on lease.
func (w *Worker) dropDetachedGuard(taskID int64, tok claimToken) {
	w.detMu.Lock()
	var coverCancel context.CancelFunc
	if g, ok := w.detGuard[taskID]; ok && g.epoch == tok.epoch && g.seq == tok.seq {
		coverCancel = g.coverCancel
		delete(w.detGuard, taskID)
	}
	w.detMu.Unlock()
	// Stop cover renewals on every non-commit path (pre-commit abort,
	// lost ownership, handler return): with the guard gone no renewal
	// may extend the lease, and an in-flight cover call must not land
	// after whatever follows. The post-commit cleanup cancels first;
	// this is the backstop (cancel funcs are idempotent).
	if coverCancel != nil {
		coverCancel()
	}
}

// tripDetachedGuard drops taskID's continuity guard after a lease-loss
// observation and aborts the in-flight result store op, if any. The
// commit runs on an independent detached context precisely so execution
// cancellation does not kill it, but a loss observed DURING the store
// call must still stop the ID-only Complete/Retry before it touches a
// peer's reclaimed task (round-10 P1b): the stashed commit cancel (see
// guardedDetachedCommit) is captured under the same lock hold as the
// deletion, so a concurrent commit gate cannot stash a new cancel in
// between, and fired after unlocking. Context cancellation is
// best-effort — a backend that ignores it still applies the op — so the
// pre-op gate remains the primary defense; the cancel covers the
// gate→op window and long-blocked calls. The token check keeps a stale
// generation from tripping or canceling its successor's commit.
//
// Tripping also cancels the commit's cover renewals (round-11 P1a):
// with continuity lost the commit is aborting, so in-flight cover
// backend calls stop instead of extending a moved-on lease, and the
// loops exit on the resulting error. Cancel funcs are idempotent, so a
// concurrent post-commit cancel cannot double-fire.
func (w *Worker) tripDetachedGuard(taskID int64, tok claimToken) {
	w.detMu.Lock()
	var cancel context.CancelFunc
	var coverCancel context.CancelFunc
	if g, ok := w.detGuard[taskID]; ok && g.epoch == tok.epoch && g.seq == tok.seq {
		cancel = g.cancel
		coverCancel = g.coverCancel
		delete(w.detGuard, taskID)
	}
	w.detMu.Unlock()
	if coverCancel != nil {
		coverCancel()
	}
	if cancel != nil {
		cancel()
	}
}

// guardedDetachedCommit runs op — one detached result store op
// (Complete/Retry/fail/nack) — after re-verifying the lease still holds,
// and drops the continuity guard once the op returns so no renewal can
// start after the commit (round-10 P1a).
//
// The gate closes the pre-commit-check gap (round-10 P1b): the
// synchronous pre-commit renewal in ensureCommitRenewal proves the
// lease only at its instant, while a periodic cover renewal may have
// reported lease loss (tripping and dropping the guard) any time up to
// the store call. Gating immediately before executing rechecks,
// including the continuity deadline itself — an op delayed past it may
// already have lost the backend lease to a peer even with the guard
// still present — and skips the ID-only store op when the lease moved
// on, instead of modifying a peer's task on an independent commit
// context. A missing or superseded guard, or a stale deadline, reports
// errLeaseLost and issues nothing. The stale-deadline drop also cancels
// this commit's cover context (round-15 P2): a cover ExtendLease already
// blocked in the backend must not stay live past the rejection and land
// by ID on the peer-reclaimed task, and the deferred joinCommitStop must
// not hold its slot until the lease-bounded renewal timeout.
//
// The result write is coordinated against cover renewals for
// row-preserving writes — RetryActivity and nack, which rewrite
// visible_at in place (round-11 P1b, hardened round-20 P1b): detMu is
// released before the store call, so without coordination a periodic
// renewal blocked in the backend across the commit would land after
// the result write and overwrite what it wrote (the retry delay, the
// nack's visible_at), which the post-commit join cannot undo — the
// post-write cover cancel is best-effort (a context-ignoring backend
// lands the renewal anyway) and the bounded post-commit join gives up
// instead of ordering it. The commit therefore sets the guard's
// writing hold BEFORE joining admitted cover renewals for this task
// (see joinCoverRenewalsForCommit) and re-gates after the wait: the
// hold stops new cover from issuing, the join orders the write after
// the raced call, and a join that gives up aborts with errLeaseLost
// instead of writing under a live renewal. Once the store op returns,
// the guard is dropped AND this commit's cover context is canceled:
// with the hold+join no cover call is in flight or issuing, so the
// drop+cancel only stops the loops (a renewal aborted this way exits
// quietly — the commit is already over — rather than recording a
// store error). Row-deleting writes (Complete/fail) skip the hold,
// the join, and the cover cancel: a renewal landing after them finds
// no row, while one running during a blocked Complete must still
// observe loss and cancel it mid-call (round-10 P1b).
//
// An ordinary (non-cover) renewal admitted just before the commit
// transfer needs the same ordering on the other side of the write
// (round-16 P1): it runs on the execution context, so the cover cancel
// above cannot stop it, and the post-commit joinCommitStop waits only
// after the write. Row-preserving commits therefore join admitted
// ordinary renewals BEFORE the store op (see
// joinOrdinaryRenewalsForCommit) and re-gate the guard after the wait;
// row-deleting writes skip the join (a late renewal finds no row).
//
// While the op runs, its commit-context cancel is stashed in the guard
// (see tripDetachedGuard) so a loss observed mid-call still aborts a
// context-aware backend op; it is cleared before returning, so a later
// trip cannot cancel an unrelated context.
//
// exclusive distinguishes row-deleting commits (Complete/fail) from
// row-preserving ones (RetryActivity/nack, see above).
//
// commitCtx bounds the pre-write ordinary-renewal join (see
// joinOrdinaryRenewalsForCommit): with a per-task join plus this bound
// an unrelated stuck renewal never blocks the commit, and even a
// same-task stuck renewal gives up and aborts with errLeaseLost
// instead of holding the activity slot indefinitely (round-17 P1).
func (w *Worker) guardedDetachedCommit(taskID int64, tok claimToken, commitCtx context.Context, commitCancel context.CancelFunc, exclusive bool, op func() error) error {
	w.detMu.Lock()
	g, ok := w.detGuard[taskID]
	if !ok || g.epoch != tok.epoch || g.seq != tok.seq {
		w.detMu.Unlock()
		w.opts.Logger.Debug("skipping detached commit; lease lost since the pre-commit renewal",
			"task_id", taskID)
		return fmt.Errorf("%w: detached commit gate found lease lost", errLeaseLost)
	}
	if !time.Now().Before(g.deadline) {
		// Continuity lost before the store op: drop the guard AND
		// cancel this commit's cover renewals (round-15 P2). A cover
		// ExtendLease already blocked in the backend would otherwise
		// stay live and land by ID on the peer-reclaimed task, and the
		// deferred joinCommitStop would hold its slot until the
		// lease-bounded renewal timeout even though the result is
		// rejected here. Same cleanup as tripDetachedGuard (cover
		// cancel fired after unlocking; detMu is never held across a
		// cancel); no commit op is running yet so the stashed commit
		// cancel is nil.
		coverCancel := g.coverCancel
		delete(w.detGuard, taskID)
		w.detMu.Unlock()
		if coverCancel != nil {
			coverCancel()
		}
		w.opts.Logger.Debug("skipping detached commit; continuity deadline passed before the store op",
			"task_id", taskID)
		return fmt.Errorf("%w: detached commit gate found lease expired", errLeaseLost)
	}
	if !exclusive {
		g.cancel = commitCancel
		w.detGuard[taskID] = g
		w.detMu.Unlock()
	} else {
		// Row-preserving write: join admitted ordinary renewals for
		// this task BEFORE the store op (round-16 P1, scoped per task
		// in round-17 P1, see joinOrdinaryRenewalsForCommit), then
		// re-gate. detMu is released across the wait so cover keeps
		// the commit covered; the wait took time, so the guard may
		// have tripped and the continuity deadline may have passed —
		// the op runs only when the gate still holds, with the
		// same rejection cleanup as the initial gate. A bounded join
		// that gives up (unrelated renewals never block it; a
		// same-task stuck renewal or a canceled commit context ends
		// it) aborts without running the op: a still-blocked renewal
		// landing after the write would overwrite it, while landing
		// after an abort only extends the lease.
		w.detMu.Unlock()
		if !w.joinOrdinaryRenewalsForCommit(taskID, commitCtx) {
			w.detMu.Lock()
			var coverCancel context.CancelFunc
			if g, ok := w.detGuard[taskID]; ok && g.epoch == tok.epoch && g.seq == tok.seq {
				coverCancel = g.coverCancel
				delete(w.detGuard, taskID)
			}
			w.detMu.Unlock()
			if coverCancel != nil {
				coverCancel()
			}
			w.opts.Logger.Debug("skipping detached commit; ordinary-renewal join timed out",
				"task_id", taskID)
			return fmt.Errorf("%w: detached commit gate timed out joining ordinary renewals", errLeaseLost)
		}
		w.detMu.Lock()
		g, ok = w.detGuard[taskID]
		if !ok || g.epoch != tok.epoch || g.seq != tok.seq {
			w.detMu.Unlock()
			w.opts.Logger.Debug("skipping detached commit; lease lost while joining ordinary renewals",
				"task_id", taskID)
			return fmt.Errorf("%w: detached commit gate found lease lost", errLeaseLost)
		}
		if !time.Now().Before(g.deadline) {
			coverCancel := g.coverCancel
			delete(w.detGuard, taskID)
			w.detMu.Unlock()
			if coverCancel != nil {
				coverCancel()
			}
			w.opts.Logger.Debug("skipping detached commit; continuity deadline passed while joining ordinary renewals",
				"task_id", taskID)
			return fmt.Errorf("%w: detached commit gate found lease expired", errLeaseLost)
		}
		// Hold cover from issuing, then join in-flight cover BEFORE
		// the store op (round-20 P1b, see joinCoverRenewalsForCommit).
		// The hold is set under detMu before the join's snapshot, so
		// the snapshot covers the raced call: a cover renewal that
		// registered before the hold is counted and joined, while one
		// arriving after it skips issuing (see renewOnceDetached) —
		// no cover call can overlap the write below. detMu is released
		// across the wait; the wait took time, so re-gate after it
		// like above. A bounded join that gives up (a same-task stuck
		// cover renewal or a canceled commit context ends it) aborts
		// without running the op, with the same drop+cancel cleanup:
		// a still-blocked cover renewal landing after the write would
		// overwrite it, while landing after an abort only extends the
		// lease.
		g.writing = true
		w.detGuard[taskID] = g
		w.detMu.Unlock()
		if !w.joinCoverRenewalsForCommit(taskID, tok, commitCtx) {
			w.detMu.Lock()
			var coverCancel context.CancelFunc
			if g, ok := w.detGuard[taskID]; ok && g.epoch == tok.epoch && g.seq == tok.seq {
				coverCancel = g.coverCancel
				delete(w.detGuard, taskID)
			}
			w.detMu.Unlock()
			if coverCancel != nil {
				coverCancel()
			}
			w.opts.Logger.Debug("skipping detached commit; cover-renewal join timed out",
				"task_id", taskID)
			return fmt.Errorf("%w: detached commit gate timed out joining cover renewals", errLeaseLost)
		}
		w.detMu.Lock()
		g, ok = w.detGuard[taskID]
		if !ok || g.epoch != tok.epoch || g.seq != tok.seq {
			w.detMu.Unlock()
			w.opts.Logger.Debug("skipping detached commit; lease lost while joining cover renewals",
				"task_id", taskID)
			return fmt.Errorf("%w: detached commit gate found lease lost", errLeaseLost)
		}
		if !time.Now().Before(g.deadline) {
			coverCancel := g.coverCancel
			delete(w.detGuard, taskID)
			w.detMu.Unlock()
			if coverCancel != nil {
				coverCancel()
			}
			w.opts.Logger.Debug("skipping detached commit; continuity deadline passed while joining cover renewals",
				"task_id", taskID)
			return fmt.Errorf("%w: detached commit gate found lease expired", errLeaseLost)
		}
		g.cancel = commitCancel
		w.detGuard[taskID] = g
		w.detMu.Unlock()
	}
	err := op()
	// Clear the stashed cancel and drop the guard: with the store op
	// done, further cover would only overwrite what the commit just
	// wrote (a RetryActivity delay, a nack's visible_at). The renewal
	// loops observe the missing guard and exit without issuing (the
	// entry is gone too), and the handler's deferred guard drop becomes
	// a no-op. For row-preserving writes also cancel the cover context:
	// the pre-write hold+join (see above) means no cover call is in
	// flight or issuing, so this only stops the loops promptly — the
	// backstop for a context-aware backend, and a no-op for one that
	// already settled.
	w.detMu.Lock()
	var coverCancel context.CancelFunc
	if g, ok := w.detGuard[taskID]; ok && g.epoch == tok.epoch && g.seq == tok.seq {
		g.cancel = nil
		coverCancel = g.coverCancel
		delete(w.detGuard, taskID)
	}
	w.detMu.Unlock()
	if exclusive && coverCancel != nil {
		coverCancel()
	}
	return err
}

// waitRenewDoneBounded joins a renewal loop's renewDone channel with the
// same commitJoinCap bound as the teardown path (round-19 P1). A renewal
// stuck in a context-ignoring backend holds renewDone open past grace
// expiry (the ordinary ExtendLease runs on the execution context, which
// such a backend ignores), so an unconditional wait would hold the
// activity slot forever: the handler would never actWg.Done nor release
// its semaphore slot, and with ActivityConcurrency==1 a restarted worker
// could not execute activities. Giving up releases the slot — but the
// stuck renewal is still live, so the caller must NOT proceed to a
// lease release (round-20 P1a, see the cancellation path in
// handleActivity): a release issued now lands before the stuck renewal,
// which then re-hides the task or extends a peer's fresh lease. The
// caller drops the guard and untracks without releasing, leaving the
// task to natural expiry. Reports true when the loop exited, false on
// give-up.
func waitRenewDoneBounded(renewDone <-chan struct{}) bool {
	if renewDone == nil {
		return true
	}
	timer := time.NewTimer(commitJoinCap)
	defer timer.Stop()
	select {
	case <-renewDone:
		return true
	case <-timer.C:
		return false
	}
}

// joinCommitStop builds the commit-scoped teardown for a stop func from
// ensureCommitRenewal so the deferred stop also terminates AND joins
// the inherited renewal loop before the handler drops its guard
// (round-10 P1a): it runs the scoped teardown, signals the inherited
// loop to exit by closing done, and waits for the loop's return —
// every ExtendLease the loop issued completes before that return, so
// none can land after the commit's store op and overwrite what it
// wrote (a RetryActivity delay, a nack's visible_at). The close and
// the wait run exactly once, so double-deferred stops cannot panic on
// a second channel close; a nil or already-closed renewDone (scoped
// replacement case) makes the wait return immediately.
//
// Callers must defer the CALL — `defer joinCommitStop(...)()` — not
// the constructor: `defer joinCommitStop(...)` only defers building
// the closure and discards it at return, so the loop is never joined
// (round-18 P1a). A direct `stop := joinCommitStop(...); stop()`
// (as in the round-10 test) invokes the closure itself and is unaffected.
//
// Both waits are bounded by commitJoinCap (round-18 P1b): a renewal
// stuck in a context-ignoring backend holds renewDone open past the
// pre-commit join cap (the ordinary ExtendLease runs on the execution
// context, which such a backend ignores), and the pre-commit abort
// does not stop it — so an unconditional post-commit wait would hold
// the activity slot forever despite the cap. The same holds for the
// scoped replacement stop, whose cover renewal can stall the same way.
// Giving up releases the slot; the stuck renewal landing later is
// harmless: ordinary renewals refresh only token-matching in-flight
// entries (the commit transferred this one out, so the refresh is a
// no-op), admission stays generation- and expiry-gated, and cover
// renewals observe the dropped guard and exit without issuing.
func joinCommitStop(stop func(), closeDone func(), renewDone <-chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			if stop != nil {
				stopDone := make(chan struct{})
				go func() {
					defer close(stopDone)
					stop()
				}()
				timer := time.NewTimer(commitJoinCap)
				select {
				case <-stopDone:
					timer.Stop()
				case <-timer.C:
				}
			}
			closeDone()
			if renewDone != nil {
				timer := time.NewTimer(commitJoinCap)
				defer timer.Stop()
				select {
				case <-renewDone:
				case <-timer.C:
				}
			}
		})
	}
}

// claimReleaseOwnership atomically removes taskID from the in-flight set,
// reporting whether this caller may release the lease. Shutdown's
// releaseInFlight and the handler's shutdown-release path both funnel through
// in-flight ownership so only one of them releases a given lease: an activity
// that ignores cancellation and returns after Shutdown already released (and
// a peer re-claimed) its lease must not ReleaseLease again. The backend
// release is fenced on the claim token (worker + attempt) as a second
// layer, but a stale second release must still be suppressed locally: not
// all paths carry fencing, and a second release widens the reclaim race
// that enables duplicate execution.
//
// Presence alone is not ownership, and neither is a matching task ID: the
// entry must still carry this invocation's token. When the Start parent is
// canceled without Shutdown, renewal stops but a cancel-ignoring activity
// can keep running past LeaseDuration, letting a peer reclaim the task —
// and across a worker restart the new generation re-tracks the same task
// ID. A locally expired lease therefore reports false (after still
// removing the entry, but only when the token matches) so the caller does
// not clear a potentially peer-owned lease; the task is already
// reclaimable via expiry. A token mismatch reports false without touching
// the entry: a newer invocation owns it now.
func (w *Worker) claimReleaseOwnership(taskID int64, tok claimToken) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.inFlight[taskID]
	if !ok || e.epoch != tok.epoch || e.seq != tok.seq {
		return false
	}
	delete(w.inFlight, taskID)
	if time.Now().After(e.expiry) {
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
	tasks := make([]backend.Task, 0, len(w.inFlight))
	for id, e := range w.inFlight {
		if now.After(e.expiry) {
			// Lease already expired locally: a peer may have reclaimed it,
			// and releasing would risk clearing the peer's fresh lease
			// (even fenced, the local estimate says ownership is gone).
			// Drop without releasing; expiry already makes it claimable.
			w.opts.Logger.Debug("shutdown lease release skipped; local lease expired",
				"task_id", id)
			delete(w.inFlight, id)
			continue
		}
		// Tasks with a result commit underway are absent: handleActivity
		// transfers ownership out via claimCommitOwnership before touching
		// the store, so they are never released from under their commit.
		t := e.task
		if t.ID == 0 {
			t = backend.Task{ID: id}
		}
		tasks = append(tasks, t)
		delete(w.inFlight, id)
	}
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
		// Fenced by the tracked claim token: if the task was reclaimed by
		// a peer while shutting down, the backend reports ErrNotFound and
		// the fresh lease is left intact.
		if err := w.backend.ReleaseLease(ctx, t); err != nil && !errors.Is(err, backend.ErrNotFound) {
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
	// Conservative lease base (see trackAt): the local expiry is measured
	// from before the claim, not after it returns.
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
	// below; entries are wall-clock only, never store time.
	for _, t := range wtasks {
		w.trackWfClaim(t.ID)
	}
	defer w.clearWfClaims(wtasks)
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
			// cannot cover it). Fenced on the just-claimed token, so this
			// only releases our own claim.
			relCtx, relCancel := w.releaseContext(ctx)
			_ = w.backend.ReleaseLease(relCtx, t)
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
				tok := w.trackTaskAt(t, claimStart)
				p, herr := w.handleWorkflow(ctx, t)
				w.untrack(t.ID, tok)
				if herr != nil {
					w.recordStoreError(ctx, "commit_workflow", herr,
						"instance_id", t.InstanceID, "task_id", t.ID)
					w.opts.Logger.Debug("workflow task error", "instance_id", t.InstanceID, "err", herr)
					if errors.Is(herr, backend.ErrConflict) || errors.Is(herr, backend.ErrSuperseded) {
						w.dropSticky(t.InstanceID)
					}
					// Contention releases immediately for fast replay;
					// anything else backs off via delayed nack so a
					// persistently failing task does not spin the poll
					// loop (see requeueWorkflowTask).
					w.requeueWorkflowTask(ctx, t, herr)
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
	// Conservative lease base (see trackAt): backends stamp the visible
	// lease during the claim, so the local expiry is measured from before
	// the call, not after it returns.
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
			// Detached (see releaseContext): the poll ctx may be canceled
			// by a concurrent Shutdown and the task was never tracked.
			// Fenced on the just-claimed token.
			relCtx, relCancel := w.releaseContext(ctx)
			_ = w.backend.ReleaseLease(relCtx, t)
			relCancel()
			continue
		}
		// Lease extension starts in the handler goroutine immediately,
		// before any semaphore wait (slot already reserved), so long
		// activities do not lose their lease while queued.
		w.opts.Metrics.AddActivityTask(ctx, 1)
		w.opts.Logger.Debug("activity task",
			"instance_id", t.InstanceID, "task_id", t.ID, "activity", t.Name, "attempt", t.Attempt)
		tok := w.trackTaskAt(t, claimStart)
		done, ok := w.trackActivity()
		if !ok {
			w.untrack(t.ID, tok)
			<-w.actSem
			// Detached: Shutdown already canceled the poll ctx and this ID
			// is untracked, so releaseInFlight cannot cover it; a canceled
			// ctx would make context-aware stores reject the release and
			// stall the task until lease expiry. Fenced on the claim token.
			relCtx, relCancel := w.releaseContext(ctx)
			_ = w.backend.ReleaseLease(relCtx, t)
			relCancel()
			continue
		}
		go func(t backend.Task, execCtx context.Context, tok claimToken) {
			defer done()
			defer func() { <-w.actSem }()
			defer w.untrack(t.ID, tok)
			// Execution outlives the poll loop ctx across Shutdown grace so
			// within-grace completions can still commit (see Shutdown).
			// execCtx is the generation captured before dispatch: a stale
			// claim from a previous run aborts via the canceled context
			// instead of running under the new run. Its in-flight token
			// additionally fences every removal: even a cancellation-
			// ignoring stale invocation cannot delete or release an entry
			// re-tracked by the new generation for the same task ID.
			if herr := w.handleActivity(execCtx, t, tok); herr != nil {
				w.opts.Logger.Debug("activity task error",
					"instance_id", t.InstanceID, "task_id", t.ID, "err", herr)
			}
		}(t, execCtx, tok)
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
	// Conservative lease base (see trackAt).
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
			// Detached (see releaseContext): the poll ctx may be canceled
			// by a concurrent Shutdown and the task was never tracked.
			// Fenced on the just-claimed token.
			relCtx, relCancel := w.releaseContext(ctx)
			_ = w.backend.ReleaseLease(relCtx, t)
			relCancel()
			continue
		}
		wg.Add(1)
		w.opts.Metrics.AddActivityTask(ctx, 1)
		w.opts.Logger.Debug("activity task",
			"instance_id", t.InstanceID, "task_id", t.ID, "activity", t.Name, "attempt", t.Attempt)
		tok := w.trackTaskAt(t, claimStart)
		done, global := w.trackActivity()
		go func(t backend.Task, tok claimToken) {
			defer wg.Done()
			if global {
				defer done()
			}
			defer func() { <-w.actSem }()
			defer w.untrack(t.ID, tok)
			if herr := w.handleActivity(ctx, t, tok); herr != nil {
				w.opts.Logger.Debug("activity task error",
					"instance_id", t.InstanceID, "task_id", t.ID, "err", herr)
			}
		}(t, tok)
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
			task:        t,
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

func (w *Worker) handleActivity(ctx context.Context, t backend.Task, tok claimToken) error {
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
		// may have already released (and a peer re-claimed) this lease,
		// or a new generation may have re-tracked the same task ID after
		// a restart. The token keeps a stale invocation from releasing
		// the new generation's lease.
		if w.claimReleaseOwnership(t.ID, tok) {
			commitCtx, commitCancel := w.commitContext(ctx)
			_ = w.backend.ReleaseLease(commitCtx, t)
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
	// closeDone terminates the inherited renewal loop; it runs at most
	// once — the commit stops below close it early so they can JOIN the
	// loop before the guard drops (see joinCommitStop) — with this
	// deferred call as the backstop for non-commit exits.
	var doneOnce sync.Once
	closeDone := func() { doneOnce.Do(func() { close(done) }) }
	defer closeDone()
	// Drop the detached-renewal continuity guard at handler return (see
	// dropDetachedGuard). Registered before the commit-scoped stop funcs
	// below, so it runs after they joined their cover loops: every
	// renewal issued for this commit still finds its guard.
	defer w.dropDetachedGuard(t.ID, tok)
	// renewDone closes when the renewal loop exits so the shutdown-release
	// path below can JOIN it before releasing (see below): joining
	// guarantees no ExtendLease is in flight that could land after the
	// ReleaseLease and re-hide the task for a full lease (or slip past the
	// release fencing onto a peer's fresh lease).
	renewDone := make(chan struct{})
	// detachedEntered closes when the renewal loop enters detached mode
	// (see extendLeaseLoop): the commit handoff waits for it or renewDone
	// when the execution context is canceled, so an open renewDone alone
	// never proves the loop is alive (round-8 P1a).
	detachedEntered := make(chan struct{})
	go func() {
		defer close(renewDone)
		w.extendLeaseLoop(ctx, t.ID, tok, done, &committing, detachedEntered)
	}()

	act, err := w.reg.activity(t.Name)
	if err != nil {
		if errors.Is(err, ErrActivityNotRegistered) {
			if !w.beginDetachedCommit(t.ID, tok, &committing) {
				w.exitDetachedCommit(ctx, renewDone, &committing)
				return ctx.Err()
			}
			stopCommitRenewal, herr := w.ensureCommitRenewal(ctx, t.ID, tok, renewDone, detachedEntered)
			if herr != nil {
				w.exitDetachedCommit(ctx, renewDone, &committing)
				return herr
			}
			defer joinCommitStop(stopCommitRenewal, closeDone, renewDone)()
			commitCtx, commitCancel := w.commitContext(ctx)
			// Nack rewrites visible_at in place: serialize against
			// cover renewals (round-11 P1b).
			rerr := w.guardedDetachedCommit(t.ID, tok, commitCtx, commitCancel, true, func() error {
				return w.nackIncompatible(commitCtx, t, "unregistered_activity", err)
			})
			commitCancel()
			return rerr
		}
		if !w.beginDetachedCommit(t.ID, tok, &committing) {
			w.exitDetachedCommit(ctx, renewDone, &committing)
			return ctx.Err()
		}
		stopCommitRenewal, herr := w.ensureCommitRenewal(ctx, t.ID, tok, renewDone, detachedEntered)
		if herr != nil {
			w.exitDetachedCommit(ctx, renewDone, &committing)
			return herr
		}
		defer joinCommitStop(stopCommitRenewal, closeDone, renewDone)()
		commitCtx, commitCancel := w.commitContext(ctx)
		// failActivity deletes the task row: shared commit, so a
		// renewal running during a blocked Complete can still cancel
		// it mid-call on observed loss (round-10 P1b).
		rerr := w.guardedDetachedCommit(t.ID, tok, commitCtx, commitCancel, false, func() error {
			return w.failActivity(commitCtx, t, err)
		})
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
			// Conservative lease base (see refreshLeaseAt): measure from
			// before the heartbeat call, not after it succeeds.
			hbStart := time.Now()
			err := w.backend.RecordHeartbeat(ctx, t.ID, w.opts.LeaseDuration, details)
			if err == nil {
				w.refreshLeaseAt(t.ID, tok, hbStart)
			}
			return err
		},
	})

	out, err := w.invokeActivity(actCtx, act.fn, t.Input)
	// Ownership is re-verified at commit entry (see beginDetachedCommit),
	// which enters detached-commit renewal mode atomically with the
	// ownership transfer: checking the live context first and transferring
	// second leaves no window where a shutdown release can land on a
	// committing task, and no window where a detached commit runs without
	// renewal. Setting the flag here, before the ownership check below,
	// would do the opposite — a grace-cancel landing between the check and
	// the transfer lets releaseInFlight remove and release while renewal
	// is already detached, and the detached ExtendLease then lands after
	// the release, re-hiding the task or modifying a peer's fresh lease.
	// With the flag set only inside the transfer, either the release wins
	// (flag still clear, renewal exits without issuing) or the transfer
	// wins (entry gone, renewal alive through the commit).
	// Shutdown (grace expired) aborted the execution: never consume an
	// attempt or record a timeout failure for a Shutdown-caused cancel.
	// Release with a fresh detached commit ctx so a peer retries promptly.
	if ctx.Err() != nil {
		// Leave detached-commit renewal mode and JOIN the renewal loop
		// before releasing. Detached mode is entered atomically with the
		// commit transfer (see beginDetachedCommit), so on this release
		// path — where no transfer ran — the flag is still clear and the
		// loop cannot enter detached mode after this point; still, the
		// loop may be mid-tick with an ExtendLease already issued, which
		// would otherwise land after the ReleaseLease below, re-hiding
		// the task for a full lease or modifying a peer's fresh lease.
		// The join waits for the loop goroutine to return, and every
		// ExtendLease it issued completes before that return, so none
		// can land after the joined release. Only the in-flight owner
		// releases (see claimReleaseOwnership): releasing a lease
		// Shutdown already handed to a peer would clear the peer's lease
		// and enable duplicate execution.
		//
		// The join is bounded by commitJoinCap (round-19 P1, see
		// waitRenewDoneBounded): grace expiry with an ordinary ExtendLease
		// blocked in a context-ignoring backend would otherwise hold
		// renewDone open forever while the context-aware activity already
		// returned — bypassing the bounded joinCommitStop teardown (not
		// yet installed on this path) and holding the activity slot
		// forever. On give-up the guard is dropped and the entry is
		// untracked WITHOUT releasing (round-20 P1a, see below): the
		// stuck renewal is still live and keeps the backend lease
		// extended, so a ReleaseLease issued now would land before it —
		// the renewal then lands after the release, re-hiding the task
		// for a full lease when the local lease is fresh, or extending
		// a peer's fresh lease when it already expired and was
		// reclaimed. The task is left to natural expiry instead; the
		// late renewal only extends the lease, which the expiry reclaim
		// already accounts for.
		committing.Store(false)
		if !waitRenewDoneBounded(renewDone) {
			w.dropDetachedGuard(t.ID, tok)
			w.untrack(t.ID, tok)
			return ctx.Err()
		}
		if w.claimReleaseOwnership(t.ID, tok) {
			commitCtx, commitCancel := w.commitContext(ctx)
			_ = w.backend.ReleaseLease(commitCtx, t)
			commitCancel()
		}
		return ctx.Err()
	}
	if runCtx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) {
		err = fmt.Errorf("activity start-to-close timeout")
	}
	if err != nil {
		if IsNonRetryable(err) || (t.MaxAttempts > 0 && t.Attempt >= t.MaxAttempts) {
			if !w.beginDetachedCommit(t.ID, tok, &committing) {
				// Shutdown's release already handed this lease to a peer
				// (or a new generation re-tracked it): leave detached
				// mode and join renewal before returning so no detached
				// ExtendLease lands post-release.
				w.exitDetachedCommit(ctx, renewDone, &committing)
				return ctx.Err()
			}
			stopCommitRenewal, herr := w.ensureCommitRenewal(ctx, t.ID, tok, renewDone, detachedEntered)
			if herr != nil {
				w.exitDetachedCommit(ctx, renewDone, &committing)
				return herr
			}
			defer joinCommitStop(stopCommitRenewal, closeDone, renewDone)()
			commitCtx, commitCancel := w.commitContext(ctx)
			// failActivity deletes the task row: shared commit (see above).
			rerr := w.guardedDetachedCommit(t.ID, tok, commitCtx, commitCancel, false, func() error {
				return w.failActivity(commitCtx, t, err)
			})
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
		if !w.beginDetachedCommit(t.ID, tok, &committing) {
			// See above: ownership lost between the live-ctx check and the
			// commit transfer — stop renewal before returning.
			w.exitDetachedCommit(ctx, renewDone, &committing)
			return ctx.Err()
		}
		stopCommitRenewal, herr := w.ensureCommitRenewal(ctx, t.ID, tok, renewDone, detachedEntered)
		if herr != nil {
			w.exitDetachedCommit(ctx, renewDone, &committing)
			return herr
		}
		defer joinCommitStop(stopCommitRenewal, closeDone, renewDone)()
		commitCtx, commitCancel := w.commitContext(ctx)
		w.opts.Metrics.AddActivityRetry(commitCtx, 1)
		// RetryActivity rewrites visible_at in place: serialize against
		// cover renewals (round-11 P1b).
		if rerr := w.guardedDetachedCommit(t.ID, tok, commitCtx, commitCancel, true, func() error {
			return w.backend.RetryActivity(commitCtx, t.ID, delay)
		}); rerr != nil {
			w.recordStoreError(commitCtx, "retry_activity", rerr, "instance_id", t.InstanceID, "activity", t.Name)
			commitCancel()
			return rerr
		}
		commitCancel()
		return nil
	}
	if !w.beginDetachedCommit(t.ID, tok, &committing) {
		// See above: ownership lost between the live-ctx check and the
		// commit transfer — stop renewal before returning.
		w.exitDetachedCommit(ctx, renewDone, &committing)
		return ctx.Err()
	}
	stopCommitRenewal, herr := w.ensureCommitRenewal(ctx, t.ID, tok, renewDone, detachedEntered)
	if herr != nil {
		w.exitDetachedCommit(ctx, renewDone, &committing)
		return herr
	}
	defer joinCommitStop(stopCommitRenewal, closeDone, renewDone)()
	commitCtx, commitCancel := w.commitContext(ctx)
	// CompleteActivity deletes the task row: shared commit (see above).
	if cerr := w.guardedDetachedCommit(t.ID, tok, commitCtx, commitCancel, false, func() error {
		return w.backend.CompleteActivity(commitCtx, t.ID, journal.Event{
			Type:    journal.TypeActivityCompleted,
			RefSeq:  t.Seq,
			Payload: out,
		})
	}); cerr != nil {
		w.recordStoreError(commitCtx, "complete_activity", cerr, "instance_id", t.InstanceID, "activity", t.Name)
		commitCancel()
		return cerr
	}
	commitCancel()
	return nil
}

// errLeaseLost marks a detached result commit skipped because its
// pre-commit detached renewal failed: the lease may have expired and a
// peer may own the task, so touching the store (ID-only Complete/Retry)
// could modify the peer's task. The task is left untracked for natural
// expiry reclaim instead.
var errLeaseLost = errors.New("tasuki: lease lost; skipping detached commit")

// exitDetachedCommit leaves detached-commit renewal mode after losing
// commit ownership (see beginDetachedCommit): the transfer failed, so no
// commit follows and renewal must stop first. The flag was never set on
// this path — detached mode is entered atomically with a successful
// transfer, so the loop cannot enter detached mode after this point — but
// the loop may still be mid-tick with an ExtendLease already issued, so
// clearing the flag alone is not enough: joining waits for the loop to
// return, and every ExtendLease it issued completes before that return, so
// none can land after a Shutdown release that already took the lease.
// The join is only needed when the execution context
// is done: with a live ctx the loop cannot be in detached mode (entry
// requires ctx.Done) and the cleared flag prevents any future entry, so
// the loop exits on done at handler return.
//
// The canceled-ctx join is bounded by commitJoinCap (round-19 P1, see
// waitRenewDoneBounded): like the cancellation-path release above, this
// runs before the bounded joinCommitStop teardown is installed, so an
// ordinary ExtendLease blocked in a context-ignoring backend would
// otherwise hold the activity slot forever. On give-up the handler's
// deferred guard drop still runs at return, and the late renewal is
// harmless per above.
func (w *Worker) exitDetachedCommit(ctx context.Context, renewDone <-chan struct{}, committing *atomic.Bool) {
	committing.Store(false)
	if ctx.Err() != nil {
		waitRenewDoneBounded(renewDone)
	}
}

// ensureCommitRenewal performs the true renewal-loop handoff for a result
// commit that just won detached-commit ownership (see beginDetachedCommit).
// It returns a stop func governing the commit-scoped renewal, or an error
// when the commit must be aborted (detached-renewal failure).
//
// Handoff synchronization (round-8 P1a): when the execution context is
// canceled, an open renewDone does NOT prove the loop is alive — the loop
// may have read committing==false and be paused before closing renewDone
// while beginDetachedCommit concurrently sets the flag. The commit path
// therefore joins/confirms the old loop's fate before proceeding: it waits
// for renewDone close (loop exited) OR the explicit detached-entry ack
// (loop entered detached renewal and will cover the commit). With a live
// execution context no wait is needed: the loop cannot have been asked to
// exit via ctx.Done, done is still open (handler hasn't returned), and the
// just-set flag exempts the ticker ownership check, so the loop is alive.
//
// Renewal-failure abort (round-8 P1b): after the fate is known, a
// synchronous detached renewal runs BEFORE the store commit. Any failure
// (transient error, timeout, ErrNotFound) is treated as loss of commit
// ownership: the caller must skip its ID-only store op and return the
// error (lease expiry reclaims naturally) instead of letting a slow
// commit outlive the lease onto a peer's task. Only logging and
// continuing would leave the commit uncovered.
//
// If the old loop exited, the stop func governs a scoped replacement
// renewal (see renewUntilDone, which renews immediately on entry) started
// synchronously after the successful pre-commit renewal, living exactly
// for the commit: the caller defers it, so it stops before handler return
// (done closes after every commit) and no renewal outlives the commit to
// touch a successor's lease (e.g. overwrite a RetryActivity delay). When
// the inherited loop is still running, detached mode keeps it alive and
// the stop func is a no-op — the post-commit join of that loop lives in
// the deferred wrapper instead (see joinCommitStop): terminating the
// inherited loop here, inside the handoff, would leave the commit that
// follows uncovered.
func (w *Worker) ensureCommitRenewal(ctx context.Context, taskID int64, tok claimToken, renewDone <-chan struct{}, detachedEntered ...<-chan struct{}) (func(), error) {
	var detachedAck <-chan struct{}
	if len(detachedEntered) > 0 {
		detachedAck = detachedEntered[0]
	}
	// Join the old loop's fate when cancellation makes its liveness
	// ambiguous. Both channels are closed exactly once by the loop (ack on
	// detached entry, renewDone on return); done stays open until handler
	// return, so a detached ack cannot be followed by an exit before the
	// commit below. Bounded by commitJoinCap (round-19 P1): a renewal
	// stuck in a context-ignoring backend holds both channels open past
	// grace expiry, and an unconditional wait here would hold the slot
	// before the bounded teardown is installed. On give-up the handoff
	// proceeds to the synchronous pre-commit renewal below; the late
	// ordinary renewal is harmless (see waitRenewDoneBounded) and the
	// post-commit join stays bounded.
	if ctx.Err() != nil && renewDone != nil && detachedAck != nil {
		timer := time.NewTimer(commitJoinCap)
		select {
		case <-renewDone:
			timer.Stop()
		case <-detachedAck:
			timer.Stop()
		case <-timer.C:
		}
	}
	// Synchronous pre-commit renewal: proves the lease is still ours and
	// covers the handoff gap before the store op. Failure aborts the
	// commit (see errLeaseLost).
	if err := w.renewOnceDetached(ctx, taskID, tok); err != nil {
		return nil, fmt.Errorf("%w: pre-commit renewal failed: %v", errLeaseLost, err)
	}
	if renewDone != nil {
		select {
		case <-renewDone:
		default:
			return func() {}, nil
		}
	} else {
		return func() {}, nil
	}
	d := w.leaseDuration() / 2
	if d <= 0 {
		return func() {}, nil
	}
	done := make(chan struct{})
	ticker := time.NewTicker(d)
	var detached atomic.Bool
	detached.Store(true)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.renewUntilDone(ctx, taskID, tok, done, ticker, &detached)
	}()
	return func() {
		close(done)
		wg.Wait()
		ticker.Stop()
	}, nil
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

func (w *Worker) extendLeaseLoop(ctx context.Context, taskID int64, tok claimToken, done <-chan struct{}, committing *atomic.Bool, detachedEntered ...chan struct{}) {
	var detachedCh chan struct{}
	if len(detachedEntered) > 0 {
		detachedCh = detachedEntered[0]
	}
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
			//
			// Signal the handoff BEFORE entering detached renewal: the
			// commit path waits for this ack (or renewDone close) when
			// the execution context is canceled, so an open renewDone
			// alone never proves the loop is alive (round-8 P1a). The
			// close happens exactly once, on the single detached entry.
			if detachedCh != nil {
				select {
				case <-detachedCh:
				default:
					close(detachedCh)
				}
			}
			w.renewUntilDone(ctx, taskID, tok, done, ticker, committing)
			return
		case <-ticker.C:
			// A stale invocation whose entry was overwritten by a
			// reclaim (new generation or new claim of the same task ID)
			// must stop renewing: extending the new owner's lease
			// hides it from nobody but masks the ownership loss and
			// refreshes an expiry the new invocation maintains
			// itself. Detached-commit mode is exempt: its entry was
			// transferred out by its own commit, so absence is
			// expected while the commit still needs renewal.
			detached := committing != nil && committing.Load()
			if !detached {
				// Ordinary renewal must still own a FRESH lease (see
				// ownsFresh): past the local expiry a peer may have
				// reclaimed the task, and an ID-only ExtendLease
				// would then extend the peer's lease — hiding the
				// peer's task while masking our ownership loss.
				// The task is left for natural expiry reclaim.
				if !w.ownsFresh(taskID, tok) {
					return
				}
				// Shutdown passed grace expiry — or a restart moved the
				// barrier to a new generation: no new ordinary
				// renewals. Registration, the generation check, and the
				// stop-check are atomic under renewMu (see
				// shutdownRenewalJoin) so no renewal slips past
				// Shutdown's release in either direction, no Add can
				// race the join by construction, and a stale ticker
				// paused across grace expiry + restart is rejected by
				// its token epoch even after the re-arm (round-15 P1).
				if !w.renewTryEnter(taskID, tok) {
					return
				}
				// Conservative lease base (see refreshLeaseAt).
				renewStart := time.Now()
				rerr := w.backend.ExtendLease(ctx, taskID, w.opts.LeaseDuration)
				w.renewExit(taskID)
				if rerr != nil {
					w.recordStoreError(ctx, "extend_lease", rerr, "task_id", taskID)
				} else {
					w.refreshLeaseAt(taskID, tok, renewStart)
				}
				continue
			}
			// Detached-commit cover: the entry transferred out at
			// commit entry, so no shutdown release can land on this
			// task. The renewal is continuity-gated (see
			// renewOnceDetached): once the lease moved on — or any
			// renewal fails so continuity can no longer be
			// guaranteed (round-11 P1a) — cover stops instead of
			// extending a peer's lease, and the loop exits.
			if derr := w.renewOnceDetached(ctx, taskID, tok); derr != nil {
				w.tripDetachedGuard(taskID, tok)
				return
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
func (w *Worker) renewUntilDone(ctx context.Context, taskID int64, tok claimToken, done <-chan struct{}, ticker *time.Ticker, committing *atomic.Bool) {
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
	// A lost lease stops the cover outright: the guard reports loss
	// when the lease moved on (see renewOnceDetached), and every
	// further success would only extend the peer's lease. ANY renewal
	// failure stops the cover too (round-11 P1a): a transient error or
	// timeout near the continuity deadline leaves the lease to expire
	// — and be reclaimed — before the next half-lease tick, and the
	// ID-only commit that follows would modify the peer's task.
	// renewOnceDetached trips the guard on every failure (canceling an
	// in-flight commit via the stashed commit cancel); the trip below
	// is the backstop for errors that bypass it, and the loop exits so
	// no further renewal can extend a moved-on lease.
	if err := w.renewOnceDetached(ctx, taskID, tok); err != nil {
		w.tripDetachedGuard(taskID, tok)
		return
	}
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
			// Any renewal failure stops the cover (round-11 P1a, see
			// above): the trip is the backstop, the return ends cover.
			if err := w.renewOnceDetached(ctx, taskID, tok); err != nil {
				w.tripDetachedGuard(taskID, tok)
				return
			}
		}
	}
}

// renewOnceDetached extends taskID once with a context detached from
// execution cancellation, bounded by the lease duration. It reports the
// renewal error (also recorded): a detached-renewal failure means commit
// ownership may be lost, and the commit path must abort instead of
// running an ID-only store op that could modify a peer's task (round-8
// P1b). Background loops log-and-continue via this return; the
// pre-commit handoff treats any error as lease loss.
//
// ANY failure aborts cover (round-11 P1a): a transient error or timeout
// near the continuity deadline leaves the lease to expire — and be
// reclaimed — before the next half-lease tick, and the ID-only commit
// that follows would modify the peer's task. Only errLeaseLost used to
// stop the periodic loops while other errors kept them alive; now every
// error trips the guard (dropping it so cover loops stop, and canceling
// the in-flight commit via the stashed commit cancel) and reports
// errLeaseLost so the commit aborts. A live lease with a healthy
// backend never trips this; a failure trades a retry for never
// modifying a peer's task.
//
// A SUCCESS can also prove loss (round-9 P1b, extended round-10 P1c):
// with the guard seeded at commit entry (see detachedGuard), a call
// that started after the continuity deadline may have extended a peer's
// lease after an expiry-and-reclaim gap — and so may a call that
// started before the deadline but only COMPLETED after it, blocked in
// the backend past the reclaim. The ID-only ExtendLease reports
// success in both cases while extending the peer's lease, so the
// start instant alone cannot prove ownership: continuity must hold
// through renewal completion. Either trip drops the guard so cover
// loops stop too, refreshes nothing, and reports errLeaseLost so the
// commit aborts. A live lease always verifies (the estimate is
// conservative-early); only a genuine renewal gap trips this, trading
// a retry for never modifying a peer's task.
//
// A backend ErrNotFound (task row gone) trips the guard the same way
// and cancels the in-flight commit: the commit gate then observes the
// loss and skips its ID-only store op instead of touching a peer's
// task (round-10 P1b).
//
// The ExtendLease runs on the commit's cover context (see
// detachedGuard): a renewal still blocked in the backend when a
// row-preserving result write completes is aborted instead of landing
// after it (round-11 P1b) — best-effort, since a context-ignoring
// backend lands it anyway — and trips/drops stop all cover. Ordering
// against such backends comes from the pre-write cover join plus the
// writing hold (round-20 P1b, see guardedDetachedCommit): no cover call
// is in flight or issuing when the write runs. detMu is released
// across the call, so a stuck backend stalls just this task, never the
// guard map. The call registers in the per-task cover barrier across
// the backend call (see coverRenewExit) so the join observes it.
func (w *Worker) renewOnceDetached(ctx context.Context, taskID int64, tok claimToken) error {
	w.detMu.Lock()
	g, ok := w.detGuard[taskID]
	w.detMu.Unlock()
	if ok && (g.epoch != tok.epoch || g.seq != tok.seq) {
		// A stale invocation's loop (previous generation, or an
		// earlier claim whose task ID was re-tracked): another
		// invocation owns the detached commit now. Issue nothing.
		return fmt.Errorf("%w: detached renewal superseded", errLeaseLost)
	}
	if !ok {
		// No detached commit owns this renewal. Only a pre-transfer
		// direct use with the entry still present may proceed (the
		// renewal-handoff tests drive ensureCommitRenewal without a
		// transfer); a stale loop past handler return — guard
		// dropped, entry gone — must not extend a lease that may
		// have moved on.
		if !w.owns(taskID, tok) {
			return fmt.Errorf("%w: no detached commit owns task", errLeaseLost)
		}
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.leaseDuration())
		defer cancel()
		// Conservative lease base (see refreshLeaseAt).
		renewStart := time.Now()
		if err := w.backend.ExtendLease(rctx, taskID, w.opts.LeaseDuration); err != nil {
			w.recordStoreError(rctx, "extend_lease", err, "task_id", taskID)
			return err
		}
		w.refreshLeaseAt(taskID, tok, renewStart)
		return nil
	}
	// Cover renewals run on the commit's cover context (see
	// detachedGuard): it is independent of execution cancellation so
	// shutdown-grace expiry does not stop cover mid-commit, but the
	// commit cancels it the moment a row-preserving store op completes
	// (round-11 P1b) and the guard drop/trip cancels it when the commit
	// is over. A renewal aborted that way exits quietly below instead
	// of recording a store error.
	//
	// While a row-preserving store op runs, its writing hold is set
	// (round-20 P1b, see guardedDetachedCommit): skip issuing without
	// tripping — the write is the commit, and the post-write cleanup
	// drops the guard — so no cover renewal can overlap the write and
	// land after it. Returning nil (rather than an error) keeps the
	// loops alive without tripping the guard or canceling the op.
	// Re-checked under detMu: the guard may have been replaced (or the
	// hold set) since the unlocked read above.
	w.detMu.Lock()
	curHold, okHold := w.detGuard[taskID]
	if !okHold || curHold.epoch != tok.epoch || curHold.seq != tok.seq {
		w.detMu.Unlock()
		return fmt.Errorf("%w: detached commit finished during renewal", errLeaseLost)
	}
	if curHold.writing {
		w.detMu.Unlock()
		return nil
	}
	// Register this cover call in the per-task barrier (round-20 P1b,
	// see joinCoverRenewalsForCommit) so a row-preserving commit can
	// join it before writing. Deregistered once the call below
	// returns, on every path.
	if w.coverInflight == nil {
		w.coverInflight = map[int64]*renewTaskEntry{}
	}
	ce := w.coverInflight[taskID]
	if ce == nil {
		ce = &renewTaskEntry{idle: make(chan struct{})}
		w.coverInflight[taskID] = ce
	}
	ce.count++
	coverCtx := curHold.coverCtx
	if coverCtx == nil {
		coverCtx = context.WithoutCancel(ctx)
	}
	w.detMu.Unlock()
	rctx, cancel := context.WithTimeout(coverCtx, w.leaseDuration())
	defer cancel()
	w.detMu.Lock()
	cur, ok := w.detGuard[taskID]
	if !ok || cur.epoch != tok.epoch || cur.seq != tok.seq {
		w.detMu.Unlock()
		w.coverRenewExit(taskID)
		return fmt.Errorf("%w: detached commit finished during renewal", errLeaseLost)
	}
	// Conservative lease base (see refreshLeaseAt).
	renewStart := time.Now()
	w.detMu.Unlock()
	if err := w.backend.ExtendLease(rctx, taskID, w.opts.LeaseDuration); err != nil {
		w.coverRenewExit(taskID)
		// Our own commit ended first (result write completed, guard
		// tripped by a concurrent loss): the cover context is
		// canceled, the write was dropped, and there is nothing to
		// record — exit quietly with loss so the loop stops.
		if coverCtx.Err() != nil {
			return fmt.Errorf("%w: detached cover canceled", errLeaseLost)
		}
		w.recordStoreError(rctx, "extend_lease", err, "task_id", taskID)
		// Any failure trips the guard (round-11 P1a): continuity can no
		// longer be guaranteed, so the commit must abort (via the gate
		// recheck) and an in-flight op must stop (via the stashed
		// cancel) instead of letting the ID-only store op modify a
		// peer's reclaimed task after the lease expires uncovered.
		w.tripDetachedGuard(taskID, tok)
		if errors.Is(err, backend.ErrNotFound) {
			return fmt.Errorf("%w: detached renewal found task gone: %v", errLeaseLost, err)
		}
		return fmt.Errorf("%w: detached renewal failed: %v", errLeaseLost, err)
	}
	// The store call settled: release the barrier slot so a concurrent
	// pre-write cover join can proceed. Gap checks below still run —
	// the join only orders the write after the call's return, not after
	// its verdict.
	w.coverRenewExit(taskID)
	completedAt := time.Now()
	// Gap detection on the call-start instant: starting after the
	// continuity deadline means the backend lease may have expired and
	// been reclaimed since the last success, so this call just extended
	// the peer's lease. Drop the guard so cover loops stop too, refresh
	// nothing, and report loss so the commit aborts. A live lease always
	// verifies (the estimate is conservative-early); only a genuine
	// renewal gap trips this, trading a retry for never modifying a
	// peer's task. Re-read under detMu first: the commit may have
	// finished (dropping the guard) while this call was in flight.
	w.detMu.Lock()
	cur, ok = w.detGuard[taskID]
	if !ok || cur.epoch != tok.epoch || cur.seq != tok.seq {
		w.detMu.Unlock()
		return fmt.Errorf("%w: detached commit finished during renewal", errLeaseLost)
	}
	if !renewStart.Before(cur.deadline) {
		w.detMu.Unlock()
		w.opts.Logger.Debug("detached renewal started after lease continuity deadline; treating as lease loss",
			"task_id", taskID)
		// A loss observed mid-commit must also stop the in-flight
		// store op (round-10 P1b); nothing is stashed yet on the
		// pre-commit path, so this is nil there. Cover renewals stop
		// with the trip (round-11 P1a).
		w.tripDetachedGuard(taskID, tok)
		return fmt.Errorf("%w: detached renewal after lease continuity deadline", errLeaseLost)
	}
	// Continuity through completion (round-10 P1c): a call that started
	// before the deadline but only completed after it — blocked in the
	// backend past the reclaim — extended the peer's lease despite
	// reporting success, and the success must not refresh the deadline
	// into a stale commit. No backend offers a claim-token-fenced
	// ExtendLease (see detachedGuard), so this worker-side
	// success-followed-by-loss trips the same way as the start gap.
	// Same detMu critical section as the start-gap check above: the
	// guard cannot change between the two checks.
	if !completedAt.Before(cur.deadline) {
		w.detMu.Unlock()
		w.opts.Logger.Debug("detached renewal completed after lease continuity deadline; treating as lease loss",
			"task_id", taskID)
		w.tripDetachedGuard(taskID, tok)
		return fmt.Errorf("%w: detached renewal completed after lease continuity deadline", errLeaseLost)
	}
	// Monotonic deadline (same pattern as refreshLeaseAt): the sync
	// pre-commit renewal overlaps the inherited renewal loop, both stamp
	// their start pre-call, and out-of-order returns would otherwise let
	// the earlier start overwrite a newer deadline — the guard then
	// expires while the backend lease is still live, the periodic
	// renewal cancels a valid completion, and the retry duplicates side
	// effects. Only a LATER expiry replaces the current one.
	if newDeadline := renewStart.Add(w.leaseDuration()); newDeadline.After(cur.deadline) {
		cur.deadline = newDeadline
		w.detGuard[taskID] = cur
	}
	w.detMu.Unlock()
	return nil
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
