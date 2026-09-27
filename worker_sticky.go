package tasuki

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

type stickyEntry struct {
	events   []journal.Event
	nextSeq  int64
	lastUsed time.Time
}

func (w *Worker) dropSticky(instanceID string) {
	w.stickyMu.Lock()
	defer w.stickyMu.Unlock()
	delete(w.sticky, instanceID)
}

func (w *Worker) setSticky(instanceID string, events []journal.Event, nextSeq int64) {
	w.stickyMu.Lock()
	defer w.stickyMu.Unlock()
	if w.sticky == nil {
		w.sticky = map[string]stickyEntry{}
	}
	w.sticky[instanceID] = stickyEntry{
		events:   append([]journal.Event(nil), events...),
		nextSeq:  nextSeq,
		lastUsed: time.Now(),
	}
}

func (w *Worker) stickyGet(instanceID string) (stickyEntry, bool) {
	w.stickyMu.Lock()
	defer w.stickyMu.Unlock()
	e, ok := w.sticky[instanceID]
	if !ok {
		return stickyEntry{}, false
	}
	e.lastUsed = time.Now()
	w.sticky[instanceID] = e
	cp := stickyEntry{
		events:   append([]journal.Event(nil), e.events...),
		nextSeq:  e.nextSeq,
		lastUsed: e.lastUsed,
	}
	return cp, true
}

func (w *Worker) evictIdleSticky(now time.Time) {
	ttl := w.opts.StickyJournalTTL
	w.stickyMu.Lock()
	defer w.stickyMu.Unlock()
	for id, e := range w.sticky {
		if now.Sub(e.lastUsed) < ttl {
			continue
		}
		delete(w.sticky, id)
	}
}

func expectedNextSeq(events []journal.Event) int64 {
	if len(events) == 0 {
		return 1
	}
	return events[len(events)-1].Seq + 1
}

func (w *Worker) loadWorkflowState(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	head, err := w.backend.LoadWorkflowHead(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if head.Instance.Status != "running" {
		w.dropSticky(instanceID)
		return head, nil
	}

	entry, ok := w.stickyGet(instanceID)
	var j []journal.Event
	switch {
	case !ok:
		j, err = w.backend.GetJournal(ctx, instanceID, 0)
		if err != nil {
			return nil, err
		}
	case head.NextSeq == entry.nextSeq:
		j = entry.events
	case head.NextSeq > entry.nextSeq:
		lastSeq := int64(0)
		if len(entry.events) > 0 {
			lastSeq = entry.events[len(entry.events)-1].Seq
		}
		extra, err := w.backend.GetJournal(ctx, instanceID, lastSeq)
		if err != nil {
			return nil, err
		}
		merged := append(append([]journal.Event{}, entry.events...), extra...)
		if expectedNextSeq(merged) != head.NextSeq {
			j, err = w.backend.GetJournal(ctx, instanceID, 0)
			if err != nil {
				return nil, err
			}
		} else {
			j = merged
		}
	default: // head.NextSeq < entry.nextSeq
		j, err = w.backend.GetJournal(ctx, instanceID, 0)
		if err != nil {
			return nil, err
		}
	}
	w.setSticky(instanceID, j, head.NextSeq)
	head.Journal = j
	return head, nil
}

func (w *Worker) commitWorkflow(ctx context.Context, task backend.Task, baseJournal []journal.Event, adv backend.Advancement) error {
	adv = w.advForCommit(task, adv)
	return w.finishWorkflowCommit(ctx, task, baseJournal, adv, w.backend.CommitAdvancement(ctx, adv))
}

func (w *Worker) advForCommit(task backend.Task, adv backend.Advancement) backend.Advancement {
	if adv.WorkerID == "" && task.WorkerID != "" {
		adv.WorkerID = task.WorkerID
		adv.Attempt = task.Attempt
	}
	return adv
}

// finishWorkflowCommit applies commitWorkflow's post-store handling for the
// result err of one CommitAdvancement call: sticky maintenance on success,
// and fenced lease release / delayed nack on failure (see
// requeueWorkflowTask) so a failed task becomes visible again. Shared by
// commitWorkflow and the flush's guarded commit path below.
func (w *Worker) finishWorkflowCommit(ctx context.Context, task backend.Task, baseJournal []journal.Event, adv backend.Advancement, err error) error {
	instanceID := task.InstanceID
	if instanceID == "" {
		instanceID = adv.InstanceID
	}
	if err != nil {
		if errors.Is(err, backend.ErrConflict) || errors.Is(err, backend.ErrNotFound) {
			w.dropSticky(instanceID)
		}
		// Contention releases immediately for fast replay; other commit
		// failures back off via delayed nack (see requeueWorkflowTask) so
		// a persistently failing task does not spin the poll loop.
		// Best-effort: the task may already be gone.
		w.requeueWorkflowTask(ctx, task, err)
		return err
	}
	w.applyStickyAfterCommit(instanceID, baseJournal, adv)
	return nil
}

// trackWfClaim records the local wall-clock claim time of a workflow task.
func (w *Worker) trackWfClaim(taskID int64) {
	w.trackWfClaimAt(taskID, time.Now())
}

func (w *Worker) trackWfClaimAt(taskID int64, at time.Time) {
	w.wfClaimMu.Lock()
	defer w.wfClaimMu.Unlock()
	if w.wfClaim == nil {
		w.wfClaim = map[int64]time.Time{}
	}
	w.wfClaim[taskID] = at
}

func (w *Worker) refreshWfClaim(taskID int64) {
	w.wfClaimMu.Lock()
	defer w.wfClaimMu.Unlock()
	if _, ok := w.wfClaim[taskID]; ok {
		w.wfClaim[taskID] = time.Now()
	}
}

// clearWfClaims drops local claim records after the tick's flush.
func (w *Worker) clearWfClaims(tasks []backend.Task) {
	w.wfClaimMu.Lock()
	defer w.wfClaimMu.Unlock()
	for _, t := range tasks {
		delete(w.wfClaim, t.ID)
	}
}

// wfLeaseExpired reports whether the local lease-expiry estimate for a
// claimed workflow task has passed. Unknown IDs (direct commitWorkflow /
// requeueWorkflowTask calls outside tickWorkflows, e.g. unit tests) report
// false so the nack proceeds as before.
func (w *Worker) wfLeaseExpired(taskID int64) bool {
	w.wfClaimMu.Lock()
	defer w.wfClaimMu.Unlock()
	claimed, ok := w.wfClaim[taskID]
	if !ok {
		return false
	}
	lease := w.opts.LeaseDuration
	if lease <= 0 {
		lease = 30 * time.Second
	}
	return !time.Now().Before(claimed.Add(lease))
}

// requeueWorkflowTask makes a failed workflow task visible again after a
// handleWorkflow/commit error. Contention (ErrConflict/ErrSuperseded) can
// succeed on replay, so the lease is released immediately for fast retry:
// the release carries the claim token (kind/instance for WF# routing,
// worker + attempt fencing) so a stale worker never clears a peer's fresh
// lease after a reclaim race. Any other failure — transient store errors or
// deterministic oversized-advancement diagnostics from checkTerminalBudget/
// fitAdvancementToBudget — is nacked with IncompatibleRetryDelay: every
// ReleaseLease also emits a task notification that wakes the poll loop, so
// an immediate release of a persistently failing task would
// reclaim-fail-notify in a tight loop, saturating the worker and backing
// store. Unlike the fast-path precheck below, NackTask itself is fenced on
// the claim ownership token (worker + attempt, plus numeric id on WF keys),
// so a delayed nack that lost the check→nack race to a peer reclaim is
// rejected by the backend with ErrNotFound without touching the peer's
// fresh lease. The local lease-expiry estimate stays as a fast path: once
// it has passed, a peer may have reclaimed the task, so the stale worker
// skips the nack and expiry reclaims naturally. Nack failures share the
// release_lease store-error op label to keep the op vocabulary bounded.
func (w *Worker) requeueWorkflowTask(ctx context.Context, t backend.Task, herr error) {
	if errors.Is(herr, backend.ErrConflict) || errors.Is(herr, backend.ErrSuperseded) || errors.Is(herr, backend.ErrNotFound) {
		if rerr := w.backend.ReleaseLease(ctx, t); rerr != nil && !errors.Is(rerr, backend.ErrNotFound) {
			w.recordStoreError(ctx, "release_lease", rerr, "task_id", t.ID)
		}
		return
	}
	if w.wfLeaseExpired(t.ID) {
		w.opts.Logger.Debug("skipping stale workflow nack; local lease expired",
			"task_id", t.ID, "instance_id", t.InstanceID)
		return
	}
	if rerr := w.backend.NackTask(ctx, t, w.opts.IncompatibleRetryDelay); rerr != nil && !errors.Is(rerr, backend.ErrNotFound) {
		w.recordStoreError(ctx, "release_lease", rerr, "task_id", t.ID)
	}
}

type pendingWorkflowCommit struct {
	instanceID  string
	baseJournal []journal.Event
	adv         backend.Advancement
	// task is the claimed workflow task (ownership token for fenced lease
	// release on commit failure). Older call sites may leave it zero; the
	// release then falls back to adv-derived routing without fencing.
	task backend.Task
	// tok is the claiming invocation's in-flight token (see claimToken),
	// stamped by tickWorkflows when the turn completes. The flush gates
	// on freshness and transfers ownership out via beginDetachedCommit
	// before touching the store, so a Shutdown releaseInFlight racing the
	// flush wins exactly once: either the release wins and the flush
	// skips, or the flush wins and the release finds nothing — and a won
	// flush keeps the lease live through the write (see startFlushCover).
	// hasTok distinguishes production pendings from older/test call sites
	// that only set adv (which commit ungated, as before).
	tok    claimToken
	hasTok bool
}

// flushGuardedCommit is one flush entry that passed the freshness gate
// and transferred out via beginDetachedCommit: its lease stays covered
// by renewal until its commit returns (see startFlushCover).
type flushGuardedCommit struct {
	p          pendingWorkflowCommit
	task       backend.Task
	committing *atomic.Bool
}

func (w *Worker) flushWorkflowCommits(ctx context.Context, pending []pendingWorkflowCommit) {
	if len(pending) == 0 {
		return
	}
	// Ownership gate (round-21 P2, hardened round-23 P2): a Shutdown
	// timeout + restart may have released a finished pending turn
	// (removing it from the in-flight set) while a peer re-claimed the
	// task; the flush runs on a detached commit ctx that outlives the
	// shutdown, and backends validate the advancement by task ID +
	// sequence alone — so committing the stale advancement would delete
	// the peer's active task after duplicate execution. Skip entries that
	// lost ownership instead of touching the store.
	//
	// The gate is freshness, not mere presence: a turn that consumed most
	// of its lease (or a CommitAdvancement blocked past the remaining
	// lease) lets a peer reclaim mid-flush even when the transfer below
	// "wins" — presence alone would still commit stale. Entries whose
	// local lease estimate already passed are skipped: a peer may own
	// them now. Gated entries transfer out via beginDetachedCommit (which
	// also seeds the detached-renewal continuity guard) and stay covered
	// by renewal until their commit returns (see startFlushCover), so a
	// blocked commit cannot outlive its lease either. Either the release
	// wins and the flush skips, or the flush wins and the release finds
	// nothing — and a won flush keeps the lease live through the write.
	var gated []flushGuardedCommit
	var legacy []pendingWorkflowCommit
	for _, p := range pending {
		if !p.hasTok {
			// Older/test call sites carrying only adv commit ungated,
			// as before.
			legacy = append(legacy, p)
			continue
		}
		t := w.taskForCommit(p)
		if !w.ownsFresh(t.ID, p.tok) {
			w.untrack(t.ID, p.tok)
			w.opts.Logger.Debug("skipping stale workflow commit; lease expired or already released",
				"instance_id", p.instanceID, "task_id", t.ID)
			continue
		}
		flag := &atomic.Bool{}
		if !w.beginDetachedCommit(t.ID, p.tok, flag, ctx) {
			w.untrack(t.ID, p.tok)
			w.opts.Logger.Debug("skipping stale workflow commit; lease already released",
				"instance_id", p.instanceID, "task_id", t.ID)
			continue
		}
		gated = append(gated, flushGuardedCommit{p: p, task: t, committing: flag})
	}
	stopCover := w.startFlushCover(ctx, gated)
	defer stopCover()
	// commitOne runs one advancement through the detached-commit gate
	// (see guardedDetachedCommit): ownership AND the continuity deadline
	// are re-verified immediately before the store op, so a loss
	// observed during the flush (cover renewal failure, tripped guard,
	// deadline passed while an earlier commit blocked) skips the ID-only
	// write instead of deleting a peer's reclaimed task. A fencing
	// rejection returns errLeaseLost: no store op ran, so — like the
	// activity retry/complete paths — it is debug-logged, not counted
	// as a store failure. Other errors share commitWorkflow's
	// post-handling (sticky + fenced requeue), with the requeue ordered
	// after in-flight cover first (see joinStaleCoverForRequeue).
	commitOne := func(g flushGuardedCommit) {
		defer g.committing.Store(false)
		cctx, ccancel := context.WithCancel(ctx)
		defer ccancel()
		// Row-deleting (exclusive=false, like Complete/fail): the
		// advancement deletes the task row, so no writing hold pauses
		// cover — the lease must stay live DURING the store op, not
		// just before it, or a blocked CommitAdvancement outlives the
		// lease and deletes a peer's reclaimed task. A cover renewal
		// racing commit completion at worst reports ErrNotFound
		// (one extend_lease store error on a slow commit); pausing
		// cover instead would risk a stale write, which is worse.
		err := w.guardedDetachedCommit(g.task.ID, g.p.tok, cctx, ccancel, false, func() error {
			adv := w.advForCommit(g.task, g.p.adv)
			return w.backend.CommitAdvancement(cctx, adv)
		})
		if err != nil {
			if errors.Is(err, errLeaseLost) {
				w.opts.Logger.Debug("skipping workflow commit; lease lost before the store op",
					"instance_id", g.p.instanceID, "task_id", g.task.ID, "err", err)
				return
			}
			w.recordStoreError(ctx, "commit_workflow", err, "instance_id", g.p.instanceID)
			// Order the failure requeue after in-flight cover
			// (round-27 P2a, see joinStaleCoverForRequeue): the
			// release/nack below rewrites visible_at in place, so a
			// periodic cover ExtendLease blocked across this failed
			// commit would otherwise land after it and overwrite
			// what it wrote. Stop new cover from issuing and join
			// the admitted calls (bounded like the exclusive-commit
			// path) before requeueing. When the join gives up
			// (round-28 P2b) a cover renewal ignoring cancellation
			// is still live: suppress the requeue and leave the
			// task to natural expiry reclaim — an immediate
			// release/nack would land before the late renewal,
			// which then replaces visible_at and hides the retry
			// for a full lease. Requeueing only after a confirmed
			// drain keeps the release/nack ordered after every
			// cover call that could overwrite it.
			g.committing.Store(false)
			if !w.joinStaleCoverForRequeue(g.task.ID, cctx) {
				w.opts.Logger.Debug("skipping workflow requeue; cover renewal still in flight, leaving to natural expiry",
					"instance_id", g.p.instanceID, "task_id", g.task.ID)
				if errors.Is(err, backend.ErrConflict) || errors.Is(err, backend.ErrSuperseded) {
					w.dropSticky(g.p.instanceID)
				}
				return
			}
			_ = w.finishWorkflowCommit(ctx, g.task, g.p.baseJournal, g.p.adv, err)
			return
		}
		w.applyStickyAfterCommit(g.p.instanceID, g.p.baseJournal, g.p.adv)
	}
	if batcher, ok := w.backend.(backend.AdvancementBatcher); ok && len(gated)+len(legacy) > 1 {
		advs := make([]backend.Advancement, 0, len(gated)+len(legacy))
		for _, g := range gated {
			advs = append(advs, w.advForCommit(g.task, g.p.adv))
		}
		for _, p := range legacy {
			advs = append(advs, p.adv)
		}
		// Aggregate detached-commit gate (round-24 P1): the batch
		// bypasses the per-item guardedDetachedCommit above, so gate
		// the whole submission here instead. The pre-call gate aborts
		// when ANY member already lost its guard (fallback below
		// re-gates per item and commits only the live ones), and the
		// stashed batch cancel lets a cover loss observed MID-call
		// (any member's renewal failure trips its guard, see
		// tripDetachedGuard) abort a context-aware backend op instead
		// of applying a stale batch by ID/sequence after a peer
		// reclaim. See guardedDetachedBatchCommit.
		bctx, bcancel := context.WithCancel(ctx)
		committed, lost, err := w.guardedDetachedBatchCommit(gated, bcancel, func() error {
			return batcher.CommitAdvancements(bctx, advs)
		})
		bcancel()
		if err != nil {
			if errors.Is(err, errBatchMemberLost) {
				// Partial batch success (round-26 P1b): the store
				// applied the batch, but members in lost lost
				// their guards mid-call (cover failure or expired
				// continuity) and may have been reclaimed by a
				// peer mid-write. Committed members really
				// committed: end their guards and apply sticky
				// exactly like the all-live path below. Lost
				// members are NOT marked committed: no sticky,
				// and the per-item fallback re-gates each one —
				// its gate finds the missing/expired guard and
				// skips the store op — instead of treating the
				// stale apply as success. Legacy batch members
				// carry no guard and were applied with the
				// batch, so they keep the success handling. No
				// store error is recorded: the batch op itself
				// succeeded; the loss is fencing, debug-logged
				// like the per-item errLeaseLost path.
				w.opts.Logger.Debug("marking batch members failed after mid-call lease loss",
					"committed", len(committed), "lost", len(lost))
				for _, g := range committed {
					g.committing.Store(false)
					w.endFlushGuard(g.task.ID, g.p.tok)
					w.applyStickyAfterCommit(g.p.instanceID, g.p.baseJournal, g.p.adv)
				}
				for _, p := range legacy {
					w.applyStickyAfterCommit(p.instanceID, p.baseJournal, p.adv)
				}
				for _, g := range lost {
					commitOne(g)
				}
				return
			}
			if errors.Is(err, errLeaseLost) {
				// Fencing rejection, not a backend failure: the
				// aggregate gate refused the batch before any store
				// op ran (initial cover failure or guard expiry), so
				// — like the per-item path above — it is
				// debug-logged, not counted as a store failure.
				// Recording it would raise false backend-error
				// alerts on routine fencing. The per-item fallback
				// below still re-gates and commits the live members.
				w.opts.Logger.Debug("skipping workflow batch commit; lease lost before the store op",
					"n", len(advs), "err", err)
			} else {
				w.recordStoreError(ctx, "commit_workflow", err, "n", len(advs))
			}
			// One conflict rolls back the whole batch transaction, so fall
			// back to per-instance commits: healthy instances still advance
			// in this tick, and failed items release their leases inside
			// finishWorkflowCommit for immediate re-visibility (independent
			// of LeaseDuration). Each gated fallback re-gates immediately
			// before its own store op (see commitOne), so a guard tripped
			// during the failed batch still skips its write.
			for _, g := range gated {
				commitOne(g)
			}
			for _, p := range legacy {
				if cerr := w.commitWorkflow(ctx, w.taskForCommit(p), p.baseJournal, p.adv); cerr != nil {
					w.recordStoreError(ctx, "commit_workflow", cerr, "instance_id", p.instanceID)
				}
			}
			return
		}
		// Batch applied atomically: end every guard (drop + stop its
		// cover, mirroring guardedDetachedCommit's post-op cleanup) and
		// apply sticky for all members.
		for _, g := range committed {
			g.committing.Store(false)
			w.endFlushGuard(g.task.ID, g.p.tok)
			w.applyStickyAfterCommit(g.p.instanceID, g.p.baseJournal, g.p.adv)
		}
		for _, p := range legacy {
			w.applyStickyAfterCommit(p.instanceID, p.baseJournal, p.adv)
		}
		return
	}
	for _, g := range gated {
		commitOne(g)
	}
	for _, p := range legacy {
		if err := w.commitWorkflow(ctx, w.taskForCommit(p), p.baseJournal, p.adv); err != nil {
			w.recordStoreError(ctx, "commit_workflow", err, "instance_id", p.instanceID)
		}
	}
}

// errBatchMemberLost marks a batch advancement store op that APPLIED
// (returned nil) while at least one member had lost its guard mid-call
// (round-26 P1b). Unlike errLeaseLost — which aborts BEFORE any store op
// runs — the batch already wrote: members that kept their guards committed
// and must keep their result (the caller ends their guards and applies
// sticky), while members that lost theirs must NOT be treated as committed
// (no sticky; the caller's per-item fallback re-gates and skips their store
// op). It is fencing, not a backend failure: like errLeaseLost it is
// debug-logged, never recorded as a store error.
var errBatchMemberLost = errors.New("tasuki: batch applied while a member lost its lease; member not committed")

// guardedDetachedBatchCommit runs one batched advancement store op under
// an aggregate detached-commit gate (round-24 P1). The batch fast path in
// flushWorkflowCommits bypasses the per-item guardedDetachedCommit, so
// without this a cover renewal that failed (or observed an
// expired-continuity deadline) mid-flush only removes its own guard while
// the batch still commits every member by task ID + sequence — deleting a
// peer's reclaimed task. Atomicity is preserved: the batch submits once,
// and the gate is all-or-nothing.
//
// The pre-call gate re-verifies EVERY gated member immediately before the
// store op: any missing/superseded guard, or any continuity deadline
// already passed, aborts the whole submission with errLeaseLost and runs
// nothing (an expired member's guard is dropped and its cover canceled,
// mirroring the single-item gate; the caller's per-item fallback then
// re-gates and commits only the live members). While the op runs, the
// batch cancel is stashed in every member's guard, so a loss observed
// mid-call trips ANY member's guard (see tripDetachedGuard) and aborts a
// context-aware backend op — the cancel is best-effort, as in the
// single-item path: a backend that ignores it still applies, and the
// fallback re-gate remains the backstop. The stash is cleared on return:
// on error the guards stay so the per-item fallback re-gates each member.
//
// A successful op is RECHECKED member-by-member before the caller treats
// anyone as committed (round-26 P1b): a cover renewal that tripped a
// member's guard — or a continuity deadline that passed — while op() was
// blocked only cancels the shared batch context, so a context-ignoring
// batcher still applies the stale batch and returns nil. Without the
// recheck the caller would mark every member committed even though a peer
// may have reclaimed one mid-call. Members that kept a valid guard are
// returned as committed (the caller ends their guards and applies sticky,
// as on the all-live path); members that lost theirs are returned as lost
// with errBatchMemberLost (the caller applies no sticky for them and runs
// the per-item fallback, which re-gates and skips their store op). The
// stale write itself cannot be undone — the recheck bounds the aftermath
// (no committed marking, no sticky, no double apply) instead of
// pretending the batch was clean.
//
// Advancements are row-deleting writes (like Complete/fail with
// exclusive=false above): no writing hold and no pre-write cover join —
// a cover renewal landing after the batch finds no rows, while one
// running during a blocked batch must still observe loss and cancel it
// mid-call.
func (w *Worker) guardedDetachedBatchCommit(gated []flushGuardedCommit, commitCancel context.CancelFunc, op func() error) (committed, lost []flushGuardedCommit, err error) {
	w.detMu.Lock()
	for _, g := range gated {
		gd, ok := w.detGuard[g.task.ID]
		if !ok || gd.epoch != g.p.tok.epoch || gd.seq != g.p.tok.seq {
			w.detMu.Unlock()
			w.opts.Logger.Debug("skipping detached batch commit; lease lost since the pre-commit renewal",
				"task_id", g.task.ID)
			return nil, nil, fmt.Errorf("%w: detached batch gate found lease lost", errLeaseLost)
		}
		if !time.Now().Before(gd.deadline) {
			coverCancel := gd.coverCancel
			delete(w.detGuard, g.task.ID)
			w.detMu.Unlock()
			if coverCancel != nil {
				coverCancel()
			}
			w.opts.Logger.Debug("skipping detached batch commit; continuity deadline passed before the store op",
				"task_id", g.task.ID)
			return nil, nil, fmt.Errorf("%w: detached batch gate found lease expired", errLeaseLost)
		}
	}
	for _, g := range gated {
		gd := w.detGuard[g.task.ID]
		gd.cancel = commitCancel
		w.detGuard[g.task.ID] = gd
	}
	w.detMu.Unlock()
	err = op()
	// Clear the stashed cancel on every path so a later trip cannot
	// cancel an unrelated context. On error the guards stay so the
	// per-item fallback re-gates each member.
	w.detMu.Lock()
	for _, g := range gated {
		if gd, ok := w.detGuard[g.task.ID]; ok && gd.epoch == g.p.tok.epoch && gd.seq == g.p.tok.seq {
			gd.cancel = nil
			w.detGuard[g.task.ID] = gd
		}
	}
	if err != nil {
		w.detMu.Unlock()
		return nil, nil, err
	}
	// Post-op recheck (round-26 P1b, see the doc comment): classify every
	// member while still holding detMu so a trip racing the return is
	// observed, not missed. Members whose guard survived the call
	// committed with it; members whose guard is gone — or whose
	// continuity deadline passed mid-call — did not, even though the
	// batch applied. An expired-but-present member is dropped with its
	// cover canceled here, mirroring the pre-call stale-deadline
	// rejection; a tripped member's guard is already gone.
	now := time.Now()
	var live, failed []flushGuardedCommit
	var coverCancels []context.CancelFunc
	for _, g := range gated {
		gd, ok := w.detGuard[g.task.ID]
		if !ok || gd.epoch != g.p.tok.epoch || gd.seq != g.p.tok.seq {
			failed = append(failed, g)
			continue
		}
		if !now.Before(gd.deadline) {
			coverCancels = append(coverCancels, gd.coverCancel)
			delete(w.detGuard, g.task.ID)
			failed = append(failed, g)
			continue
		}
		live = append(live, g)
	}
	w.detMu.Unlock()
	for _, cancel := range coverCancels {
		if cancel != nil {
			cancel()
		}
	}
	if len(failed) > 0 {
		w.opts.Logger.Debug("detached batch applied while members lost their lease; treating them as failed",
			"committed", len(live), "lost", len(failed))
		return live, failed, errBatchMemberLost
	}
	return live, nil, nil
}

// round28CoverStoreHook pauses Phase 1 success goroutines between the
// renewal return and the fenceMu publish (round-28 P2a regression test
// only; nil in production). It lets the test force the
// success-vs-timeout interleaving deterministically: the renewal has
// returned nil (guard present) but the covered store has not run, so the
// timeout fence trips first and the resuming goroutine must compensate
// instead of marking covered.
var round28CoverStoreHook func()

// startFlushCover keeps every gated flush entry's lease live until the
// flush returns (round-23 P2). Workflow turns run no renewal loop —
// unlike activities, whose extendLeaseLoop hands off to detached cover
// at commit entry — so without this a turn that consumed most of its
// lease, or a CommitAdvancement blocked past the remaining lease, lets
// a peer reclaim mid-flush and the stale ID+sequence commit deletes the
// peer's task.
//
// The FIRST renewal for every entry completes synchronously before this
// returns (round-24 P2): the turn may already be near expiry, and an
// async cover that has not run yet admits the commit below onto a lease
// that a scheduling delay already let lapse — a slow CommitAdvancement
// then applies stale while the late ID-only renewal only observes the
// loss after the fact. Blocking here (bounded by the flush commit context
// and the renewal's own lease-duration timeout, entries in parallel —
// see round-25 P1) means no store op runs until continuity is proven or
// the guard trips. Afterwards each covered entry renews on a half-lease
// ticker via the detached-renewal path (see renewOnceDetached): any
// failure trips the guard so the commit gate aborts instead of writing
// stale, and the loops exit when the flush is over (stop func) or their
// entry commits (flag cleared by the committer). Entries whose initial
// renewal failed start no loop — the gate already excludes them. The
// returned stop func joins every loop (bounded by the same flush context):
// no renewal is in flight when the flush returns.
func (w *Worker) startFlushCover(ctx context.Context, gated []flushGuardedCommit) func() {
	if len(gated) == 0 {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Round-25 P1: rebase every gated entry's cover context onto the flush
	// (commit) context. beginDetachedCommit seeds coverCtx from
	// context.Background so cover outlives execution cancellation, but the
	// flush itself runs under a bounded commit context (see commitContext:
	// CommitTimeout during normal operation) and its documented bound must
	// hold for the whole flush — including cover. renewOnceDetached derives
	// its store-call context via WithTimeout(coverCtx, leaseDuration), so a
	// coverCtx that is a child of the flush ctx yields
	// min(flush deadline, lease-based timeout) for every renewal: a
	// context-aware backend aborts promptly on flush expiry, while a
	// context-ignoring one is cut off by the bounded joins below instead
	// of pinning the polling loop / PollOnce past CommitTimeout. The old
	// background-derived cancel is fired on replacement; no renewal is in
	// flight yet (Phase 1 has not started), so nothing is disturbed.
	w.detMu.Lock()
	for _, g := range gated {
		if gd, ok := w.detGuard[g.task.ID]; ok && gd.epoch == g.p.tok.epoch && gd.seq == g.p.tok.seq {
			oldCancel := gd.coverCancel
			nctx, ncancel := context.WithCancel(ctx)
			gd.coverCtx = nctx
			gd.coverCancel = ncancel
			w.detGuard[g.task.ID] = gd
			if oldCancel != nil {
				oldCancel()
			}
		}
	}
	w.detMu.Unlock()
	// Phase 1: the initial renewal for every entry, in parallel, joined
	// before admitting any write. A failure trips the guard (the
	// backstop trip below covers errors that bypass the internal one)
	// and starts no ticker loop; the commit gate then skips the entry.
	covered := make([]atomic.Bool, len(gated))
	// fenceMu makes the success publish and the timeout fencing atomic
	// (round-28 P2a): without it a renewal that returns success
	// concurrently with the flush context expiring is descheduled
	// before its covered store, the timeout branch observes
	// covered==false and trips the guard, and the goroutine then marks
	// covered without compensation — the commit gates reject the
	// missing guard, but the successful backend-side renewal already
	// hid the unowned task for a full lease. Holding fenceMu across
	// the timedOut check + covered store on the success path and
	// across the timedOut set + covered check + trip collection on the
	// timeout path publishes exactly once: either the success wins
	// (covered, no trip, commit admitted) or the timeout wins
	// (tripped, uncovered, success compensates below).
	var fenceMu sync.Mutex
	timedOut := false
	var initWg sync.WaitGroup
	for i, g := range gated {
		initWg.Add(1)
		// Snapshot the continuity deadline for the stale-call fence
		// below (round-26 P1a): the guard is seeded by
		// beginDetachedCommit from the entry's local lease-expiry
		// estimate, and no success has refreshed it yet. A zero value
		// means no guard was ever seeded, so there is nothing to
		// fence; any non-zero value fences unconditionally (round-27
		// P2b — even a still-fresh lease, since no commit is ever
		// admitted for a tripped entry).
		go func(i int, g flushGuardedCommit, leaseExpiry time.Time) {
			defer initWg.Done()
			// Renew immediately instead of waiting for the first tick:
			// the turn may have consumed most of its lease, and the
			// next tick is a half-lease away (== the original expiry
			// for the first renewal). Waiting would let a peer
			// reclaim the still-committing task mid-commit (same
			// reasoning as renewUntilDone's immediate renewal).
			if err := w.renewOnceDetached(ctx, g.task.ID, g.p.tok); err != nil {
				w.tripDetachedGuard(g.task.ID, g.p.tok)
				// Fence the survived call (round-26 P1a, see
				// compensateStaleCoverRenewal): when the commit
				// context expires while this renewal is still
				// blocked in a context-ignoring backend, the
				// bounded join below trips the guard and lets the
				// flush return with the call still in flight. Its
				// result is already dropped (a missing guard
				// reports loss without refreshing, see
				// renewOnceDetached), but its backend-side ID-only
				// write may still land afterwards and re-hide the
				// task for a full lease with no renewal loop left
				// to own it.
				w.compensateStaleCoverRenewal(ctx, g.task, leaseExpiry)
				return
			}
			if round28CoverStoreHook != nil {
				round28CoverStoreHook()
			}
			fenceMu.Lock()
			if timedOut {
				fenceMu.Unlock()
				// Lost the race to the timeout fence (round-28
				// P2a): the guard is already tripped and no
				// commit will ever be admitted for this entry,
				// but this successful backend-side renewal
				// already extended the lease. Compensate with
				// the same fenced release as the failure path
				// so the task becomes reclaimable promptly
				// instead of sitting unowned and hidden for a
				// full lease. covered stays false so Phase 2
				// starts no loop for the tripped entry.
				w.compensateStaleCoverRenewal(ctx, g.task, leaseExpiry)
				return
			}
			covered[i].Store(true)
			fenceMu.Unlock()
		}(i, g, w.detGuardDeadline(g.task.ID, g.p.tok))
	}
	// Bounded join (round-25 P1): the pre-fix initWg.Wait held the flush
	// past CommitTimeout when ExtendLease stalled in a context-ignoring
	// backend. Waiting on the flush context instead bounds the whole Phase
	// 1 by the documented flush bound; with a Background flush ctx (no
	// deadline, e.g. older tests) the Done channel is nil and the wait is
	// effectively unbounded, as before. On give-up every still-unproven
	// entry is tripped so its gate skips without touching the store —
	// continuity was never proven — and a late renewal landing after the
	// trip finds the missing guard and reports loss without refreshing
	// (see renewOnceDetached), so it cannot resurrect the entry. A late
	// backend-side landing is fenced separately: the Phase 1 wrapper
	// always issues a token-fenced compensation release (see
	// compensateStaleCoverRenewal) — no commit is ever admitted for a
	// tripped entry, so the survived call cannot re-hide the task past
	// return (round-26 P1a, extended round-27 P2b to fresh leases).
	initDone := make(chan struct{})
	go func() {
		initWg.Wait()
		close(initDone)
	}()
	select {
	case <-initDone:
	case <-ctx.Done():
		// Round-28 P2a: collect the still-unproven entries under
		// fenceMu so a concurrent success publish cannot slip
		// between the covered check and the trip (see fenceMu
		// above). Trips fire after unlocking; detMu is never held
		// across fenceMu and vice versa, so the order cannot invert.
		fenceMu.Lock()
		timedOut = true
		var toTrip []flushGuardedCommit
		for i, g := range gated {
			if !covered[i].Load() {
				toTrip = append(toTrip, g)
			}
		}
		fenceMu.Unlock()
		for _, g := range toTrip {
			w.tripDetachedGuard(g.task.ID, g.p.tok)
		}
	}
	// Phase 2: periodic cover for the entries proven live above.
	coverDone := make(chan struct{})
	var coverWg sync.WaitGroup
	for i, g := range gated {
		if !covered[i].Load() {
			continue
		}
		coverWg.Add(1)
		go func(taskID int64, tok claimToken, flag *atomic.Bool) {
			defer coverWg.Done()
			d := w.leaseDuration() / 2
			if d <= 0 {
				return
			}
			ticker := time.NewTicker(d)
			defer ticker.Stop()
			for {
				select {
				case <-coverDone:
					return
				case <-ticker.C:
					if !flag.Load() {
						return
					}
					// Any failure trips the guard (see
					// renewOnceDetached): continuity can no longer be
					// guaranteed, so the per-commit gate aborts
					// instead of running an ID-only write that could
					// delete a peer's reclaimed task — and the loop
					// exits so no further renewal can extend a
					// moved-on lease. The trip is the backstop for
					// errors that bypass the internal one, and a
					// no-op when the commit already finished.
					if err := w.renewOnceDetached(ctx, taskID, tok); err != nil {
						w.tripDetachedGuard(taskID, tok)
						return
					}
				}
			}
		}(g.task.ID, g.p.tok, g.committing)
	}
	return func() {
		close(coverDone)
		// Bounded join (round-25 P1): a periodic cover renewal stalled
		// in a context-ignoring backend must not hold the flush past
		// its commit bound after the store ops already returned. With
		// a Background flush ctx the Done channel is nil and the wait
		// is effectively unbounded, as before.
		joinDone := make(chan struct{})
		go func() {
			coverWg.Wait()
			close(joinDone)
		}()
		select {
		case <-joinDone:
		case <-ctx.Done():
		}
		for _, g := range gated {
			g.committing.Store(false)
			// Backstop for entries whose commit never ran (batch
			// success path ends guards explicitly; skipped
			// fallbacks drop theirs in guardedDetachedCommit):
			// with the guard gone no renewal may extend the lease,
			// and the cover context stops so a blocked renewal
			// aborts instead of landing after the result write.
			w.dropDetachedGuard(g.task.ID, g.p.tok)
		}
	}
}

// detGuardDeadline reports the continuity deadline of taskID's detached
// guard when it still carries tok, or the zero time when the guard is
// missing or superseded. Callers snapshot it to fence a renewal that may
// still be in flight after the guard is gone (see
// compensateStaleCoverRenewal).
func (w *Worker) detGuardDeadline(taskID int64, tok claimToken) time.Time {
	w.detMu.Lock()
	defer w.detMu.Unlock()
	if gd, ok := w.detGuard[taskID]; ok && gd.epoch == tok.epoch && gd.seq == tok.seq {
		return gd.deadline
	}
	return time.Time{}
}

// compensateStaleCoverRenewal fences a detached cover renewal whose call
// survived its guard's teardown (round-26 P1a, extended round-27 P2b).
// When the flush commit context expires while an initial renewal is still
// blocked in a context-ignoring backend, the bounded Phase 1 join trips
// the guard and lets the flush return with the backend call still in
// flight: its result is dropped on return (see renewOnceDetached), but
// the ID-only ExtendLease may still land afterwards. Landing on our own
// lease re-hides the task for a full lease with no renewal loop left to
// own it, delaying peer reclaim until the extension lapses — and no
// commit was admitted for the entry (a tripped entry's gates all skip),
// so the task would sit unowned and hidden.
//
// This therefore ALWAYS issues a best-effort token-fenced ReleaseLease
// undoing such an extension so the task becomes reclaimable promptly
// instead — even when the original local lease still looks fresh
// (round-27 P2b): a renewal that lands just before the original expiry
// still moved visible_at a full lease out on a task no commit will ever
// reference (every successor gate re-checks the tripped guard and
// skips), and the fence keeps the release safe pre-reclaim too. No peer
// can hold the lease before the original expiry (reclaim requires
// expiry), so the release either undoes our own extension or — when a
// peer did reclaim after expiry — is rejected with ErrNotFound without
// disturbing the peer's fresh lease; a deleted (committed) row reports
// ErrNotFound the same way. Both rejections are quiet.
//
// Like the other fencing paths this is debug-logged, never recorded as a
// store error: a routine teardown race must not raise backend-error alerts.
func (w *Worker) compensateStaleCoverRenewal(ctx context.Context, task backend.Task, leaseExpiry time.Time) {
	if leaseExpiry.IsZero() {
		return
	}
	relCtx, cancel := w.releaseContext(ctx)
	defer cancel()
	if err := w.backend.ReleaseLease(relCtx, task); err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			w.opts.Logger.Debug("stale cover renewal compensation skipped; lease already moved on",
				"task_id", task.ID)
			return
		}
		w.opts.Logger.Debug("stale cover renewal compensation release failed",
			"task_id", task.ID, "err", err)
		return
	}
	w.opts.Logger.Debug("released lease after stale cover renewal; task reclaimable",
		"task_id", task.ID)
}

// endFlushGuard applies guardedDetachedCommit's post-op cleanup for one
// flush entry committed outside that helper (the batch fast path): the
// guard is dropped and this commit's cover context is canceled so no
// cover renewal can land after the result write (a post-delete
// ExtendLease would report ErrNotFound noisily, or worse, extend a
// same-ID row on a backend that recycles IDs).
func (w *Worker) endFlushGuard(taskID int64, tok claimToken) {
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
}

// taskForCommit resolves the fenced release token for a pending commit.
// Production paths (handleWorkflow) populate p.task; legacy/test paths that
// only set adv fall back to an adv-derived task (workflow routing without
// ownership fencing).
func (w *Worker) taskForCommit(p pendingWorkflowCommit) backend.Task {
	if p.task.ID != 0 || p.task.InstanceID != "" {
		return p.task
	}
	return backend.Task{
		ID:         p.adv.TaskID,
		Kind:       "workflow",
		InstanceID: p.instanceID,
	}
}

func (w *Worker) applyStickyAfterCommit(instanceID string, baseJournal []journal.Event, adv backend.Advancement) {
	if adv.Terminal != nil {
		w.dropSticky(instanceID)
		return
	}
	newNext := adv.ExpectedSeq
	for _, e := range adv.NewEvents {
		if e.Seq+1 > newNext {
			newNext = e.Seq + 1
		}
	}
	updated := append(append([]journal.Event{}, baseJournal...), adv.NewEvents...)
	w.setSticky(instanceID, updated, newNext)
}
