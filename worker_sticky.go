package tasuki

import (
	"context"
	"errors"
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
	return w.finishWorkflowCommit(ctx, task, baseJournal, adv, w.backend.CommitAdvancement(ctx, adv))
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
		if errors.Is(err, backend.ErrConflict) {
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
	w.wfClaimMu.Lock()
	defer w.wfClaimMu.Unlock()
	if w.wfClaim == nil {
		w.wfClaim = map[int64]time.Time{}
	}
	w.wfClaim[taskID] = time.Now()
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
	if errors.Is(herr, backend.ErrConflict) || errors.Is(herr, backend.ErrSuperseded) {
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
			w.opts.Logger.Debug("skipping stale workflow commit; lease expired or already released",
				"instance_id", p.instanceID, "task_id", t.ID)
			continue
		}
		flag := &atomic.Bool{}
		if !w.beginDetachedCommit(t.ID, p.tok, flag) {
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
	// post-handling (sticky + fenced requeue).
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
			return w.backend.CommitAdvancement(cctx, g.p.adv)
		})
		if err != nil {
			if errors.Is(err, errLeaseLost) {
				w.opts.Logger.Debug("skipping workflow commit; lease lost before the store op",
					"instance_id", g.p.instanceID, "task_id", g.task.ID, "err", err)
				return
			}
			w.recordStoreError(ctx, "commit_workflow", err, "instance_id", g.p.instanceID)
			_ = w.finishWorkflowCommit(ctx, g.task, g.p.baseJournal, g.p.adv, err)
			return
		}
		w.applyStickyAfterCommit(g.p.instanceID, g.p.baseJournal, g.p.adv)
	}
	if batcher, ok := w.backend.(backend.AdvancementBatcher); ok && len(gated)+len(legacy) > 1 {
		advs := make([]backend.Advancement, 0, len(gated)+len(legacy))
		for _, g := range gated {
			advs = append(advs, g.p.adv)
		}
		for _, p := range legacy {
			advs = append(advs, p.adv)
		}
		if err := batcher.CommitAdvancements(ctx, advs); err != nil {
			w.recordStoreError(ctx, "commit_workflow", err, "n", len(advs))
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
		for _, g := range gated {
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

// startFlushCover keeps every gated flush entry's lease live until the
// flush returns (round-23 P2). Workflow turns run no renewal loop —
// unlike activities, whose extendLeaseLoop hands off to detached cover
// at commit entry — so without this a turn that consumed most of its
// lease, or a CommitAdvancement blocked past the remaining lease, lets
// a peer reclaim mid-flush and the stale ID+sequence commit deletes the
// peer's task. Each entry renews immediately (the turn may already be
// near expiry; waiting half a lease would reclaim first) and then on a
// half-lease ticker via the detached-renewal path (see
// renewOnceDetached): any failure trips the guard so the per-commit
// gate above aborts instead of writing stale, and the loops exit when
// the flush is over (stop func) or their entry commits (flag cleared by
// the committer). The returned stop func joins every loop: no renewal
// is in flight when the flush returns.
func (w *Worker) startFlushCover(ctx context.Context, gated []flushGuardedCommit) func() {
	if len(gated) == 0 {
		return func() {}
	}
	coverDone := make(chan struct{})
	var coverWg sync.WaitGroup
	for _, g := range gated {
		coverWg.Add(1)
		go func(taskID int64, tok claimToken, flag *atomic.Bool) {
			defer coverWg.Done()
			// Renew immediately instead of waiting for the first tick:
			// the turn may have consumed most of its lease, and the
			// next tick is a half-lease away (== the original expiry
			// for the first renewal). Waiting would let a peer
			// reclaim the still-committing task mid-commit (same
			// reasoning as renewUntilDone's immediate renewal).
			if err := w.renewOnceDetached(ctx, taskID, tok); err != nil {
				w.tripDetachedGuard(taskID, tok)
				return
			}
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
		coverWg.Wait()
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
