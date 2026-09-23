package tasuki

import (
	"context"
	"errors"
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
	instanceID := task.InstanceID
	if instanceID == "" {
		instanceID = adv.InstanceID
	}
	// Stamp the claim generation for legacy/test paths that built adv
	// without it (see Advancement): production handleWorkflow already
	// stamps, but taskForCommit callers may carry only adv. A fenced commit
	// that lost its lease reports ErrConflict/ErrNotFound and must not be
	// retried — the worker treats it as lost (see requeueWorkflowTask).
	adv = w.advForCommit(task, adv)
	err := w.backend.CommitAdvancement(ctx, adv)
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

// refreshWfClaim moves a workflow task's local claim time forward after a
// successful lease renewal (see extendLeaseLoop), so the delayed nack in
// requeueWorkflowTask measures staleness against the renewed lease — not
// the original claim. Unknown IDs are ignored: activity turns share the
// renewal loop but never enter wfClaim, and flushed claims were already
// dropped by clearWfClaims. Without the refresh, a turn renewed past its
// original claim time that then fails non-contentiously skips its nack
// (the precheck sees the original time as expired) and stays hidden
// until lease expiry instead of backing off for a retry delay.
func (w *Worker) refreshWfClaim(taskID int64) {
	w.wfClaimMu.Lock()
	defer w.wfClaimMu.Unlock()
	if _, ok := w.wfClaim[taskID]; ok {
		w.wfClaim[taskID] = time.Now()
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
	// A fenced commit that lost its lease reports ErrConflict (generation
	// mismatch) or ErrNotFound (task gone): the turn is lost, release
	// immediately for fast replay (fenced, so a peer's fresh lease is
	// untouched) and never retry the commit. Other failures back off via
	// delayed nack.
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
	// task is the claimed task awaiting commit (ownership token for fenced
	// lease release on commit failure). The commit stays tracked under its
	// ID until flush succeeds (see tickWorkflows); failures return in the
	// failed subset so the caller can untrack or release. Older call sites
	// may leave it zero; taskForCommit then falls back to adv-derived
	// routing without fencing.
	task backend.Task
}

// flushWorkflowCommits commits pending advancements and returns the subset
// whose commit did not succeed. Successful commits are untracked: the task
// is deleted by the commit and must no longer be visible to
// releaseInFlight. Failed commits stay tracked for the caller to dispose
// (release when the flush context was canceled, untrack otherwise).
//
// Entries that lost in-flight ownership before the flush (see
// ownsWorkflowCommit) are skipped without touching the store and returned
// in the failed subset: Shutdown's releaseInFlight may have released a
// finished pending turn mid-flush and a peer may have re-claimed it, and
// backends validate the advancement by task ID alone, so committing it
// would delete the peer's active task after duplicate execution. The
// caller's disposal is ownership-gated (see claimWorkflowRelease), so a
// skipped entry is never released twice.
func (w *Worker) flushWorkflowCommits(ctx context.Context, pending []pendingWorkflowCommit) []pendingWorkflowCommit {
	if len(pending) == 0 {
		return nil
	}
	owned, skipped := pending[:0:0], pending[:0:0]
	for _, p := range pending {
		if w.ownsWorkflowCommit(p.task) {
			owned = append(owned, p)
		} else {
			w.opts.Logger.Debug("skipping stale workflow commit; lease already released",
				"instance_id", p.instanceID, "task_id", p.adv.TaskID)
			skipped = append(skipped, p)
		}
	}
	failed := append([]pendingWorkflowCommit(nil), skipped...)
	untrack := func(p pendingWorkflowCommit) {
		w.untrack(p.adv.TaskID)
	}
	if batcher, ok := w.backend.(backend.AdvancementBatcher); ok && len(owned) > 1 {
		advs := make([]backend.Advancement, len(owned))
		for i, p := range owned {
			advs[i] = w.advForCommit(w.taskForCommit(p), p.adv)
		}
		if err := batcher.CommitAdvancements(ctx, advs); err != nil {
			w.recordStoreError(ctx, "commit_workflow", err, "n", len(owned))
			// One conflict rolls back the whole batch transaction, so fall
			// back to per-instance commits over the owned subset only:
			// healthy instances still advance in this tick, and each
			// commit disposes its own failure inside commitWorkflow
			// (sticky drop on conflict, fenced release/nack via
			// requeueWorkflowTask for immediate re-visibility). Failures
			// still return below so the caller untracks them — or issues
			// an ownership-gated detached release when the tick is
			// canceled (the in-commit requeue may have run on a canceled
			// context and been rejected before touching the store).
			// Skipped (stale) entries never reach the store (see above).
			for _, p := range owned {
				if cerr := w.commitWorkflow(ctx, w.taskForCommit(p), p.baseJournal, p.adv); cerr != nil {
					w.recordStoreError(ctx, "commit_workflow", cerr, "instance_id", p.instanceID)
					failed = append(failed, p)
					continue
				}
				untrack(p)
			}
			return failed
		}
		for _, p := range owned {
			w.applyStickyAfterCommit(p.instanceID, p.baseJournal, p.adv)
			untrack(p)
		}
		return failed
	}
	for _, p := range owned {
		if err := w.commitWorkflow(ctx, w.taskForCommit(p), p.baseJournal, p.adv); err != nil {
			w.recordStoreError(ctx, "commit_workflow", err, "instance_id", p.instanceID)
			failed = append(failed, p)
			continue
		}
		untrack(p)
	}
	return failed
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

// advForCommit resolves the fenced commit advancement for a pending commit:
// the claim generation (worker + attempt) travels in the Advancement itself
// so the backend can condition the transactional commit on it (see
// Advancement). Production handleWorkflow already stamps; legacy/test
// advancements built without generation inherit the pending task's token
// here, keeping the worker preflight as fast path and the backend fence as
// the atomic check-to-commit guard. A zero WorkerID stays unfenced for
// older callers.
func (w *Worker) advForCommit(task backend.Task, adv backend.Advancement) backend.Advancement {
	if adv.WorkerID == "" && task.WorkerID != "" {
		adv.WorkerID = task.WorkerID
		adv.Attempt = task.Attempt
	}
	return adv
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
