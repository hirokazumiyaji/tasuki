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
	err := w.backend.CommitAdvancement(ctx, adv)
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
	// stamped by tickWorkflows when the turn completes. The flush transfers
	// ownership out via claimCommitOwnership before touching the store (see
	// skipStaleCommit), so a Shutdown releaseInFlight racing the flush wins
	// exactly once: either the release wins and the flush skips, or the
	// flush wins and the release finds nothing. hasTok distinguishes
	// production pendings from older/test call sites that only set adv
	// (which commit ungated, as before).
	tok    claimToken
	hasTok bool
}

func (w *Worker) flushWorkflowCommits(ctx context.Context, pending []pendingWorkflowCommit) {
	if len(pending) == 0 {
		return
	}
	// Ownership gate (round-21 P2): a Shutdown timeout + restart may have
	// released a finished pending turn (removing it from the in-flight set)
	// while a peer re-claimed the task; the flush runs on a detached commit
	// ctx that outlives the shutdown, and backends validate the advancement
	// by task ID + sequence alone — so committing the stale advancement
	// would delete the peer's active task after duplicate execution. Skip
	// entries that lost ownership instead of touching the store.
	owned := make([]pendingWorkflowCommit, 0, len(pending))
	for _, p := range pending {
		if w.skipStaleCommit(p) {
			continue
		}
		owned = append(owned, p)
	}
	pending = owned
	if len(pending) == 0 {
		return
	}
	if batcher, ok := w.backend.(backend.AdvancementBatcher); ok && len(pending) > 1 {
		advs := make([]backend.Advancement, len(pending))
		for i, p := range pending {
			advs[i] = p.adv
		}
		if err := batcher.CommitAdvancements(ctx, advs); err != nil {
			w.recordStoreError(ctx, "commit_workflow", err, "n", len(pending))
			// One conflict rolls back the whole batch transaction, so fall
			// back to per-instance commits: healthy instances still advance
			// in this tick, and failed items release their leases inside
			// commitWorkflow for immediate re-visibility (independent of
			// LeaseDuration).
			for _, p := range pending {
				if cerr := w.commitWorkflow(ctx, w.taskForCommit(p), p.baseJournal, p.adv); cerr != nil {
					w.recordStoreError(ctx, "commit_workflow", cerr, "instance_id", p.instanceID)
				}
			}
			return
		}
		for _, p := range pending {
			w.applyStickyAfterCommit(p.instanceID, p.baseJournal, p.adv)
		}
		return
	}
	for _, p := range pending {
		if err := w.commitWorkflow(ctx, w.taskForCommit(p), p.baseJournal, p.adv); err != nil {
			w.recordStoreError(ctx, "commit_workflow", err, "instance_id", p.instanceID)
		}
	}
}

// skipStaleCommit gates one pending workflow commit on in-flight ownership
// (round-21 P2, ownsWorkflowCommit-style). It transfers the entry out via
// claimCommitOwnership — atomically with the ownership check under mu — so
// the entry is gone before the store op runs and a concurrent Shutdown
// releaseInFlight cannot hand the committing task to a peer mid-commit.
// It reports true when the commit must be skipped: Shutdown already
// released the lease (entry absent — a peer may own it now), or a restart
// re-tracked the same task ID under a new token (entry mismatched — the new
// generation owns it). Skipped entries issue no store op and need no local
// cleanup: absent entries were already released, and mismatched entries
// belong to the live owner. Pendings without a production token (older or
// test call sites carrying only adv) commit ungated, as before.
func (w *Worker) skipStaleCommit(p pendingWorkflowCommit) bool {
	if !p.hasTok {
		return false
	}
	t := w.taskForCommit(p)
	if w.claimCommitOwnership(t.ID, p.tok) {
		return false
	}
	w.opts.Logger.Debug("skipping stale workflow commit; lease already released",
		"instance_id", p.instanceID, "task_id", t.ID)
	return true
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
