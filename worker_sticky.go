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

func (w *Worker) commitWorkflow(ctx context.Context, instanceID string, baseJournal []journal.Event, adv backend.Advancement) error {
	err := w.backend.CommitAdvancement(ctx, adv)
	if err != nil {
		if errors.Is(err, backend.ErrConflict) {
			w.dropSticky(instanceID)
		}
		return err
	}
	w.applyStickyAfterCommit(instanceID, baseJournal, adv)
	return nil
}

type pendingWorkflowCommit struct {
	instanceID  string
	baseJournal []journal.Event
	adv         backend.Advancement
}

func (w *Worker) flushWorkflowCommits(ctx context.Context, pending []pendingWorkflowCommit) {
	if len(pending) == 0 {
		return
	}
	if batcher, ok := w.backend.(backend.AdvancementBatcher); ok && len(pending) > 1 {
		advs := make([]backend.Advancement, len(pending))
		for i, p := range pending {
			advs[i] = p.adv
		}
		if err := batcher.CommitAdvancements(ctx, advs); err != nil {
			for _, p := range pending {
				w.dropSticky(p.instanceID)
			}
			w.recordStoreError(ctx, "commit_workflow", err, "n", len(pending))
			return
		}
		for _, p := range pending {
			w.applyStickyAfterCommit(p.instanceID, p.baseJournal, p.adv)
		}
		return
	}
	for _, p := range pending {
		if err := w.commitWorkflow(ctx, p.instanceID, p.baseJournal, p.adv); err != nil {
			w.recordStoreError(ctx, "commit_workflow", err, "instance_id", p.instanceID)
		}
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
