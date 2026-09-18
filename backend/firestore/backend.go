package firestore

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	gcf "cloud.google.com/go/firestore"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{MaxAdvancementEffects: 400}
}
func (b *Backend) col(name string) *gcf.CollectionRef  { return b.client.Collection(name) }
func (b *Backend) ref(col, id string) *gcf.DocumentRef { return b.col(col).Doc(id) }

func instanceDoc(inst backend.NewInstance, queue string, now time.Time) map[string]any {
	m := map[string]any{
		"id": inst.ID, "name": inst.Name, "queue": queue, "status": "running",
		"input": jsonString(inst.Input), "next_seq": int64(2), "created_at": now, "updated_at": now,
		"search_attributes": searchAttrsDoc(inst.SearchAttributes),
		"memo":              searchAttrsDoc(inst.Memo),
	}
	if inst.ParentID != "" {
		m["parent_id"] = inst.ParentID
	}
	if inst.ParentSeq != 0 {
		m["parent_seq"] = inst.ParentSeq
	}
	return m
}
func workflowTaskDoc(id, queue string, taskID int64, now time.Time) map[string]any {
	return map[string]any{"id": taskID, "kind": "workflow", "queue": queue, "instance_id": id, "attempt": int64(0), "visible_at": now, "created_at": now}
}
func journalDoc(id string, seq int64, ev journal.Event, now time.Time) map[string]any {
	return map[string]any{"instance_id": id, "seq": seq, "type": string(ev.Type), "name": ev.Name, "ref_seq": ev.RefSeq, "payload": jsonString(ev.Payload), "recorded_at": now}
}
func activityTaskDoc(t backend.NewTask, id int64, now time.Time) map[string]any {
	q := t.Queue
	if q == "" {
		q = "default"
	}
	p, _ := json.Marshal(activityPayload{Name: t.Name, Input: t.Input, Retry: retryJSON{InitialIntervalMs: t.Retry.InitialInterval.Milliseconds(), BackoffCoefficient: t.Retry.BackoffCoefficient, MaxIntervalMs: t.Retry.MaxInterval.Milliseconds(), MaxAttempts: t.MaxAttempts}, StartToCloseTimeoutMs: t.StartToCloseTimeout.Milliseconds()})
	return map[string]any{"id": id, "kind": "activity", "queue": q, "instance_id": t.InstanceID, "ref_seq": t.Seq, "payload": string(p), "attempt": int64(0), "max_attempts": int64(t.MaxAttempts), "visible_at": now, "created_at": now}
}
func inboxDoc(instanceID string, id, seq int64, ev journal.Event, now time.Time) map[string]any {
	return map[string]any{"instance_id": instanceID, "id": id, "seq": seq, "type": string(ev.Type), "ref_seq": ev.RefSeq, "payload": inboxPayload(ev), "created_at": now}
}

// inboxSeqAlloc tracks per-instance inbox sequence allocations within one
// Firestore transaction. Base values are seeded during the read phase
// (Firestore requires all reads before writes) and flushed after writes.
// The counter lives in the wf_inbox_seq collection, kept off wf_instances so
// signal appends never contend with advancement commits on the instance doc.
type inboxSeqAlloc struct {
	base    map[string]int64
	used    map[string]int64
	existed map[string]bool
}

func newInboxSeqAlloc() *inboxSeqAlloc {
	return &inboxSeqAlloc{base: map[string]int64{}, used: map[string]int64{}, existed: map[string]bool{}}
}

func (a *inboxSeqAlloc) seed(instanceID string, v int64, existed bool) {
	if _, ok := a.base[instanceID]; ok {
		return
	}
	a.base[instanceID] = v
	a.used[instanceID] = 0
	a.existed[instanceID] = existed
}

func (a *inboxSeqAlloc) next(instanceID string) int64 {
	a.used[instanceID]++
	return a.base[instanceID] + a.used[instanceID]
}

// seedInboxSeqTx pre-reads an instance's inbox counter within tx.
func seedInboxSeqTx(b *Backend, tx *gcf.Transaction, a *inboxSeqAlloc, instanceID string) error {
	snap, err := tx.Get(b.ref("wf_inbox_seq", instanceID))
	if isNotFound(err) {
		a.seed(instanceID, 0, false)
		return nil
	}
	if err != nil {
		return err
	}
	a.seed(instanceID, i64(snap.Data(), "n"), true)
	return nil
}

func (b *Backend) flushInboxSeqs(tx *gcf.Transaction, a *inboxSeqAlloc) error {
	for id, used := range a.used {
		if used == 0 {
			continue
		}
		ref := b.ref("wf_inbox_seq", id)
		var err error
		if a.existed[id] {
			err = tx.Update(ref, []gcf.Update{{Path: "n", Value: a.base[id] + used}})
		} else {
			err = tx.Create(ref, map[string]any{"n": a.base[id] + used})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// InboxSeq reports the raw per-instance inbox sequence counter for tests
// (found=false when the counter does not exist).
func (b *Backend) InboxSeq(ctx context.Context, instanceID string) (int64, bool, error) {
	snap, err := b.ref("wf_inbox_seq", instanceID).Get(ctx)
	if isNotFound(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return i64(snap.Data(), "n"), true, nil
}

func (b *Backend) CreateInstance(ctx context.Context, inst backend.NewInstance) error {
	q := inst.Queue
	if q == "" {
		q = "default"
	}
	now := nowUTC()
	err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		r := b.ref("wf_instances", inst.ID)
		s, err := tx.Get(r)
		if err != nil && !isNotFound(err) {
			return err
		}
		if err == nil && s.Exists() {
			return backend.ErrAlreadyExists
		}
		if err := tx.Create(r, instanceDoc(inst, q, now)); err != nil {
			return err
		}
		if err := tx.Create(b.ref("wf_journal", journalID(inst.ID, 1)), journalDoc(inst.ID, 1, journal.Event{Type: journal.TypeWorkflowStarted, Name: inst.Name, Payload: inst.Input}, now)); err != nil {
			return err
		}
		return tx.Create(b.ref("wf_tasks", wfTaskID(inst.ID)), workflowTaskDoc(inst.ID, q, newID(), now))
	})
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}
func (b *Backend) GetInstance(ctx context.Context, id string) (*backend.Instance, error) {
	s, err := b.ref("wf_instances", id).Get(ctx)
	if isNotFound(err) {
		return nil, backend.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if !s.Exists() {
		return nil, backend.ErrNotFound
	}
	return decodeInstance(s.Data()), nil
}
func (b *Backend) GetJournal(ctx context.Context, id string, after int64) ([]journal.Event, error) {
	it := b.col("wf_journal").Where("instance_id", "==", id).Where("seq", ">", after).OrderBy("seq", gcf.Asc).Documents(ctx)
	defer it.Stop()
	var out []journal.Event
	for {
		s, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		m := s.Data()
		out = append(out, journal.Event{Seq: i64(m, "seq"), Type: journal.Type(str(m, "type")), Name: str(m, "name"), RefSeq: i64(m, "ref_seq"), Payload: bytes(m, "payload")})
	}
	return out, nil
}
func (b *Backend) ListInstances(ctx context.Context, f backend.InstanceFilter) ([]backend.Instance, error) {
	it := b.col("wf_instances").Documents(ctx)
	defer it.Stop()
	var all []backend.Instance
	for {
		s, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		x := decodeInstance(s.Data())
		if (f.Status == "" || x.Status == f.Status) && (f.Name == "" || x.Name == f.Name) &&
			backend.MatchesSearchAttributes(x.SearchAttributes, f.SearchAttributes) {
			all = append(all, *x)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	if f.Offset >= len(all) {
		return nil, nil
	}
	all = all[f.Offset:]
	n := f.Limit
	if n <= 0 {
		n = 100
	}
	if len(all) > n {
		all = all[:n]
	}
	return all, nil
}
func (b *Backend) TerminateInstance(ctx context.Context, id string) error {
	now := nowUTC()
	// Status flips inside a small transaction (one read + one update) so the
	// write count never scales with the instance's task/timer/dedupe rows.
	// Child documents are swept afterwards in paged batches: a single
	// transaction deleting them would breach the 500-write limit once dedupe
	// keys accumulate (DynamoDB parity: status update first, paged deletes).
	err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		s, err := tx.Get(b.ref("wf_instances", id))
		if isNotFound(err) {
			return backend.ErrNotFound
		}
		if err != nil {
			return err
		}
		if !s.Exists() {
			return backend.ErrNotFound
		}
		return tx.Update(b.ref("wf_instances", id), []gcf.Update{{Path: "status", Value: "terminated"}, {Path: "updated_at", Value: now}, {Path: "completed_at", Value: now}})
	})
	if err != nil {
		return err
	}
	// Await the sweep before returning so SendToInbox with a previously seen
	// DedupeID correctly inserts anew (conformance SignalDedupe) and claimed
	// tasks observe no leftovers. Terminal instances are immutable, so the
	// non-transactional sweep cannot race with advancement commits; purge
	// reaps anything left by a failed sweep.
	if err := b.sweepTerminateDocs(ctx, id); err != nil {
		return err
	}
	b.notifyTerminal(id)
	return nil
}

func (b *Backend) CountClaimableTasks(ctx context.Context, kind string, queues []string) (map[string]int64, error) {
	if len(queues) == 0 {
		return map[string]int64{}, nil
	}
	now := nowUTC()
	out := map[string]int64{}
	for _, q := range queues {
		it := b.col("wf_tasks").
			Where("kind", "==", kind).
			Where("queue", "==", q).
			Where("visible_at", "<=", now).
			Documents(ctx)
		var n int64
		for {
			_, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				it.Stop()
				return nil, err
			}
			n++
		}
		it.Stop()
		if n > 0 {
			out[q] = n
		}
	}
	return out, nil
}

func (b *Backend) ClaimTasks(ctx context.Context, req backend.ClaimRequest) ([]backend.Task, error) {
	if req.Limit <= 0 {
		req.Limit = 1
	}
	if len(req.Queues) == 0 {
		return nil, nil
	}
	now := nowUTC()
	var out []backend.Task
	for _, q := range req.Queues {
		if len(out) >= req.Limit {
			break
		}
		it := b.col("wf_tasks").Where("kind", "==", req.Kind).Where("queue", "==", q).Where("visible_at", "<=", now).OrderBy("visible_at", gcf.Asc).Limit(req.Limit - len(out)).Documents(ctx)
		for {
			d, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				it.Stop()
				return nil, err
			}
			old := timestamp(d.Data(), "visible_at")
			var claimed backend.Task
			err = b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
				s, e := tx.Get(d.Ref)
				if isNotFound(e) {
					return backend.ErrConflict
				}
				if e != nil {
					return e
				}
				if !s.Exists() || !timestamp(s.Data(), "visible_at").Equal(old) {
					return backend.ErrConflict
				}
				m := s.Data()
				claimed = decodeTask(m)
				claimed.Attempt++
				claimed.VisibleAt = now.Add(req.Lease)
				claimed.WorkerID = req.WorkerID
				return tx.Update(d.Ref, []gcf.Update{{Path: "visible_at", Value: claimed.VisibleAt}, {Path: "worker_id", Value: req.WorkerID}, {Path: "attempt", Value: int64(claimed.Attempt)}})
			})
			if err == backend.ErrConflict {
				continue
			}
			if err != nil {
				it.Stop()
				return nil, err
			}
			out = append(out, claimed)
		}
		it.Stop()
	}
	return out, nil
}
func (b *Backend) updateTask(ctx context.Context, id int64, activity bool, fields []gcf.Update) error {
	r := b.ref("wf_tasks", actTaskID(id))
	return b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		s, e := tx.Get(r)
		if isNotFound(e) {
			return backend.ErrNotFound
		}
		if e != nil {
			return e
		}
		if !s.Exists() || (activity && str(s.Data(), "kind") != "activity") {
			return backend.ErrNotFound
		}
		return tx.Update(r, fields)
	})
}
func (b *Backend) ExtendLease(ctx context.Context, id int64, d time.Duration) error {
	return b.updateTask(ctx, id, false, []gcf.Update{{Path: "visible_at", Value: nowUTC().Add(d)}})
}

func (b *Backend) RecordHeartbeat(ctx context.Context, taskID int64, lease time.Duration, details []byte) error {
	fields := []gcf.Update{{Path: "visible_at", Value: nowUTC().Add(lease)}}
	if details != nil {
		fields = append(fields, gcf.Update{Path: "heartbeat", Value: string(details)})
	}
	return b.updateTask(ctx, taskID, false, fields)
}
func (b *Backend) ReleaseLease(ctx context.Context, id int64) error {
	if err := b.updateTask(ctx, id, false, []gcf.Update{{Path: "visible_at", Value: nowUTC()}, {Path: "worker_id", Value: gcf.Delete}}); err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}
func (b *Backend) NackTask(ctx context.Context, t backend.Task, delay time.Duration) error {
	ref := b.ref("wf_tasks", actTaskID(t.ID))
	if t.Kind == "workflow" {
		ref = b.ref("wf_tasks", wfTaskID(t.InstanceID))
	}
	err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		s, e := tx.Get(ref)
		if isNotFound(e) {
			return backend.ErrNotFound
		}
		if e != nil {
			return e
		}
		if !s.Exists() {
			return backend.ErrNotFound
		}
		return tx.Update(ref, []gcf.Update{{Path: "visible_at", Value: nowUTC().Add(delay)}, {Path: "worker_id", Value: gcf.Delete}})
	})
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}
func (b *Backend) RetryActivity(ctx context.Context, id int64, delay time.Duration) error {
	return b.updateTask(ctx, id, true, []gcf.Update{{Path: "visible_at", Value: nowUTC().Add(delay)}, {Path: "worker_id", Value: gcf.Delete}})
}
func (b *Backend) LoadWorkflowHead(ctx context.Context, id string) (*backend.WorkflowState, error) {
	inst, err := b.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	st := &backend.WorkflowState{Instance: *inst, NextSeq: inst.NextSeq, Now: nowUTC()}
	it := b.col("wf_inbox").Where("instance_id", "==", id).Documents(ctx)
	defer it.Stop()
	entries := make([]backend.InboxEntry, 0)
	for {
		d, e := it.Next()
		if e == iterator.Done {
			break
		}
		if e != nil {
			return nil, e
		}
		m := d.Data()
		name, p := unwrapInboxPayload(bytes(m, "payload"))
		entries = append(entries, backend.InboxEntry{
			Seq:       i64(m, "seq"),
			CreatedAt: timestamp(m, "created_at").UnixNano(),
			ID:        i64(m, "id"),
			Event:     journal.Event{Type: journal.Type(str(m, "type")), Name: name, RefSeq: i64(m, "ref_seq"), Payload: p},
		})
	}
	backend.SortInbox(entries)
	for _, entry := range entries {
		st.Inbox = append(st.Inbox, backend.InboxEvent{ID: entry.ID, Event: entry.Event})
	}
	return st, nil
}

func (b *Backend) LoadWorkflow(ctx context.Context, id string) (*backend.WorkflowState, error) {
	st, err := b.LoadWorkflowHead(ctx, id)
	if err != nil {
		return nil, err
	}
	j, err := b.GetJournal(ctx, id, 0)
	if err != nil {
		return nil, err
	}
	st.Journal = j
	return st, nil
}

func (b *Backend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	return b.CommitAdvancements(ctx, []backend.Advancement{adv})
}

type advancementPrep struct {
	instRef  *gcf.DocumentRef
	taskRef  *gcf.DocumentRef
	inst     *backend.Instance
	hasInbox bool
}

func (b *Backend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	if len(advs) == 0 {
		return nil
	}
	now := nowUTC()
	err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		// Firestore requires all reads before any writes in a transaction.
		alloc := newInboxSeqAlloc()
		preps := make([]advancementPrep, len(advs))
		for i, adv := range advs {
			prep, err := b.readAdvancementTx(tx, adv, alloc)
			if err != nil {
				return err
			}
			preps[i] = prep
		}
		for i, adv := range advs {
			if err := b.writeAdvancementTx(tx, adv, preps[i], now, alloc); err != nil {
				return err
			}
		}
		return b.flushInboxSeqs(tx, alloc)
	})
	if err != nil {
		return err
	}
	for _, adv := range advs {
		if adv.ParentNotify != nil {
			inst, _ := b.GetInstance(ctx, adv.InstanceID)
			if inst != nil && inst.ParentID != "" {
				if err := b.ensureWorkflowTask(ctx, inst.ParentID); err != nil {
					return err
				}
			}
		}
		if adv.EnsureWorkflowTask {
			if err := b.ensureWorkflowTaskForced(ctx, adv.InstanceID); err != nil {
				return err
			}
			continue
		}
		if err := b.ensureWorkflowTask(ctx, adv.InstanceID); err != nil {
			return err
		}
	}
	b.notifyTasks()
	for _, adv := range advs {
		if adv.Terminal != nil {
			// Dedupe rows are deliberately cleaned outside the advancement
			// transaction: a terminal commit with hundreds of dedupe keys
			// would otherwise exceed the 500-write transaction limit.
			// Best-effort (DynamoDB parity); leftovers are reaped by purge.
			_ = b.sweepSignalDedupe(context.Background(), adv.InstanceID)
			b.notifyTerminal(adv.InstanceID)
		}
	}
	return nil
}

func (b *Backend) readAdvancementTx(tx *gcf.Transaction, adv backend.Advancement, alloc *inboxSeqAlloc) (advancementPrep, error) {
	instSnap, err := tx.Get(b.ref("wf_instances", adv.InstanceID))
	if isNotFound(err) {
		return advancementPrep{}, backend.ErrConflict
	}
	if err != nil {
		return advancementPrep{}, err
	}
	if !instSnap.Exists() || i64(instSnap.Data(), "next_seq") != adv.ExpectedSeq {
		return advancementPrep{}, backend.ErrConflict
	}
	taskRef := b.ref("wf_tasks", wfTaskID(adv.InstanceID))
	taskSnap, err := tx.Get(taskRef)
	if isNotFound(err) {
		return advancementPrep{}, backend.ErrConflict
	}
	if err != nil {
		return advancementPrep{}, err
	}
	if !taskSnap.Exists() || i64(taskSnap.Data(), "id") != adv.TaskID {
		return advancementPrep{}, backend.ErrConflict
	}
	inst := decodeInstance(instSnap.Data())
	if adv.ParentNotify != nil && inst.ParentID != "" {
		if err := seedInboxSeqTx(b, tx, alloc, inst.ParentID); err != nil {
			return advancementPrep{}, err
		}
	}
	drained := make(map[int64]struct{}, len(adv.DrainedInbox))
	for _, id := range adv.DrainedInbox {
		drained[id] = struct{}{}
	}
	hasInbox := false
	inboxIter := tx.Documents(b.col("wf_inbox").Where("instance_id", "==", adv.InstanceID))
	for {
		inbox, nextErr := inboxIter.Next()
		if nextErr == iterator.Done {
			break
		}
		if nextErr != nil {
			inboxIter.Stop()
			return advancementPrep{}, nextErr
		}
		if _, ok := drained[i64(inbox.Data(), "id")]; !ok {
			hasInbox = true
		}
	}
	inboxIter.Stop()
	return advancementPrep{instRef: instSnap.Ref, taskRef: taskRef, inst: inst, hasInbox: hasInbox}, nil
}

func (b *Backend) writeAdvancementTx(tx *gcf.Transaction, adv backend.Advancement, prep advancementPrep, now time.Time, alloc *inboxSeqAlloc) error {
	inst := prep.inst
	next := adv.ExpectedSeq
	for _, e := range adv.NewEvents {
		if e.Seq >= next {
			next = e.Seq + 1
		}
	}
	updates := []gcf.Update{{Path: "next_seq", Value: next}, {Path: "updated_at", Value: now}}
	if adv.Terminal != nil {
		updates = append(updates, gcf.Update{Path: "status", Value: adv.Terminal.Status}, gcf.Update{Path: "result", Value: jsonString(adv.Terminal.Result)}, gcf.Update{Path: "failure", Value: jsonString(adv.Terminal.Failure)}, gcf.Update{Path: "completed_at", Value: now})
	}
	if backend.HasSearchAttributesUpdate(adv.NewEvents) {
		updates = append(updates, gcf.Update{
			Path:  "search_attributes",
			Value: searchAttrsDoc(backend.LastSearchAttributesUpdate(adv.NewEvents)),
		})
	}
	if backend.HasMemoUpdate(adv.NewEvents) {
		updates = append(updates, gcf.Update{
			Path:  "memo",
			Value: searchAttrsDoc(backend.LastMemoUpdate(adv.NewEvents)),
		})
	}
	if err := tx.Update(prep.instRef, updates); err != nil {
		return err
	}
	for _, e := range adv.NewEvents {
		if err := tx.Create(b.ref("wf_journal", journalID(adv.InstanceID, e.Seq)), journalDoc(adv.InstanceID, e.Seq, e, now)); err != nil {
			return err
		}
	}
	for _, at := range adv.ActivityTasks {
		id := newID()
		if err := tx.Create(b.ref("wf_tasks", actTaskID(id)), activityTaskDoc(at, id, now)); err != nil {
			return err
		}
	}
	for _, tm := range adv.Timers {
		if err := tx.Create(b.ref("wf_timers", journalID(adv.InstanceID, tm.Seq)), map[string]any{"instance_id": adv.InstanceID, "seq": tm.Seq, "fire_at": tm.FireAt.UTC(), "created_at": now}); err != nil {
			return err
		}
	}
	for _, id := range adv.DrainedInbox {
		if err := tx.Delete(b.ref("wf_inbox", inboxID(adv.InstanceID, id))); err != nil {
			return err
		}
	}
	for _, ch := range adv.Children {
		q := ch.Queue
		if q == "" {
			q = "default"
		}
		if err := tx.Create(b.ref("wf_instances", ch.ID), instanceDoc(ch, q, now)); err != nil {
			return err
		}
		if err := tx.Create(b.ref("wf_journal", journalID(ch.ID, 1)), journalDoc(ch.ID, 1, journal.Event{Type: journal.TypeWorkflowStarted, Name: ch.Name, Payload: ch.Input}, now)); err != nil {
			return err
		}
		if err := tx.Create(b.ref("wf_tasks", wfTaskID(ch.ID)), workflowTaskDoc(ch.ID, q, newID(), now)); err != nil {
			return err
		}
	}
	if adv.ParentNotify != nil && inst.ParentID != "" {
		ev := *adv.ParentNotify
		if ev.RefSeq == 0 {
			ev.RefSeq = inst.ParentSeq
		}
		id := newID()
		seq := alloc.next(inst.ParentID)
		if err := tx.Create(b.ref("wf_inbox", inboxID(inst.ParentID, id)), inboxDoc(inst.ParentID, id, seq, ev, now)); err != nil {
			return err
		}
	}
	if prep.hasInbox && adv.Terminal == nil {
		return tx.Set(prep.taskRef, workflowTaskDoc(adv.InstanceID, inst.Queue, newID(), now))
	}
	if adv.EnsureWorkflowTask && adv.Terminal == nil {
		return tx.Set(prep.taskRef, workflowTaskDoc(adv.InstanceID, inst.Queue, newID(), now))
	}
	return tx.Delete(prep.taskRef)
}

func (b *Backend) ensureWorkflowTask(ctx context.Context, instanceID string) error {
	return b.ensureWorkflowTaskWithForce(ctx, instanceID, false)
}

func (b *Backend) ensureWorkflowTaskForced(ctx context.Context, instanceID string) error {
	return b.ensureWorkflowTaskWithForce(ctx, instanceID, true)
}

func (b *Backend) ensureWorkflowTaskWithForce(ctx context.Context, instanceID string, force bool) error {
	inst, err := b.GetInstance(ctx, instanceID)
	if err != nil {
		if err == backend.ErrNotFound {
			return nil
		}
		return err
	}
	if inst.Status != "running" {
		return nil
	}
	if !force {
		it := b.col("wf_inbox").Where("instance_id", "==", instanceID).Limit(1).Documents(ctx)
		_, err = it.Next()
		it.Stop()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
	}
	_, err = b.ref("wf_tasks", wfTaskID(instanceID)).Create(ctx, workflowTaskDoc(instanceID, inst.Queue, newID(), nowUTC()))
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}

func (b *Backend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	now := nowUTC()
	var instanceID string
	err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		alloc := newInboxSeqAlloc()
		r := b.ref("wf_tasks", actTaskID(taskID))
		task, err := tx.Get(r)
		if isNotFound(err) {
			return backend.ErrSuperseded
		}
		if err != nil {
			return err
		}
		if !task.Exists() || str(task.Data(), "kind") != "activity" {
			return backend.ErrSuperseded
		}
		m := task.Data()
		instanceID = str(m, "instance_id")
		inst, err := tx.Get(b.ref("wf_instances", instanceID))
		if isNotFound(err) {
			return backend.ErrSuperseded
		}
		if err != nil {
			return err
		}
		if !inst.Exists() {
			return backend.ErrSuperseded
		}
		if str(inst.Data(), "status") == "running" {
			if err := seedInboxSeqTx(b, tx, alloc, instanceID); err != nil {
				return err
			}
		}
		if err = tx.Delete(r); err != nil {
			return err
		}
		if str(inst.Data(), "status") != "running" {
			return nil
		}
		if ev.RefSeq == 0 {
			ev.RefSeq = i64(m, "ref_seq")
		}
		id := newID()
		seq := alloc.next(instanceID)
		if err := tx.Create(b.ref("wf_inbox", inboxID(instanceID, id)), inboxDoc(instanceID, id, seq, ev, now)); err != nil {
			return err
		}
		return b.flushInboxSeqs(tx, alloc)
	})
	if err != nil {
		return err
	}
	if err := b.ensureWorkflowTask(ctx, instanceID); err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}
func (b *Backend) SendToInbox(ctx context.Context, instanceID string, ev journal.Event, dedupeID string) error {
	return b.SendToInboxBatch(ctx, instanceID, []backend.InboxItem{{Event: ev, DedupeID: dedupeID}})
}

func (b *Backend) SendToInboxBatch(ctx context.Context, instanceID string, items []backend.InboxItem) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) > backend.InboxBatchLimit(b.Capabilities()) {
		return backend.ErrBatchTooLarge
	}
	inst, err := b.GetInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	now := nowUTC()
	var inserted int
	err = b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		inserted = 0
		// Read the parent inside the transaction: PurgeInstances deletes
		// wf_instances after sweeping children, and Firestore aborts a
		// transaction whose read documents changed, so an in-flight send
		// either commits before the purge deletes the parent (its documents
		// are reaped by the purge's second sweep) or retries into this
		// ErrNotFound branch. Without this read a send could create inbox
		// rows for an instance that no longer exists.
		isnap, err := tx.Get(b.ref("wf_instances", instanceID))
		if isNotFound(err) {
			return backend.ErrNotFound
		}
		if err != nil {
			return err
		}
		if !isnap.Exists() {
			return backend.ErrNotFound
		}
		alloc := newInboxSeqAlloc()
		if err := seedInboxSeqTx(b, tx, alloc, instanceID); err != nil {
			return err
		}
		// Firestore requires all reads before writes; also skip same-batch DedupeID dups.
		skip := make([]bool, len(items))
		created := map[string]bool{}
		for i, it := range items {
			if it.DedupeID == "" {
				continue
			}
			if created[it.DedupeID] {
				skip[i] = true
				continue
			}
			dref := b.ref("wf_signal_dedupe", signalDedupeID(instanceID, it.DedupeID))
			snap, err := tx.Get(dref)
			if err != nil && !isNotFound(err) {
				return err
			}
			if err == nil && snap.Exists() {
				skip[i] = true
				continue
			}
			created[it.DedupeID] = true
		}
		for i, it := range items {
			if skip[i] {
				continue
			}
			if it.DedupeID != "" {
				if err := tx.Create(b.ref("wf_signal_dedupe", signalDedupeID(instanceID, it.DedupeID)), map[string]any{
					"instance_id": instanceID,
					"dedupe_id":   it.DedupeID,
					"created_at":  now,
				}); err != nil {
					return err
				}
			}
			id := newID()
			seq := alloc.next(instanceID)
			if err := tx.Create(b.ref("wf_inbox", inboxID(instanceID, id)), inboxDoc(instanceID, id, seq, it.Event, now)); err != nil {
				return err
			}
			inserted++
		}
		return b.flushInboxSeqs(tx, alloc)
	})
	if err != nil {
		return err
	}
	// Even on pure dedupe hits, ensure a task when inbox remains: a prior
	// crash between commit and ensure must not stall the instance forever.
	if inst.Status == "running" {
		_ = b.ensureWorkflowTask(ctx, instanceID)
	}
	if inserted == 0 {
		return nil
	}
	if inst.Status == "running" {
		if err := b.ensureWorkflowTask(ctx, instanceID); err != nil {
			return err
		}
	}
	b.notifyTasks()
	return nil
}

// RecoverOrphanedWorkflowTasks re-creates workflow tasks for running
// instances with inbox events but no workflow task (crash between inbox
// commit and ensureWorkflowTask). notify is only a hint, so recovery is
// persistent via the durable task row.
//
// The scan resumes from a persisted document cursor on each pass and rotates
// through the fleet, so orphans beyond the per-call bound are eventually
// visited instead of starving behind the first page on every pass. Only
// newly created tasks are counted: instances that already have a task are
// skipped without inflating the recovered count (and its log line).
func (b *Backend) RecoverOrphanedWorkflowTasks(ctx context.Context) (int, error) {
	b.recoverMu.Lock()
	cursor := b.recoverCursor
	b.recoverMu.Unlock()
	const bound = 200
	q := b.col("wf_instances").Where("status", "==", "running").OrderBy(gcf.DocumentID, gcf.Asc).Limit(bound)
	if cursor != "" {
		q = q.StartAfter(cursor)
	}
	it := q.Documents(ctx)
	defer it.Stop()
	recovered := 0
	last := ""
	exhausted := false
	for {
		s, err := it.Next()
		if err == iterator.Done {
			exhausted = true
			break
		}
		if err != nil {
			return recovered, err
		}
		id := s.Ref.ID
		if id == "" {
			if mid, _ := s.Data()["id"].(string); mid != "" {
				id = mid
			}
		}
		last = id
		inboxIt := b.col("wf_inbox").Where("instance_id", "==", id).Limit(1).Documents(ctx)
		_, err = inboxIt.Next()
		inboxIt.Stop()
		if err == iterator.Done {
			continue
		}
		if err != nil {
			continue
		}
		// Check task existence first: ensureWorkflowTask reports success
		// even when the task already exists, which would miscount healthy
		// instances as recovered on every pass.
		tsnap, terr := b.ref("wf_tasks", wfTaskID(id)).Get(ctx)
		if terr == nil && tsnap.Exists() {
			continue
		}
		if terr != nil && !isNotFound(terr) {
			continue
		}
		if err := b.ensureWorkflowTask(ctx, id); err == nil {
			recovered++
		}
	}
	b.recoverMu.Lock()
	if exhausted {
		// Full fleet visited: restart from the beginning next pass.
		b.recoverCursor = ""
	} else {
		b.recoverCursor = last
	}
	b.recoverMu.Unlock()
	return recovered, nil
}
func (b *Backend) FireDueTimers(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 1
	}
	it := b.col("wf_timers").Where("fire_at", "<=", nowUTC()).OrderBy("fire_at", gcf.Asc).Limit(limit).Documents(ctx)
	defer it.Stop()
	n := 0
	for {
		d, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return n, err
		}
		m := d.Data()
		id := str(m, "instance_id")
		seq := i64(m, "seq")
		claimed := false
		err = b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
			alloc := newInboxSeqAlloc()
			s, e := tx.Get(d.Ref)
			if isNotFound(e) {
				return backend.ErrConflict
			}
			if e != nil {
				return e
			}
			if !s.Exists() {
				return backend.ErrConflict
			}
			inst, e := tx.Get(b.ref("wf_instances", id))
			if isNotFound(e) {
				return tx.Delete(d.Ref)
			}
			if e != nil {
				return e
			}
			if inst.Exists() && str(inst.Data(), "status") == "running" {
				if e = seedInboxSeqTx(b, tx, alloc, id); e != nil {
					return e
				}
			}
			if e = tx.Delete(d.Ref); e != nil {
				return e
			}
			if !inst.Exists() || str(inst.Data(), "status") != "running" {
				return nil
			}
			inbox := newID()
			next := alloc.next(id)
			if e = tx.Create(b.ref("wf_inbox", inboxID(id, inbox)), inboxDoc(id, inbox, next, journal.Event{Type: journal.TypeTimerFired, RefSeq: seq}, nowUTC())); e != nil {
				return e
			}
			return b.flushInboxSeqs(tx, alloc)
		})
		if err == backend.ErrConflict {
			continue
		}
		if err != nil {
			return n, err
		}
		claimed = true
		if claimed {
			n++
			if err = b.ensureWorkflowTask(ctx, id); err != nil {
				return n, err
			}
		}
	}
	if n > 0 {
		b.notifyTasks()
	}
	return n, nil
}
