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
	return backend.Capabilities{MaxAdvancementEffects: 400, FairDispatch: true}
}
func (b *Backend) col(name string) *gcf.CollectionRef  { return b.client.Collection(name) }
func (b *Backend) ref(col, id string) *gcf.DocumentRef { return b.col(col).Doc(id) }

// probeDedupeKey fetches one stored dedupe/marker key under both doc-ID
// framings (see selectOwnedDedupeDoc): both existing rows plus per-framing
// availability for guard creation. A row at either framing blocks creation
// there (a Create over it would collide) but counts as a match only when
// owned by this instance AND guarding the requested ID (see
// matchOwnedDedupeRow).
func (b *Backend) probeDedupeKey(tx *gcf.Transaction, col, instanceID, key string) (dedupeKeyProbe, error) {
	var framed, legacy map[string]any
	framedID := frameDedupeDocID(instanceID, key)
	for _, docID := range dedupeDocIDs(instanceID, []string{key}) {
		snap, err := tx.Get(b.ref(col, docID))
		if err != nil && !isNotFound(err) {
			return dedupeKeyProbe{}, err
		}
		if err == nil && snap.Exists() {
			if docID == framedID {
				framed = snap.Data()
			} else {
				legacy = snap.Data()
			}
		}
	}
	return dedupeKeyProbe{framedDoc: framed, legacyDoc: legacy, framedFree: framed == nil, legacyFree: legacy == nil}, nil
}

func instanceDoc(inst backend.NewInstance, queue string, now time.Time) map[string]any {
	m := map[string]any{
		"id": inst.ID, "name": inst.Name, "queue": queue, "status": "running",
		"input": jsonString(inst.Input), "next_seq": int64(2), "created_at": now, "updated_at": now,
		incarnationField:    newIncarnation(),
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
		// Fence ID reuse while crash recovery is pending: a previous
		// incarnation's purge wrote its marker atomically with the victim
		// delete and has not finished its trailing sweep (the marker is
		// cleared only after the second sweep/reap). Creating a replacement
		// now would let it consume the old incarnation's leftover inbox
		// rows long before its own purge. Fail fast so the caller retries;
		// the next PurgeInstances resumes the crashed cleanup via the
		// marker (sweep + clear) and unblocks the ID. The check rides in
		// this same transaction: the victim-delete transaction is the
		// serialization point, so a delete committing after this read
		// aborts the create on the conflicting instance-row write, and a
		// delete that committed first leaves its marker visible here.
		msnap, merr := tx.Get(b.ref(purgeMarkersCollection, inst.ID))
		if merr != nil && !isNotFound(merr) {
			return merr
		}
		if merr == nil && msnap.Exists() {
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
	// Status flips inside a small transaction so the write count never scales
	// with the instance's task/timer/dedupe rows. Child documents are swept
	// afterwards in paged batches: a single transaction deleting them would
	// breach the 500-write limit once dedupe keys accumulate (DynamoDB parity:
	// status update first, paged deletes).
	// Dedupe keys are snapshotted INSIDE the flip transaction (read phase,
	// before the update): the snapshot is serializable, so a post-terminal
	// SendToInbox committing after the flip — its marker plus inbox event —
	// is never in the snapshot and survives the sweep, while pre-termination
	// keys are reaped (Codex round 8 on #327: an unqualified sweep deleted
	// post-terminal retry markers while leaving their inbox events, so the
	// next retry re-inserted a duplicate). Markers in the snapshot itself are
	// filtered out as well (a redundant TerminateInstance after terminal
	// sends must not strip them); purge reaps all leftovers.
	var dedupeSnapshot []string
	var createdAt time.Time
	var incarnation string
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
		// Capture the pre-termination incarnation for the post-commit sweep
		// fence (see sweepTerminateDocs): created_at is written once by
		// CreateInstance and never updated, so a doc carrying a different
		// value after a purge is a replacement whose documents the sweep
		// must never touch. The incarnation token pins the same identity
		// against clock rollback, VM restore, and precision truncation,
		// which can all reproduce the same created_at (see newIncarnation).
		createdAt = timestamp(s.Data(), "created_at")
		incarnation = str(s.Data(), incarnationField)
		ids, err := listSignalDedupeIDsTx(tx, b.col("wf_signal_dedupe"), id)
		if err != nil {
			return err
		}
		dedupeSnapshot = ids
		return tx.Update(b.ref("wf_instances", id), []gcf.Update{{Path: "status", Value: "terminated"}, {Path: "updated_at", Value: now}, {Path: "completed_at", Value: now}})
	})
	if err != nil {
		return err
	}
	// Await the sweep before returning so SendToInbox with a previously seen
	// DedupeID correctly inserts anew (conformance SignalDedupe) and claimed
	// tasks observe no leftovers. Terminal instances are immutable, so the
	// sweep cannot race with advancement commits — but it can race with a
	// purge that deletes the instance and lets CreateInstance reuse the ID,
	// which the incarnation fence aborts on (see sweepTerminateDocs); purge
	// reaps anything left by a failed or fenced sweep.
	// The status flip above already committed, so subscribers must wake even
	// when the sweep fails: GetInstance permanently reports terminated while
	// a skipped notifyTerminal would leave waiters asleep until a retry.
	if err := b.sweepTerminateDocs(ctx, purgeVictim{id: id, createdAt: createdAt, incarnation: incarnation}, dedupeSnapshot); err != nil {
		b.notifyTerminal(id)
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
	// Share one picker across queues so MaxPerInstance caps the whole claim
	// batch, not each queue independently.
	var picker *backend.FairPicker
	if req.MaxPerInstance > 0 {
		picker = backend.NewFairPicker(req.Limit, req.MaxPerInstance)
	}
	for _, q := range req.Queues {
		if len(out) >= req.Limit {
			break
		}
		if picker != nil && picker.Full() {
			break
		}
		// skip holds IDs already attempted from this queue. A conflicted
		// snapshot's stale index entry can resurface on a re-query before
		// Firestore converges, so refills must exclude attempted IDs
		// instead of reselecting the same stale entry. It stays small:
		// only attempted (picked) IDs are recorded, never every examined
		// row, and entries behind the committed cursor are pruned on each
		// refill (see below), so it is bounded by the current window's
		// attempts rather than the whole stale backlog.
		skip := map[int64]struct{}{}
		// cursor carries the (visible_at, __name__) scan position across
		// conflict refills within this queue: each window is fetched once
		// per ClaimTasks call instead of restarting from the head on
		// every refill. exhausted marks the index end so a refill never
		// restarts from nil and the loop terminates.
		var cursor *gcf.DocumentSnapshot
		exhausted := false
		for len(out) < req.Limit && (picker == nil || !picker.Full()) && !exhausted {
			cands, next, done, err := b.listClaimCandidates(ctx, req.Kind, q, now, req.Limit-len(out), picker, skip, cursor)
			if err != nil {
				return nil, err
			}
			cursor, exhausted = next, done
			// Prune attempted IDs behind the committed cursor: the
			// (visible_at, __name__) scan is forward-only, so documents
			// before the resume point cannot recur on the next refill.
			// Every existing entry sorts before next — picks come from
			// documents at or before the resume document and earlier
			// windows are further behind — so dropping them cannot
			// reselect, and the next fetch starts after next. Only the
			// current window's attempts are re-added below, bounding skip
			// to O(batch) under prolonged index lag instead of O(stale
			// backlog). Termination is unchanged: exhausted plus the
			// empty/release breaks below.
			clear(skip)
			if len(cands) == 0 {
				break
			}
			released := false
			for _, d := range cands {
				m := d.Data()
				id := i64(m, "id")
				skip[id] = struct{}{}
				old := timestamp(m, "visible_at")
				var claimed backend.Task
				skipped := false
				err = b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
					// The transaction function may run more than once; reset
					// per-attempt outcome state on entry.
					claimed = backend.Task{}
					skipped = false
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
					// Fence against TerminateInstance: never lease a task whose
					// instance already left running. Reading the instance doc
					// inside the claim transaction also conflicts with a
					// concurrent status flip, restoring the exclusion the
					// pre-chunk single-transaction terminate had. A stale
					// terminal task is removed in the same transaction so
					// later polls (and the refill pass below) reach live
					// tasks; a commit conflict drops the delete and the next
					// poll retries.
					instID := str(m, "instance_id")
					isnap, e := tx.Get(b.ref("wf_instances", instID))
					if e != nil && !isNotFound(e) {
						return e
					}
					if e == nil && isnap.Exists() && str(isnap.Data(), "status") == "running" {
						claimed = decodeTask(m)
						claimed.Attempt++
						claimed.VisibleAt = now.Add(req.Lease)
						claimed.WorkerID = req.WorkerID
						return tx.Update(d.Ref, []gcf.Update{{Path: "visible_at", Value: claimed.VisibleAt}, {Path: "worker_id", Value: req.WorkerID}, {Path: "attempt", Value: int64(claimed.Attempt)}})
					}
					skipped = true
					return tx.Delete(d.Ref)
				})
				if err == backend.ErrConflict {
					// Another worker leased this snapshot first: free its picker
					// slot so Full below does not stop later candidates and
					// queues from filling the batch, then re-query this queue
					// for a replacement (the loop above) instead of moving on.
					// The refill flag is set even without a picker (Codex round
					// 8 on #327): a default nil-picker claim must also loop to
					// fill Limit instead of returning undersized after one pass.
					if picker != nil {
						picker.Release(backend.FairTaskRef{ID: id, InstanceID: str(m, "instance_id")})
					}
					released = true
					continue
				}
				if err != nil {
					return nil, err
				}
				if skipped {
					// Terminal residue deleted above; free its picker slot
					// so Full does not stop later candidates and queues
					// from filling the batch, then re-query this queue
					// for a replacement (the loop above) instead of
					// moving on. The refill flag is set even without a picker
					// (Codex round 8 on #327): deleting a stale task must loop
					// until Limit is filled or candidates are exhausted.
					if picker != nil {
						picker.Release(backend.FairTaskRef{ID: id, InstanceID: str(m, "instance_id")})
					}
					released = true
					continue
				}
				out = append(out, claimed)
				if len(out) >= req.Limit {
					break
				}
			}
			if !released {
				break
			}
		}
	}
	return out, nil
}

// listClaimCandidates returns FIFO-ordered task snapshots for one queue.
// With a nil picker it pages the (kind, queue, visible_at, __name__)
// composite index Limit-at-a-time, advancing with the caller's cursor across
// refills; otherwise it pages the same index in
// FairOverfetch windows feeding the shared picker, so a victim hidden behind
// a flooding instance is still found beyond the first page. Pages advance
// with a document cursor over (visible_at, __name__) ordering — each query
// fetches only its own window instead of reprocessing all preceding
// documents, and concurrently leased rows do not shift later pages the way
// offsets do. The DocumentID tie-breaker keeps the order deterministic when
// tasks share the same visible_at. The picker is shared across the outer
// queue loop in ClaimTasks so the per-instance cap applies to the whole claim
// batch, and only snapshots picked during this call are returned (earlier
// queues' picks are not re-attempted). Callers claim the returned snapshots
// with a visible_at re-check inside a transaction; concurrently leased rows
// conflict, must be released from the picker via Release (so Full does not
// stop later queues), and are skipped; the caller then re-queries for
// replacements, passing attempted IDs in skip so a stale index entry is never
// reselected.
//
// No unbounded dedup set is kept here: the StartAfter cursor advances
// monotonically over (visible_at, __name__), so each matching document is
// visited exactly once per scan and repeats are impossible without concurrent
// writes shifting page boundaries. The only cross-row state is byID, which
// retains snapshots solely for picker-accepted candidates (bounded by the
// batch size) and doubles as a guard against double-offering an accepted ID
// if a concurrent update ever surfaces a duplicate within one scan.
//
// The caller threads cursor through conflict refills (it is both the resume
// point and, via exhausted, the termination signal), so refills continue past
// already-consumed windows instead of re-fetching the prefix: every window is
// read once per ClaimTasks call and the loop ends when the index is
// exhausted. When the batch fills mid-window the cursor points after the last
// examined document (not the window end), so a refill re-examines the
// unexamined window suffix instead of skipping it. Attempted IDs stay in skip
// so a stale index entry of a released snapshot is never reselected after its
// slot is freed; only the current window's attempts are retained, entries
// behind the committed cursor being pruned on each refill (they cannot recur
// past the forward-only resume point), which bounds skip to O(batch).
// Trade-off: documents rejected by the fair cap before a
// conflict freed a slot are picked up on a later poll rather than in the same
// call; liveness holds because they stay claimable.
func (b *Backend) listClaimCandidates(ctx context.Context, kind, queue string, now time.Time, remaining int, picker *backend.FairPicker, skip map[int64]struct{}, cursor *gcf.DocumentSnapshot) ([]*gcf.DocumentSnapshot, *gcf.DocumentSnapshot, bool, error) {
	base := b.col("wf_tasks").Where("kind", "==", kind).Where("queue", "==", queue).Where("visible_at", "<=", now).OrderBy("visible_at", gcf.Asc).OrderBy(gcf.DocumentID, gcf.Asc)
	collect := func(it *gcf.DocumentIterator) ([]*gcf.DocumentSnapshot, error) {
		defer it.Stop()
		var docs []*gcf.DocumentSnapshot
		for {
			d, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}
			docs = append(docs, d)
		}
		return docs, nil
	}
	if picker == nil {
		// Nil-picker path pages with the same (visible_at, __name__) cursor
		// the caller threads across refills (Codex round 8 on #327): without
		// it a refill after deleting a terminal task would re-fetch the same
		// head window on every pass (one poll per stale row with big
		// backlogs) instead of advancing. The resume point is the last
		// FETCHED document and exhaustion is a short page, mirroring the
		// picker path.
		q := base.Limit(remaining)
		if cursor != nil {
			q = q.StartAfter(cursor)
		}
		docs, err := collect(q.Documents(ctx))
		if err != nil {
			return nil, cursor, false, err
		}
		if len(docs) == 0 {
			return nil, cursor, true, nil
		}
		next := docs[len(docs)-1]
		exhausted := len(docs) < remaining
		if len(skip) == 0 {
			return docs, next, exhausted, nil
		}
		kept := docs[:0]
		for _, d := range docs {
			if _, ok := skip[i64(d.Data(), "id")]; !ok {
				kept = append(kept, d)
			}
		}
		return kept, next, exhausted, nil
	}
	pageSize := backend.FairOverfetch(remaining)
	// byID retains snapshots only for picker-accepted candidates so
	// rejected backlog scanned past an over-quota flood does not accumulate
	// in memory.
	// fresh counts the picks made during this call: the shared picker may
	// already hold earlier queues' picks, which must not be re-attempted
	// (a re-attempt would conflict with our own claim and wrongly release
	// an already-successful pick).
	byID := map[int64]*gcf.DocumentSnapshot{}
	fresh := len(picker.Picked())
	startAfter := cursor
	exhausted := false
	for !picker.Full() {
		q := base.Limit(pageSize)
		if startAfter != nil {
			q = q.StartAfter(startAfter)
		}
		docs, err := collect(q.Documents(ctx))
		if err != nil {
			return nil, startAfter, false, err
		}
		if len(docs) == 0 {
			exhausted = true
			break
		}
		examined := -1
		for i, d := range docs {
			examined = i
			m := d.Data()
			id := i64(m, "id")
			if _, ok := skip[id]; ok {
				continue
			}
			if _, ok := byID[id]; ok {
				continue
			}
			before := len(picker.Picked())
			full := picker.Offer(backend.FairTaskRef{ID: id, InstanceID: str(m, "instance_id")})
			if len(picker.Picked()) > before {
				byID[id] = d
			}
			if full {
				break
			}
		}
		// Resume after the last EXAMINED document, not the last fetched
		// one, so a refill after a claim conflict re-examines the
		// unexamined window suffix instead of skipping it. When the whole
		// window was consumed this is the last document, as before.
		startAfter = docs[examined]
		if picker.Full() {
			// Batch filled: unscanned documents may remain behind.
			break
		}
		if len(docs) < pageSize {
			exhausted = true
			break
		}
	}
	picked := picker.Picked()[fresh:]
	docs := make([]*gcf.DocumentSnapshot, 0, len(picked))
	for _, r := range picked {
		if d, ok := byID[r.ID]; ok {
			docs = append(docs, d)
		}
	}
	return docs, startAfter, exhausted, nil
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
func (b *Backend) ReleaseLease(ctx context.Context, t backend.Task) error {
	// Workflow tasks live under WF#<instanceID> (not ACT#<id>), so route by
	// kind like NackTask does. The release is fenced on the claim ownership
	// token: a stale worker whose task was reclaimed or atomically refreshed
	// sees a mismatch and reports ErrNotFound instead of clearing the fresh
	// lease.
	ref := b.ref("wf_tasks", actTaskID(t.ID))
	if t.Kind == "workflow" && t.InstanceID != "" {
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
		m := s.Data()
		if t.Kind == "workflow" && t.InstanceID != "" {
			if t.ID != 0 && i64(m, "id") != t.ID {
				return backend.ErrNotFound
			}
		}
		if t.WorkerID != "" && (str(m, "worker_id") != t.WorkerID || int(i64(m, "attempt")) != t.Attempt) {
			return backend.ErrNotFound
		}
		return tx.Update(ref, []gcf.Update{{Path: "visible_at", Value: nowUTC()}, {Path: "worker_id", Value: gcf.Delete}})
	})
	if err != nil {
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
		// Fence the nack on the claim ownership token (see ReleaseLease):
		// a stale worker whose task was reclaimed or atomically refreshed
		// sees a mismatch and reports ErrNotFound instead of clearing the
		// fresh lease.
		m := s.Data()
		if t.Kind == "workflow" && t.InstanceID != "" {
			if t.ID != 0 && i64(m, "id") != t.ID {
				return backend.ErrNotFound
			}
		}
		if t.WorkerID != "" && (str(m, "worker_id") != t.WorkerID || int(i64(m, "attempt")) != t.Attempt) {
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
	// createdAt is the pre-commit incarnation captured inside the commit
	// transaction (see readAdvancementTx). The post-commit dedupe sweep
	// re-validates it on every page so a purge plus ID reuse interleaved
	// with the sweep aborts instead of deleting the replacement's guard.
	createdAt time.Time
	// incarnation is the instance's unique per-incarnation token captured
	// alongside createdAt (see newIncarnation). Fences compare it exactly;
	// createdAt stays as the legacy fallback for rows predating the field.
	incarnation string
	// dedupeSnapshot holds the terminal advancement's dedupe keys as read
	// inside the commit transaction (see readAdvancementTx). The post-commit
	// sweep deletes exactly these IDs.
	dedupeSnapshot []string
}

// terminalSweep carries one terminal advancement's post-commit sweep: the
// pre-commit incarnation fencing it plus the exact dedupe keys to remove.
type terminalSweep struct {
	createdAt   time.Time
	incarnation string
	dedupeIDs   []string
}

func (b *Backend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	if len(advs) == 0 {
		return nil
	}
	// Dedupe keys for terminal advancements are snapshotted INSIDE the commit
	// transaction (see readAdvancementTx): the snapshot is a serializable
	// read, so a SendToInbox serializing before the terminal commit is
	// included in the post-commit sweep instead of lingering until purge
	// (where a later ID reuse would mistake it for a duplicate of a
	// promised post-terminal event). Keys created after the snapshot stay
	// for purge. The sweep itself still runs after the commit: a terminal
	// commit with hundreds of keys must not scale one transaction past the
	// 500-write limit.
	var snapshots map[string]terminalSweep
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
		// Capture the snapshots from this attempt only: the transaction
		// function may run more than once, and only the committing
		// attempt's reads classify the sweep.
		snapshots = make(map[string]terminalSweep, len(advs))
		for i, adv := range advs {
			if adv.Terminal == nil {
				continue
			}
			if _, ok := snapshots[adv.InstanceID]; ok {
				continue
			}
			snapshots[adv.InstanceID] = terminalSweep{createdAt: preps[i].createdAt, incarnation: preps[i].incarnation, dedupeIDs: preps[i].dedupeSnapshot}
		}
		return b.flushInboxSeqs(tx, alloc)
	})
	if err != nil {
		return err
	}
	// Wake terminal subscribers immediately after the successful commit,
	// before the fallible ensureWorkflowTask loop below: the terminal
	// status already committed, and a transient ensure error returns early
	// while the advancement only retries on conflict. Notification must
	// never be skipped because post-commit cleanup errored.
	for _, adv := range advs {
		if adv.Terminal != nil {
			b.notifyTerminal(adv.InstanceID)
		}
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
			// Only keys snapshotted before the commit are removed: a
			// concurrent SendToInbox with a new DedupeID can land after
			// notifyTerminal fired above, and sweeping its key while the
			// inbox event remains would duplicate a later retry.
			// Every page re-validates the pre-commit incarnation: a purge
			// plus ID reuse interleaved with the sweep aborts it instead
			// of deleting the replacement's recreated guard (see
			// sweepSignalDedupeIDs); purge owns the leftovers.
			// The sweep stays synchronous so a redelivered DedupeID inserts
			// anew once this call returns, but runs under a bounded context
			// so a stuck store delays only this cleanup, never the caller.
			cctx, cancel := context.WithTimeout(context.Background(), signalDedupeSweepTimeout)
			sw := snapshots[adv.InstanceID]
			_ = b.sweepSignalDedupeIDs(cctx, purgeFence{victim: purgeVictim{id: adv.InstanceID, createdAt: sw.createdAt, incarnation: sw.incarnation}}, sw.dedupeIDs)
			cancel()
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
	// Reject commits for instances that already left running: a task leased
	// before TerminateInstance still carries a matching ExpectedSeq/TaskID,
	// and the post-flip sweep no longer deletes the task inside the flip
	// transaction, so without this gate a terminal advancement would
	// overwrite terminated → completed/failed (and a suspended one would
	// append journal/children post-termination). Reading the status in-txn
	// also conflicts with a concurrent status flip, serializing the commit
	// against termination.
	if inst.Status != "running" {
		return advancementPrep{}, backend.ErrConflict
	}
	// Fence child creation on purge markers (Codex round 12 on #296): only
	// direct CreateInstance checked the marker, so a child — or a
	// Continue-As-New successor, which is also an adv.Children entry —
	// reusing a purged ID recreated the instance while the old incarnation's
	// rows were still pending, and the replacement consumed purged signals.
	// The check rides in the read phase (Firestore rejects reads after
	// writes in a transaction) alongside the other advancement reads; a hit
	// fails the advancement with ErrConflict so the worker retries after
	// purge recovery clears the marker.
	for _, ch := range adv.Children {
		msnap, merr := tx.Get(b.ref(purgeMarkersCollection, ch.ID))
		if merr != nil && !isNotFound(merr) {
			return advancementPrep{}, merr
		}
		if merr == nil && msnap.Exists() {
			return advancementPrep{}, backend.ErrConflict
		}
	}
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
	prep := advancementPrep{instRef: instSnap.Ref, taskRef: taskRef, inst: inst, hasInbox: hasInbox}
	prep.createdAt = timestamp(instSnap.Data(), "created_at")
	prep.incarnation = str(instSnap.Data(), incarnationField)
	if adv.Terminal != nil {
		// Snapshot the dedupe keys inside the commit transaction (still the
		// read phase: no writes have been buffered yet). A SendToInbox
		// serializing before this commit is included in the post-commit
		// sweep; anything landing after stays for purge.
		ids, err := listSignalDedupeIDsTx(tx, b.col("wf_signal_dedupe"), adv.InstanceID)
		if err != nil {
			return advancementPrep{}, err
		}
		prep.dedupeSnapshot = ids
	}
	return prep, nil
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

// firestoreTerminalInboxBatchLimit caps terminal-instance signal batches
// below the generic InboxBatchLimit (Codex round-25 P2 on #296): a terminal
// first-send with a fresh DedupeID writes up to two marker docs (framed plus
// rolling-upgrade dual), two base-guard docs (framed plus dual), and one
// inbox doc — 5 writes per item — plus one flushInboxSeqs write. A 100-item
// terminal batch therefore needs 100*5+1=501 writes, exceeding Firestore's
// 500-write transaction limit even though InboxBatchLimit permits 100 items.
// Running batches stay at the generic limit (worst case two guard docs plus
// one inbox per item: 100*3+1=301 writes). 99*5+1=496 fits with headroom.
const firestoreTerminalInboxBatchLimit = 99

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
	// Fast path for the terminal write budget (see above): the in-transaction
	// check below covers a running→terminal race between this read and the
	// commit, but rejecting here avoids opening a doomed transaction.
	if inst.Status != "running" && len(items) > firestoreTerminalInboxBatchLimit {
		return backend.ErrBatchTooLarge
	}
	var inserted int
	err = b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		// Stamp inside the transaction (per attempt): a transaction that
		// loses a race and retries must not commit with a created_at
		// captured before the conflicting commit.
		now := nowUTC()
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
		// Terminal sends must not be swallowed by pre-terminal dedupe keys:
		// a SendToInbox racing a terminal transition (CommitAdvancements or
		// TerminateInstance) can observe a key snapshotted for the
		// post-commit sweep. The sweep deletes exactly the snapshotted keys,
		// so a send that commits in the notify-to-sweep window must insert
		// its own event immediately: suppressing it on the doomed
		// pre-terminal row (legacy or versioned) while stamping only a
		// marker loses the signal permanently once the sweep removes the
		// row — the marker then suppresses every retry (Codex round-15 on
		// #296). Sends that commit while the instance is already terminal
		// always insert their event on the first post-terminal send;
		// retries still dedupe via a post-terminal marker (see
		// postTerminalDedupeMarker): the first terminal send with a DedupeID
		// creates the marker alongside the event, and later retries see
		// the marker and skip. The base key is created when absent (so
		// sweeps/purge stay consistent) but an owned base key — legacy or
		// versioned — never suppresses a terminal insert, only the marker
		// does. (Same-batch duplicates still collapse to one insert
		// so a batch never issues conflicting Creates.)
		terminal := str(isnap.Data(), "status") != "running"
		// Enforce the terminal write budget inside the transaction as well:
		// the instance may have flipped to terminal after the pre-read above
		// (running→terminal race), turning a 100-item running batch into a
		// 501-write terminal commit. Fail fast with ErrBatchTooLarge instead
		// of a deterministic Firestore limit error.
		if terminal && len(items) > firestoreTerminalInboxBatchLimit {
			return backend.ErrBatchTooLarge
		}
		// Firestore requires all reads before writes; also skip same-batch DedupeID dups.
		skip := make([]bool, len(items))
		createDoc := make([]string, len(items))
		createDocDual := make([]string, len(items))
		createVer := make([]int64, len(items))
		markerDoc := make([]string, len(items))
		markerDocDual := make([]string, len(items))
		created := map[string]bool{}
		// reserved tracks guard document IDs chosen earlier in this batch
		// (round-18 P2): transaction reads don't see buffered Creates.
		reserved := map[string]bool{}
		for i, it := range items {
			if it.DedupeID == "" {
				continue
			}
			if created[it.DedupeID] {
				skip[i] = true
				continue
			}
			if terminal {
				// Retry check first: a marker means this DedupeID already
				// inserted post-terminal, so dedupe the retry. Markers
				// live in their own collection (see
				// postTerminalMarkersCollection), never in the dedupe
				// keyspace: no legacy verbatim user row — however
				// marker-shaped — can match this probe, and pre-upgrade
				// marker rows left behind in wf_signal_dedupe are inert
				// (a pre-upgrade retry may duplicate once, never drop;
				// purge reaps the rows). Both doc-ID framings are probed
				// (framed first, legacy for pre-framing markers), and the
				// stored instance_id is validated: under legacy framing
				// two (instance, marker) pairs could share one document,
				// so a foreign row never suppresses this instance (Codex
				// round-16 on #296, see docInstanceMatches).
				markerExists := false
				mpr, merr := b.probeDedupeKey(tx, postTerminalMarkersCollection, instanceID, postTerminalDedupeMarker(it.DedupeID))
				if merr != nil {
					return merr
				}
				if owned, _ := selectOwnedDedupeDoc(mpr.framedDoc, mpr.legacyDoc, instanceID); owned != nil {
					markerExists = true
				}
				if markerExists {
					created[it.DedupeID] = true
					skip[i] = true
					continue
				}
				// First post-terminal send: insert + stamp the marker.
				// Create the base key too when absent for sweep/purge
				// consistency. An owned base guard (versioned or legacy)
				// never suppresses a terminal insert: a pre-terminal key's
				// event was retired by the terminal sweep, so the reset
				// semantics still promise a fresh post-terminal delivery
				// (see TestTerminalSendBypassesStaleDedupe) — and a key
				// observed here may itself be snapshotted for a sweep that
				// has not run yet (notify-to-sweep window, Codex round-15
				// on #296): suppressing on it while stamping only a marker
				// loses the signal once the sweep removes the row, with
				// the marker then suppressing every retry. Only the marker
				// suppresses terminal retries. (A pre-upgrade legacy
				// post-terminal retry guard therefore duplicates once on
				// its first post-upgrade retry instead of suppressing —
				// the safe direction: never drop. The marker stamped
				// alongside still dedupes all later retries.)
				// Marker-shaped candidates are never user keys: markers
				// live in their own collection now, so a row shaped like
				// one is either an inert pre-upgrade marker or a legacy
				// verbatim row no probe may mistake for this DedupeID's
				// guard (skipping it duplicates at worst, never drops).
				// Ownership is version-aware (Codex round 13 on #296, see
				// matchDedupeRow): a foreign-owner row at this DedupeID's
				// canonical key already satisfies sweep consistency (the
				// key is occupied), so only the marker is stamped —
				// creating over it would fail and must not suppress the
				// insert.
				baseExists := false
				canonicalOccupied := false
				canonFramedFree := true
				rawLegacyFree := true
				canonicalKey := escapeDedupeID(it.DedupeID)
				for _, bk := range dedupeKeyCandidates(it.DedupeID) {
					if isPostTerminalMarkerKey(bk) {
						continue
					}
					pr, err := b.probeDedupeKey(tx, "wf_signal_dedupe", instanceID, bk)
					if err != nil {
						return err
					}
					if bk == canonicalKey {
						canonFramedFree = pr.framedFree
					}
					if bk == it.DedupeID {
						rawLegacyFree = pr.legacyFree
					}
					if bk == escapeDedupeID(it.DedupeID) && (!pr.framedFree || !pr.legacyFree) {
						// Either framing occupied — even by a foreign row
						// under a colliding legacy doc ID (Codex round-16
						// on #296, round-18 ownership-aware probing): never
						// this DedupeID's guard, but the key still counts
						// as occupied (the write would collide), so only
						// the marker is stamped for it.
						canonicalOccupied = true
					}
					if matchOwnedDedupeRow(it.DedupeID, bk, pr, instanceID) {
						baseExists = true
						break
					}
				}
				created[it.DedupeID] = true
				// Stamp the marker under rolling-upgrade dual-write (Codex
				// round-24 P1 on #296): the framed doc plus its legacy
				// counterpart when free, else the legacy doc alone when the
				// framed leg is foreign-occupied, else no marker (the event
				// still inserts; a retry may duplicate once rather than the
				// send failing deterministically).
				if md, ok := markerGuardTarget(instanceID, it.DedupeID, mpr.framedFree, mpr.legacyFree, reserved); ok {
					reserved[md] = true
					markerDoc[i] = md
					if dual, ok := dualMarkerDoc(instanceID, it.DedupeID, mpr.legacyFree, reserved); ok && dual != md {
						reserved[dual] = true
						markerDocDual[i] = dual
					}
				}
				if baseExists || canonicalOccupied {
					continue
				}
				// New base guards dual-write both framings (same round-24
				// P1, corrected round-26 P1 on #296): pre-framing nodes probe
				// only the raw legacy concatenation and would otherwise miss
				// the framed-only guard. canonicalOccupied is false here, so
				// the framed leg is free; the raw legacy leg rides along when
				// free (see dualDedupeGuardDoc).
				target := dedupeGuardTarget{docID: signalDedupeID(instanceID, it.DedupeID), ver: int64(dedupeFormatVersion)}
				if !canonFramedFree || reserved[target.docID] {
					// Framed leg lost a same-batch race (reserved) or a
					// concurrent commit: fall back to marker-only like the
					// occupied case above (duplicate-never-drop) instead of
					// failing the batch deterministically.
					continue
				}
				reserved[target.docID] = true
				createDoc[i] = target.docID
				createVer[i] = target.ver
				if dual, ok := dualDedupeGuardDoc(instanceID, target, it.DedupeID, canonicalKey, rawFallbackDedupeKey(it.DedupeID), rawLegacyFree, reserved); ok {
					reserved[dual.docID] = true
					createDocDual[i] = dual.docID
				}
				continue
			}
			// Probe every stored user-key form, legacy raw first (Codex round 8
			// on #327): pre-escape rows stored "__" IDs verbatim.
			// Marker-shaped candidates are honored here (Codex round 12 on
			// #296): live markers live outside the dedupe keyspace, and a
			// running instance cannot own a post-terminal marker, so a
			// marker-shaped row on a running instance is unambiguously a
			// legacy user key (e.g. DedupeID "__post_terminal__:x" stored
			// raw pre-escape). Skipping it would miss the guard, write a
			// second escaped key, and duplicate the event. Terminal
			// instances keep the marker-only rule (see the terminal base
			// check above).
			// Ownership is version-aware (Codex round 13 on #296, see
			// matchDedupeRow) and instance-aware (Codex round-16 on #296,
			// see docInstanceMatches): a hit counts only when the row guards
			// THIS DedupeID of THIS instance — a legacy row on exact raw
			// equality, a versioned row on canonical-form equality (v1) or
			// fallback-key equality (v2). A foreign-owner row at this
			// DedupeID's canonical key means the canonical guard cannot be
			// created (it would collide), so the guard falls back to the
			// rawFallbackDedupeKey with an explicit version
			// (dedupeFormatRawKeyVersion): delivery is preserved and retries
			// keep deduping. The fallback key is the raw ID for short IDs
			// but a bounded second-level hash for over-budget IDs, so the
			// write stays within the shared STRING(255) budget on Spanner
			// (Codex round-16 on #296). Probing is ownership-aware (Codex
			// round-18 on #296, see probeDedupeKey): a foreign row at the
			// framed doc no longer hides the owned legacy candidate, and
			// when both framed docs are foreign-occupied the guard is
			// created at a free legacy framing (see pickDedupeGuardTarget)
			// instead of inserting unguarded. Guard docs chosen earlier in
			// this batch are reserved (round-18 P2): transaction reads
			// don't see buffered Creates, so without the reservation two
			// items choosing one doc fail the whole batch deterministically
			// on every retry. Only when no slot is free does the event
			// insert unguarded (duplicate-never-drop).
			baseHit := false
			canonicalKey := escapeDedupeID(it.DedupeID)
			fallbackKey := rawFallbackDedupeKey(it.DedupeID)
			canonProbe := dedupeKeyProbe{framedFree: true, legacyFree: true}
			fbProbe := dedupeKeyProbe{framedFree: true, legacyFree: true}
			rawProbe := dedupeKeyProbe{framedFree: true, legacyFree: true}
			for _, bk := range dedupeKeyCandidates(it.DedupeID) {
				pr, err := b.probeDedupeKey(tx, "wf_signal_dedupe", instanceID, bk)
				if err != nil {
					return err
				}
				if bk == canonicalKey {
					canonProbe = pr
				}
				if bk == fallbackKey {
					fbProbe = pr
				}
				if bk == it.DedupeID {
					rawProbe = pr
				}
				if matchOwnedDedupeRow(it.DedupeID, bk, pr, instanceID) {
					baseHit = true
					break
				}
			}
			if baseHit {
				created[it.DedupeID] = true
				skip[i] = true
				continue
			}
			created[it.DedupeID] = true
			if target, ok := pickDedupeGuardTarget(instanceID, it.DedupeID, canonicalKey, fallbackKey, canonProbe, fbProbe, reserved); ok {
				reserved[target.docID] = true
				createDoc[i] = target.docID
				createVer[i] = target.ver
				// Rolling-upgrade dual-write (Codex round-24 P1 on #296,
				// corrected round-26 P1): a framed guard also lands under
				// the RAW legacy-format doc ID so pre-framing nodes (which
				// probe instanceID + ":" + raw DedupeID) see the guard.
				// Legacy targets need no counterpart (old readers see them
				// directly). Dual-write needs the PHYSICAL raw-legacy
				// availability (a foreign-occupied raw leg still collides),
				// not the effective availability pick used above.
				if dual, ok := dualDedupeGuardDoc(instanceID, target, it.DedupeID, canonicalKey, fallbackKey, rawProbe.legacyFree, reserved); ok {
					reserved[dual.docID] = true
					createDocDual[i] = dual.docID
				}
			}
		}
		for i, it := range items {
			if skip[i] {
				// Suppressed same-batch duplicate (or a running-state
				// dedupe hit): no inbox insert and no marker (markers are
				// terminal-only; terminal marker hits return above without
				// stamping). Kept as an explicit no-op branch so a future
				// marker-on-skip never silently inserts an inbox row.
				continue
			}
			if it.DedupeID != "" && createDoc[i] != "" {
				if err := tx.Create(b.ref("wf_signal_dedupe", createDoc[i]), map[string]any{
					"instance_id":            instanceID,
					"dedupe_id":              escapeDedupeID(it.DedupeID),
					dedupeFormatVersionField: createVer[i],
					"created_at":             now,
				}); err != nil {
					return err
				}
				if createDocDual[i] != "" {
					if err := tx.Create(b.ref("wf_signal_dedupe", createDocDual[i]), map[string]any{
						"instance_id":            instanceID,
						"dedupe_id":              escapeDedupeID(it.DedupeID),
						dedupeFormatVersionField: createVer[i],
						"created_at":             now,
					}); err != nil {
						return err
					}
				}
			}
			if it.DedupeID != "" && markerDoc[i] != "" {
				if err := tx.Create(b.ref(postTerminalMarkersCollection, markerDoc[i]), map[string]any{
					"instance_id": instanceID,
					"dedupe_id":   postTerminalDedupeMarker(it.DedupeID),
					"created_at":  now,
				}); err != nil {
					return err
				}
				if markerDocDual[i] != "" {
					if err := tx.Create(b.ref(postTerminalMarkersCollection, markerDocDual[i]), map[string]any{
						"instance_id": instanceID,
						"dedupe_id":   postTerminalDedupeMarker(it.DedupeID),
						"created_at":  now,
					}); err != nil {
						return err
					}
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
