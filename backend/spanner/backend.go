package spanner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
)

func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{SweepsTerminalInbox: true, CleansTerminalState: true, SupportsBulkCleanup: true}
}

// readInboxSeq returns the instance's inbox counter (0, false when unset).
// The counter lives in wf_inbox_seq, kept off wf_instances so signal appends
// never contend with advancement commits on the instance row.
func readInboxSeq(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string) (int64, bool, error) {
	row, err := txn.ReadRow(ctx, "wf_inbox_seq", spanner.Key{instanceID}, []string{"seq"})
	if isNotFound(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var seq int64
	if err := row.Columns(&seq); err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

func inboxSeqMuts(instanceID string, seq int64, existed bool) []*spanner.Mutation {
	m := map[string]any{"instance_id": instanceID, "seq": seq}
	if existed {
		return []*spanner.Mutation{spanner.UpdateMap("wf_inbox_seq", m)}
	}
	return []*spanner.Mutation{spanner.InsertMap("wf_inbox_seq", m)}
}

// InboxSeq reports the raw per-instance inbox sequence counter for tests
// (found=false when the counter does not exist).
func (b *Backend) InboxSeq(ctx context.Context, instanceID string) (int64, bool, error) {
	row, err := b.client.Single().ReadRow(ctx, "wf_inbox_seq", spanner.Key{instanceID}, []string{"seq"})
	if isNotFound(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var seq int64
	if err := row.Columns(&seq); err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

func (b *Backend) CreateInstance(ctx context.Context, inst backend.NewInstance) error {
	queue := inst.Queue
	if queue == "" {
		queue = "default"
	}
	now := nowUTC()
	taskID := newID()
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
		if _, merr := txn.ReadRow(ctx, "wf_purge_markers", spanner.Key{inst.ID}, []string{"instance_id"}); merr == nil {
			return backend.ErrAlreadyExists
		} else if !isNotFound(merr) {
			return merr
		}
		muts := []*spanner.Mutation{
			spanner.InsertMap("wf_instances", map[string]any{
				"id":                inst.ID,
				"name":              inst.Name,
				"queue":             queue,
				"status":            "running",
				"input":             jsonVal(inst.Input),
				"next_seq":          int64(2),
				"parent_id":         nullStr(inst.ParentID),
				"parent_seq":        nullInt(inst.ParentSeq),
				"search_attributes": jsonVal(backend.MarshalSearchAttributes(inst.SearchAttributes)),
				"memo":              jsonVal(backend.MarshalSearchAttributes(inst.Memo)),
				"created_at":        now,
				"updated_at":        now,
				incarnationColumn:   newIncarnation(),
			}),
			spanner.InsertMap("wf_journal", map[string]any{
				"instance_id": inst.ID,
				"seq":         int64(1),
				"type":        string(journal.TypeWorkflowStarted),
				"name":        inst.Name,
				"payload":     jsonVal(inst.Input),
				"recorded_at": now,
			}),
			spanner.InsertMap("wf_tasks", map[string]any{
				"id":          taskID,
				"kind":        "workflow",
				"queue":       queue,
				"instance_id": inst.ID,
				"attempt":     int64(0),
				"visible_at":  now,
				"created_at":  commitTimestamp(),
			}),
		}
		return txn.BufferWrite(muts)
	})
	if isAlreadyExists(err) {
		return backend.ErrAlreadyExists
	}
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) GetInstance(ctx context.Context, id string) (*backend.Instance, error) {
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id},
		[]string{"id", "name", "queue", "status", "input", "result", "failure", "next_seq", "parent_id", "parent_seq", "search_attributes", "memo"})
	if err != nil {
		if isNotFound(err) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	return scanInstance(row)
}

func scanInstance(row *spanner.Row) (*backend.Instance, error) {
	var inst backend.Instance
	var input, result, failure, searchAttrs, memo spanner.NullJSON
	var parentID spanner.NullString
	var parentSeq spanner.NullInt64
	if err := row.Columns(&inst.ID, &inst.Name, &inst.Queue, &inst.Status,
		&input, &result, &failure, &inst.NextSeq, &parentID, &parentSeq, &searchAttrs, &memo); err != nil {
		return nil, err
	}
	inst.Input = jsonBytes(input)
	inst.Result = jsonBytes(result)
	inst.Failure = jsonBytes(failure)
	inst.SearchAttributes, _ = backend.SearchAttributesFromPayload(jsonBytes(searchAttrs))
	inst.Memo, _ = backend.SearchAttributesFromPayload(jsonBytes(memo))
	if parentID.Valid {
		inst.ParentID = parentID.StringVal
	}
	if parentSeq.Valid {
		inst.ParentSeq = parentSeq.Int64
	}
	return &inst, nil
}

func (b *Backend) GetJournal(ctx context.Context, id string, afterSeq int64) ([]journal.Event, error) {
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT seq, type, name, ref_seq, payload FROM wf_journal
			WHERE instance_id = @id AND seq > @after ORDER BY seq`,
		Params: map[string]any{"id": id, "after": afterSeq},
	})
	defer iter.Stop()
	var out []journal.Event
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var e journal.Event
		var typ string
		var refSeq spanner.NullInt64
		var payload spanner.NullJSON
		if err := row.Columns(&e.Seq, &typ, &e.Name, &refSeq, &payload); err != nil {
			return nil, err
		}
		e.Type = journal.Type(typ)
		if refSeq.Valid {
			e.RefSeq = refSeq.Int64
		}
		e.Payload = jsonBytes(payload)
		out = append(out, e)
	}
	return out, nil
}

func (b *Backend) ListInstances(ctx context.Context, f backend.InstanceFilter) ([]backend.Instance, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	sql := `SELECT id, name, queue, status, input, result, failure, next_seq, parent_id, parent_seq, search_attributes, memo
			FROM wf_instances
			WHERE (@status = '' OR status = @status)
			  AND (@name = '' OR name = @name)
			ORDER BY created_at, id`
	params := map[string]any{
		"status": f.Status, "name": f.Name,
	}
	if len(f.SearchAttributes) == 0 {
		sql += ` LIMIT @limit OFFSET @offset`
		params["limit"] = int64(limit)
		params["offset"] = int64(f.Offset)
	}
	iter := b.client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()
	var out []backend.Instance
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		inst, err := scanInstance(row)
		if err != nil {
			return nil, err
		}
		if !backend.MatchesSearchAttributes(inst.SearchAttributes, f.SearchAttributes) {
			continue
		}
		out = append(out, *inst)
	}
	if len(f.SearchAttributes) > 0 {
		if f.Offset >= len(out) {
			return nil, nil
		}
		out = out[f.Offset:]
		if len(out) > limit {
			out = out[:limit]
		}
	}
	return out, nil
}

func (b *Backend) TerminateInstance(ctx context.Context, id string) error {
	now := nowUTC()
	// Status flips in one small transaction so the mutation count never scales
	// with the instance's task/timer/dedupe rows (DynamoDB parity). Child rows
	// are swept afterwards in paged transactions.
	// Dedupe keys are snapshotted INSIDE the flip transaction (serializable
	// read): a post-terminal SendToInbox committing after the flip is never in
	// the snapshot and survives the sweep, while pre-termination keys are
	// reaped (Codex round 8 on #327: an unqualified sweep deleted
	// post-terminal retry markers while leaving their inbox events, so the
	// next retry re-inserted a duplicate). Markers in the snapshot itself are
	// filtered out as well; purge reaps leftovers.
	var dedupeSnapshot []string
	var victim purgeVictim
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		v, err := queryInstanceVictimTx(ctx, txn, id)
		if err != nil {
			if isNotFound(err) {
				return backend.ErrNotFound
			}
			return err
		}
		victim = v
		ids, err := querySignalDedupeIDsTx(ctx, txn, id)
		if err != nil {
			return err
		}
		dedupeSnapshot = ids
		// Status flip plus a bounded in-transaction sweep of the full
		// residual set (tasks, timers, signal dedupe, inbox). Residual
		// rows are unbounded (they span many turns), and one delete
		// mutation per row can exceed the per-commit mutation limit, so
		// at most terminalCleanupMutationBudget deletions ride along;
		// the remainder is swept post-commit in bounded transactions
		// below (cleanupTerminalInstance).
		var muts []*spanner.Mutation
		muts = append(muts, spanner.UpdateMap("wf_instances", map[string]any{
			"id":              id,
			"status":          "terminated",
			"updated_at":      now,
			"completed_at":    now,
			"sweep_commit_ts": commitTimestamp(),
		}))
		budget := terminalCleanupMutationBudget
		dMuts, err := deleteSignalDedupe(ctx, txn, id, budget, time.Time{}, false)
		if err != nil {
			return err
		}
		muts = append(muts, dMuts...)
		budget = max(budget-len(dMuts), 0)
		tMuts, err := deleteTasksForInstance(ctx, txn, id, 0, budget, time.Time{}, false)
		if err != nil {
			return err
		}
		muts = append(muts, tMuts...)
		budget = max(budget-len(tMuts), 0)
		tmMuts, err := deleteTimersForInstance(ctx, txn, id, budget)
		if err != nil {
			return err
		}
		muts = append(muts, tmMuts...)
		budget = max(budget-len(tmMuts), 0)
		inMuts, err := deleteInboxForInstance(ctx, txn, id, budget, time.Time{}, false)
		if err != nil {
			return err
		}
		muts = append(muts, inMuts...)
		return txn.BufferWrite(muts)
	})
	if err != nil {
		return err
	}
	// Post-commit bounded sweep for residuals beyond the in-transaction
	// budget (or raced in concurrently). includeInbox=true keeps the full
	// table set: TerminateInstance leaves no inbox rows behind. Claims
	// refuse tasks of non-running instances, so leftovers are never
	// executed in the meantime.
	if err := b.sweepTerminateDocs(context.Background(), victim, dedupeSnapshot); err != nil {
		b.notifyTerminal(id)
		return err
	}
	if err := b.cleanupTerminalInstance(context.Background(), id, true); err != nil {
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
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT queue, COUNT(*) AS n FROM wf_tasks
			WHERE kind = @kind AND visible_at <= @now AND queue IN UNNEST(@queues)
			GROUP BY queue`,
		Params: map[string]any{"kind": kind, "now": now, "queues": queues},
	})
	defer iter.Stop()
	out := map[string]int64{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var q string
		var n int64
		if err := row.Columns(&q, &n); err != nil {
			return nil, err
		}
		out[q] = n
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
	visAt := now.Add(req.Lease)
	var out []backend.Task
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		out = nil
		// Terminal-instance tasks are deleted on sight (best-effort) and
		// the select repeats while deletions free slots: without the
		// delete a Limit:1 poll stuck behind one terminal task would
		// return it on every call and starve the live tasks queued
		// behind it. Each repeat deletes at least one row, so the loop
		// terminates; lease races alone never trigger a repeat.
		// The total deletes per claim are capped (see
		// spannerClaimStaleDeleteCap): an unbounded terminal backlog must
		// not buffer one DELETE per row in a single commit, or DML limits
		// abort every claim and the live task stays unreachable. The cap
		// drains across polls; Firestore needs no equivalent cap because
		// its claim path commits one document per transaction (already
		// bounded by the 500-write limit).
		totalDeleted := 0
		for len(out) < req.Limit {
			if totalDeleted >= spannerClaimStaleDeleteCap {
				break
			}
			need := int64(req.Limit - len(out))
			iter := txn.Query(ctx, spanner.Statement{
				SQL: `SELECT id, visible_at, instance_id FROM wf_tasks
					WHERE kind = @kind AND visible_at <= @now AND queue IN UNNEST(@queues)
					ORDER BY visible_at, id LIMIT @limit`,
				Params: map[string]any{
					"kind": req.Kind, "now": now, "queues": req.Queues, "limit": need,
				},
			})
			type cand struct {
				id         int64
				vis        time.Time
				instanceID string
			}
			var cands []cand
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					iter.Stop()
					return err
				}
				var c cand
				if err := row.Columns(&c.id, &c.vis, &c.instanceID); err != nil {
					iter.Stop()
					return err
				}
				cands = append(cands, c)
			}
			iter.Stop()
			if len(cands) == 0 {
				break
			}
			deleted := 0
			for _, c := range cands {
				if len(out) >= req.Limit {
					break
				}
				if totalDeleted+deleted >= spannerClaimStaleDeleteCap {
					break
				}
				// Fence against TerminateInstance: never lease a task whose
				// instance already left running. Reading the instance row inside
				// the claim transaction also conflicts with a concurrent status
				// flip, restoring the exclusion the pre-chunk single-transaction
				// terminate had (status + task deletes committed atomically).
				// A stale terminal task is deleted here so later polls (and
				// the repeat select above) reach live tasks.
				irow, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{c.instanceID}, []string{"status"})
				if err != nil {
					if isNotFound(err) {
						if _, err := txn.Update(ctx, spanner.Statement{
							SQL:    `DELETE FROM wf_tasks WHERE id = @id`,
							Params: map[string]any{"id": c.id},
						}); err != nil {
							return err
						}
						deleted++
						continue
					}
					return err
				}
				var st string
				if err := irow.Columns(&st); err != nil {
					return err
				}
				if st != "running" {
					if _, err := txn.Update(ctx, spanner.Statement{
						SQL:    `DELETE FROM wf_tasks WHERE id = @id`,
						Params: map[string]any{"id": c.id},
					}); err != nil {
						return err
					}
					deleted++
					continue
				}
				n, err := txn.Update(ctx, spanner.Statement{
					SQL: `UPDATE wf_tasks SET visible_at = @vis, attempt = attempt + 1, worker_id = @wid
						WHERE id = @id AND visible_at = @old`,
					Params: map[string]any{
						"vis": visAt, "wid": req.WorkerID, "id": c.id, "old": c.vis,
					},
				})
				if err != nil {
					return err
				}
				if n == 0 {
					continue
				}
				row, err := txn.ReadRow(ctx, "wf_tasks", spanner.Key{c.id},
					[]string{"id", "kind", "queue", "instance_id", "ref_seq", "payload", "attempt", "visible_at", "worker_id", "heartbeat"})
				if err != nil {
					return err
				}
				t, err := scanTask(row)
				if err != nil {
					return err
				}
				out = append(out, t)
			}
			if deleted == 0 {
				break
			}
			totalDeleted += deleted
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return b.fenceClaimedTasks(ctx, out)
}

// fenceClaimedTasks re-validates freshly claimed tasks before handing them
// out (Codex round-18 P1 on #291, mirroring DynamoDB's round-17 post-claim
// gate): the in-transaction status gate above covers a terminal commit
// landing BEFORE the claim, but a terminal commit landing AFTER the claim
// commit still leaves a live lease in the worker's hands, which would
// invoke user code post-completion — the terminal sweep deletes the row
// but cannot recall the in-memory task. Each claimed task whose instance
// is no longer running is dropped and its residue deleted best-effort here
// (the sweep owns whatever remains).
//
// RESIDUAL (Codex round-19 P1 on #291): this fence is itself a separate
// status read, so a terminal commit landing between this read and the
// worker's dispatch still dispatches post-completion. No backend-side read
// can close that gap — fencing THROUGH the handoff requires the worker's
// pre-invoke re-check immediately before invokeActivity, which lives on the
// issue-296 branch (checkActivityFence, fail-closed) and is absent here.
// Defense in depth across the two branches, narrowest window last:
//  1. In-claim gate in the claim transaction (this backend: status read in
//     the claim RW txn; DynamoDB: atomic ConditionCheck in
//     claimTransactItems; Firestore: instance read in the claim txn) —
//     covers terminals landing before the claim.
//  2. This post-claim fence — narrows the window to fence-read→dispatch.
//  3. Worker pre-invoke re-check (issue-296) — narrows it to ~0.
//
// A status-read failure releases every claimed lease best-effort — fenced
// by the claim token, detached from cancellation — instead of abandoning
// the batch hidden for a full lease.
func (b *Backend) fenceClaimedTasks(ctx context.Context, tasks []backend.Task) ([]backend.Task, error) {
	// Batch the status probe into ONE single-timestamp strong read over the
	// distinct owning instances (Codex round-27 P1 on #291): the previous
	// loop issued one full GetInstance RPC per task, so a batch spread its
	// fence checks across N timestamps — later tasks were fenced strictly
	// after earlier ones, widening each task's fence-read→dispatch window,
	// besides reading whole rows (input/result/failure payloads) only to
	// inspect the status. One key-set read of (id, status) at a single
	// timestamp fences every task against the same snapshot in one round
	// trip. Semantics are unchanged: a missing instance is terminal residue
	// (the sweep owns it), and any read error releases every claimed lease
	// instead of abandoning the batch hidden for a full lease.
	ids := make([]string, 0, len(tasks))
	seen := make(map[string]struct{}, len(tasks))
	for _, t := range tasks {
		if _, ok := seen[t.InstanceID]; !ok {
			seen[t.InstanceID] = struct{}{}
			ids = append(ids, t.InstanceID)
		}
	}
	statuses := make(map[string]string, len(ids))
	if len(ids) > 0 {
		keys := make([]spanner.Key, 0, len(ids))
		for _, id := range ids {
			keys = append(keys, spanner.Key{id})
		}
		iter := b.client.Single().Read(ctx, "wf_instances", spanner.KeySetFromKeys(keys...), []string{"id", "status"})
		defer iter.Stop()
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				b.releaseClaimedLeases(ctx, tasks)
				return nil, err
			}
			var id, status string
			if err := row.Columns(&id, &status); err != nil {
				b.releaseClaimedLeases(ctx, tasks)
				return nil, err
			}
			statuses[id] = status
		}
	}
	kept := make([]backend.Task, 0, len(tasks))
	for _, t := range tasks {
		if status, ok := statuses[t.InstanceID]; !ok || status != "running" {
			b.deleteClaimedTask(ctx, t.ID)
			continue
		}
		kept = append(kept, t)
	}
	return kept, nil
}

// releaseClaimedLeases best-effort releases durable leases acquired during a
// ClaimTasks call that is about to fail. Without this, a throttled/transient
// post-claim status read abandons every already-claimed task — each holds a
// committed lease hiding it for the full lease duration. Releases are fenced
// on the claim ownership token (see ReleaseLease), so a task reclaimed or
// refreshed since the claim matches nothing and is left alone; release
// errors are ignored because the original error is already being returned.
// Detached from cancellation so a cancelled claim still frees what it
// leased.
func (b *Backend) releaseClaimedLeases(ctx context.Context, tasks []backend.Task) {
	if len(tasks) == 0 {
		return
	}
	rctx := context.WithoutCancel(ctx)
	for _, t := range tasks {
		_ = b.ReleaseLease(rctx, t)
	}
}

// deleteClaimedTask best-effort deletes one claimed task row that turned out
// to be terminal residue. Deleting a missing row is a no-op, so a concurrent
// terminal sweep racing this delete is harmless; failures are ignored
// because the sweep owns whatever remains.
func (b *Backend) deleteClaimedTask(ctx context.Context, id int64) {
	_ = b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{spanner.Delete("wf_tasks", spanner.Key{id})})
	})
}

func scanTask(row *spanner.Row) (backend.Task, error) {
	var t backend.Task
	var refSeq spanner.NullInt64
	var payload spanner.NullJSON
	var worker spanner.NullString
	var hb []byte
	var attempt int64
	if err := row.Columns(&t.ID, &t.Kind, &t.Queue, &t.InstanceID, &refSeq, &payload,
		&attempt, &t.VisibleAt, &worker, &hb); err != nil {
		return t, err
	}
	t.Attempt = int(attempt)
	t.HeartbeatDetails = hb
	if refSeq.Valid {
		t.Seq = refSeq.Int64
	}
	if worker.Valid {
		t.WorkerID = worker.StringVal
	}
	if t.Kind == "activity" {
		var p activityPayload
		_ = json.Unmarshal(jsonBytes(payload), &p)
		t.Name = p.Name
		t.Input = p.Input
		t.MaxAttempts = p.Retry.MaxAttempts
		t.Retry = backend.RetryPolicy{
			InitialInterval:    time.Duration(p.Retry.InitialIntervalMs) * time.Millisecond,
			BackoffCoefficient: p.Retry.BackoffCoefficient,
			MaxInterval:        time.Duration(p.Retry.MaxIntervalMs) * time.Millisecond,
			MaxAttempts:        p.Retry.MaxAttempts,
		}
		t.StartToCloseTimeout = time.Duration(p.StartToCloseTimeoutMs) * time.Millisecond
	}
	return t, nil
}

func (b *Backend) ExtendLease(ctx context.Context, t backend.Task, d time.Duration) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, extendLeaseStatement(nowUTC().Add(d), t))
		if err != nil {
			return err
		}
		if n == 0 {
			return backend.ErrNotFound
		}
		return nil
	})
	return err
}

func extendLeaseStatement(v time.Time, t backend.Task) spanner.Statement {
	return spanner.Statement{
		SQL:    `UPDATE wf_tasks SET visible_at = @v WHERE id = @id AND worker_id = @w AND attempt = @a`,
		Params: map[string]any{"v": v, "id": t.ID, "w": t.WorkerID, "a": int64(t.Attempt)},
	}
}

func (b *Backend) RecordHeartbeat(ctx context.Context, task backend.Task, lease time.Duration, details []byte) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL:    `UPDATE wf_tasks SET visible_at = @v, heartbeat = @h WHERE id = @id AND worker_id = @w AND attempt = @a`,
			Params: map[string]any{"v": nowUTC().Add(lease), "h": details, "id": task.ID, "w": task.WorkerID, "a": int64(task.Attempt)},
		})
		if err != nil {
			return err
		}
		if n == 0 {
			return backend.ErrNotFound
		}
		return nil
	})
	return err
}

// fencedReleaseStatement builds the conditional release DML for the claimed
// task generation. The attempt bind must be INT64 (Go int64): the Spanner
// client rejects a native Go int for an INT64 column, which would fail the
// DML and hide the task until lease expiry.
func fencedReleaseStatement(now time.Time, t backend.Task) spanner.Statement {
	if t.WorkerID != "" {
		// Conditional on the claim ownership token (see sqlite backend).
		return spanner.Statement{
			SQL:    `UPDATE wf_tasks SET visible_at = @v, worker_id = NULL WHERE id = @id AND worker_id = @wid AND attempt = @attempt`,
			Params: map[string]any{"v": now, "id": t.ID, "wid": t.WorkerID, "attempt": int64(t.Attempt)},
		}
	}
	return spanner.Statement{
		SQL:    `UPDATE wf_tasks SET visible_at = @v, worker_id = NULL WHERE id = @id`,
		Params: map[string]any{"v": now, "id": t.ID},
	}
}

// fencedNackStatement builds the conditional nack DML for the claimed task
// generation. See fencedReleaseStatement: attempt must be int64 for INT64.
func fencedNackStatement(now time.Time, t backend.Task, delay time.Duration) spanner.Statement {
	if t.WorkerID != "" {
		// Conditional on the claim ownership token (see ReleaseLease).
		return spanner.Statement{
			SQL:    `UPDATE wf_tasks SET visible_at = @v, worker_id = NULL WHERE id = @id AND worker_id = @wid AND attempt = @attempt`,
			Params: map[string]any{"v": now.Add(delay), "id": t.ID, "wid": t.WorkerID, "attempt": int64(t.Attempt)},
		}
	}
	return spanner.Statement{
		SQL:    `UPDATE wf_tasks SET visible_at = @v, worker_id = NULL WHERE id = @id`,
		Params: map[string]any{"v": now.Add(delay), "id": t.ID},
	}
}

func (b *Backend) ReleaseLease(ctx context.Context, t backend.Task) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := fencedReleaseStatement(nowUTC(), t)
		n, err := txn.Update(ctx, stmt)
		if err != nil {
			return err
		}
		if n == 0 {
			return backend.ErrNotFound
		}
		return nil
	})
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) NackTask(ctx context.Context, t backend.Task, delay time.Duration) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := fencedNackStatement(nowUTC(), t, delay)
		n, err := txn.Update(ctx, stmt)
		if err != nil {
			return err
		}
		if n == 0 {
			return backend.ErrNotFound
		}
		return nil
	})
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) LoadWorkflowHead(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	inst, err := b.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	st := &backend.WorkflowState{
		Instance: *inst,
		NextSeq:  inst.NextSeq,
		Now:      nowUTC(),
	}

	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT id, seq, type, ref_seq, payload, created_at FROM wf_inbox WHERE instance_id = @id ORDER BY seq, created_at, id`,
		Params: map[string]any{"id": instanceID},
	})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var item backend.InboxEvent
		// seq is NULL on rows written before inbox sequencing.
		var seq spanner.NullInt64
		var typ string
		var refSeq spanner.NullInt64
		var payload spanner.NullJSON
		var createdAt time.Time
		if err := row.Columns(&item.ID, &seq, &typ, &refSeq, &payload, &createdAt); err != nil {
			return nil, err
		}
		item.Event.Type = journal.Type(typ)
		if refSeq.Valid {
			item.Event.RefSeq = refSeq.Int64
		}
		item.Event.Name, item.Event.Payload = unwrapInboxPayload(jsonBytes(payload))
		st.Inbox = append(st.Inbox, item)
	}
	return st, nil
}

func (b *Backend) LoadWorkflow(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	st, err := b.LoadWorkflowHead(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	events, err := b.GetJournal(ctx, instanceID, 0)
	if err != nil {
		return nil, err
	}
	st.Journal = events
	return st, nil
}

func (b *Backend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	return b.CommitAdvancements(ctx, []backend.Advancement{adv})
}

func (b *Backend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	if len(advs) == 0 {
		return nil
	}
	// Dedupe keys for terminal advancements are snapshotted INSIDE the commit
	// transaction (serializable read): a SendToInbox serializing before the
	// terminal commit is included in the post-commit sweep instead of
	// lingering until purge (where a later ID reuse would mistake it for a
	// duplicate of a promised post-terminal event). Keys created after the
	// snapshot read stay for purge. The sweep itself still runs after the
	// commit so the mutation count never scales with accumulated keys.
	// The pre-commit incarnation is captured alongside for the sweep fence
	// (see terminalSweep): a purge plus ID reuse interleaved with the sweep
	// aborts it instead of deleting the replacement's recreated guard.
	var snapshots map[string]terminalSweep
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Fresh per attempt: the client retries the closure on abort, and
		// buffered mutations are discarded, so read-your-writes state must
		// reset with it.
		st := newSpannerTxnState()
		snapshots = make(map[string]terminalSweep, len(advs))
		for _, adv := range advs {
			if adv.Terminal != nil {
				if _, ok := snapshots[adv.InstanceID]; !ok {
					ids, err := querySignalDedupeIDsTx(ctx, txn, adv.InstanceID)
					if err != nil {
						return err
					}
					victim, err := queryInstanceVictimTx(ctx, txn, adv.InstanceID)
					if err != nil {
						return err
					}
					snapshots[adv.InstanceID] = terminalSweep{createdAt: victim.createdAt, incarnation: victim.incarnation, dedupeIDs: ids}
				}
			}
			if err := b.commitAdvancementTxn(ctx, txn, st, adv); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Terminal sweeps run before any wake hint and before terminal success
	// is reported: rows beyond the in-transaction mutation budget are
	// removed here in bounded transactions. The sweep uses a detached
	// context so parent cancellation cannot strand survivors, and a
	// persistent failure is surfaced rather than leaving claimable rows
	// behind. A per-instance cleanup failure no longer aborts the batch
	// (Codex round-21 P2 on #291, mirroring the DynamoDB/Firestore
	// continuation fix from round-19): cleanup-retries-exhausted used to
	// return immediately, skipping later instances' sweeps (residue with no
	// recovery: the advancement already committed). The first error is
	// retained while every committed instance is still swept; it is
	// returned at the end, after notifications.
	var cleanupErr error
	for _, adv := range advs {
		if adv.Terminal != nil {
			if err := terminalCleanupFunc(b, context.Background(), adv.InstanceID, true); err != nil && cleanupErr == nil {
				cleanupErr = err
			}
		}
	}
	// Wake terminal subscribers before the fallible ensure loop below: the
	// terminal status already committed, and an ensure error returns early.
	for _, adv := range advs {
		if adv.Terminal != nil {
			b.notifyTerminal(adv.InstanceID)
		}
	}
	for _, adv := range advs {
		if adv.Terminal != nil {
			// Terminal instances take no follow-up task:
			// ensureWorkflowTaskIfInbox is a no-op for non-running
			// instances, so skip the fallible transaction instead of
			// risking terminal success after cleanup already ran.
			continue
		}
		if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return ensureWorkflowTaskIfInbox(ctx, txn, nil, adv.InstanceID)
		}); err != nil {
			return err
		}
	}
	b.notifyTasks()
	for _, adv := range advs {
		if adv.Terminal != nil {
			// Dedupe cleanup stays out of the advancement transaction so the
			// mutation count never scales with accumulated dedupe keys.
			// Best-effort (DynamoDB/Firestore parity); purge reaps leftovers.
			// Only keys snapshotted before the commit are removed: a
			// concurrent SendToInbox with a new DedupeID can land after
			// notifyTerminal fired above, and sweeping its key while the
			// inbox event remains would duplicate a later retry. Every page
			// re-validates the pre-commit incarnation: a purge plus ID
			// reuse interleaved with the sweep aborts it instead of
			// deleting the replacement's recreated guard (see
			// sweepSignalDedupeIDs); purge owns the leftovers. The sweep
			// stays synchronous so a redelivered DedupeID inserts anew once
			// this call returns, but runs under a bounded context so a
			// stuck store delays only this cleanup, never the caller.
			cctx, cancel := context.WithTimeout(context.Background(), signalDedupeSweepTimeout)
			sw := snapshots[adv.InstanceID]
			victim := purgeVictim{id: adv.InstanceID, createdAt: sw.createdAt, incarnation: sw.incarnation}
			guard := func(ctx context.Context) error { return b.checkPurgeVictim(ctx, victim) }
			guardTx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				return b.checkPurgeVictimTx(ctx, txn, victim)
			}
			_ = b.sweepSignalDedupeIDs(cctx, adv.InstanceID, guard, guardTx, sw.dedupeIDs)
			cancel()
		}
	}
	// A failed terminal sweep still surfaces, but only after every later
	// victim was swept and every notification above had its chance.
	return cleanupErr
}

func (b *Backend) withRW(ctx context.Context, fn func(context.Context, *spanner.ReadWriteTransaction) error) error {
	_, err := b.client.ReadWriteTransaction(ctx, fn)
	return err
}

// terminalCleanupMutationBudget caps terminal-cleanup deletions buffered in
// one advancement commit. Cloud Spanner allows 20,000 mutations per commit;
// residual tasks, timers, inbox entries and dedupe rows accumulate over many
// turns, so deleting them all in the terminal commit can exceed that limit
// and leave the terminal transition permanently uncommittable. Cleanup
// beyond the budget is swept post-commit (cleanupTerminalInstance).
const terminalCleanupMutationBudget = 1000

// terminalCleanupSweepBatch bounds the deletions per table of one
// post-commit sweep transaction (at most four batches per commit),
// keeping every commit far below the mutation limit.
const terminalCleanupSweepBatch = 500

// terminalCleanupFunc sweeps an instance's residual docs after a terminal
// advancement commits. It is a variable (not a direct method call) so tests
// can fault-inject a cleanup failure and assert later victims are still
// swept before the retained error surfaces (Codex round-21 P2 on #291);
// production always uses cleanupTerminalInstance.
var terminalCleanupFunc = (*Backend).cleanupTerminalInstance

// cleanupTerminalInstance removes an instance's residual tasks, timers,
// inbox entries and signal dedupe rows left outside the advancement commit
// by the mutation budget, retrying transient failures before terminal
// success is reported. TerminateInstance passes includeInbox=false to
// preserve its long-standing table set (tasks, timers, dedupe only),
// matching the SQL backends.
func (b *Backend) cleanupTerminalInstance(ctx context.Context, id string, includeInbox bool) error {
	const attempts = 5
	var err error
	for i := 0; i < attempts; i++ {
		if err = b.cleanupTerminalInstanceOnce(ctx, id, includeInbox); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		time.Sleep(time.Duration(100*(1<<i)) * time.Millisecond)
	}
	return fmt.Errorf("spanner: terminal cleanup for %s failed after %d attempts: %w", id, attempts, err)
}

func (b *Backend) cleanupTerminalInstanceOnce(ctx context.Context, id string, includeInbox bool) error {
	for {
		n, err := b.deleteTerminalBatch(ctx, id, includeInbox)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// deleteTerminalBatch deletes up to terminalCleanupSweepBatch residual rows
// per table in one transaction and reports how many rows were removed. Only
// rows predating the terminal transition are swept (Codex round-21 P2 on
// #291): the sweep runs post-commit, so a SendToInboxBatch that starts after
// the status commit can accept and insert its dedupe marker plus inbox row
// mid-sweep, and an unrestricted sweep deletes the inbox row while the
// dedupe phase already passed — the marker survives, the event is lost, and
// every later send under the same DedupeID is discarded. The cutoff is the
// instance's sweep_commit_ts read in this same transaction: the terminal
// commit's own commit timestamp (see commitTimestamp), and every swept
// table's created_at is a commit timestamp too, so the comparison orders the
// two commits exactly with no cross-process clock skew (Codex round-22 P2 on
// #291). Rows created after it are the racing sends and must survive. A
// missing instance row or ticks on neither column (legacy rows) falls back
// to the unbounded sweep, and wf_timers has no created_at column — timers
// can only be created by a non-terminal advancement, so no racing send can
// add one post-commit and the unbounded timer sweep is exact.
// Pre-commit-timestamp rows (created_at stamped by writer wall clocks
// before the migration, or swept under a NULL sweep_commit_ts via the
// completed_at fallback in readCompletedAt) still compare by wall time
// against the cutoff — the same approximation as before, converging as old
// rows age out.
//
// The in-commit budget deletes (commitAdvancementTxn, TerminateInstance)
// stay unbounded: their reads are snapshot-isolated inside the status-flip
// transaction, so they can only ever see pre-commit rows, and both sides of
// a swept send (dedupe marker and inbox row) are removed atomically with the
// flip — the marker-without-inbox split the cutoff guards against cannot
// arise there.
func (b *Backend) deleteTerminalBatch(ctx context.Context, id string, includeInbox bool) (int, error) {
	var n int
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n = 0
		cutoff, hasCutoff, err := readCompletedAt(ctx, txn, id)
		if err != nil {
			return err
		}
		dMuts, err := deleteSignalDedupe(ctx, txn, id, terminalCleanupSweepBatch, cutoff, hasCutoff)
		if err != nil {
			return err
		}
		tMuts, err := deleteTasksForInstance(ctx, txn, id, 0, terminalCleanupSweepBatch, cutoff, hasCutoff)
		if err != nil {
			return err
		}
		tmMuts, err := deleteTimersForInstance(ctx, txn, id, terminalCleanupSweepBatch)
		if err != nil {
			return err
		}
		muts := append(append(dMuts, tMuts...), tmMuts...)
		if includeInbox {
			inMuts, err := deleteInboxForInstance(ctx, txn, id, terminalCleanupSweepBatch, cutoff, hasCutoff)
			if err != nil {
				return err
			}
			muts = append(muts, inMuts...)
		}
		n = len(muts)
		return txn.BufferWrite(muts)
	})
	return n, err
}

func (b *Backend) commitAdvancementTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, st *spannerTxnState, adv backend.Advancement) error {
	if st == nil {
		st = newSpannerTxnState()
	}
	row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{adv.InstanceID},
		[]string{"status", "next_seq", "queue", "parent_id", "parent_seq"})
	if err != nil {
		if isNotFound(err) {
			return backend.ErrConflict
		}
		return err
	}
	var status string
	var nextSeq int64
	var queue string
	var parentID spanner.NullString
	var parentSeq spanner.NullInt64
	if err := row.Columns(&status, &nextSeq, &queue, &parentID, &parentSeq); err != nil {
		return err
	}
	// Reject commits for instances that already left running (Codex round-25
	// P1 on #328, mirroring #327's round-23 status gate): only part of the
	// terminal cleanup rides in the TerminateInstance flip transaction
	// (terminalCleanupMutationBudget), so the owned workflow task can survive
	// the flip when dedupe rows exhaust the budget. A worker holding that
	// task still matches ExpectedSeq/TaskID, and without this gate a
	// nonterminal advancement commits during the post-commit sweep —
	// post-sweep activities/children then survive/execute after termination.
	// Reading the status in-txn also conflicts with a concurrent flip,
	// serializing the commit against termination.
	if status != "running" {
		return backend.ErrConflict
	}
	if nextSeq != adv.ExpectedSeq {
		return backend.ErrConflict
	}
	newSeq := adv.ExpectedSeq
	for _, ev := range adv.NewEvents {
		if ev.Seq+1 > newSeq {
			newSeq = ev.Seq + 1
		}
	}
	now := nowUTC()

	taskRow, err := txn.ReadRow(ctx, "wf_tasks", spanner.Key{adv.TaskID}, []string{"kind", "instance_id", "worker_id", "attempt"})
	if err != nil {
		if isNotFound(err) {
			return backend.ErrConflict
		}
		return err
	}
	var kind, taskInst string
	var worker spanner.NullString
	var attempt int64
	if err := taskRow.Columns(&kind, &taskInst, &worker, &attempt); err != nil {
		return err
	}
	if kind != "workflow" || taskInst != adv.InstanceID {
		return backend.ErrConflict
	}
	if adv.WorkerID != "" {
		got := ""
		if worker.Valid {
			got = worker.StringVal
		}
		if got != adv.WorkerID || int(attempt) != adv.Attempt {
			return backend.ErrConflict
		}
	}
	// Read-your-writes: Spanner buffers mutations client-side until commit,
	// so queries below do not see them. The owned workflow task is deleted
	// later in this commit; record it before any existence check, otherwise
	// the check sees the stale row and skips the follow-up insert, leaving
	// no claimable task (TerminalCleanup "claim wf for terminal" empty).
	st.wfDeleted[adv.TaskID] = true
	if adv.Terminal != nil {
		st.terminal[adv.InstanceID] = true
	} else {
		for _, inboxID := range adv.DrainedInbox {
			st.inboxDeleted[inboxID] = true
		}
	}

	var muts []*spanner.Mutation
	muts = append(muts, spanner.UpdateMap("wf_instances", map[string]any{
		"id":         adv.InstanceID,
		"next_seq":   newSeq,
		"updated_at": now,
	}))
	for _, ev := range adv.NewEvents {
		muts = append(muts, spanner.InsertMap("wf_journal", map[string]any{
			"instance_id": adv.InstanceID,
			"seq":         ev.Seq,
			"type":        string(ev.Type),
			"name":        ev.Name,
			"ref_seq":     nullInt(ev.RefSeq),
			"payload":     jsonVal(ev.Payload),
			"recorded_at": now,
		}))
	}
	if backend.HasSearchAttributesUpdate(adv.NewEvents) {
		muts = append(muts, spanner.UpdateMap("wf_instances", map[string]any{
			"id":                adv.InstanceID,
			"search_attributes": jsonVal(backend.MarshalSearchAttributes(backend.LastSearchAttributesUpdate(adv.NewEvents))),
			"updated_at":        now,
		}))
	}
	if backend.HasMemoUpdate(adv.NewEvents) {
		muts = append(muts, spanner.UpdateMap("wf_instances", map[string]any{
			"id":         adv.InstanceID,
			"memo":       jsonVal(backend.MarshalSearchAttributes(backend.LastMemoUpdate(adv.NewEvents))),
			"updated_at": now,
		}))
	}
	for _, at := range adv.ActivityTasks {
		if adv.Terminal != nil {
			break
		}
		payload, _ := json.Marshal(activityPayload{
			Name:  at.Name,
			Input: at.Input,
			Retry: retryJSON{
				InitialIntervalMs:  at.Retry.InitialInterval.Milliseconds(),
				BackoffCoefficient: at.Retry.BackoffCoefficient,
				MaxIntervalMs:      at.Retry.MaxInterval.Milliseconds(),
				MaxAttempts:        at.MaxAttempts,
			},
			StartToCloseTimeoutMs: at.StartToCloseTimeout.Milliseconds(),
		})
		q := at.Queue
		if q == "" {
			q = "default"
		}
		m := map[string]any{
			"id":          newID(),
			"kind":        "activity",
			"queue":       q,
			"instance_id": at.InstanceID,
			"ref_seq":     nullInt(at.Seq),
			"payload":     jsonVal(payload),
			"attempt":     int64(0),
			"visible_at":  now,
			"created_at":  commitTimestamp(),
		}
		if at.MaxAttempts > 0 {
			m["max_attempts"] = int64(at.MaxAttempts)
		}
		muts = append(muts, spanner.InsertMap("wf_tasks", m))
	}
	for _, tm := range adv.Timers {
		if adv.Terminal != nil {
			break
		}
		muts = append(muts, spanner.InsertMap("wf_timers", map[string]any{
			"instance_id": adv.InstanceID,
			"seq":         tm.Seq,
			"fire_at":     tm.FireAt.UTC(),
		}))
	}
	if adv.Terminal != nil {
		m := map[string]any{
			"id":              adv.InstanceID,
			"status":          adv.Terminal.Status,
			"result":          jsonVal(adv.Terminal.Result),
			"failure":         jsonVal(adv.Terminal.Failure),
			"updated_at":      now,
			"completed_at":    now,
			"sweep_commit_ts": commitTimestamp(),
		}
		muts = append(muts, spanner.UpdateMap("wf_instances", m))
		// Terminal cleanup cannot ride along unbounded: tasks, timers,
		// inbox entries and dedupe rows accumulate across turns, and one
		// delete mutation per row can exceed Cloud Spanner's per-commit
		// mutation limit, wedging the terminal transition permanently.
		// Buffer at most the budget below; the remainder is swept
		// post-commit in bounded transactions (cleanupTerminalInstance).
		// ClaimTasks refuses tasks of non-running instances, so leftovers
		// are never executed in the meantime.
		budget := terminalCleanupMutationBudget
		dMuts, err := deleteSignalDedupe(ctx, txn, adv.InstanceID, budget, time.Time{}, false)
		if err != nil {
			return err
		}
		muts = append(muts, dMuts...)
		budget = max(budget-len(dMuts), 0)
		tMuts, err := deleteTasksForInstance(ctx, txn, adv.InstanceID, adv.TaskID, budget, time.Time{}, false)
		if err != nil {
			return err
		}
		muts = append(muts, tMuts...)
		budget = max(budget-len(tMuts), 0)
		tmMuts, err := deleteTimersForInstance(ctx, txn, adv.InstanceID, budget)
		if err != nil {
			return err
		}
		muts = append(muts, tmMuts...)
		budget = max(budget-len(tmMuts), 0)
		inMuts, err := deleteInboxForInstance(ctx, txn, adv.InstanceID, budget, time.Time{}, false)
		if err != nil {
			return err
		}
		muts = append(muts, inMuts...)
	}
	if adv.Terminal != nil {
		// Full inbox sweep above already removed every row; the drained
		// deletes would be duplicates.
	} else {
		for _, inboxID := range adv.DrainedInbox {
			muts = append(muts, spanner.Delete("wf_inbox", spanner.Key{inboxID}))
		}
	}
	// Fence child creation on purge markers (Codex round 12 on #296): only
	// direct CreateInstance checked the marker, so a child — or a
	// Continue-As-New successor, which is also an adv.Children entry —
	// reusing a purged ID recreated the instance while the old incarnation's
	// rows were still pending, and the replacement consumed purged signals.
	// A hit fails the advancement with ErrConflict so the worker retries
	// after purge recovery clears the marker.
	for _, ch := range adv.Children {
		if _, merr := txn.ReadRow(ctx, "wf_purge_markers", spanner.Key{ch.ID}, []string{"instance_id"}); merr == nil {
			return backend.ErrConflict
		} else if !isNotFound(merr) {
			return merr
		}
	}
	for _, ch := range adv.Children {
		q := ch.Queue
		if q == "" {
			q = "default"
		}
		muts = append(muts,
			spanner.InsertMap("wf_instances", map[string]any{
				"id": ch.ID, "name": ch.Name, "queue": q, "status": "running",
				"input": jsonVal(ch.Input), "next_seq": int64(2),
				"parent_id": ch.ParentID, "parent_seq": ch.ParentSeq,
				"search_attributes": jsonVal(backend.MarshalSearchAttributes(ch.SearchAttributes)),
				"memo":              jsonVal(backend.MarshalSearchAttributes(ch.Memo)),
				"created_at":        now, "updated_at": now,
				incarnationColumn: newIncarnation(),
			}),
			spanner.InsertMap("wf_journal", map[string]any{
				"instance_id": ch.ID, "seq": int64(1),
				"type": string(journal.TypeWorkflowStarted), "name": ch.Name,
				"payload": jsonVal(ch.Input), "recorded_at": now,
			}),
			spanner.InsertMap("wf_tasks", map[string]any{
				"id": newID(), "kind": "workflow", "queue": q, "instance_id": ch.ID,
				"attempt": int64(0), "visible_at": now, "created_at": commitTimestamp(),
			}),
		)
	}
	if err := txn.BufferWrite(muts); err != nil {
		return err
	}
	muts = nil

	if adv.ParentNotify != nil && parentID.Valid && parentID.StringVal != "" {
		pRow, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{parentID.StringVal}, []string{"status", "queue"})
		if err != nil {
			return err
		}
		var parentStatus, parentQueue string
		if err := pRow.Columns(&parentStatus, &parentQueue); err != nil {
			return err
		}
		ev := *adv.ParentNotify
		if ev.RefSeq == 0 && parentSeq.Valid {
			ev.RefSeq = parentSeq.Int64
		}
		inboxID := newID()
		pseq, existed, err := readInboxSeq(ctx, txn, parentID.StringVal)
		if err != nil {
			return err
		}
		pseq++
		muts := append(inboxSeqMuts(parentID.StringVal, pseq, existed),
			spanner.InsertMap("wf_inbox", map[string]any{
				"id": inboxID, "instance_id": parentID.StringVal,
				"seq": pseq, "type": string(ev.Type), "ref_seq": nullInt(ev.RefSeq),
				"payload": jsonVal(ev.Payload), "created_at": commitTimestamp(),
			}))
		if err := txn.BufferWrite(muts); err != nil {
			return err
		}
		st.inboxAdded[parentID.StringVal]++
		if parentStatus == "running" {
			if err := enqueueWorkflowTask(ctx, txn, st, parentID.StringVal, parentQueue, now); err != nil {
				return err
			}
		}
	}

	if err := txn.BufferWrite([]*spanner.Mutation{
		spanner.Delete("wf_tasks", spanner.Key{adv.TaskID}),
	}); err != nil {
		return err
	}
	if adv.Terminal != nil {
		// Terminal instances take no follow-up task. The status flip is
		// only buffered, so the running check below would still see the
		// stale pre-commit row; skip explicitly instead.
		return nil
	}
	if err := ensureWorkflowTaskIfInbox(ctx, txn, st, adv.InstanceID); err != nil {
		return err
	}
	if adv.EnsureWorkflowTask {
		// Truncated fanout: force a follow-up tick even though the
		// remaining work replays (no inbox yet). enqueueWorkflowTask reads
		// the instance row itself, so no separate pre-read is needed, and
		// its error must fail the advancement: without the follow-up task
		// the uncommitted remainder could never be reached.
		return enqueueWorkflowTask(ctx, txn, st, adv.InstanceID, "", nowUTC())
	}
	return nil
}

// spannerTxnState tracks buffered writes within one Spanner read-write
// transaction for read-your-writes. Spanner buffers mutations client-side
// until commit: queries in the same transaction do NOT see them, so the
// check-then-insert workflow-task helpers consult this state to observe
// post-commit state.
type spannerTxnState struct {
	// wfEnqueued marks instances with a workflow-task insert buffered in
	// this transaction (dedupes repeat enqueues, which would otherwise
	// violate the wf_tasks_wf_singleton unique index at commit).
	wfEnqueued map[string]bool
	// wfDeleted marks task IDs with a delete buffered in this transaction
	// (the owned workflow task being committed, terminal sweep victims).
	wfDeleted map[int64]bool
	// terminal marks instances known terminal in this transaction (the
	// status flip is only buffered, so status reads still see running).
	terminal map[string]bool
	// inboxDeleted marks inbox IDs with a delete buffered in this
	// transaction (drained rows); inboxAdded counts inbox inserts buffered
	// per instance.
	inboxDeleted map[int64]bool
	inboxAdded   map[string]int
}

func newSpannerTxnState() *spannerTxnState {
	return &spannerTxnState{
		wfEnqueued:   map[string]bool{},
		wfDeleted:    map[int64]bool{},
		terminal:     map[string]bool{},
		inboxDeleted: map[int64]bool{},
		inboxAdded:   map[string]int{},
	}
}

func enqueueWorkflowTask(ctx context.Context, txn *spanner.ReadWriteTransaction, st *spannerTxnState, instanceID, queue string, now time.Time) error {
	if st != nil {
		if st.terminal[instanceID] {
			return nil
		}
		if st.wfEnqueued[instanceID] {
			return nil
		}
	}
	row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{instanceID}, []string{"queue", "status"})
	if err != nil {
		return err
	}
	var status string
	var q string
	if err := row.Columns(&q, &status); err != nil {
		return err
	}
	if status != "running" {
		return nil
	}
	if queue == "" {
		queue = q
	}
	// Spanner reports unique violations at commit; check first (insert-or-ignore).
	// The check must ignore tasks deleted earlier in this transaction: the
	// owned workflow task's delete is only buffered, so the pre-commit row
	// is still returned and would wrongly suppress the follow-up insert.
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT id FROM wf_tasks WHERE kind = 'workflow' AND instance_id = @id`,
		Params: map[string]any{"id": instanceID},
	})
	live := false
	for {
		r, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter.Stop()
			return err
		}
		var tid int64
		if err := r.Columns(&tid); err != nil {
			iter.Stop()
			return err
		}
		if st != nil && st.wfDeleted[tid] {
			continue
		}
		live = true
		break
	}
	iter.Stop()
	if live {
		if st != nil {
			st.wfEnqueued[instanceID] = true
		}
		return nil
	}
	if err := txn.BufferWrite([]*spanner.Mutation{
		spanner.InsertMap("wf_tasks", map[string]any{
			"id": newID(), "kind": "workflow", "queue": queue, "instance_id": instanceID,
			"attempt": int64(0), "visible_at": now, "created_at": commitTimestamp(),
		}),
	}); err != nil {
		return err
	}
	if st != nil {
		st.wfEnqueued[instanceID] = true
	}
	return nil
}

func ensureWorkflowTaskIfInbox(ctx context.Context, txn *spanner.ReadWriteTransaction, st *spannerTxnState, instanceID string) error {
	if st != nil && st.terminal[instanceID] {
		return nil
	}
	row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{instanceID}, []string{"status", "queue"})
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	var status, queue string
	if err := row.Columns(&status, &queue); err != nil {
		return err
	}
	if status != "running" {
		return nil
	}
	if st != nil && st.inboxAdded[instanceID] > 0 {
		return enqueueWorkflowTask(ctx, txn, st, instanceID, queue, nowUTC())
	}
	// The inbox check must ignore rows drained earlier in this transaction:
	// their deletes are only buffered, so drained rows still read back and
	// would otherwise cause a spurious follow-up task.
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT id FROM wf_inbox WHERE instance_id = @id`,
		Params: map[string]any{"id": instanceID},
	})
	for {
		r, err := iter.Next()
		if err == iterator.Done {
			iter.Stop()
			return nil
		}
		if err != nil {
			iter.Stop()
			return err
		}
		var inboxID int64
		if err := r.Columns(&inboxID); err != nil {
			iter.Stop()
			return err
		}
		if st != nil && st.inboxDeleted[inboxID] {
			continue
		}
		iter.Stop()
		return enqueueWorkflowTask(ctx, txn, st, instanceID, queue, nowUTC())
	}
}

func (b *Backend) CompleteActivity(ctx context.Context, claim backend.Task, ev journal.Event) error {
	now := nowUTC()
	var wake bool
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "wf_tasks", spanner.Key{claim.ID},
			[]string{"instance_id", "ref_seq", "kind", "worker_id", "attempt"})
		if err != nil {
			if isNotFound(err) {
				return backend.ErrSuperseded
			}
			return err
		}
		var instanceID, kind string
		var workerID spanner.NullString
		var refSeq spanner.NullInt64
		var attempt int64
		if err := row.Columns(&instanceID, &refSeq, &kind, &workerID, &attempt); err != nil {
			return err
		}
		if kind != "activity" || !workerID.Valid || workerID.StringVal != claim.WorkerID || attempt != int64(claim.Attempt) {
			return backend.ErrSuperseded
		}
		if err := txn.BufferWrite([]*spanner.Mutation{
			spanner.Delete("wf_tasks", spanner.Key{claim.ID}),
		}); err != nil {
			return err
		}
		inst, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{instanceID}, []string{"status", "queue"})
		if err != nil {
			return err
		}
		var status, queue string
		if err := inst.Columns(&status, &queue); err != nil {
			return err
		}
		if status != "running" {
			return nil
		}
		if ev.RefSeq == 0 && refSeq.Valid {
			ev.RefSeq = refSeq.Int64
		}
		seq, existed, err := readInboxSeq(ctx, txn, instanceID)
		if err != nil {
			return err
		}
		seq++
		if err := txn.BufferWrite(append(inboxSeqMuts(instanceID, seq, existed),
			spanner.InsertMap("wf_inbox", map[string]any{
				"id": newID(), "instance_id": instanceID, "seq": seq, "type": string(ev.Type),
				"ref_seq": nullInt(ev.RefSeq), "payload": jsonVal(ev.Payload), "created_at": commitTimestamp(),
			}))); err != nil {
			return err
		}
		wake = true
		return enqueueWorkflowTask(ctx, txn, nil, instanceID, queue, now)
	})
	if err != nil {
		return err
	}
	if wake {
		b.notifyTasks()
	}
	return nil
}

func (b *Backend) RetryActivity(ctx context.Context, claim backend.Task, delay time.Duration) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE wf_tasks SET visible_at = @v, worker_id = NULL
				WHERE id = @id AND kind = 'activity' AND worker_id = @wid AND attempt = @attempt`,
			Params: map[string]any{"v": nowUTC().Add(delay), "id": claim.ID, "wid": claim.WorkerID, "attempt": int64(claim.Attempt)},
		})
		if err != nil {
			return err
		}
		if n == 0 {
			return backend.ErrSuperseded
		}
		return nil
	})
	return err
}

func (b *Backend) FireDueTimers(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 1
	}
	var n int
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n = 0
		now := nowUTC()
		// Shared across dues in this transaction: repeat fires for the same
		// instance must not buffer duplicate workflow-task inserts (reads
		// do not see the first insert; the unique index would fail commit).
		st := newSpannerTxnState()
		iter := txn.Query(ctx, spanner.Statement{
			SQL: `SELECT instance_id, seq FROM wf_timers
				WHERE fire_at <= @now ORDER BY fire_at, instance_id, seq LIMIT @limit`,
			Params: map[string]any{"now": now, "limit": int64(limit)},
		})
		type due struct {
			instanceID string
			seq        int64
		}
		var dues []due
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				return err
			}
			var d due
			if err := row.Columns(&d.instanceID, &d.seq); err != nil {
				iter.Stop()
				return err
			}
			dues = append(dues, d)
		}
		iter.Stop()
		for _, d := range dues {
			delN, err := txn.Update(ctx, spanner.Statement{
				SQL:    `DELETE FROM wf_timers WHERE instance_id = @id AND seq = @seq`,
				Params: map[string]any{"id": d.instanceID, "seq": d.seq},
			})
			if err != nil {
				return err
			}
			if delN == 0 {
				continue
			}
			n++
			instRow, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{d.instanceID}, []string{"status"})
			if err != nil {
				if isNotFound(err) {
					continue
				}
				return err
			}
			var status string
			if err := instRow.Columns(&status); err != nil {
				return err
			}
			if status != "running" {
				continue
			}
			seq, existed, err := readInboxSeq(ctx, txn, d.instanceID)
			if err != nil {
				return err
			}
			seq++
			muts := append(inboxSeqMuts(d.instanceID, seq, existed),
				spanner.InsertMap("wf_inbox", map[string]any{
					"id": newID(), "instance_id": d.instanceID, "seq": seq,
					"type": string(journal.TypeTimerFired), "ref_seq": d.seq, "created_at": commitTimestamp(),
				}))
			if err := txn.BufferWrite(muts); err != nil {
				return err
			}
			if err := enqueueWorkflowTask(ctx, txn, st, d.instanceID, "", now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return n, err
	}
	if n > 0 {
		b.notifyTasks()
	}
	return n, nil
}

func (b *Backend) SendToInbox(ctx context.Context, instanceID string, ev journal.Event, dedupeID string) error {
	return b.SendToInboxBatch(ctx, instanceID, []backend.InboxItem{{Event: ev, DedupeID: dedupeID}})
}

// readDedupeRow reads a dedupe guard row with its format version and fallback
// owner. ok=false when the row is absent. Rows predating the format_version
// column read a NULL version, i.e. legacy (v0); rows predating the
// fallback_owner column read a NULL owner (see matchDedupeRow). The stored
// instance_id is validated against the probing instance (owned=false on
// mismatch): under composite keys a mismatch is structurally impossible, but
// the check mirrors Firestore's docInstanceMatches so both backends enforce
// the same ownership invariant (Codex round-16 on #296). Callers must still
// count an unowned row as occupying its key (a Create over it would collide)
// while never treating it as a match.
func readDedupeRow(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID, key string) (stored string, owner spanner.NullString, version spanner.NullInt64, owned, ok bool, err error) {
	row, err := txn.ReadRow(ctx, "wf_signal_dedupe", spanner.Key{instanceID, key}, []string{"instance_id", "dedupe_id", dedupeFallbackOwnerColumn, dedupeFormatVersionColumn})
	if err != nil {
		if isNotFound(err) {
			return "", spanner.NullString{}, spanner.NullInt64{}, false, false, nil
		}
		return "", spanner.NullString{}, spanner.NullInt64{}, false, false, err
	}
	var instOwner string
	if err := row.Columns(&instOwner, &stored, &owner, &version); err != nil {
		return "", spanner.NullString{}, spanner.NullInt64{}, false, false, err
	}
	return stored, owner, version, instOwner == instanceID, true, nil
}

func (b *Backend) SendToInboxBatch(ctx context.Context, instanceID string, items []backend.InboxItem) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) > backend.InboxBatchLimit(b.Capabilities()) {
		return backend.ErrBatchTooLarge
	}
	// No terminal-specific cap here (cf. Firestore's
	// firestoreTerminalInboxBatchLimit, Codex round-25 P2 on #296): a Spanner
	// terminal first-send costs at most one marker row plus one base-guard
	// row plus one inbox row (3 mutations) plus one inbox-seq mutation —
	// 100*3+1=301 mutations, far below the 20,000-mutation commit limit, and
	// composite (instance_id, dedupe_id) keys never alias across IDs, so no
	// FRAMING dual-write is needed (cf. Firestore's framed/legacy doc IDs).
	// Old-node visibility comes from the guards' keys themselves, not a
	// second leg: ambiguously-encoded IDs guard solely at the fallback key
	// (the raw verbatim key for short IDs — exactly what old nodes probe),
	// and identity-encoded IDs store verbatim anyway (Codex round-28 P1 on
	// #296, superseding the round-26/27 raw compat leg, which always
	// duplicated the sole guard or exceeded the key budget).
	var inserted int
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Row creation order comes from commit timestamps, not this clock
		// (see commitTimestamp): dedupe and inbox rows stamp the
		// transaction's commit time, so a send that loses to the terminal
		// commit and retries is ordered after the terminal completed_at no
		// matter how skewed this worker's clock is. now is still refreshed
		// per attempt for visible_at below.
		now := nowUTC()
		inserted = 0
		row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{instanceID}, []string{"status", "queue"})
		if err != nil {
			if isNotFound(err) {
				return backend.ErrNotFound
			}
			return err
		}
		var status string
		var queue string
		if err := row.Columns(&status, &queue); err != nil {
			return err
		}
		seq, existed, err := readInboxSeq(ctx, txn, instanceID)
		if err != nil {
			return err
		}
		var muts []*spanner.Mutation
		// Track DedupeIDs reserved in this transaction: ReadRow only sees committed
		// rows, so same-batch duplicates would otherwise emit colliding InsertMaps.
		// The same holds for guard storage keys across DISTINCT IDs (Codex
		// round-18 on #296 P2): one item's fallback key can be another's
		// canonical key, and buffered inserts stay invisible to later probes,
		// so every chosen guard key is reserved in reservedGuardKeys below
		// and treated as occupied.
		// Terminal sends must not be swallowed by pre-terminal dedupe keys:
		// a SendToInbox racing a terminal transition (CommitAdvancements or
		// TerminateInstance) can observe a key snapshotted for the post-commit
		// sweep. The sweep deletes exactly the snapshotted keys, so a send
		// that commits in the notify-to-sweep window must insert its own
		// event immediately: suppressing it on the doomed pre-terminal row
		// while stamping only a marker loses the signal permanently once the
		// sweep removes the row — the marker then suppresses every retry
		// (Codex round-15 on #296). Sends that commit while the instance is
		// already terminal insert on the first post-terminal send; retries
		// dedupe via a post-terminal marker (see postTerminalDedupeMarker):
		// the first terminal send creates the marker alongside the event,
		// later retries see it and skip. The base key is created when absent
		// but an owned base key — legacy or versioned — never suppresses
		// a terminal insert — only the marker does.
		terminal := status != "running"
		created := map[string]bool{}
		reservedGuardKeys := map[string]bool{}
		for _, it := range items {
			if it.DedupeID != "" {
				if created[it.DedupeID] {
					continue
				}
				if terminal {
					// Retry check first: marker present means this DedupeID
					// already inserted post-terminal. Markers live in their
					// own table (see postTerminalMarkersTable), never in
					// the dedupe keyspace: no legacy verbatim user row —
					// however marker-shaped — can match this probe, and
					// pre-upgrade marker rows left behind in
					// wf_signal_dedupe are inert (a pre-upgrade retry may
					// duplicate once, never drop; purge reaps the rows).
					// The stored instance_id is validated like every
					// dedupe read (Codex round-16 on #296): a no-op under
					// composite keys, kept identical to Firestore.
					mowner, merr := txn.ReadRow(ctx, postTerminalMarkersTable, spanner.Key{instanceID, dedupeMarkerKey(it.DedupeID)}, []string{"instance_id"})
					if merr == nil {
						var mInstanceID string
						if cerr := mowner.Columns(&mInstanceID); cerr != nil {
							return cerr
						}
						if mInstanceID == instanceID {
							created[it.DedupeID] = true
							continue
						}
					} else if !isNotFound(merr) {
						return merr
					}
					// First post-terminal send: stamp the marker; create
					// the base key too when absent for sweep consistency.
					// An owned base guard (versioned or legacy) never
					// suppresses the insert: a pre-terminal key's event was
					// swept, so a fresh post-terminal delivery is promised
					// (reset semantics — see TestTerminalSendBypassesStaleDedupe)
					// — and a key observed here may itself be snapshotted for
					// a sweep that has not run yet (notify-to-sweep window,
					// Codex round-15 on #296): suppressing on it while
					// stamping only a marker loses the signal once the sweep
					// removes the row, with the marker then suppressing every
					// retry. Only the marker suppresses terminal retries.
					// (A pre-upgrade legacy post-terminal retry guard therefore
					// duplicates once on its first post-upgrade retry instead
					// of suppressing — the safe direction: never drop. The
					// marker mutation above still dedupes all later retries.)
					// Marker-shaped candidates are never user keys:
					// markers live in their own table now, so a row shaped
					// like one is either an inert pre-upgrade marker or a
					// legacy verbatim row no probe may mistake for this
					// DedupeID's guard (skipping it duplicates at worst,
					// never drops).
					muts = append(muts, spanner.InsertMap(postTerminalMarkersTable, map[string]any{
						"instance_id": instanceID, "marker_key": dedupeMarkerKey(it.DedupeID), "created_at": now,
					}))
					baseExists := false
					canonicalOccupied := false
					canonKey := escapeDedupeID(it.DedupeID)
					fallbackKey := rawFallbackDedupeKey(it.DedupeID)
					fallbackOccupied := false
					for _, bk := range dedupeKeyCandidates(it.DedupeID) {
						if isPostTerminalMarkerKey(bk) {
							continue
						}
						stored, owner, version, owned, ok, err := readDedupeRow(ctx, txn, instanceID, bk)
						if err != nil {
							return err
						}
						if !ok {
							continue
						}
						if bk == canonKey {
							canonicalOccupied = true
						}
						if bk == fallbackKey {
							fallbackOccupied = true
						}
						if !owned {
							continue
						}
						if matchDedupeRow(it.DedupeID, bk, stored, owner, version) {
							baseExists = true
							break
						}
					}
					// Same-batch guard keys are invisible to the probes above
					// (reads see committed rows only): a canonical key
					// reserved by an earlier terminal item counts as occupied,
					// so the second insert degrades to marker-only instead of
					// failing the commit on a duplicate insert.
					if reservedGuardKeys[canonKey] {
						canonicalOccupied = true
					}
					if reservedGuardKeys[fallbackKey] {
						fallbackOccupied = true
					}
					if canonKey != it.DedupeID {
						// Ambiguously-encoded ID (Codex round-28 P1 on #296):
						// the escaped base guard would sit at another ID's
						// verbatim probe key, where a pre-upgrade node
						// mistakes it for its own guard and silently drops
						// that ID's first send. Guard solely at the fallback
						// key instead (see soleAmbiguousGuardKey, same shape
						// as the running path above); an occupied fallback
						// degrades to marker-only like the occupied case
						// below. No round-27 raw leg rides along — short: it
						// would duplicate the primary; long: over budget.
						if baseExists || fallbackOccupied {
							created[it.DedupeID] = true
						} else if key, ver, ok := soleAmbiguousGuardKey(it.DedupeID, fallbackKey, false, reservedGuardKeys); ok {
							reservedGuardKeys[key] = true
							m := map[string]any{
								"instance_id": instanceID, "dedupe_id": key, "created_at": commitTimestamp(),
							}
							if ver >= dedupeFormatRawKeyVersion {
								m[dedupeFormatVersionColumn] = ver
								m[dedupeFallbackOwnerColumn] = escapeDedupeID(it.DedupeID)
							}
							muts = append(muts, spanner.InsertMap("wf_signal_dedupe", m))
							created[it.DedupeID] = true
						} else {
							created[it.DedupeID] = true
						}
					} else if baseExists || canonicalOccupied {
						created[it.DedupeID] = true
					} else {
						reservedGuardKeys[canonKey] = true
						muts = append(muts, spanner.InsertMap("wf_signal_dedupe", map[string]any{
							"instance_id": instanceID, "dedupe_id": canonKey,
							dedupeFormatVersionColumn: dedupeFormatVersion, "created_at": commitTimestamp(),
						}))
						created[it.DedupeID] = true
					}
				} else {
					// Probe every stored user-key form, legacy raw first (Codex
					// round 8 on #327): pre-escape rows stored "__" IDs verbatim.
					// Ownership is version-aware (Codex round 13 on #296, see
					// matchDedupeRow): a hit counts only when the row guards
					// THIS DedupeID.
					// Marker-shaped candidates are honored here (Codex round 12
					// on #296): live markers live in their own table, and a
					// running instance cannot own a post-terminal marker, so a
					// marker-shaped row on a running instance is unambiguously
					// a legacy user key (e.g. DedupeID "__post_terminal__:x"
					// stored raw pre-escape). Skipping it would miss the guard
					// and duplicate the event. Terminal instances keep the
					// marker-only rule (see the terminal base check above).
					baseHit := false
					canonicalOccupied := false
					fallbackKey := rawFallbackDedupeKey(it.DedupeID)
					fallbackOccupied := false
					for _, bk := range dedupeKeyCandidates(it.DedupeID) {
						stored, owner, version, owned, ok, err := readDedupeRow(ctx, txn, instanceID, bk)
						if err != nil {
							return err
						}
						if !ok {
							continue
						}
						if bk == escapeDedupeID(it.DedupeID) {
							canonicalOccupied = true
						}
						if bk == fallbackKey {
							fallbackOccupied = true
						}
						if !owned {
							continue
						}
						if matchDedupeRow(it.DedupeID, bk, stored, owner, version) {
							baseHit = true
							break
						}
					}
					if baseHit {
						created[it.DedupeID] = true
						continue
					}
					// A foreign-owner row at this DedupeID's canonical key
					// means the canonical guard cannot be created (it would
					// collide), so the guard falls back to the
					// rawFallbackDedupeKey with an explicit version
					// (dedupeFormatRawKeyVersion) plus the owner's canonical
					// form in fallback_owner (Codex round-17 on #296):
					// delivery is preserved and retries keep deduping, and
					// no other ID's probe can claim the guard. The fallback
					// key stays within the STRING(255) budget even for
					// over-budget IDs (bounded second-level hash — Codex
					// round-16 on #296). Guard keys chosen earlier in this
					// batch count as occupied (Codex round-18 on #296 P2,
					// see pickSpannerDedupeInsert): without the reservation
					// two items choosing one key fail the whole batch
					// deterministically. Only when no key is free does the
					// event insert unguarded (duplicate-never-drop).
					canonKey := dedupeKey(it.DedupeID)
					if canonKey != it.DedupeID {
						// Ambiguously-encoded ID (Codex round-28 P1 on #296):
						// a canonical row at escapeDedupeID(X) would sit at
						// another ID's verbatim probe key, where a
						// pre-upgrade node (existence-only probes, no
						// version metadata) mistakes it for its own guard
						// and silently drops that ID's first send. The sole
						// guard lives at the fallback key instead (see
						// soleAmbiguousGuardKey): no other ID probes that
						// key as anything but X's own raw/fallback
						// candidate, and when it matches it matches only X
						// (legacy exact-raw rule for short IDs, owner-gated
						// v2 rule for over-budget IDs). No canonical row and
						// no round-26 raw leg ride along — short: the leg
						// would duplicate the primary; long: the raw form is
						// over budget. Fallback occupied/reserved degrades
						// to an unguarded insert (duplicate-never-drop),
						// same as pickSpannerDedupeInsert's ok=false below.
						if key, ver, ok := soleAmbiguousGuardKey(it.DedupeID, fallbackKey, fallbackOccupied, reservedGuardKeys); ok {
							reservedGuardKeys[key] = true
							m := map[string]any{
								"instance_id": instanceID, "dedupe_id": key, "created_at": commitTimestamp(),
							}
							if ver >= dedupeFormatRawKeyVersion {
								m[dedupeFormatVersionColumn] = ver
								m[dedupeFallbackOwnerColumn] = escapeDedupeID(it.DedupeID)
							}
							muts = append(muts, spanner.InsertMap("wf_signal_dedupe", m))
						} else if fallbackOccupied && !reservedGuardKeys[fallbackKey] && !canonicalOccupied && !reservedGuardKeys[canonKey] {
							// Last resort (round-17 preservation): the fallback
							// key is foreign-occupied by a committed row —
							// reachable only for over-budget IDs (for short
							// IDs every occupant matches its own key, so an
							// unmatched occupant is impossible and short IDs
							// never land here). Without a guard every retry
							// of X would insert unguarded forever, so the
							// canonical v1 row keeps a permanent guard for
							// current readers (ownership rules stop foreign
							// IDs from claiming it). Pre-upgrade nodes
							// probing escape(X) verbatim may mistake it
							// during the rollout window — the round-28
							// residual, now confined to hash-shaped third
							// IDs — while the "__x"/"____x" natural pair
							// stays fully isolated (short IDs only ever
							// write the sole guard above).
							reservedGuardKeys[canonKey] = true
							muts = append(muts, spanner.InsertMap("wf_signal_dedupe", map[string]any{
								"instance_id": instanceID, "dedupe_id": canonKey,
								dedupeFormatVersionColumn: dedupeFormatVersion, "created_at": commitTimestamp(),
							}))
						}
						// else: batch-reserved or fully occupied — insert
						// unguarded (duplicate-never-drop), same as
						// pickSpannerDedupeInsert's ok=false below.
					} else if key, ver, ok := pickSpannerDedupeInsert(canonKey, fallbackKey, canonicalOccupied, fallbackOccupied, reservedGuardKeys); ok {
						reservedGuardKeys[key] = true
						m := map[string]any{
							"instance_id": instanceID, "dedupe_id": key,
							dedupeFormatVersionColumn: ver, "created_at": commitTimestamp(),
						}
						if ver >= dedupeFormatRawKeyVersion {
							m[dedupeFallbackOwnerColumn] = escapeDedupeID(it.DedupeID)
						}
						muts = append(muts, spanner.InsertMap("wf_signal_dedupe", m))
					}
					created[it.DedupeID] = true
				}
			}
			payload := inboxPayload(it.Event)
			seq++
			muts = append(muts, spanner.InsertMap("wf_inbox", map[string]any{
				"id": newID(), "instance_id": instanceID, "seq": seq, "type": string(it.Event.Type),
				"ref_seq": nullInt(it.Event.RefSeq), "payload": jsonVal(payload), "created_at": commitTimestamp(),
			}))
			inserted++
		}
		if inserted > 0 {
			muts = append(muts, inboxSeqMuts(instanceID, seq, existed)...)
		}
		if len(muts) > 0 {
			if err := txn.BufferWrite(muts); err != nil {
				return err
			}
		}
		if inserted > 0 && status == "running" {
			return enqueueWorkflowTask(ctx, txn, nil, instanceID, queue, now)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if inserted == 0 {
		return nil
	}
	if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return ensureWorkflowTaskIfInbox(ctx, txn, nil, instanceID)
	}); err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

// readCompletedAt returns the instance's terminal-transition time for the
// post-commit sweep cutoff (Codex round-21 P2 on #291): the status commit's
// own commit timestamp recorded in sweep_commit_ts (see commitTimestamp),
// so comparing it against the commit-timestamp created_at of swept rows
// orders the commits exactly (Codex round-22 P2 on #291). Rows created
// after it are the racing sends and must survive. ok=false when the
// instance row is gone (a concurrent purge owns the leftovers) or neither
// tick is set (legacy rows predate both columns): the sweep then falls back
// to unbounded, matching the pre-cutoff behavior so legacy residue
// converges. A set completed_at with a NULL sweep_commit_ts (rows written
// between the flip and a crashed migration, or by an older binary) falls
// back to the client-time completed_at — the pre-round-22 skew
// approximation, converging as those rows age out.
func readCompletedAt(ctx context.Context, txn *spanner.ReadWriteTransaction, id string) (cutoff time.Time, ok bool, err error) {
	row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"sweep_commit_ts", "completed_at"})
	if err != nil {
		if isNotFound(err) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	var sweepTs, completedAt spanner.NullTime
	if err := row.Columns(&sweepTs, &completedAt); err != nil {
		return time.Time{}, false, err
	}
	if sweepTs.Valid {
		return sweepTs.Time, true, nil
	}
	if completedAt.Valid {
		return completedAt.Time, true, nil
	}
	return time.Time{}, false, nil
}

func deleteSignalDedupe(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string, limit int, cutoff time.Time, hasCutoff bool) ([]*spanner.Mutation, error) {
	stmt := spanner.Statement{
		SQL:    `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id LIMIT @limit`,
		Params: map[string]any{"id": instanceID, "limit": int64(limit)},
	}
	if hasCutoff {
		stmt.SQL = `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id AND created_at <= @cutoff LIMIT @limit`
		stmt.Params["cutoff"] = cutoff
	}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()
	var muts []*spanner.Mutation
	for {
		r, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var dedupeID string
		if err := r.Columns(&dedupeID); err != nil {
			return nil, err
		}
		muts = append(muts, spanner.Delete("wf_signal_dedupe", spanner.Key{instanceID, dedupeID}))
	}
	return muts, nil
}

// deleteTasksForInstance returns deletions for up to limit tasks of the
// instance except excludeTaskID (the owned workflow task, removed
// separately). newID never returns 0, so a sweep outside the advancement
// passes 0 to exclude nothing. The instance_id filter is served by
// wf_tasks_instance_idx (see schema.sql and ensureTasksInstanceIndex), so
// cleanup cost stays proportional to the instance's rows; ORDER BY id keeps
// the paged sweep deterministic. When hasCutoff holds, only rows with
// created_at at or before the terminal transition are returned, so racing
// post-commit sends survive the sweep (Codex round-21 P2 on #291).
func deleteTasksForInstance(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string, excludeTaskID int64, limit int, cutoff time.Time, hasCutoff bool) ([]*spanner.Mutation, error) {
	stmt := spanner.Statement{
		SQL:    `SELECT id FROM wf_tasks WHERE instance_id = @id ORDER BY id LIMIT @limit`,
		Params: map[string]any{"id": instanceID, "limit": int64(limit)},
	}
	if hasCutoff {
		stmt.SQL = `SELECT id FROM wf_tasks WHERE instance_id = @id AND created_at <= @cutoff ORDER BY id LIMIT @limit`
		stmt.Params["cutoff"] = cutoff
	}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()
	var muts []*spanner.Mutation
	for {
		r, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var tid int64
		if err := r.Columns(&tid); err != nil {
			return nil, err
		}
		if tid == excludeTaskID {
			continue
		}
		muts = append(muts, spanner.Delete("wf_tasks", spanner.Key{tid}))
	}
	return muts, nil
}

func deleteTimersForInstance(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string, limit int) ([]*spanner.Mutation, error) {
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT seq FROM wf_timers WHERE instance_id = @id LIMIT @limit`,
		Params: map[string]any{"id": instanceID, "limit": int64(limit)},
	})
	defer iter.Stop()
	var muts []*spanner.Mutation
	for {
		r, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var seq int64
		if err := r.Columns(&seq); err != nil {
			return nil, err
		}
		muts = append(muts, spanner.Delete("wf_timers", spanner.Key{instanceID, seq}))
	}
	return muts, nil
}

func deleteInboxForInstance(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string, limit int, cutoff time.Time, hasCutoff bool) ([]*spanner.Mutation, error) {
	stmt := spanner.Statement{
		SQL:    `SELECT id FROM wf_inbox WHERE instance_id = @id LIMIT @limit`,
		Params: map[string]any{"id": instanceID, "limit": int64(limit)},
	}
	if hasCutoff {
		stmt.SQL = `SELECT id FROM wf_inbox WHERE instance_id = @id AND created_at <= @cutoff LIMIT @limit`
		stmt.Params["cutoff"] = cutoff
	}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()
	var muts []*spanner.Mutation
	for {
		r, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var inboxID int64
		if err := r.Columns(&inboxID); err != nil {
			return nil, err
		}
		muts = append(muts, spanner.Delete("wf_inbox", spanner.Key{inboxID}))
	}
	return muts, nil
}
