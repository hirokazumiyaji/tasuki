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
	return backend.Capabilities{SweepsTerminalInbox: true}
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
		muts := []*spanner.Mutation{
			spanner.InsertMap("wf_instances", map[string]any{
				"id":         inst.ID,
				"name":       inst.Name,
				"queue":      queue,
				"status":     "running",
				"input":      jsonVal(inst.Input),
				"next_seq":   int64(2),
				"parent_id":  nullStr(inst.ParentID),
				"parent_seq": nullInt(inst.ParentSeq),
				"search_attributes": jsonVal(backend.MarshalSearchAttributes(inst.SearchAttributes)),
				"memo":              jsonVal(backend.MarshalSearchAttributes(inst.Memo)),
				"created_at": now,
				"updated_at": now,
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
				"created_at":  now,
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
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"id"})
		if err != nil {
			if isNotFound(err) {
				return backend.ErrNotFound
			}
			return err
		}
		_ = row
		// Status flip plus a bounded in-transaction sweep of the full
		// residual set (tasks, timers, signal dedupe, inbox). Residual
		// rows are unbounded (they span many turns), and one delete
		// mutation per row can exceed the per-commit mutation limit, so
		// at most terminalCleanupMutationBudget deletions ride along;
		// the remainder is swept post-commit in bounded transactions
		// below (cleanupTerminalInstance).
		var muts []*spanner.Mutation
		muts = append(muts, spanner.UpdateMap("wf_instances", map[string]any{
			"id":           id,
			"status":       "terminated",
			"updated_at":   now,
			"completed_at": now,
		}))
		budget := terminalCleanupMutationBudget
		dMuts, err := deleteSignalDedupe(ctx, txn, id, budget)
		if err != nil {
			return err
		}
		muts = append(muts, dMuts...)
		budget = max(budget-len(dMuts), 0)
		tMuts, err := deleteTasksForInstance(ctx, txn, id, 0, budget)
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
		inMuts, err := deleteInboxForInstance(ctx, txn, id, budget)
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
	if err := b.cleanupTerminalInstance(context.Background(), id, true); err != nil {
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
		iter := txn.Query(ctx, spanner.Statement{
			SQL: `SELECT id, visible_at FROM wf_tasks
				WHERE kind = @kind AND visible_at <= @now AND queue IN UNNEST(@queues)
				ORDER BY visible_at, id LIMIT @limit`,
			Params: map[string]any{
				"kind": req.Kind, "now": now, "queues": req.Queues, "limit": int64(req.Limit),
			},
		})
		type cand struct {
			id  int64
			vis time.Time
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
			if err := row.Columns(&c.id, &c.vis); err != nil {
				iter.Stop()
				return err
			}
			cands = append(cands, c)
		}
		iter.Stop()

		for _, c := range cands {
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
			// Only part of the terminal cleanup rides in the advancement
			// commit (mutation budget); leftovers are swept post-commit,
			// so a poll can observe a task whose instance already
			// completed. Handing it out would execute user code after
			// completion, so verify the owning instance is still running
			// in the same transaction. A residual task of a terminal
			// instance is deleted here; the sweep removes the rest.
			instRow, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{t.InstanceID}, []string{"status"})
			if isNotFound(err) {
				if _, err := txn.Update(ctx, spanner.Statement{
					SQL:    `DELETE FROM wf_tasks WHERE id = @id`,
					Params: map[string]any{"id": c.id},
				}); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			var status string
			if err := instRow.Columns(&status); err != nil {
				return err
			}
			if status != "running" {
				if _, err := txn.Update(ctx, spanner.Statement{
					SQL:    `DELETE FROM wf_tasks WHERE id = @id`,
					Params: map[string]any{"id": c.id},
				}); err != nil {
					return err
				}
				continue
			}
			out = append(out, t)
		}
		return nil
	})
	return out, err
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

func (b *Backend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL:    `UPDATE wf_tasks SET visible_at = @v WHERE id = @id`,
			Params: map[string]any{"v": nowUTC().Add(d), "id": taskID},
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

func (b *Backend) RecordHeartbeat(ctx context.Context, taskID int64, lease time.Duration, details []byte) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE wf_tasks SET visible_at = @v, heartbeat = @h WHERE id = @id`,
			Params: map[string]any{"v": nowUTC().Add(lease), "h": details, "id": taskID},
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
	// Reject duplicate instances up front: the loop below applies every
	// advancement in one read-write transaction with the same pre-mutation
	// reads, so two advancements for the same instance both pass the
	// ExpectedSeq check and then collide on the second journal insert
	// (a native AlreadyExists commit error, not ErrConflict). Preflight
	// keeps the batch all-or-nothing with a conflict error (see backendtest
	// CommitAdvancementsAtomic).
	seen := make(map[string]struct{}, len(advs))
	for _, adv := range advs {
		if _, dup := seen[adv.InstanceID]; dup {
			return backend.ErrConflict
		}
		seen[adv.InstanceID] = struct{}{}
	}
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Fresh per attempt: the client retries the closure on abort, and
		// buffered mutations are discarded, so read-your-writes state must
		// reset with it.
		st := newSpannerTxnState()
		for _, adv := range advs {
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
	// behind.
	for _, adv := range advs {
		if adv.Terminal != nil {
			if err := b.cleanupTerminalInstance(context.Background(), adv.InstanceID, true); err != nil {
				return err
			}
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
			b.notifyTerminal(adv.InstanceID)
		}
	}
	return nil
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
// per table in one transaction and reports how many rows were removed.
func (b *Backend) deleteTerminalBatch(ctx context.Context, id string, includeInbox bool) (int, error) {
	var n int
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n = 0
		dMuts, err := deleteSignalDedupe(ctx, txn, id, terminalCleanupSweepBatch)
		if err != nil {
			return err
		}
		tMuts, err := deleteTasksForInstance(ctx, txn, id, 0, terminalCleanupSweepBatch)
		if err != nil {
			return err
		}
		tmMuts, err := deleteTimersForInstance(ctx, txn, id, terminalCleanupSweepBatch)
		if err != nil {
			return err
		}
		muts := append(append(dMuts, tMuts...), tmMuts...)
		if includeInbox {
			inMuts, err := deleteInboxForInstance(ctx, txn, id, terminalCleanupSweepBatch)
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

	taskRow, err := txn.ReadRow(ctx, "wf_tasks", spanner.Key{adv.TaskID}, []string{"kind", "instance_id"})
	if err != nil {
		if isNotFound(err) {
			return backend.ErrConflict
		}
		return err
	}
	var kind, taskInst string
	if err := taskRow.Columns(&kind, &taskInst); err != nil {
		return err
	}
	if kind != "workflow" || taskInst != adv.InstanceID {
		return backend.ErrConflict
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
			"created_at":  now,
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
			"id":           adv.InstanceID,
			"status":       adv.Terminal.Status,
			"result":       jsonVal(adv.Terminal.Result),
			"failure":      jsonVal(adv.Terminal.Failure),
			"updated_at":   now,
			"completed_at": now,
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
		dMuts, err := deleteSignalDedupe(ctx, txn, adv.InstanceID, budget)
		if err != nil {
			return err
		}
		muts = append(muts, dMuts...)
		budget = max(budget-len(dMuts), 0)
		tMuts, err := deleteTasksForInstance(ctx, txn, adv.InstanceID, adv.TaskID, budget)
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
		inMuts, err := deleteInboxForInstance(ctx, txn, adv.InstanceID, budget)
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
				"created_at": now, "updated_at": now,
			}),
			spanner.InsertMap("wf_journal", map[string]any{
				"instance_id": ch.ID, "seq": int64(1),
				"type": string(journal.TypeWorkflowStarted), "name": ch.Name,
				"payload": jsonVal(ch.Input), "recorded_at": now,
			}),
			spanner.InsertMap("wf_tasks", map[string]any{
				"id": newID(), "kind": "workflow", "queue": q, "instance_id": ch.ID,
				"attempt": int64(0), "visible_at": now, "created_at": now,
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
				"payload": jsonVal(ev.Payload), "created_at": now,
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
			"attempt": int64(0), "visible_at": now, "created_at": now,
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

func (b *Backend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	now := nowUTC()
	var wake bool
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Reset per attempt: ReadWriteTransaction may retry this closure,
		// and a stale wake (or a mutated ev.RefSeq) from an aborted attempt
		// must not leak into the retry.
		wake = false
		ev := ev
		row, err := txn.ReadRow(ctx, "wf_tasks", spanner.Key{taskID},
			[]string{"instance_id", "ref_seq", "kind"})
		if err != nil {
			if isNotFound(err) {
				return backend.ErrSuperseded
			}
			return err
		}
		var instanceID, kind string
		var refSeq spanner.NullInt64
		if err := row.Columns(&instanceID, &refSeq, &kind); err != nil {
			return err
		}
		if kind != "activity" {
			return backend.ErrSuperseded
		}
		if err := txn.BufferWrite([]*spanner.Mutation{
			spanner.Delete("wf_tasks", spanner.Key{taskID}),
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
				"ref_seq": nullInt(ev.RefSeq), "payload": jsonVal(ev.Payload), "created_at": now,
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

func (b *Backend) RetryActivity(ctx context.Context, taskID int64, delay time.Duration) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE wf_tasks SET visible_at = @v, worker_id = NULL
				WHERE id = @id AND kind = 'activity'`,
			Params: map[string]any{"v": nowUTC().Add(delay), "id": taskID},
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
					"type": string(journal.TypeTimerFired), "ref_seq": d.seq, "created_at": now,
				}))
			if err := txn.BufferWrite(muts); err != nil {
				return err
			}
			if err := enqueueWorkflowTask(ctx, txn, st, d.instanceID, "", now); err != nil {
				return err
			}
			n++
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

func (b *Backend) SendToInboxBatch(ctx context.Context, instanceID string, items []backend.InboxItem) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) > backend.InboxBatchLimit(b.Capabilities()) {
		return backend.ErrBatchTooLarge
	}
	now := nowUTC()
	var inserted int
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
		created := map[string]bool{}
		for _, it := range items {
			if it.DedupeID != "" {
				if created[it.DedupeID] {
					continue
				}
				_, err := txn.ReadRow(ctx, "wf_signal_dedupe", spanner.Key{instanceID, it.DedupeID}, []string{"dedupe_id"})
				if err == nil {
					created[it.DedupeID] = true
					continue
				}
				if !isNotFound(err) {
					return err
				}
				muts = append(muts, spanner.InsertMap("wf_signal_dedupe", map[string]any{
					"instance_id": instanceID, "dedupe_id": it.DedupeID, "created_at": now,
				}))
				created[it.DedupeID] = true
			}
			payload := inboxPayload(it.Event)
			seq++
			muts = append(muts, spanner.InsertMap("wf_inbox", map[string]any{
				"id": newID(), "instance_id": instanceID, "seq": seq, "type": string(it.Event.Type),
				"ref_seq": nullInt(it.Event.RefSeq), "payload": jsonVal(payload), "created_at": now,
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

func deleteSignalDedupe(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string, limit int) ([]*spanner.Mutation, error) {
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id LIMIT @limit`,
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
// the paged sweep deterministic.
func deleteTasksForInstance(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string, excludeTaskID int64, limit int) ([]*spanner.Mutation, error) {
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT id FROM wf_tasks WHERE instance_id = @id ORDER BY id LIMIT @limit`,
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

func deleteInboxForInstance(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string, limit int) ([]*spanner.Mutation, error) {
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT id FROM wf_inbox WHERE instance_id = @id LIMIT @limit`,
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
		var inboxID int64
		if err := r.Columns(&inboxID); err != nil {
			return nil, err
		}
		muts = append(muts, spanner.Delete("wf_inbox", spanner.Key{inboxID}))
	}
	return muts, nil
}
