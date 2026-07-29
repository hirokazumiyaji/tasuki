package spanner

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
)

func (b *Backend) Capabilities() backend.Capabilities { return backend.Capabilities{} }

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
		var muts []*spanner.Mutation
		muts = append(muts, spanner.UpdateMap("wf_instances", map[string]any{
			"id":           id,
			"status":       "terminated",
			"updated_at":   now,
			"completed_at": now,
		}))
		tIter := txn.Query(ctx, spanner.Statement{
			SQL:    `SELECT id FROM wf_tasks WHERE instance_id = @id`,
			Params: map[string]any{"id": id},
		})
		for {
			r, err := tIter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				tIter.Stop()
				return err
			}
			var tid int64
			if err := r.Columns(&tid); err != nil {
				tIter.Stop()
				return err
			}
			muts = append(muts, spanner.Delete("wf_tasks", spanner.Key{tid}))
		}
		tIter.Stop()
		tmIter := txn.Query(ctx, spanner.Statement{
			SQL:    `SELECT seq FROM wf_timers WHERE instance_id = @id`,
			Params: map[string]any{"id": id},
		})
		for {
			r, err := tmIter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				tmIter.Stop()
				return err
			}
			var seq int64
			if err := r.Columns(&seq); err != nil {
				tmIter.Stop()
				return err
			}
			muts = append(muts, spanner.Delete("wf_timers", spanner.Key{id, seq}))
		}
		tmIter.Stop()
		dMuts, err := deleteSignalDedupe(ctx, txn, id)
		if err != nil {
			return err
		}
		muts = append(muts, dMuts...)
		return txn.BufferWrite(muts)
	})
	if err != nil {
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

func (b *Backend) ReleaseLease(ctx context.Context, taskID int64) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL:    `UPDATE wf_tasks SET visible_at = @v, worker_id = NULL WHERE id = @id`,
			Params: map[string]any{"v": nowUTC(), "id": taskID},
		})
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

func (b *Backend) NackTask(ctx context.Context, t backend.Task, visibleAt time.Time) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL:    `UPDATE wf_tasks SET visible_at = @v, worker_id = NULL WHERE id = @id`,
			Params: map[string]any{"v": visibleAt.UTC(), "id": t.ID},
		})
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
		SQL:    `SELECT id, type, ref_seq, payload FROM wf_inbox WHERE instance_id = @id ORDER BY id`,
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
		var typ string
		var refSeq spanner.NullInt64
		var payload spanner.NullJSON
		if err := row.Columns(&item.ID, &typ, &refSeq, &payload); err != nil {
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
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		for _, adv := range advs {
			if err := b.commitAdvancementTxn(ctx, txn, adv); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, adv := range advs {
		if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return ensureWorkflowTaskIfInbox(ctx, txn, adv.InstanceID)
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

func (b *Backend) commitAdvancementTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, adv backend.Advancement) error {
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
		dMuts, err := deleteSignalDedupe(ctx, txn, adv.InstanceID)
		if err != nil {
			return err
		}
		muts = append(muts, dMuts...)
	}
	for _, inboxID := range adv.DrainedInbox {
		muts = append(muts, spanner.Delete("wf_inbox", spanner.Key{inboxID}))
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
		if err := txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_inbox", map[string]any{
				"id": inboxID, "instance_id": parentID.StringVal,
				"type": string(ev.Type), "ref_seq": nullInt(ev.RefSeq),
				"payload": jsonVal(ev.Payload), "created_at": now,
			}),
		}); err != nil {
			return err
		}
		if parentStatus == "running" {
			if err := enqueueWorkflowTask(ctx, txn, parentID.StringVal, parentQueue, now); err != nil {
				return err
			}
		}
	}

	if err := txn.BufferWrite([]*spanner.Mutation{
		spanner.Delete("wf_tasks", spanner.Key{adv.TaskID}),
	}); err != nil {
		return err
	}
	return ensureWorkflowTaskIfInbox(ctx, txn, adv.InstanceID)
}

func enqueueWorkflowTask(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID, queue string, now time.Time) error {
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
	exist := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT id FROM wf_tasks WHERE kind = 'workflow' AND instance_id = @id LIMIT 1`,
		Params: map[string]any{"id": instanceID},
	})
	_, err = exist.Next()
	exist.Stop()
	if err == nil {
		return nil
	}
	if err != iterator.Done {
		return err
	}
	return txn.BufferWrite([]*spanner.Mutation{
		spanner.InsertMap("wf_tasks", map[string]any{
			"id": newID(), "kind": "workflow", "queue": queue, "instance_id": instanceID,
			"attempt": int64(0), "visible_at": now, "created_at": now,
		}),
	})
}

func ensureWorkflowTaskIfInbox(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string) error {
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
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT 1 FROM wf_inbox WHERE instance_id = @id LIMIT 1`,
		Params: map[string]any{"id": instanceID},
	})
	_, err = iter.Next()
	iter.Stop()
	if err == iterator.Done {
		return nil
	}
	if err != nil {
		return err
	}
	return enqueueWorkflowTask(ctx, txn, instanceID, queue, nowUTC())
}

func (b *Backend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	now := nowUTC()
	var wake bool
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
		if err := txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_inbox", map[string]any{
				"id": newID(), "instance_id": instanceID, "type": string(ev.Type),
				"ref_seq": nullInt(ev.RefSeq), "payload": jsonVal(ev.Payload), "created_at": now,
			}),
		}); err != nil {
			return err
		}
		wake = true
		return enqueueWorkflowTask(ctx, txn, instanceID, queue, now)
	})
	if err != nil {
		return err
	}
	if wake {
		b.notifyTasks()
	}
	return nil
}

func (b *Backend) RetryActivity(ctx context.Context, taskID int64, visibleAt time.Time) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE wf_tasks SET visible_at = @v, worker_id = NULL
				WHERE id = @id AND kind = 'activity'`,
			Params: map[string]any{"v": visibleAt.UTC(), "id": taskID},
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
			if err := txn.BufferWrite([]*spanner.Mutation{
				spanner.InsertMap("wf_inbox", map[string]any{
					"id": newID(), "instance_id": d.instanceID,
					"type": string(journal.TypeTimerFired), "ref_seq": d.seq, "created_at": now,
				}),
			}); err != nil {
				return err
			}
			if err := enqueueWorkflowTask(ctx, txn, d.instanceID, "", now); err != nil {
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
	now := nowUTC()
	var skipped bool
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{instanceID}, []string{"status", "queue"})
		if err != nil {
			if isNotFound(err) {
				return backend.ErrNotFound
			}
			return err
		}
		var status, queue string
		if err := row.Columns(&status, &queue); err != nil {
			return err
		}
		if dedupeID != "" {
			_, err := txn.ReadRow(ctx, "wf_signal_dedupe", spanner.Key{instanceID, dedupeID}, []string{"dedupe_id"})
			if err == nil {
				skipped = true
				return nil
			}
			if !isNotFound(err) {
				return err
			}
			if err := txn.BufferWrite([]*spanner.Mutation{
				spanner.InsertMap("wf_signal_dedupe", map[string]any{
					"instance_id": instanceID, "dedupe_id": dedupeID, "created_at": now,
				}),
			}); err != nil {
				return err
			}
		}
		payload := inboxPayload(ev)
		if err := txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_inbox", map[string]any{
				"id": newID(), "instance_id": instanceID, "type": string(ev.Type),
				"ref_seq": nullInt(ev.RefSeq), "payload": jsonVal(payload), "created_at": now,
			}),
		}); err != nil {
			return err
		}
		if status == "running" {
			return enqueueWorkflowTask(ctx, txn, instanceID, queue, now)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if skipped {
		return nil
	}
	if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return ensureWorkflowTaskIfInbox(ctx, txn, instanceID)
	}); err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func deleteSignalDedupe(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string) ([]*spanner.Mutation, error) {
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id`,
		Params: map[string]any{"id": instanceID},
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
