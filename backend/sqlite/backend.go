package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{FairDispatch: true}
}

var _ backend.SchemaValidator = (*Backend)(nil)

func (b *Backend) CreateInstance(ctx context.Context, inst backend.NewInstance) error {
	queue := inst.Queue
	if queue == "" {
		queue = "default"
	}
	err := withTx(ctx, b.db, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `
		INSERT INTO wf_instances (id, name, queue, status, input, next_seq, parent_id, parent_seq, search_attributes, memo, created_at, updated_at)
		VALUES (?, ?, ?, 'running', ?, 2, NULLIF(?, ''), NULLIF(?, 0), ?, ?, ?, ?)`,
			inst.ID, inst.Name, queue, jsonOrNull(inst.Input), inst.ParentID, inst.ParentSeq,
			string(backend.MarshalSearchAttributes(inst.SearchAttributes)),
			string(backend.MarshalSearchAttributes(inst.Memo)), nowStr(), nowStr())
		if err != nil {
			if isUniqueViolation(err) {
				return backend.ErrAlreadyExists
			}
			return err
		}
		_, err = conn.ExecContext(ctx, `
		INSERT INTO wf_journal (instance_id, seq, type, name, payload, recorded_at)
		VALUES (?, 1, ?, ?, ?, ?)`,
			inst.ID, string(journal.TypeWorkflowStarted), inst.Name, jsonOrNull(inst.Input), nowStr())
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `
		INSERT INTO wf_tasks (kind, queue, instance_id, visible_at, created_at)
		VALUES ('workflow', ?, ?, ?, ?)`, queue, inst.ID, nowStr(), nowStr())
		return err
	})
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) GetInstance(ctx context.Context, id string) (*backend.Instance, error) {
	row := b.db.QueryRowContext(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq,
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0), COALESCE(search_attributes, '{}'), COALESCE(memo, '{}')
		FROM wf_instances WHERE id = ?`, id)
	var inst backend.Instance
	var input, result, failure, searchAttrs, memo sql.NullString
	if err := row.Scan(&inst.ID, &inst.Name, &inst.Queue, &inst.Status,
		&input, &result, &failure, &inst.NextSeq, &inst.ParentID, &inst.ParentSeq, &searchAttrs, &memo); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	inst.Input = scanJSONText(input)
	inst.Result = scanJSONText(result)
	inst.Failure = scanJSONText(failure)
	inst.SearchAttributes = scanSearchAttrs(searchAttrs)
	inst.Memo = scanSearchAttrs(memo)
	return &inst, nil
}

func (b *Backend) GetJournal(ctx context.Context, id string, afterSeq int64) ([]journal.Event, error) {
	rows, err := b.db.QueryContext(ctx, `
		SELECT seq, type, name, COALESCE(ref_seq, 0), payload
		FROM wf_journal WHERE instance_id = ? AND seq > ? ORDER BY seq`, id, afterSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []journal.Event
	for rows.Next() {
		var e journal.Event
		var typ string
		var payload sql.NullString
		if err := rows.Scan(&e.Seq, &typ, &e.Name, &e.RefSeq, &payload); err != nil {
			return nil, err
		}
		e.Type = journal.Type(typ)
		e.Payload = scanJSONText(payload)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (b *Backend) ListInstances(ctx context.Context, f backend.InstanceFilter) ([]backend.Instance, error) {
	query, args := listInstancesQuery(f)
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Instance
	for rows.Next() {
		var inst backend.Instance
		var input, result, failure, searchAttrs, memo sql.NullString
		if err := rows.Scan(&inst.ID, &inst.Name, &inst.Queue, &inst.Status,
			&input, &result, &failure, &inst.NextSeq, &inst.ParentID, &inst.ParentSeq, &searchAttrs, &memo); err != nil {
			return nil, err
		}
		inst.Input = scanJSONText(input)
		inst.Result = scanJSONText(result)
		inst.Failure = scanJSONText(failure)
		inst.SearchAttributes = scanSearchAttrs(searchAttrs)
		inst.Memo = scanSearchAttrs(memo)
		out = append(out, inst)
	}
	return out, rows.Err()
}

// listInstancesQuery builds the ListInstances SELECT with SQL-side
// SearchAttributes filtering (one json_each literal lookup per key, ANDed)
// and an unconditional LIMIT/OFFSET, so filtered listings only read the
// requested page instead of the full table. Keys are matched with plain `=`
// against json_each.key, so unrestricted key strings — including ones that
// look like JSON paths (`$.tenant`), or that carry dots, quotes, or
// backslashes — are always treated as literal object keys. (`->>` cannot be
// used here: its right operand is parsed as a JSON path when it begins with
// `$`, so a filter for literal key `$.tenant` would read key `tenant`.)
// A NULL search_attributes column never matches a non-empty filter.
func listInstancesQuery(f backend.InstanceFilter) (string, []any) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	query := `
		SELECT id, name, queue, status, input, result, failure, next_seq,
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0), COALESCE(search_attributes, '{}'), COALESCE(memo, '{}')
		FROM wf_instances
		WHERE (? = '' OR status = ?)
		  AND (? = '' OR name = ?)`
	args := []any{f.Status, f.Status, f.Name, f.Name}
	for _, k := range sortedSearchAttributeKeys(f.SearchAttributes) {
		query += ` AND (EXISTS (SELECT 1 FROM json_each(COALESCE(search_attributes, '{}')) WHERE key = ? AND value = ?))`
		args = append(args, k, f.SearchAttributes[k])
	}
	query += ` ORDER BY created_at, id LIMIT ? OFFSET ?`
	args = append(args, limit, f.Offset)
	return query, args
}

func (b *Backend) TerminateInstance(ctx context.Context, id string) error {
	conn, err := beginImmediate(ctx, b.db)
	if err != nil {
		return err
	}
	defer rollbackConn(ctx, conn)

	res, err := conn.ExecContext(ctx, `
		UPDATE wf_instances SET status = 'terminated', updated_at = ?, completed_at = ?
		WHERE id = ?`, nowStr(), nowStr(), id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return backend.ErrNotFound
	}
	if _, err = conn.ExecContext(ctx, `DELETE FROM wf_tasks WHERE instance_id = ?`, id); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `DELETE FROM wf_timers WHERE instance_id = ?`, id); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `DELETE FROM wf_signal_dedupe WHERE instance_id = ?`, id); err != nil {
		return err
	}
	if err := commitConn(ctx, conn); err != nil {
		return err
	}
	b.notifyTerminal(id)
	return nil
}

func (b *Backend) CountClaimableTasks(ctx context.Context, kind string, queues []string) (map[string]int64, error) {
	if len(queues) == 0 {
		return map[string]int64{}, nil
	}
	args := make([]any, 0, 2+len(queues))
	args = append(args, kind, nowStr())
	for _, q := range queues {
		args = append(args, q)
	}
	query := fmt.Sprintf(`
		SELECT queue, COUNT(*) FROM wf_tasks
		WHERE kind = ? AND visible_at <= ? AND queue IN (%s)
		GROUP BY queue`, inClause(len(queues)))
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var q string
		var n int64
		if err := rows.Scan(&q, &n); err != nil {
			return nil, err
		}
		out[q] = n
	}
	return out, rows.Err()
}

func (b *Backend) ClaimTasks(ctx context.Context, req backend.ClaimRequest) ([]backend.Task, error) {
	if req.Limit <= 0 {
		req.Limit = 1
	}
	if len(req.Queues) == 0 {
		return nil, nil
	}

	conn, err := beginImmediate(ctx, b.db)
	if err != nil {
		return nil, err
	}
	defer rollbackConn(ctx, conn)

	now := nowStr()
	visAt := leaseVisibleAt(req.Lease)

	ids, err := selectClaimCandidates(ctx, conn, req, now)
	if err != nil {
		return nil, err
	}

	var out []backend.Task
	for _, id := range ids {
		res, err := conn.ExecContext(ctx, `
			UPDATE wf_tasks
			SET visible_at = ?, attempt = attempt + 1, worker_id = ?
			WHERE id = ? AND kind = ? AND visible_at <= ?`,
			visAt, req.WorkerID, id, req.Kind, now)
		if err != nil {
			return nil, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue
		}

		row := conn.QueryRowContext(ctx, `
			SELECT id, kind, queue, instance_id, ref_seq, payload, attempt, visible_at, worker_id, heartbeat
			FROM wf_tasks WHERE id = ?`, id)
		var t backend.Task
		var refSeq sql.NullInt64
		var payload sql.NullString
		var visStr string
		var hb sql.NullString
		if err := row.Scan(&t.ID, &t.Kind, &t.Queue, &t.InstanceID, &refSeq, &payload,
			&t.Attempt, &visStr, &t.WorkerID, &hb); err != nil {
			return nil, err
		}
		t.Seq = scanNullableInt64(refSeq)
		if hb.Valid {
			t.HeartbeatDetails = []byte(hb.String)
		}
		t.VisibleAt, err = parseTime(visStr)
		if err != nil {
			return nil, err
		}
		if t.Kind == "activity" {
			var p activityPayload
			_ = json.Unmarshal(scanJSONText(payload), &p)
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
		out = append(out, t)
	}

	if err := commitConn(ctx, conn); err != nil {
		return nil, err
	}
	return out, nil
}

// selectClaimCandidates returns the ids to claim in one batch. With
// MaxPerInstance it pages FIFO-ordered candidates (keyset on visible_at, id)
// through the fair picker until the batch fills, so a victim hidden behind a
// flooding instance is still found beyond the first page.
func selectClaimCandidates(ctx context.Context, conn *sql.Conn, req backend.ClaimRequest, now string) ([]int64, error) {
	if req.MaxPerInstance <= 0 {
		query := fmt.Sprintf(`
			SELECT id FROM wf_tasks
			WHERE kind = ? AND visible_at <= ? AND queue IN (%s)
			ORDER BY visible_at, id
			LIMIT ?`, inClause(len(req.Queues)))
		args := make([]any, 0, 2+len(req.Queues)+1)
		args = append(args, req.Kind, now)
		for _, q := range req.Queues {
			args = append(args, q)
		}
		args = append(args, req.Limit)
		rows, err := conn.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		return ids, rows.Err()
	}

	pageSize := backend.FairOverfetch(req.Limit)
	picker := backend.NewFairPicker(req.Limit, req.MaxPerInstance)
	prefix := fmt.Sprintf(`
		SELECT id, instance_id, visible_at FROM wf_tasks
		WHERE kind = ? AND visible_at <= ? AND queue IN (%s)`, inClause(len(req.Queues)))
	first := true
	var lastVis string
	var lastID int64
	for !picker.Full() {
		query := prefix
		args := make([]any, 0, 2+len(req.Queues)+4)
		args = append(args, req.Kind, now)
		for _, q := range req.Queues {
			args = append(args, q)
		}
		if !first {
			query += ` AND (visible_at > ? OR (visible_at = ? AND id > ?))`
			args = append(args, lastVis, lastVis, lastID)
		}
		query += `
			ORDER BY visible_at, id
			LIMIT ?`
		args = append(args, pageSize)
		rows, err := conn.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		full := false
		page := 0
		for rows.Next() {
			var r backend.FairTaskRef
			var vis string
			if err := rows.Scan(&r.ID, &r.InstanceID, &vis); err != nil {
				rows.Close()
				return nil, err
			}
			page++
			first = false
			lastVis, lastID = vis, r.ID
			if picker.Offer(r) {
				full = true
				break
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		if full || page < pageSize {
			break
		}
	}
	picked := picker.Picked()
	ids := make([]int64, 0, len(picked))
	for _, r := range picked {
		ids = append(ids, r.ID)
	}
	return ids, nil
}

func (b *Backend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	res, err := b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ? WHERE id = ?`, formatTime(nowUTC().Add(d)), taskID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return backend.ErrNotFound
	}
	return nil
}

func (b *Backend) RecordHeartbeat(ctx context.Context, taskID int64, lease time.Duration, details []byte) error {
	var hb any
	if details != nil {
		hb = string(details)
	}
	res, err := b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, heartbeat = ? WHERE id = ?`,
		formatTime(nowUTC().Add(lease)), hb, taskID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return backend.ErrNotFound
	}
	return nil
}

func (b *Backend) ReleaseLease(ctx context.Context, t backend.Task) error {
	var res sql.Result
	var err error
	if t.WorkerID != "" {
		// Conditional on the claim ownership token: a stale worker whose
		// task was reclaimed (new worker/attempt) matches zero rows and
		// reports ErrNotFound instead of clearing the fresh lease.
		res, err = b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL WHERE id = ? AND worker_id = ? AND attempt = ?`,
			nowStr(), t.ID, t.WorkerID, t.Attempt)
	} else {
		res, err = b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL WHERE id = ?`, nowStr(), t.ID)
	}
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return backend.ErrNotFound
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) NackTask(ctx context.Context, t backend.Task, delay time.Duration) error {
	var res sql.Result
	var err error
	if t.WorkerID != "" {
		// Conditional on the claim ownership token (see ReleaseLease): a
		// stale worker whose task was reclaimed (new worker/attempt)
		// matches zero rows and reports ErrNotFound instead of clearing
		// the fresh lease.
		res, err = b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL WHERE id = ? AND worker_id = ? AND attempt = ?`,
			formatTime(nowUTC().Add(delay)), t.ID, t.WorkerID, t.Attempt)
	} else {
		res, err = b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL WHERE id = ?`, formatTime(nowUTC().Add(delay)), t.ID)
	}
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return backend.ErrNotFound
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) LoadWorkflowHead(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var st backend.WorkflowState
	var input, result, failure, searchAttrs, memo sql.NullString
	err = conn.QueryRowContext(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq,
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0), COALESCE(search_attributes, '{}'), COALESCE(memo, '{}')
		FROM wf_instances WHERE id = ?`, instanceID).Scan(
		&st.Instance.ID, &st.Instance.Name, &st.Instance.Queue, &st.Instance.Status,
		&input, &result, &failure, &st.NextSeq, &st.Instance.ParentID, &st.Instance.ParentSeq, &searchAttrs, &memo)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	st.Instance.Input = scanJSONText(input)
	st.Instance.Result = scanJSONText(result)
	st.Instance.Failure = scanJSONText(failure)
	st.Instance.SearchAttributes = scanSearchAttrs(searchAttrs)
	st.Instance.Memo = scanSearchAttrs(memo)
	st.Instance.NextSeq = st.NextSeq
	st.Now = nowUTC()

	irows, err := conn.QueryContext(ctx, `
		SELECT id, type, COALESCE(ref_seq, 0), payload FROM wf_inbox
		WHERE instance_id = ? ORDER BY id`, instanceID)
	if err != nil {
		return nil, err
	}
	for irows.Next() {
		var item backend.InboxEvent
		var typ string
		var payload sql.NullString
		if err := irows.Scan(&item.ID, &typ, &item.Event.RefSeq, &payload); err != nil {
			irows.Close()
			return nil, err
		}
		item.Event.Type = journal.Type(typ)
		item.Event.Name, item.Event.Payload = unwrapInboxPayload(scanJSONText(payload))
		st.Inbox = append(st.Inbox, item)
	}
	irows.Close()
	if err := irows.Err(); err != nil {
		return nil, err
	}
	return &st, nil
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
	err := withTx(ctx, b.db, func(conn *sql.Conn) error {
		for _, adv := range advs {
			if err := b.commitAdvancementConn(ctx, conn, adv); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	b.notifyTasks()
	for _, adv := range advs {
		if adv.Terminal != nil {
			b.notifyTerminal(adv.InstanceID)
		}
	}
	return nil
}

func (b *Backend) commitAdvancementConn(ctx context.Context, conn *sql.Conn, adv backend.Advancement) error {
	newSeq := adv.ExpectedSeq
	for _, ev := range adv.NewEvents {
		if ev.Seq+1 > newSeq {
			newSeq = ev.Seq + 1
		}
	}
	res, err := conn.ExecContext(ctx, `
		UPDATE wf_instances SET next_seq = ?, updated_at = ?
		WHERE id = ? AND next_seq = ?`, newSeq, nowStr(), adv.InstanceID, adv.ExpectedSeq)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return backend.ErrConflict
	}

	var kind string
	err = conn.QueryRowContext(ctx, `SELECT kind FROM wf_tasks WHERE id = ? AND instance_id = ?`,
		adv.TaskID, adv.InstanceID).Scan(&kind)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return backend.ErrConflict
		}
		return err
	}
	if kind != "workflow" {
		return backend.ErrConflict
	}

	for _, ev := range adv.NewEvents {
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_journal (instance_id, seq, type, name, ref_seq, payload, recorded_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			adv.InstanceID, ev.Seq, string(ev.Type), ev.Name, nullIfZeroRefSeq(ev.RefSeq),
			jsonOrNull(ev.Payload), nowStr())
		if err != nil {
			return err
		}
	}
	if backend.HasSearchAttributesUpdate(adv.NewEvents) {
		_, err = conn.ExecContext(ctx, `
			UPDATE wf_instances SET search_attributes = ?, updated_at = ? WHERE id = ?`,
			string(backend.MarshalSearchAttributes(backend.LastSearchAttributesUpdate(adv.NewEvents))),
			nowStr(), adv.InstanceID)
		if err != nil {
			return err
		}
	}
	if backend.HasMemoUpdate(adv.NewEvents) {
		_, err = conn.ExecContext(ctx, `
			UPDATE wf_instances SET memo = ?, updated_at = ? WHERE id = ?`,
			string(backend.MarshalSearchAttributes(backend.LastMemoUpdate(adv.NewEvents))),
			nowStr(), adv.InstanceID)
		if err != nil {
			return err
		}
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
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_tasks (kind, queue, instance_id, ref_seq, payload, max_attempts, visible_at, created_at)
			VALUES ('activity', ?, ?, ?, ?, NULLIF(?, 0), ?, ?)`,
			at.Queue, at.InstanceID, at.Seq, string(payload), at.MaxAttempts, nowStr(), nowStr())
		if err != nil {
			return err
		}
	}
	for _, tm := range adv.Timers {
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_timers (instance_id, seq, fire_at) VALUES (?, ?, ?)`,
			adv.InstanceID, tm.Seq, formatTime(tm.FireAt))
		if err != nil {
			return err
		}
	}
	if adv.Terminal != nil {
		_, err = conn.ExecContext(ctx, `
			UPDATE wf_instances
			SET status = ?, result = ?, failure = ?, updated_at = ?, completed_at = ?
			WHERE id = ?`,
			adv.Terminal.Status, jsonOrNull(adv.Terminal.Result), jsonOrNull(adv.Terminal.Failure),
			nowStr(), nowStr(), adv.InstanceID)
		if err != nil {
			return err
		}
		if _, err = conn.ExecContext(ctx, `DELETE FROM wf_signal_dedupe WHERE instance_id = ?`, adv.InstanceID); err != nil {
			return err
		}
	}
	if len(adv.DrainedInbox) > 0 {
		for _, inboxID := range adv.DrainedInbox {
			_, err = conn.ExecContext(ctx, `DELETE FROM wf_inbox WHERE id = ?`, inboxID)
			if err != nil {
				return err
			}
		}
	}
	for _, ch := range adv.Children {
		q := ch.Queue
		if q == "" {
			q = "default"
		}
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_instances (id, name, queue, status, input, next_seq, parent_id, parent_seq, search_attributes, memo, created_at, updated_at)
			VALUES (?, ?, ?, 'running', ?, 2, ?, ?, ?, ?, ?, ?)`,
			ch.ID, ch.Name, q, jsonOrNull(ch.Input), ch.ParentID, ch.ParentSeq,
			string(backend.MarshalSearchAttributes(ch.SearchAttributes)),
			string(backend.MarshalSearchAttributes(ch.Memo)), nowStr(), nowStr())
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_journal (instance_id, seq, type, name, payload, recorded_at)
			VALUES (?, 1, ?, ?, ?, ?)`,
			ch.ID, string(journal.TypeWorkflowStarted), ch.Name, jsonOrNull(ch.Input), nowStr())
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_tasks (kind, queue, instance_id, visible_at, created_at)
			VALUES ('workflow', ?, ?, ?, ?)`, q, ch.ID, nowStr(), nowStr())
		if err != nil {
			return err
		}
	}
	if adv.ParentNotify != nil {
		var parentID string
		var parentSeq int64
		err = conn.QueryRowContext(ctx, `
			SELECT COALESCE(parent_id, ''), COALESCE(parent_seq, 0) FROM wf_instances WHERE id = ?`,
			adv.InstanceID).Scan(&parentID, &parentSeq)
		if err != nil {
			return err
		}
		if parentID != "" {
			var parentStatus string
			err = conn.QueryRowContext(ctx, `SELECT status FROM wf_instances WHERE id = ?`, parentID).
				Scan(&parentStatus)
			if err != nil {
				return err
			}
			ev := *adv.ParentNotify
			if ev.RefSeq == 0 {
				ev.RefSeq = parentSeq
			}
			_, err = conn.ExecContext(ctx, `
				INSERT INTO wf_inbox (instance_id, type, ref_seq, payload, created_at)
				VALUES (?, ?, ?, ?, ?)`,
				parentID, string(ev.Type), nullIfZeroRefSeq(ev.RefSeq), jsonOrNull(ev.Payload), nowStr())
			if err != nil {
				return err
			}
			if parentStatus == "running" {
				if err := enqueueWorkflowTask(ctx, conn, parentID); err != nil {
					return err
				}
			}
		}
	}
	_, err = conn.ExecContext(ctx, `DELETE FROM wf_tasks WHERE id = ?`, adv.TaskID)
	if err != nil {
		return err
	}
	if err := ensureWorkflowTaskIfInbox(ctx, conn, adv.InstanceID); err != nil {
		return err
	}
	if adv.EnsureWorkflowTask {
		if err := enqueueWorkflowTask(ctx, conn, adv.InstanceID); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	conn, err := beginImmediate(ctx, b.db)
	if err != nil {
		return err
	}
	defer rollbackConn(ctx, conn)

	row := conn.QueryRowContext(ctx, `
		SELECT instance_id, COALESCE(ref_seq, 0), kind FROM wf_tasks WHERE id = ?`, taskID)
	var instanceID string
	var refSeq int64
	var kind string
	err = row.Scan(&instanceID, &refSeq, &kind)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return backend.ErrSuperseded
		}
		return err
	}
	if kind != "activity" {
		return backend.ErrSuperseded
	}
	res, err := conn.ExecContext(ctx, `DELETE FROM wf_tasks WHERE id = ? AND kind = 'activity'`, taskID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return backend.ErrSuperseded
	}

	var status string
	if err := conn.QueryRowContext(ctx, `SELECT status FROM wf_instances WHERE id = ?`, instanceID).
		Scan(&status); err != nil {
		return err
	}
	if status != "running" {
		return commitConn(ctx, conn)
	}
	if ev.RefSeq == 0 {
		ev.RefSeq = refSeq
	}
	_, err = conn.ExecContext(ctx, `
		INSERT INTO wf_inbox (instance_id, type, ref_seq, payload, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		instanceID, string(ev.Type), nullIfZeroRefSeq(ev.RefSeq), jsonOrNull(ev.Payload), nowStr())
	if err != nil {
		return err
	}
	if err := enqueueWorkflowTask(ctx, conn, instanceID); err != nil {
		return err
	}
	if err := commitConn(ctx, conn); err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) RetryActivity(ctx context.Context, taskID int64, delay time.Duration) error {
	res, err := b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL
		WHERE id = ? AND kind = 'activity'`, formatTime(nowUTC().Add(delay)), taskID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return backend.ErrNotFound
	}
	return nil
}

// PurgeInstances deletes terminal instances and their dependent rows inside a
// single immediate transaction. Child-row deletes are chunked so the bound
// parameter count stays well under SQLITE_MAX_VARIABLE_NUMBER.
func (b *Backend) PurgeInstances(ctx context.Context, olderThan time.Duration, statuses []string, limit int) (int, error) {
	sts, lim, err := backend.ValidatePurgeArgs(olderThan, statuses, limit)
	if err != nil {
		return 0, err
	}
	cutoff := formatTime(nowUTC().Add(-olderThan))
	var ids []string
	err = withTx(ctx, b.db, func(conn *sql.Conn) error {
		args := make([]any, 0, len(sts)+2)
		for _, s := range sts {
			args = append(args, s)
		}
		args = append(args, cutoff, lim)
		rows, err := conn.QueryContext(ctx, `
		SELECT id FROM wf_instances
		WHERE status IN (`+inClause(len(sts))+`)
		  AND completed_at IS NOT NULL AND completed_at <= ?
		ORDER BY completed_at, id
		LIMIT ?`, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		for start := 0; start < len(ids); start += purgeChunkSize {
			end := min(start+purgeChunkSize, len(ids))
			chunk := ids[start:end]
			idArgs := make([]any, 0, len(chunk))
			for _, id := range chunk {
				idArgs = append(idArgs, id)
			}
			for _, table := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe", "wf_inbox", "wf_journal"} {
				if _, err := conn.ExecContext(ctx,
					`DELETE FROM `+table+` WHERE instance_id IN (`+inClause(len(chunk))+`)`, idArgs...); err != nil {
					return err
				}
			}
			if _, err := conn.ExecContext(ctx,
				`DELETE FROM wf_instances WHERE id IN (`+inClause(len(chunk))+`)`, idArgs...); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

const purgeChunkSize = 500

func (b *Backend) FireDueTimers(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 1
	}
	conn, err := beginImmediate(ctx, b.db)
	if err != nil {
		return 0, err
	}
	defer rollbackConn(ctx, conn)

	now := nowStr()
	rows, err := conn.QueryContext(ctx, `
		SELECT instance_id, seq FROM wf_timers
		WHERE fire_at <= ?
		ORDER BY fire_at, instance_id, seq
		LIMIT ?`, now, limit)
	if err != nil {
		return 0, err
	}
	type due struct {
		instanceID string
		seq        int64
	}
	var dues []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.instanceID, &d.seq); err != nil {
			rows.Close()
			return 0, err
		}
		dues = append(dues, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	n := 0
	for _, d := range dues {
		res, err := conn.ExecContext(ctx, `DELETE FROM wf_timers WHERE instance_id = ? AND seq = ?`,
			d.instanceID, d.seq)
		if err != nil {
			return 0, err
		}
		aff, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		if aff == 0 {
			continue
		}
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_inbox (instance_id, type, ref_seq, created_at)
			VALUES (?, ?, ?, ?)`,
			d.instanceID, string(journal.TypeTimerFired), d.seq, nowStr())
		if err != nil {
			return 0, err
		}
		if err := enqueueWorkflowTask(ctx, conn, d.instanceID); err != nil {
			return 0, err
		}
		n++
	}
	if err := commitConn(ctx, conn); err != nil {
		return 0, err
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
	var inserted int
	err := withTx(ctx, b.db, func(conn *sql.Conn) error {
		var status string
		err := conn.QueryRowContext(ctx, `SELECT status FROM wf_instances WHERE id = ?`, instanceID).Scan(&status)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return backend.ErrNotFound
			}
			return err
		}
		for _, it := range items {
			if it.DedupeID != "" {
				res, err := conn.ExecContext(ctx, `
					INSERT OR IGNORE INTO wf_signal_dedupe (instance_id, dedupe_id, created_at)
					VALUES (?, ?, ?)`, instanceID, it.DedupeID, nowStr())
				if err != nil {
					return err
				}
				n, err := res.RowsAffected()
				if err != nil {
					return err
				}
				if n == 0 {
					continue
				}
			}
			payload := inboxPayload(it.Event)
			_, err = conn.ExecContext(ctx, `
				INSERT INTO wf_inbox (instance_id, type, ref_seq, payload, created_at)
				VALUES (?, ?, ?, ?, ?)`,
				instanceID, string(it.Event.Type), nullIfZeroRefSeq(it.Event.RefSeq), jsonOrNull(payload), nowStr())
			if err != nil {
				return err
			}
			inserted++
		}
		if inserted > 0 && status == "running" {
			if err := enqueueWorkflowTask(ctx, conn, instanceID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if inserted == 0 {
		return nil
	}
	if err := withTx(ctx, b.db, func(conn *sql.Conn) error {
		return ensureWorkflowTaskIfInbox(ctx, conn, instanceID)
	}); err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}
