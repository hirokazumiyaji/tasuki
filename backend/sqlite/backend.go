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

func (b *Backend) Capabilities() backend.Capabilities { return backend.Capabilities{} }

func (b *Backend) CreateInstance(ctx context.Context, inst backend.NewInstance) error {
	queue := inst.Queue
	if queue == "" {
		queue = "default"
	}
	return withTx(ctx, b.db, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `
		INSERT INTO wf_instances (id, name, queue, status, input, next_seq, parent_id, parent_seq, created_at, updated_at)
		VALUES (?, ?, ?, 'running', ?, 2, NULLIF(?, ''), NULLIF(?, 0), ?, ?)`,
			inst.ID, inst.Name, queue, jsonOrNull(inst.Input), inst.ParentID, inst.ParentSeq, nowStr(), nowStr())
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
}

func (b *Backend) GetInstance(ctx context.Context, id string) (*backend.Instance, error) {
	row := b.db.QueryRowContext(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq,
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0)
		FROM wf_instances WHERE id = ?`, id)
	var inst backend.Instance
	var input, result, failure sql.NullString
	if err := row.Scan(&inst.ID, &inst.Name, &inst.Queue, &inst.Status,
		&input, &result, &failure, &inst.NextSeq, &inst.ParentID, &inst.ParentSeq); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	inst.Input = scanJSONText(input)
	inst.Result = scanJSONText(result)
	inst.Failure = scanJSONText(failure)
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
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	rows, err := b.db.QueryContext(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq,
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0)
		FROM wf_instances
		WHERE (? = '' OR status = ?)
		  AND (? = '' OR name = ?)
		ORDER BY created_at, id
		LIMIT ? OFFSET ?`, f.Status, f.Status, f.Name, f.Name, limit, f.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Instance
	for rows.Next() {
		var inst backend.Instance
		var input, result, failure sql.NullString
		if err := rows.Scan(&inst.ID, &inst.Name, &inst.Queue, &inst.Status,
			&input, &result, &failure, &inst.NextSeq, &inst.ParentID, &inst.ParentSeq); err != nil {
			return nil, err
		}
		inst.Input = scanJSONText(input)
		inst.Result = scanJSONText(result)
		inst.Failure = scanJSONText(failure)
		out = append(out, inst)
	}
	return out, rows.Err()
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
	_, _ = conn.ExecContext(ctx, `DELETE FROM wf_tasks WHERE instance_id = ?`, id)
	_, _ = conn.ExecContext(ctx, `DELETE FROM wf_timers WHERE instance_id = ?`, id)
	return commitConn(ctx, conn)
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

	args := make([]any, 0, 2+len(req.Queues)+1)
	args = append(args, req.Kind, now)
	for _, q := range req.Queues {
		args = append(args, q)
	}
	args = append(args, req.Limit)

	query := fmt.Sprintf(`
		SELECT id FROM wf_tasks
		WHERE kind = ? AND visible_at <= ? AND queue IN (%s)
		ORDER BY visible_at, id
		LIMIT ?`, inClause(len(req.Queues)))

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
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
			SELECT id, kind, queue, instance_id, ref_seq, payload, attempt, visible_at, worker_id
			FROM wf_tasks WHERE id = ?`, id)
		var t backend.Task
		var refSeq sql.NullInt64
		var payload sql.NullString
		var visStr string
		if err := row.Scan(&t.ID, &t.Kind, &t.Queue, &t.InstanceID, &refSeq, &payload,
			&t.Attempt, &visStr, &t.WorkerID); err != nil {
			return nil, err
		}
		t.Seq = scanNullableInt64(refSeq)
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
		}
		out = append(out, t)
	}

	if err := commitConn(ctx, conn); err != nil {
		return nil, err
	}
	return out, nil
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

func (b *Backend) ReleaseLease(ctx context.Context, taskID int64) error {
	res, err := b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL WHERE id = ?`, nowStr(), taskID)
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

func (b *Backend) LoadWorkflowHead(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var st backend.WorkflowState
	var input, result, failure sql.NullString
	err = conn.QueryRowContext(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq,
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0)
		FROM wf_instances WHERE id = ?`, instanceID).Scan(
		&st.Instance.ID, &st.Instance.Name, &st.Instance.Queue, &st.Instance.Status,
		&input, &result, &failure, &st.NextSeq, &st.Instance.ParentID, &st.Instance.ParentSeq)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	st.Instance.Input = scanJSONText(input)
	st.Instance.Result = scanJSONText(result)
	st.Instance.Failure = scanJSONText(failure)
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
	return withTx(ctx, b.db, func(conn *sql.Conn) error {
		return b.commitAdvancementConn(ctx, conn, adv)
	})
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
			INSERT INTO wf_instances (id, name, queue, status, input, next_seq, parent_id, parent_seq, created_at, updated_at)
			VALUES (?, ?, ?, 'running', ?, 2, ?, ?, ?, ?)`,
			ch.ID, ch.Name, q, jsonOrNull(ch.Input), ch.ParentID, ch.ParentSeq, nowStr(), nowStr())
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
	return commitConn(ctx, conn)
}

func (b *Backend) RetryActivity(ctx context.Context, taskID int64, visibleAt time.Time) error {
	res, err := b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL
		WHERE id = ? AND kind = 'activity'`, formatTime(visibleAt), taskID)
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
	return n, nil
}

func (b *Backend) SendToInbox(ctx context.Context, instanceID string, ev journal.Event) error {
	err := withTx(ctx, b.db, func(conn *sql.Conn) error {
		var status string
		err := conn.QueryRowContext(ctx, `SELECT status FROM wf_instances WHERE id = ?`, instanceID).Scan(&status)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return backend.ErrNotFound
			}
			return err
		}
		payload := inboxPayload(ev)
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_inbox (instance_id, type, ref_seq, payload, created_at)
			VALUES (?, ?, ?, ?, ?)`,
			instanceID, string(ev.Type), nullIfZeroRefSeq(ev.RefSeq), jsonOrNull(payload), nowStr())
		if err != nil {
			return err
		}
		if status == "running" {
			if err := enqueueWorkflowTask(ctx, conn, instanceID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Second pass: if a concurrent commit deleted the only workflow task after we
	// OR IGNORE'd against it, recreate from inbox (I1).
	return withTx(ctx, b.db, func(conn *sql.Conn) error {
		return ensureWorkflowTaskIfInbox(ctx, conn, instanceID)
	})
}
