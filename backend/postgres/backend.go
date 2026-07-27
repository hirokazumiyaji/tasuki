package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (b *Backend) Capabilities() backend.Capabilities { return backend.Capabilities{} }

// Reset truncates all workflow tables (test helper).
func (b *Backend) Reset(ctx context.Context) error {
	_, err := b.pool.Exec(ctx, `
		TRUNCATE wf_schedules, wf_timers, wf_tasks, wf_inbox, wf_signal_dedupe, wf_journal, wf_instances RESTART IDENTITY CASCADE`)
	return err
}

func (b *Backend) CreateInstance(ctx context.Context, inst backend.NewInstance) error {
	queue := inst.Queue
	if queue == "" {
		queue = "default"
	}
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO wf_instances (id, name, queue, status, input, next_seq, parent_id, parent_seq)
		VALUES ($1, $2, $3, 'running', $4::jsonb, 2, NULLIF($5, ''), NULLIF($6, 0))`,
		inst.ID, inst.Name, queue, jsonbOrNull(inst.Input), inst.ParentID, inst.ParentSeq)
	if err != nil {
		if isUniqueViolation(err) {
			return backend.ErrAlreadyExists
		}
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO wf_journal (instance_id, seq, type, name, payload)
		VALUES ($1, 1, $2, $3, $4::jsonb)`,
		inst.ID, string(journal.TypeWorkflowStarted), inst.Name, jsonbOrNull(inst.Input))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO wf_tasks (kind, queue, instance_id, visible_at)
		VALUES ('workflow', $1, $2, now())`, queue, inst.ID)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	b.notifyTasks(ctx)
	return nil
}

func (b *Backend) GetInstance(ctx context.Context, id string) (*backend.Instance, error) {
	row := b.pool.QueryRow(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq, COALESCE(parent_id, ''), COALESCE(parent_seq, 0)
		FROM wf_instances WHERE id = $1`, id)
	var inst backend.Instance
	var input, result, failure []byte
	if err := row.Scan(&inst.ID, &inst.Name, &inst.Queue, &inst.Status, &input, &result, &failure, &inst.NextSeq, &inst.ParentID, &inst.ParentSeq); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	inst.Input, inst.Result, inst.Failure = input, result, failure
	return &inst, nil
}

func (b *Backend) GetJournal(ctx context.Context, id string, afterSeq int64) ([]journal.Event, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT seq, type, name, COALESCE(ref_seq, 0), payload
		FROM wf_journal WHERE instance_id = $1 AND seq > $2 ORDER BY seq`, id, afterSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []journal.Event
	for rows.Next() {
		var e journal.Event
		var typ string
		var payload []byte
		if err := rows.Scan(&e.Seq, &typ, &e.Name, &e.RefSeq, &payload); err != nil {
			return nil, err
		}
		e.Type = journal.Type(typ)
		e.Payload = payload
		out = append(out, e)
	}
	return out, rows.Err()
}

func (b *Backend) ListInstances(ctx context.Context, f backend.InstanceFilter) ([]backend.Instance, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	rows, err := b.pool.Query(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq,
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0)
		FROM wf_instances
		WHERE ($1 = '' OR status = $1)
		  AND ($2 = '' OR name = $2)
		ORDER BY created_at, id
		LIMIT $3 OFFSET $4`, f.Status, f.Name, limit, f.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Instance
	for rows.Next() {
		var inst backend.Instance
		var input, result, failure []byte
		if err := rows.Scan(&inst.ID, &inst.Name, &inst.Queue, &inst.Status, &input, &result, &failure,
			&inst.NextSeq, &inst.ParentID, &inst.ParentSeq); err != nil {
			return nil, err
		}
		inst.Input, inst.Result, inst.Failure = input, result, failure
		out = append(out, inst)
	}
	return out, rows.Err()
}

func (b *Backend) TerminateInstance(ctx context.Context, id string) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		UPDATE wf_instances SET status = 'terminated', updated_at = now(), completed_at = now()
		WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return backend.ErrNotFound
	}
	_, _ = tx.Exec(ctx, `DELETE FROM wf_tasks WHERE instance_id = $1`, id)
	_, _ = tx.Exec(ctx, `DELETE FROM wf_timers WHERE instance_id = $1`, id)
	_, _ = tx.Exec(ctx, `DELETE FROM wf_signal_dedupe WHERE instance_id = $1`, id)
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	b.notifyTerminal(ctx, id)
	return nil
}

func (b *Backend) CountClaimableTasks(ctx context.Context, kind string, queues []string) (map[string]int64, error) {
	if len(queues) == 0 {
		return map[string]int64{}, nil
	}
	rows, err := b.pool.Query(ctx, `
		SELECT queue, COUNT(*) FROM wf_tasks
		WHERE kind = $1 AND queue = ANY($2) AND visible_at <= now()
		GROUP BY queue`, kind, queues)
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
	rows, err := b.pool.Query(ctx, `
		WITH picked AS (
			SELECT id FROM wf_tasks
			WHERE kind = $1 AND queue = ANY($2) AND visible_at <= now()
			ORDER BY visible_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		UPDATE wf_tasks t
		SET visible_at = now() + $4::interval,
		    attempt = t.attempt + 1,
		    worker_id = $5
		FROM picked
		WHERE t.id = picked.id
		RETURNING t.id, t.kind, t.queue, t.instance_id, t.ref_seq, t.payload, t.attempt, t.visible_at, t.worker_id, t.heartbeat`,
		req.Kind, req.Queues, req.Limit, interval(req.Lease), req.WorkerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Task
	for rows.Next() {
		var t backend.Task
		var refSeq *int64
		var payload []byte
		if err := rows.Scan(&t.ID, &t.Kind, &t.Queue, &t.InstanceID, &refSeq, &payload, &t.Attempt, &t.VisibleAt, &t.WorkerID, &t.HeartbeatDetails); err != nil {
			return nil, err
		}
		if refSeq != nil {
			t.Seq = *refSeq
		}
		if t.Kind == "activity" {
			var p activityPayload
			_ = json.Unmarshal(payload, &p)
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
	return out, rows.Err()
}

func (b *Backend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	tag, err := b.pool.Exec(ctx, `
		UPDATE wf_tasks SET visible_at = now() + $2::interval WHERE id = $1`, taskID, interval(d))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return backend.ErrNotFound
	}
	return nil
}

func (b *Backend) RecordHeartbeat(ctx context.Context, taskID int64, lease time.Duration, details []byte) error {
	tag, err := b.pool.Exec(ctx, `
		UPDATE wf_tasks SET visible_at = now() + $2::interval, heartbeat = $3 WHERE id = $1`,
		taskID, interval(lease), details)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return backend.ErrNotFound
	}
	return nil
}

func (b *Backend) ReleaseLease(ctx context.Context, taskID int64) error {
	tag, err := b.pool.Exec(ctx, `
		UPDATE wf_tasks SET visible_at = now(), worker_id = NULL WHERE id = $1`, taskID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return backend.ErrNotFound
	}
	b.notifyTasks(ctx)
	return nil
}

func (b *Backend) LoadWorkflowHead(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var st backend.WorkflowState
	var input, result, failure []byte
	var now time.Time
	err = tx.QueryRow(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq, COALESCE(parent_id, ''), COALESCE(parent_seq, 0), now()
		FROM wf_instances WHERE id = $1`, instanceID).Scan(
		&st.Instance.ID, &st.Instance.Name, &st.Instance.Queue, &st.Instance.Status,
		&input, &result, &failure, &st.NextSeq, &st.Instance.ParentID, &st.Instance.ParentSeq, &now)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	st.Instance.Input, st.Instance.Result, st.Instance.Failure = input, result, failure
	st.Instance.NextSeq = st.NextSeq
	st.Now = now

	irows, err := tx.Query(ctx, `
		SELECT id, type, COALESCE(ref_seq, 0), payload FROM wf_inbox
		WHERE instance_id = $1 ORDER BY id`, instanceID)
	if err != nil {
		return nil, err
	}
	for irows.Next() {
		var item backend.InboxEvent
		var typ string
		var payload []byte
		if err := irows.Scan(&item.ID, &typ, &item.Event.RefSeq, &payload); err != nil {
			irows.Close()
			return nil, err
		}
		item.Event.Type = journal.Type(typ)
		item.Event.Name, item.Event.Payload = unwrapInboxPayload(payload)
		st.Inbox = append(st.Inbox, item)
	}
	irows.Close()
	if err := irows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
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
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var terminals []string
	for _, adv := range advs {
		if err := b.applyAdvancement(ctx, tx, adv); err != nil {
			return err
		}
		if adv.Terminal != nil {
			terminals = append(terminals, adv.InstanceID)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	b.notifyTasks(ctx)
	for _, id := range terminals {
		b.notifyTerminal(ctx, id)
	}
	return nil
}

func (b *Backend) applyAdvancement(ctx context.Context, tx pgx.Tx, adv backend.Advancement) error {
	newSeq := adv.ExpectedSeq
	for _, ev := range adv.NewEvents {
		if ev.Seq+1 > newSeq {
			newSeq = ev.Seq + 1
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE wf_instances SET next_seq = $2, updated_at = now()
		WHERE id = $1 AND next_seq = $3`, adv.InstanceID, newSeq, adv.ExpectedSeq)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return backend.ErrConflict
	}

	// Verify own task exists
	var kind string
	err = tx.QueryRow(ctx, `SELECT kind FROM wf_tasks WHERE id = $1 AND instance_id = $2`,
		adv.TaskID, adv.InstanceID).Scan(&kind)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return backend.ErrConflict
		}
		return err
	}
	if kind != "workflow" {
		return backend.ErrConflict
	}

	for _, ev := range adv.NewEvents {
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_journal (instance_id, seq, type, name, ref_seq, payload)
			VALUES ($1, $2, $3, $4, NULLIF($5, 0), $6::jsonb)`,
			adv.InstanceID, ev.Seq, string(ev.Type), ev.Name, ev.RefSeq, jsonbOrNull(ev.Payload))
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
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_tasks (kind, queue, instance_id, ref_seq, payload, max_attempts, visible_at)
			VALUES ('activity', $1, $2, $3, $4::jsonb, NULLIF($5, 0), now())`,
			at.Queue, at.InstanceID, at.Seq, payload, at.MaxAttempts)
		if err != nil {
			return err
		}
	}
	for _, tm := range adv.Timers {
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_timers (instance_id, seq, fire_at) VALUES ($1, $2, $3)`,
			adv.InstanceID, tm.Seq, tm.FireAt.UTC())
		if err != nil {
			return err
		}
	}
	if adv.Terminal != nil {
		_, err = tx.Exec(ctx, `
			UPDATE wf_instances
			SET status = $2, result = $3::jsonb, failure = $4::jsonb,
			    updated_at = now(), completed_at = now()
			WHERE id = $1`,
			adv.InstanceID, adv.Terminal.Status,
			jsonbOrNull(adv.Terminal.Result), jsonbOrNull(adv.Terminal.Failure))
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM wf_signal_dedupe WHERE instance_id = $1`, adv.InstanceID); err != nil {
			return err
		}
	}
	if len(adv.DrainedInbox) > 0 {
		_, err = tx.Exec(ctx, `DELETE FROM wf_inbox WHERE id = ANY($1)`, adv.DrainedInbox)
		if err != nil {
			return err
		}
	}
	for _, ch := range adv.Children {
		q := ch.Queue
		if q == "" {
			q = "default"
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_instances (id, name, queue, status, input, next_seq, parent_id, parent_seq)
			VALUES ($1, $2, $3, 'running', $4::jsonb, 2, $5, $6)`,
			ch.ID, ch.Name, q, jsonbOrNull(ch.Input), ch.ParentID, ch.ParentSeq)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_journal (instance_id, seq, type, name, payload)
			VALUES ($1, 1, $2, $3, $4::jsonb)`,
			ch.ID, string(journal.TypeWorkflowStarted), ch.Name, jsonbOrNull(ch.Input))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_tasks (kind, queue, instance_id, visible_at)
			VALUES ('workflow', $1, $2, now())`, q, ch.ID)
		if err != nil {
			return err
		}
	}
	if adv.ParentNotify != nil {
		var parentID string
		var parentSeq int64
		err = tx.QueryRow(ctx, `SELECT COALESCE(parent_id,''), COALESCE(parent_seq,0) FROM wf_instances WHERE id = $1`, adv.InstanceID).
			Scan(&parentID, &parentSeq)
		if err != nil {
			return err
		}
		if parentID != "" {
			// Serialize with concurrent parent CommitAdvancement / SendToInbox (I1).
			var parentStatus string
			err = tx.QueryRow(ctx, `SELECT status FROM wf_instances WHERE id = $1 FOR UPDATE`, parentID).
				Scan(&parentStatus)
			if err != nil {
				return err
			}
			ev := *adv.ParentNotify
			if ev.RefSeq == 0 {
				ev.RefSeq = parentSeq
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO wf_inbox (instance_id, type, ref_seq, payload)
				VALUES ($1, $2, $3, $4::jsonb)`,
				parentID, string(ev.Type), ev.RefSeq, jsonbOrNull(ev.Payload))
			if err != nil {
				return err
			}
			if parentStatus == "running" {
				_, err = tx.Exec(ctx, `
					INSERT INTO wf_tasks (kind, instance_id, queue)
					SELECT 'workflow', $1, queue FROM wf_instances WHERE id = $1 AND status = 'running'
					ON CONFLICT (instance_id) WHERE kind = 'workflow' DO NOTHING`, parentID)
				if err != nil {
					return err
				}
			}
		}
	}
	_, err = tx.Exec(ctx, `DELETE FROM wf_tasks WHERE id = $1`, adv.TaskID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO wf_tasks (kind, instance_id, queue)
		SELECT 'workflow', $1, i.queue
		FROM wf_instances i
		WHERE i.id = $1 AND i.status = 'running'
		  AND EXISTS (SELECT 1 FROM wf_inbox WHERE instance_id = $1)
		ON CONFLICT (instance_id) WHERE kind = 'workflow' DO NOTHING`, adv.InstanceID)
	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var instanceID string
	var refSeq int64
	err = tx.QueryRow(ctx, `
		DELETE FROM wf_tasks WHERE id = $1 AND kind = 'activity'
		RETURNING instance_id, COALESCE(ref_seq, 0)`, taskID).Scan(&instanceID, &refSeq)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return backend.ErrSuperseded
		}
		return err
	}
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM wf_instances WHERE id = $1`, instanceID).Scan(&status); err != nil {
		return err
	}
	if status != "running" {
		return tx.Commit(ctx)
	}
	if ev.RefSeq == 0 {
		ev.RefSeq = refSeq
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO wf_inbox (instance_id, type, ref_seq, payload)
		VALUES ($1, $2, $3, $4::jsonb)`,
		instanceID, string(ev.Type), ev.RefSeq, jsonbOrNull(ev.Payload))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO wf_tasks (kind, instance_id, queue)
		SELECT 'workflow', $1, queue FROM wf_instances WHERE id = $1 AND status = 'running'
		ON CONFLICT (instance_id) WHERE kind = 'workflow' DO NOTHING`, instanceID)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	b.notifyTasks(ctx)
	return nil
}

func (b *Backend) RetryActivity(ctx context.Context, taskID int64, visibleAt time.Time) error {
	tag, err := b.pool.Exec(ctx, `
		UPDATE wf_tasks SET visible_at = $2, worker_id = NULL
		WHERE id = $1 AND kind = 'activity'`, taskID, visibleAt.UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return backend.ErrNotFound
	}
	return nil
}

func (b *Backend) FireDueTimers(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 1
	}
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT instance_id, seq FROM wf_timers
		WHERE fire_at <= now()
		ORDER BY fire_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
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
		tag, err := tx.Exec(ctx, `DELETE FROM wf_timers WHERE instance_id = $1 AND seq = $2`, d.instanceID, d.seq)
		if err != nil {
			return 0, err
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_inbox (instance_id, type, ref_seq) VALUES ($1, $2, $3)`,
			d.instanceID, string(journal.TypeTimerFired), d.seq)
		if err != nil {
			return 0, err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_tasks (kind, instance_id, queue)
			SELECT 'workflow', $1, queue FROM wf_instances WHERE id = $1 AND status = 'running'
			ON CONFLICT (instance_id) WHERE kind = 'workflow' DO NOTHING`, d.instanceID)
		if err != nil {
			return 0, err
		}
		n++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if n > 0 {
		b.notifyTasks(ctx)
	}
	return n, nil
}


func (b *Backend) SendToInbox(ctx context.Context, instanceID string, ev journal.Event, dedupeID string) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Lock the instance row so this serialize with CommitAdvancement's next_seq CAS
	// and cannot DO NOTHING against a task that the commit is about to delete (I1).
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM wf_instances WHERE id = $1 FOR UPDATE`, instanceID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return backend.ErrNotFound
		}
		return err
	}
	if dedupeID != "" {
		var got string
		err = tx.QueryRow(ctx, `
			INSERT INTO wf_signal_dedupe (instance_id, dedupe_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING RETURNING dedupe_id`, instanceID, dedupeID).Scan(&got)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	payload := inboxPayload(ev)
	_, err = tx.Exec(ctx, `
		INSERT INTO wf_inbox (instance_id, type, ref_seq, payload)
		VALUES ($1, $2, NULLIF($3, 0), $4::jsonb)`,
		instanceID, string(ev.Type), ev.RefSeq, jsonbOrNull(payload))
	if err != nil {
		return err
	}
	if status == "running" {
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_tasks (kind, instance_id, queue)
			SELECT 'workflow', $1, queue FROM wf_instances WHERE id = $1 AND status = 'running'
			ON CONFLICT (instance_id) WHERE kind = 'workflow' DO NOTHING`, instanceID)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if status == "running" {
		b.notifyTasks(ctx)
	}
	return nil
}

type activityPayload struct {
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
	Retry retryJSON       `json:"retry"`
}

type retryJSON struct {
	InitialIntervalMs  int64   `json:"initial_interval_ms"`
	BackoffCoefficient float64 `json:"backoff_coefficient"`
	MaxIntervalMs      int64   `json:"max_interval_ms"`
	MaxAttempts        int     `json:"max_attempts"`
}


type inboxEnv struct {
	Name string          `json:"_name,omitempty"`
	Body json.RawMessage `json:"_body,omitempty"`
}

func inboxPayload(ev journal.Event) []byte {
	if ev.Name == "" {
		return ev.Payload
	}
	b, _ := json.Marshal(inboxEnv{Name: ev.Name, Body: ev.Payload})
	return b
}

func unwrapInboxPayload(payload []byte) (string, []byte) {
	var env inboxEnv
	if err := json.Unmarshal(payload, &env); err == nil && env.Name != "" {
		return env.Name, []byte(env.Body)
	}
	return "", payload
}

func jsonbOrNull(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func interval(d time.Duration) string {
	return fmt.Sprintf("%f seconds", d.Seconds())
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
