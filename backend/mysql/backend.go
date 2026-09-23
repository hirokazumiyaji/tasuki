package mysql

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
		now := nowUTC()
		_, err := conn.ExecContext(ctx, `
		INSERT INTO wf_instances (id, name, queue, status, input, next_seq, parent_id, parent_seq, search_attributes, memo, created_at, updated_at)
		VALUES (?, ?, ?, 'running', ?, 2, NULLIF(?, ''), NULLIF(?, 0), CAST(? AS JSON), CAST(? AS JSON), ?, ?)`,
			inst.ID, inst.Name, queue, jsonOrNull(inst.Input), inst.ParentID, inst.ParentSeq,
			string(backend.MarshalSearchAttributes(inst.SearchAttributes)),
			string(backend.MarshalSearchAttributes(inst.Memo)), now, now)
		if err != nil {
			if isUniqueViolation(err) {
				return backend.ErrAlreadyExists
			}
			return err
		}
		_, err = conn.ExecContext(ctx, `
		INSERT INTO wf_journal (instance_id, seq, type, name, payload, recorded_at)
		VALUES (?, 1, ?, ?, ?, ?)`,
			inst.ID, string(journal.TypeWorkflowStarted), inst.Name, jsonOrNull(inst.Input), now)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `
		INSERT INTO wf_tasks (kind, queue, instance_id, visible_at, created_at)
		VALUES ('workflow', ?, ?, ?, ?)`, queue, inst.ID, now, now)
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
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0), COALESCE(search_attributes, CAST('{}' AS JSON)), COALESCE(memo, CAST('{}' AS JSON))
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
	inst.Input = scanJSONNullString(input)
	inst.Result = scanJSONNullString(result)
	inst.Failure = scanJSONNullString(failure)
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
		e.Payload = scanJSONNullString(payload)
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
		inst.Input = scanJSONNullString(input)
		inst.Result = scanJSONNullString(result)
		inst.Failure = scanJSONNullString(failure)
		inst.SearchAttributes = scanSearchAttrs(searchAttrs)
		inst.Memo = scanSearchAttrs(memo)
		out = append(out, inst)
	}
	return out, rows.Err()
}

// listInstancesQuery builds the ListInstances SELECT with SQL-side
// SearchAttributes filtering (one JSON_CONTAINS equality per key, ANDed) and
// an unconditional LIMIT/OFFSET, so filtered listings only read the requested
// page instead of the full table. Keys and values travel as bound parameters
// inside JSON_OBJECT, so JSON metacharacters need no manual escaping.
// A NULL search_attributes column never matches a non-empty filter.
func listInstancesQuery(f backend.InstanceFilter) (string, []any) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	query := `
		SELECT id, name, queue, status, input, result, failure, next_seq,
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0), COALESCE(search_attributes, CAST('{}' AS JSON)), COALESCE(memo, CAST('{}' AS JSON))
		FROM wf_instances
		WHERE (? = '' OR status = ?)
		  AND (? = '' OR name = ?)`
	args := []any{f.Status, f.Status, f.Name, f.Name}
	for _, k := range sortedSearchAttributeKeys(f.SearchAttributes) {
		query += ` AND JSON_CONTAINS(COALESCE(search_attributes, CAST('{}' AS JSON)), JSON_OBJECT(?, ?))`
		args = append(args, k, f.SearchAttributes[k])
	}
	query += ` ORDER BY created_at, id LIMIT ? OFFSET ?`
	args = append(args, limit, f.Offset)
	return query, args
}

func (b *Backend) TerminateInstance(ctx context.Context, id string) error {
	conn, err := beginTx(ctx, b.db)
	if err != nil {
		return err
	}
	defer rollbackConn(ctx, conn)

	now := nowUTC()
	res, err := conn.ExecContext(ctx, `
		UPDATE wf_instances SET status = 'terminated', updated_at = ?, completed_at = ?
		WHERE id = ?`, now, now, id)
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
	args = append(args, kind, nowUTC())
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

	conn, err := beginTx(ctx, b.db)
	if err != nil {
		return nil, err
	}
	defer rollbackConn(ctx, conn)

	now := nowUTC()
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
		var hb []byte
		if err := row.Scan(&t.ID, &t.Kind, &t.Queue, &t.InstanceID, &refSeq, &payload,
			&t.Attempt, &t.VisibleAt, &t.WorkerID, &hb); err != nil {
			return nil, err
		}
		t.Seq = scanNullableInt64(refSeq)
		t.HeartbeatDetails = hb
		if t.Kind == "activity" {
			var p activityPayload
			_ = json.Unmarshal(scanJSONNullString(payload), &p)
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
// flooding instance is still found beyond the first page. Rows the picker
// accepts but a concurrent claimer locks first are dropped and refilled from
// later candidates, so the batch never comes back empty while claimable
// tasks remain. Candidates the picker rejects are carried forward as well: a
// rejected row can become eligible once the pick that blocked it is lost
// (e.g. FIFO A1,A2,B1 with Limit=2 and MaxPerInstance=1 picks A1,B1 and
// rejects A2; losing both picks to concurrent locks must revisit A2 instead
// of resuming after B1).
//
// Candidate paging runs as plain SELECTs so rows the picker rejects are never
// locked: only picker-accepted IDs are locked (SELECT ... FOR UPDATE SKIP
// LOCKED with a visibility re-check) before the claiming UPDATEs.
func selectClaimCandidates(ctx context.Context, conn *sql.Conn, req backend.ClaimRequest, now time.Time) ([]int64, error) {
	if req.MaxPerInstance <= 0 {
		query := fmt.Sprintf(`
			SELECT id FROM wf_tasks
			WHERE kind = ? AND visible_at <= ? AND queue IN (%s)
			ORDER BY visible_at, id
			LIMIT ?
			FOR UPDATE SKIP LOCKED`, inClause(len(req.Queues)))
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
	prefix := fmt.Sprintf(`
		SELECT id, instance_id, visible_at FROM wf_tasks
		WHERE kind = ? AND visible_at <= ? AND queue IN (%s)`, inClause(len(req.Queues)))
	first := true
	var lastVis time.Time
	var lastID int64
	// A concurrent fair claimer may lock every row picked below between the
	// plain-SELECT scan and the SKIP LOCKED lock step. Lock-skipped rows are
	// dropped, and the scan continues past them (keyset on visible_at, id),
	// refilling the batch from later candidates, so a claim never returns
	// empty while claimable tasks remain behind contended head rows. Rows
	// accepted so far seed each refill pass, keeping the per-instance cap
	// across passes. Rejected rows are carried forward for the same reason:
	// losing a pick can free the cap for a row rejected earlier in FIFO order.
	var accepted []backend.FairTaskRef
	var pending []backend.FairTaskRef
	// Ordering for the secured batch travels on the refs themselves: each
	// scanned candidate captures its (visible_at, id) scan key, so the
	// batch can be restored to FIFO (scan) order before returning without
	// retaining every scanned candidate in a rank map (issue #294 round-12
	// P2). Refill passes secure later rows first (e.g. pass 1 locks B1
	// while the FIFO-earlier A2 is only secured on a refill after the pick
	// that blocked it is lost), and returning lock order would emit [B1
	// A2]. Re-offered pending rows keep the key captured on their original
	// scan pass. Only picked and retained/rejected-carry refs (bounded by
	// limit+cap) ever reach the sort; the scan itself stays streaming.
	// Overflow-requery state (issue #294 follow-up): when scan-phase rejected
	// retention overflows FairRejectedCap, rows past the cap are dropped while
	// the SQL cursor advances past them. If every retained candidate is then
	// lost to concurrent locks, the dropped tail would stay unclaimed for this
	// batch even though it may hold eligible rows (e.g. Limit=2,
	// MaxPerInstance=1 over A1..A2002 with A1..A2001 locked: A2002 is dropped
	// by the cap and never revisited). Remember the pre-overflow keyset
	// position and the IDs already attempted, so the batch can re-issue
	// bounded requery passes from the snapshot instead of returning
	// underfilled. A requery pass that itself overflows arms the next pass
	// from its own fresher snapshot (successive segments), so multi-cap
	// floods (A1..A4003, all locked, or A18010 needing ~9 segments) are
	// walked through segment by segment while each segment makes progress
	// (newly attempts or secures a row). Passes stop when the batch fills,
	// when a pass makes no progress, when a pass proves no unattempted
	// candidates remain (no overflow), or — as a backstop only — at
	// MaxOverflowRequeryPasses (64) additional segments. Worst case per batch
	// is 64 extra bounded segment scans; each segment advances past up to
	// FairRejectedCap dropped rows. Rows still dropped afterwards stay
	// claimable for a later poll, which restarts from the head.
	var (
		overflowSnapValid bool
		overflowSnapVis   time.Time
		overflowSnapID    int64
		overflowSeen      bool
		requeryPasses     int
		requeryAttempted  int
		requeryAccepted   int
		attempted         map[int64]struct{}
	)
	// startOverflowRequery arms the next bounded requery pass from the
	// pre-overflow snapshot when the batch would otherwise return
	// underfilled after retention overflowed AND a lock loss actually freed
	// quota (lost>0 at the call site). When every pick succeeded (lost==0)
	// no slot was freed, so a requery would rescan the same dropped tail
	// against the same per-instance caps and return an identical result —
	// up to 2x the scan cost for nothing. Call sites therefore gate on
	// lost>0 and skip the requery (break) when nothing was lost; the
	// dropped rows stay claimable for a later poll starting from the head.
	// It reports whether the caller
	// should continue to the extra pass instead of breaking. At either break
	// point the retained carry is exhausted (a pass that leaves un-offered
	// pending rows behind either fills the picker or keeps scanning), so
	// resetting the keyset cursor to the snapshot and dropping the empty
	// carry loses nothing. The guard is underfilled (len(accepted) < Limit),
	// not empty: pass 1 may secure B1 while the retained As drain on locks,
	// leaving dropped A2002 eligible for slot 2 (Limit=2/MaxPerInstance=1
	// over A1..A2002(locked)/B1 returns [B1] without this). Arming consumes
	// the overflow flag and the snapshot, so the next pass takes a fresh
	// snapshot further along (successive segments); passes beyond the first
	// additionally require progress (a newly attempted or secured row) since
	// the previous arm — progress is the primary stop condition — and the
	// total is backstopped by MaxOverflowRequeryPasses (64).
	startOverflowRequery := func() bool {
		if len(accepted) >= req.Limit || !overflowSeen || !overflowSnapValid ||
			requeryPasses >= backend.MaxOverflowRequeryPasses {
			return false
		}
		if requeryPasses > 0 && len(attempted) <= requeryAttempted && len(accepted) <= requeryAccepted {
			return false
		}
		requeryPasses++
		requeryAttempted, requeryAccepted = len(attempted), len(accepted)
		overflowSeen = false
		first = false
		lastVis, lastID = overflowSnapVis, overflowSnapID
		overflowSnapValid = false
		pending = nil
		return true
	}
	for len(accepted) < req.Limit {
		picker := backend.NewFairPicker(req.Limit-len(accepted), req.MaxPerInstance).TrackRejected()
		picker.Seed(accepted)
		// Reconsider candidates rejected by an earlier pass first: they are
		// FIFO-earlier than the scan cursor and may now fit under the cap.
		offered := 0
		for _, r := range pending {
			if picker.Full() {
				break
			}
			picker.Offer(r)
			offered++
		}
		// The scan runs to the end of the queue (or a full batch) even when
		// rejected retention overflows FairRejectedCap: the cap bounds the
		// carry list, not the scan. Stopping at the cap would strand the
		// unscanned tail: the next refill re-offers the retained rows,
		// rejections fill the fresh carry to the cap with no picks, and the
		// pass exits empty while later polls restart at the head, so rows
		// past the flood (B) starve and batches underfill. Offer drops
		// rejections beyond the cap, so scanning on stays O(cap) in memory
		// while still reaching victims past the flood; dropped rows stay
		// claimable for later polls.
		if !picker.Full() {
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
					if err := rows.Scan(&r.ID, &r.InstanceID, &r.VisibleAt); err != nil {
						rows.Close()
						return nil, err
					}
					vis := r.VisibleAt
					page++
					// Snapshot the pre-row cursor while retention is intact, so
					// an overflow-triggered requery can resume from the last
					// retained position. First snapshot per pass wins: it covers
					// the largest dropped tail. Arming a requery consumes the
					// overflow flag and the snapshot, so each successive
					// segment takes its own snapshot further along. Only SQL
					// scan rows reach here;
					// pending-phase re-offers never advance the cursor, and the
					// pending carry is bounded by FairRejectedCap so it cannot
					// overflow on its own.
					if !overflowSeen && !picker.RejectedCapped() {
						overflowSnapVis, overflowSnapID, overflowSnapValid = lastVis, lastID, !first
					}
					first = false
					lastVis, lastID = vis, r.ID
					// No rank map: r carries its (visible_at, id) scan key
					// (see above), so ordering needs no per-candidate retention.
					// Overflow-requery passes skip IDs already put through the
					// lock step this batch, so never-attempted dropped rows
					// get priority in each bounded segment.
					if _, dup := attempted[r.ID]; !(requeryPasses > 0 && dup) {
						offerFull := picker.Offer(r)
						if picker.RejectedCapped() {
							overflowSeen = true
						}
						if offerFull {
							full = true
							break
						}
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
		}
		// The paging loop only stops short of a full picker at the end of
		// the queue; a full picker may still have unscanned rows behind it.
		scanExhausted := !picker.Full()
		picked := picker.Picked()
		iterRejected := picker.Rejected()
		// Record every pick attempt so the overflow requery below can skip
		// IDs already put through the lock step this batch.
		for _, r := range picked {
			if attempted == nil {
				attempted = make(map[int64]struct{})
			}
			attempted[r.ID] = struct{}{}
		}
		if len(picked) == 0 {
			// No picks means no lock losses freed quota (lost==0 by
			// definition): an overflow requery would re-offer the dropped
			// tail against identical caps and return the same empty pick,
			// so skip it and break. Dropped rows stay claimable for later
			// polls.
			break
		}
		// Lock only the accepted IDs. Rows locked by a concurrent claimant
		// are skipped here and refilled above, and rows leased since the
		// scan fail the visibility re-check here and again at UPDATE time.
		ids := make([]int64, 0, len(picked))
		for _, r := range picked {
			ids = append(ids, r.ID)
		}
		lockArgs := make([]any, 0, len(ids)+2)
		for _, id := range ids {
			lockArgs = append(lockArgs, id)
		}
		lockArgs = append(lockArgs, req.Kind, now)
		lrows, err := conn.QueryContext(ctx, fmt.Sprintf(`
			SELECT id FROM wf_tasks
			WHERE id IN (%s) AND kind = ? AND visible_at <= ?
			FOR UPDATE SKIP LOCKED`, inClause(len(ids))), lockArgs...)
		if err != nil {
			return nil, err
		}
		locked := make(map[int64]bool, len(ids))
		for lrows.Next() {
			var id int64
			if err := lrows.Scan(&id); err != nil {
				lrows.Close()
				return nil, err
			}
			locked[id] = true
		}
		if err := lrows.Err(); err != nil {
			lrows.Close()
			return nil, err
		}
		lrows.Close()
		prevAccepted := len(accepted)
		for _, r := range picked {
			if locked[r.ID] {
				accepted = append(accepted, r)
			}
		}
		if len(accepted) >= req.Limit {
			break
		}
		if scanExhausted {
			// No unscanned rows remain, so the only way to make progress is
			// to revisit rejected candidates freed by lost picks. When no
			// pick was lost (lost==0) every pick succeeded and no quota was
			// freed: the dropped overflow tail would face the same caps and
			// reproduce the same pick, so skip the wasteful full rescan
			// (up to 2x) and return underfilled. Only a loss frees a slot
			// that can admit a previously rejected/dropped row.
			lost := len(picked) - (len(accepted) - prevAccepted)
			if lost == 0 {
				break
			}
			if len(iterRejected) == 0 {
				// Overflow may have dropped eligible rows past the cursor
				// (see above): with the batch still underfilled, re-issue a
				// bounded scan from the pre-overflow snapshot instead of
				// returning short. A requery segment that itself overflows
				// arms the next segment; rows still dropped after the pass
				// bound stay claimable for a later poll.
				if startOverflowRequery() {
					continue
				}
				break
			}
		}
		// Preserve the unvisited tail of pending alongside this pass's
		// rejected rows. When an offered pending candidate fills the batch
		// the offer loop above breaks, leaving later pending rows unoffered;
		// they are FIFO-earlier than the scan cursor and never rescanned, so
		// keeping only iterRejected would drop claimable tasks (e.g. FIFO
		// A1,A2,A3,B1 with Limit=2 and MaxPerInstance=1 picks A1,B1 and
		// rejects A2,A3; locking A2 then losing it to a concurrent claim
		// must still revisit the unvisited A3).
		pending = append(iterRejected, pending[offered:]...)
		if len(pending) > backend.FairRejectedCap {
			// Bound the cross-pass carry as well: each pass contributes up
			// to FairRejectedCap rows. Overflow rows stay claimable and
			// resurface on a later poll, which restarts from the head.
			pending = pending[:backend.FairRejectedCap]
		}
	}
	if len(accepted) == 0 {
		return nil, nil
	}
	// Restore FIFO (scan) order: refill passes secure later rows before
	// earlier ones (e.g. B1 on pass 1, A2 on the refill), so lock order is
	// not queue order. Sort by the refs' captured scan keys.
	backend.SortFairRefs(accepted)
	out := make([]int64, 0, len(accepted))
	for _, r := range accepted {
		out = append(out, r.ID)
	}
	return out, nil
}

func (b *Backend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	res, err := b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ? WHERE id = ?`, nowUTC().Add(d), taskID)
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
	res, err := b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, heartbeat = ? WHERE id = ?`,
		nowUTC().Add(lease), details, taskID)
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
		// Conditional on the claim ownership token (see sqlite backend).
		res, err = b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL WHERE id = ? AND worker_id = ? AND attempt = ?`,
			nowUTC(), t.ID, t.WorkerID, t.Attempt)
	} else {
		res, err = b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL WHERE id = ?`, nowUTC(), t.ID)
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
		// Conditional on the claim ownership token (see ReleaseLease).
		res, err = b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL WHERE id = ? AND worker_id = ? AND attempt = ?`,
			nowUTC().Add(delay), t.ID, t.WorkerID, t.Attempt)
	} else {
		res, err = b.db.ExecContext(ctx, `
		UPDATE wf_tasks SET visible_at = ?, worker_id = NULL WHERE id = ?`, nowUTC().Add(delay), t.ID)
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
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0), COALESCE(search_attributes, CAST('{}' AS JSON)), COALESCE(memo, CAST('{}' AS JSON))
		FROM wf_instances WHERE id = ?`, instanceID).Scan(
		&st.Instance.ID, &st.Instance.Name, &st.Instance.Queue, &st.Instance.Status,
		&input, &result, &failure, &st.NextSeq, &st.Instance.ParentID, &st.Instance.ParentSeq, &searchAttrs, &memo)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	st.Instance.Input = scanJSONNullString(input)
	st.Instance.Result = scanJSONNullString(result)
	st.Instance.Failure = scanJSONNullString(failure)
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
		item.Event.Name, item.Event.Payload = unwrapInboxPayload(scanJSONNullString(payload))
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
	// Second pass: under snapshot isolation (e.g. TiDB optimistic), a concurrent
	// SendToInbox may commit an inbox row that this txn's ensure did not see after
	// deleting the workflow task (I1). Re-check in a fresh snapshot.
	for _, adv := range advs {
		if err := withTx(ctx, b.db, func(conn *sql.Conn) error {
			return ensureWorkflowTaskIfInbox(ctx, conn, adv.InstanceID)
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

func (b *Backend) commitAdvancementConn(ctx context.Context, conn *sql.Conn, adv backend.Advancement) error {
	var status string
	err := conn.QueryRowContext(ctx, `SELECT status FROM wf_instances WHERE id = ? FOR UPDATE`, adv.InstanceID).
		Scan(&status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return backend.ErrConflict
		}
		return err
	}

	newSeq := adv.ExpectedSeq
	for _, ev := range adv.NewEvents {
		if ev.Seq+1 > newSeq {
			newSeq = ev.Seq + 1
		}
	}
	now := nowUTC()
	res, err := conn.ExecContext(ctx, `
		UPDATE wf_instances SET next_seq = ?, updated_at = ?
		WHERE id = ? AND next_seq = ?`, newSeq, now, adv.InstanceID, adv.ExpectedSeq)
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
			jsonOrNull(ev.Payload), now)
		if err != nil {
			return err
		}
	}
	if backend.HasSearchAttributesUpdate(adv.NewEvents) {
		_, err = conn.ExecContext(ctx, `
			UPDATE wf_instances SET search_attributes = CAST(? AS JSON), updated_at = ? WHERE id = ?`,
			string(backend.MarshalSearchAttributes(backend.LastSearchAttributesUpdate(adv.NewEvents))),
			now, adv.InstanceID)
		if err != nil {
			return err
		}
	}
	if backend.HasMemoUpdate(adv.NewEvents) {
		_, err = conn.ExecContext(ctx, `
			UPDATE wf_instances SET memo = CAST(? AS JSON), updated_at = ? WHERE id = ?`,
			string(backend.MarshalSearchAttributes(backend.LastMemoUpdate(adv.NewEvents))),
			now, adv.InstanceID)
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
			at.Queue, at.InstanceID, at.Seq, string(payload), at.MaxAttempts, now, now)
		if err != nil {
			return err
		}
	}
	for _, tm := range adv.Timers {
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_timers (instance_id, seq, fire_at) VALUES (?, ?, ?)`,
			adv.InstanceID, tm.Seq, tm.FireAt.UTC())
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
			now, now, adv.InstanceID)
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
			VALUES (?, ?, ?, 'running', ?, 2, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, ?)`,
			ch.ID, ch.Name, q, jsonOrNull(ch.Input), ch.ParentID, ch.ParentSeq,
			string(backend.MarshalSearchAttributes(ch.SearchAttributes)),
			string(backend.MarshalSearchAttributes(ch.Memo)), now, now)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_journal (instance_id, seq, type, name, payload, recorded_at)
			VALUES (?, 1, ?, ?, ?, ?)`,
			ch.ID, string(journal.TypeWorkflowStarted), ch.Name, jsonOrNull(ch.Input), now)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `
			INSERT INTO wf_tasks (kind, queue, instance_id, visible_at, created_at)
			VALUES ('workflow', ?, ?, ?, ?)`, q, ch.ID, now, now)
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
			err = conn.QueryRowContext(ctx, `SELECT status FROM wf_instances WHERE id = ? FOR UPDATE`, parentID).
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
				parentID, string(ev.Type), nullIfZeroRefSeq(ev.RefSeq), jsonOrNull(ev.Payload), now)
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
	conn, err := beginTx(ctx, b.db)
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
	now := nowUTC()
	_, err = conn.ExecContext(ctx, `
		INSERT INTO wf_inbox (instance_id, type, ref_seq, payload, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		instanceID, string(ev.Type), nullIfZeroRefSeq(ev.RefSeq), jsonOrNull(ev.Payload), now)
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
		WHERE id = ? AND kind = 'activity'`, nowUTC().Add(delay), taskID)
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

// PurgeInstances deletes terminal instances and their dependent rows in one
// transaction. Victim rows are selected FOR UPDATE SKIP LOCKED so concurrent
// purge jobs make progress without blocking each other.
func (b *Backend) PurgeInstances(ctx context.Context, olderThan time.Duration, statuses []string, limit int) (int, error) {
	sts, lim, err := backend.ValidatePurgeArgs(olderThan, statuses, limit)
	if err != nil {
		return 0, err
	}
	cutoff := nowUTC().Add(-olderThan)
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
		LIMIT ?
		FOR UPDATE SKIP LOCKED`, args...)
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
		if len(ids) == 0 {
			return nil
		}
		idArgs := make([]any, 0, len(ids))
		for _, id := range ids {
			idArgs = append(idArgs, id)
		}
		for _, table := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe", "wf_inbox", "wf_journal"} {
			if _, err := conn.ExecContext(ctx,
				`DELETE FROM `+table+` WHERE instance_id IN (`+inClause(len(ids))+`)`, idArgs...); err != nil {
				return err
			}
		}
		_, err = conn.ExecContext(ctx, `DELETE FROM wf_instances WHERE id IN (`+inClause(len(ids))+`)`, idArgs...)
		return err
	})
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

func (b *Backend) FireDueTimers(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 1
	}
	conn, err := beginTx(ctx, b.db)
	if err != nil {
		return 0, err
	}
	defer rollbackConn(ctx, conn)

	now := nowUTC()
	rows, err := conn.QueryContext(ctx, `
		SELECT instance_id, seq FROM wf_timers
		WHERE fire_at <= ?
		ORDER BY fire_at, instance_id, seq
		LIMIT ?
		FOR UPDATE SKIP LOCKED`, now, limit)
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
			d.instanceID, string(journal.TypeTimerFired), d.seq, now)
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
		err := conn.QueryRowContext(ctx, `SELECT status FROM wf_instances WHERE id = ? FOR UPDATE`, instanceID).
			Scan(&status)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return backend.ErrNotFound
			}
			return err
		}
		now := nowUTC()
		for _, it := range items {
			if it.DedupeID != "" {
				res, err := conn.ExecContext(ctx, `
					INSERT IGNORE INTO wf_signal_dedupe (instance_id, dedupe_id, created_at)
					VALUES (?, ?, ?)`, instanceID, it.DedupeID, now)
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
				instanceID, string(it.Event.Type), nullIfZeroRefSeq(it.Event.RefSeq), jsonOrNull(payload), now)
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
