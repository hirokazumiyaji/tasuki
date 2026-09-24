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

func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{FairDispatch: true}
}

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
		INSERT INTO wf_instances (id, name, queue, status, input, next_seq, parent_id, parent_seq, search_attributes, memo)
		VALUES ($1, $2, $3, 'running', $4::jsonb, 2, NULLIF($5, ''), NULLIF($6, 0), $7::jsonb, $8::jsonb)`,
		inst.ID, inst.Name, queue, jsonbOrNull(inst.Input), inst.ParentID, inst.ParentSeq,
		backend.MarshalSearchAttributes(inst.SearchAttributes),
		backend.MarshalSearchAttributes(inst.Memo))
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
		SELECT id, name, queue, status, input, result, failure, next_seq, COALESCE(parent_id, ''), COALESCE(parent_seq, 0),
		       COALESCE(search_attributes, '{}'::jsonb), COALESCE(memo, '{}'::jsonb)
		FROM wf_instances WHERE id = $1`, id)
	var inst backend.Instance
	var input, result, failure, searchAttrs, memo []byte
	if err := row.Scan(&inst.ID, &inst.Name, &inst.Queue, &inst.Status, &input, &result, &failure, &inst.NextSeq, &inst.ParentID, &inst.ParentSeq, &searchAttrs, &memo); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	inst.Input, inst.Result, inst.Failure = input, result, failure
	inst.SearchAttributes, _ = backend.SearchAttributesFromPayload(searchAttrs)
	inst.Memo, _ = backend.SearchAttributesFromPayload(memo)
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
	filterJSON := backend.MarshalSearchAttributes(f.SearchAttributes)
	rows, err := b.pool.Query(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq,
		       COALESCE(parent_id, ''), COALESCE(parent_seq, 0), COALESCE(search_attributes, '{}'::jsonb), COALESCE(memo, '{}'::jsonb)
		FROM wf_instances
		WHERE ($1 = '' OR status = $1)
		  AND ($2 = '' OR name = $2)
		  AND ($3::jsonb = '{}'::jsonb OR search_attributes @> $3::jsonb)
		ORDER BY created_at, id
		LIMIT $4 OFFSET $5`, f.Status, f.Name, filterJSON, limit, f.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Instance
	for rows.Next() {
		var inst backend.Instance
		var input, result, failure, searchAttrs, memo []byte
		if err := rows.Scan(&inst.ID, &inst.Name, &inst.Queue, &inst.Status, &input, &result, &failure,
			&inst.NextSeq, &inst.ParentID, &inst.ParentSeq, &searchAttrs, &memo); err != nil {
			return nil, err
		}
		inst.Input, inst.Result, inst.Failure = input, result, failure
		inst.SearchAttributes, _ = backend.SearchAttributesFromPayload(searchAttrs)
		inst.Memo, _ = backend.SearchAttributesFromPayload(memo)
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
	_, err = tx.Exec(ctx, `DELETE FROM wf_tasks WHERE instance_id = $1`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM wf_timers WHERE instance_id = $1`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM wf_signal_dedupe WHERE instance_id = $1`, id)
	if err != nil {
		return err
	}
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
	if req.MaxPerInstance > 0 {
		return b.claimTasksFair(ctx, req)
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
		t, payload, err := scanClaimedTask(rows)
		if err != nil {
			return nil, err
		}
		if t.Kind == "activity" {
			decodeActivityTask(&t, payload)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// claimTasksFair caps claims per instance (ClaimRequest.MaxPerInstance).
// It pages FIFO-ordered candidates (keyset on visible_at, id) through the
// fair picker until the batch fills, so a victim hidden behind a flooding
// instance is still found, then claims each survivor with a visibility
// re-check: concurrent claimants lose the race on already-leased rows (their
// visible_at moved to the future) and simply skip.
//
// Rows the picker accepts but a concurrent claimer locks first are dropped
// and refilled from later candidates, so a claim never returns empty while
// claimable tasks remain behind contended head rows. Claimed rows seed each
// refill pass, keeping the per-instance cap across passes. Candidates the
// picker rejects are carried forward as well: a rejected row can become
// eligible once the pick that blocked it is lost (e.g. FIFO A1,A2,B1 with
// Limit=2 and MaxPerInstance=1 picks A1,B1 and rejects A2; losing both picks
// to concurrent locks must revisit A2 instead of resuming after B1).
//
// Candidate paging runs as plain SELECTs so rows the picker rejects are
// never locked: only picker-accepted IDs are locked (SELECT ... FOR UPDATE
// SKIP LOCKED with a visibility re-check) before the claiming UPDATEs.
// Locking every scanned row instead would let one batch hold the whole ready
// queue while claiming a single task, starving concurrent claimers that skip
// locked rows (issue #294).
func (b *Backend) claimTasksFair(ctx context.Context, req backend.ClaimRequest) ([]backend.Task, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	pageSize := backend.FairOverfetch(req.Limit)
	first := true
	var lastVis time.Time
	var lastID int64
	var (
		out     []backend.Task
		claimed []backend.FairTaskRef
		pending []backend.FairTaskRef
	)
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
	// claim even though it may hold eligible rows (e.g. Limit=2,
	// MaxPerInstance=1 over A1..A2002 with A1..A2001 locked: A2002 is dropped
	// by the cap and never revisited). Remember the pre-overflow keyset
	// position and the IDs already attempted, so the claim can re-issue
	// bounded requery passes from the snapshot instead of returning
	// underfilled. A requery pass that itself overflows arms the next pass
	// from its own fresher snapshot (successive segments), so multi-cap
	// floods (A1..A4003, all locked, or A18010 needing ~9 segments) are
	// walked through segment by segment while each segment makes progress
	// (newly attempts or secures a row). Passes stop when the batch fills,
	// when a pass makes no progress, when a pass proves no unattempted
	// candidates remain (no overflow), or — as a backstop only — at
	// MaxOverflowRequeryPasses (64) additional segments. Worst case per claim
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
		requeryOut        int
		attempted         map[int64]struct{}
		// lostLock remembers whether ANY earlier pass in this claim lost
		// picks to concurrent locks, freeing quota a later pass can reuse
		// (round-13 P2 on #294). A pass that picks nothing loses nothing
		// itself, so without this cross-pass flag it cannot tell whether a
		// requery could admit a previously dropped row.
		lostLock bool
	)
	// startOverflowRequery arms the next bounded requery pass from the
	// pre-overflow snapshot when the batch would otherwise return
	// underfilled after retention overflowed AND a lock loss actually freed
	// quota. When every pick succeeded no slot was freed, so a requery would
	// rescan the same dropped tail against the same per-instance caps and
	// return an identical result — up to 2x the scan cost for nothing. The
	// scan-exhausted call site therefore gates on its own pass lost>0 OR the
	// cross-pass lostLock flag (an earlier pass may have freed quota even
	// when this pass secured all its picks), while
	// the zero-pick call site gates on the cross-pass lostLock flag: a pass
	// that picks nothing loses nothing itself, but an earlier pass may have
	// freed quota (round-13 P2 on #294). Call sites skip the requery (break)
	// when no loss freed quota; the dropped rows stay claimable for a later
	// poll starting from the head.
	// It reports whether the caller
	// should continue to the extra pass instead of breaking. At either break
	// point the retained carry is exhausted (a pass that leaves un-offered
	// pending rows behind either fills the picker or keeps scanning), so
	// resetting the keyset cursor to the snapshot and dropping the empty
	// carry loses nothing. The guard is underfilled (len(out) < Limit), not
	// empty: pass 1 may secure B1 while the retained As drain on locks,
	// leaving dropped A2002 eligible for slot 2 (Limit=2/MaxPerInstance=1
	// over A1..A2002(locked)/B1 returns [B1] without this). Arming consumes
	// the overflow flag and the snapshot, so the next pass takes a fresh
	// snapshot further along (successive segments); passes beyond the first
	// additionally require progress (a newly attempted or secured row) since
	// the previous arm — progress is the primary stop condition — and the
	// total is backstopped by MaxOverflowRequeryPasses (64).
	startOverflowRequery := func() bool {
		if len(out) >= req.Limit || !overflowSeen || !overflowSnapValid ||
			requeryPasses >= backend.MaxOverflowRequeryPasses {
			return false
		}
		if requeryPasses > 0 && len(attempted) <= requeryAttempted && len(out) <= requeryOut {
			return false
		}
		requeryPasses++
		requeryAttempted, requeryOut = len(attempted), len(out)
		overflowSeen = false
		first = false
		lastVis, lastID = overflowSnapVis, overflowSnapID
		overflowSnapValid = false
		pending = nil
		return true
	}
	for len(out) < req.Limit {
		picker := backend.NewFairPicker(req.Limit-len(out), req.MaxPerInstance).TrackRejected()
		picker.Seed(claimed)
		// Batch-probe the retained carry in one lock-free query before
		// re-offering it (issue #294 round-15 P2, round-16 fix): re-offering
		// a large carry one pick per pass costs a lock query per pass plus
		// quadratic re-offers when every pick is lost (a retained run over
		// one invisible instance drains a single row per pass, so
		// A1..A2002 with A1..A2001 leased needs ~2001 lock queries in one
		// long txn). The probe drops rows leased since the scan in one
		// plain SELECT — taking no locks, so concurrent claimers never skip
		// claimable work held by this claim — and only visible survivors
		// are re-offered, in FIFO order, so fair-cap semantics are
		// unchanged. Rows locked between the probe and the pick are skipped
		// by the picker's lock step below (which locks only accepted rows)
		// and refilled as lost picks, exactly as before the batch probe
		// existed. Probe-dropped rows count as lost for the
		// overflow-requery gate below (a later pass may reuse the freed
		// position, same as a lost pick).
		if len(pending) > 0 {
			kept, dropped, err := b.probeRetainedCarry(ctx, tx, req, pending)
			if err != nil {
				return nil, err
			}
			pending = kept
			if dropped {
				lostLock = true
			}
		}
		// Reconsider candidates rejected by an earlier pass first: they are
		// FIFO-earlier than the scan cursor and may now fit under the cap
		// once the picks that blocked them were lost to concurrent locks.
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
				q := `
				SELECT id, instance_id, visible_at FROM wf_tasks
				WHERE kind = $1 AND queue = ANY($2) AND visible_at <= now()`
				args := []any{req.Kind, req.Queues}
				if !first {
					q += ` AND (visible_at, id) > ($3, $4)`
					args = append(args, lastVis, lastID)
				}
				q += fmt.Sprintf(` ORDER BY visible_at, id LIMIT $%d`, len(args)+1)
				args = append(args, pageSize)
				rows, err := tx.Query(ctx, q, args...)
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
					// lock step this claim, so never-attempted dropped rows
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
		// Record pick attempts only while an overflow requery may need them
		// (round-13 P2 on #294): the set is consulted solely by requery
		// passes (to skip already-attempted IDs in favor of never-attempted
		// dropped rows), so recording every pick on claims that never
		// overflow grows O(queue) on lock-heavy claims for nothing.
		// Overflow passes record their picks; requery passes keep recording
		// theirs for the progress guard and successive segments.
		if overflowSeen || requeryPasses > 0 {
			for _, r := range picked {
				if attempted == nil {
					attempted = make(map[int64]struct{})
				}
				attempted[r.ID] = struct{}{}
			}
		}
		if len(picked) == 0 {
			// No picks means this pass lost nothing (lost==0 by
			// definition): without a prior loss an overflow requery would
			// re-offer the dropped tail against identical caps and return
			// the same empty pick, so skip it and break. But when an
			// earlier pass lost picks to locks (lostLock), quota was freed
			// that can admit a previously dropped row (e.g. Limit=3/
			// MaxPerInstance=1 over A1,B1,B2..B2001,A2 with A1 locked:
			// pass 1 secures B1 and drops A2 past the cap, pass 2
			// re-rejects the carry and picks nothing — only a requery from
			// the pre-overflow snapshot revisits A2). Dropped rows stay
			// claimable for later polls.
			if lostLock && startOverflowRequery() {
				continue
			}
			break
		}

		// Lock only the accepted IDs. Rows locked by a concurrent claimant are
		// skipped here and refilled above, and rows leased since the scan fail
		// the visibility re-check here and again at UPDATE time.
		locked := make(map[int64]bool, len(picked))
		{
			ids := make([]int64, 0, len(picked))
			for _, r := range picked {
				ids = append(ids, r.ID)
			}
			lrows, err := tx.Query(ctx, `
				SELECT id FROM wf_tasks
				WHERE id = ANY($1) AND visible_at <= now()
				FOR UPDATE SKIP LOCKED`, ids)
			if err != nil {
				return nil, err
			}
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
		}

		prevOut := len(out)
		for _, r := range picked {
			if !locked[r.ID] {
				continue // locked or leased concurrently; refilled above
			}
			row := tx.QueryRow(ctx, `
				UPDATE wf_tasks
				SET visible_at = now() + $2::interval, attempt = attempt + 1, worker_id = $3
				WHERE id = $1 AND visible_at <= now()
				RETURNING id, kind, queue, instance_id, ref_seq, payload, attempt, visible_at, worker_id, heartbeat`,
				r.ID, interval(req.Lease), req.WorkerID)
			t, payload, err := scanClaimedTask(row)
			if errors.Is(err, pgx.ErrNoRows) {
				continue // claimed concurrently between select and update
			}
			if err != nil {
				return nil, err
			}
			if t.Kind == "activity" {
				decodeActivityTask(&t, payload)
			}
			out = append(out, t)
			claimed = append(claimed, r)
		}
		if len(out)-prevOut < len(picked) {
			// Picks lost to concurrent locks (or to a concurrent claim
			// between select and update) free quota a later pass can
			// reuse; remember across passes for the zero-pick requery gate
			// above (round-13 P2 on #294).
			lostLock = true
		}
		if len(out) >= req.Limit {
			break
		}
		if scanExhausted {
			// No unscanned rows remain, so the only way to make progress is
			// to revisit rejected candidates freed by lost picks. When no
			// pick was lost on this pass (lost==0) AND no earlier pass lost
			// one either (!lostLock), every pick succeeded and no quota was
			// freed: the dropped overflow tail would face the same caps and
			// reproduce the same pick, so skip the wasteful full rescan
			// (up to 2x) and return underfilled. A current OR prior loss
			// frees a slot that can admit a previously rejected/dropped row
			// (e.g. Limit=4/MaxPerInstance=1 over A1,C1,B1,C2,B2..B2001,A2
			// with A1,C1 locked: pass 1 secures B1 and drops A2 past the
			// cap, the carry pass secures C2 with lost==0 — only the
			// cross-pass lostLock still admits A2 via requery).
			lost := len(picked) - (len(out) - prevOut)
			if lost == 0 && !lostLock {
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
		// rejects A2,A3; claiming B1 then losing A2 to a concurrent lock
		// must still revisit the unvisited A3).
		pending = append(iterRejected, pending[offered:]...)
		if len(pending) > backend.FairRejectedCap {
			// Bound the cross-pass carry as well: each pass contributes up
			// to FairRejectedCap rows. Overflow rows stay claimable and
			// resurface on a later poll, which restarts from the head.
			pending = pending[:backend.FairRejectedCap]
		}
	}
	// Restore FIFO (scan) order: refill passes secure later rows before
	// earlier ones (e.g. B1 on pass 1, A2 on the refill), so lock order is
	// not queue order. out[i] corresponds to claimed[i]; reorder both by
	// the refs' captured scan keys.
	if len(out) > 1 {
		backend.SortFairRefs(claimed)
		byID := make(map[int64]backend.Task, len(out))
		for _, t := range out {
			byID[t.ID] = t
		}
		for i, r := range claimed {
			out[i] = byID[r.ID]
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		b.notifyTasks(ctx)
	}
	return out, nil
}

// probeRetainedCarry batch-filters a retained carry to its visible rows
// with a single plain SELECT over the carry IDs (see the call site in
// claimTasksFair). The probe takes NO row locks: probing with SELECT ...
// FOR UPDATE SKIP LOCKED over every unlocked carry row held up to ~2000
// locks until commit though the picker accepts only a few, so concurrent
// claimers skipped claimable work (Codex round-16 on #294). Only the
// picker's lock step below takes locks, and only on accepted rows. Rows
// locked between the probe and the pick are skipped there (counted as lost
// for the overflow-requery gate, same as before the batch probe existed),
// and rows leased since the scan fail the visibility re-check here and
// again at UPDATE time — so probe staleness in either direction is covered
// without retaining locks. Re-offered survivors stay in FIFO (visible_at,
// id) order, preserving fair-cap semantics while invisible rows are dropped
// in one query instead of one lost pick each.
func (b *Backend) probeRetainedCarry(ctx context.Context, tx pgx.Tx, req backend.ClaimRequest, pending []backend.FairTaskRef) ([]backend.FairTaskRef, bool, error) {
	ids := make([]int64, 0, len(pending))
	for _, r := range pending {
		ids = append(ids, r.ID)
	}
	rows, err := tx.Query(ctx, `
		SELECT id, instance_id, visible_at FROM wf_tasks
		WHERE id = ANY($1) AND kind = $2 AND visible_at <= now()
		ORDER BY visible_at, id`, ids, req.Kind)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	kept := make([]backend.FairTaskRef, 0, len(pending))
	for rows.Next() {
		var r backend.FairTaskRef
		if err := rows.Scan(&r.ID, &r.InstanceID, &r.VisibleAt); err != nil {
			return nil, false, err
		}
		kept = append(kept, r)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return kept, len(kept) < len(pending), nil
}

// scanClaimedTask scans the shared claim RETURNING column list and returns
// the raw payload for activity decoding.
func scanClaimedTask(sc interface{ Scan(dest ...any) error }) (backend.Task, []byte, error) {
	var t backend.Task
	var refSeq *int64
	var payload []byte
	if err := sc.Scan(&t.ID, &t.Kind, &t.Queue, &t.InstanceID, &refSeq, &payload, &t.Attempt, &t.VisibleAt, &t.WorkerID, &t.HeartbeatDetails); err != nil {
		return t, nil, err
	}
	if refSeq != nil {
		t.Seq = *refSeq
	}
	return t, payload, nil
}

// decodeActivityTask fills the activity-specific fields from the payload blob.
func decodeActivityTask(t *backend.Task, payload []byte) {
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
	t.StartToCloseTimeout = time.Duration(p.StartToCloseTimeoutMs) * time.Millisecond
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

func (b *Backend) ReleaseLease(ctx context.Context, t backend.Task) error {
	var tag pgconn.CommandTag
	var err error
	if t.WorkerID != "" {
		// Conditional on the claim ownership token (see sqlite backend).
		tag, err = b.pool.Exec(ctx, `
		UPDATE wf_tasks SET visible_at = now(), worker_id = NULL WHERE id = $1 AND worker_id = $2 AND attempt = $3`,
			t.ID, t.WorkerID, t.Attempt)
	} else {
		tag, err = b.pool.Exec(ctx, `
		UPDATE wf_tasks SET visible_at = now(), worker_id = NULL WHERE id = $1`, t.ID)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return backend.ErrNotFound
	}
	b.notifyTasks(ctx)
	return nil
}

func (b *Backend) NackTask(ctx context.Context, t backend.Task, delay time.Duration) error {
	var tag pgconn.CommandTag
	var err error
	if t.WorkerID != "" {
		// Conditional on the claim ownership token (see ReleaseLease).
		tag, err = b.pool.Exec(ctx, `
		UPDATE wf_tasks SET visible_at = now() + $2::interval, worker_id = NULL WHERE id = $1 AND worker_id = $3 AND attempt = $4`,
			t.ID, interval(delay), t.WorkerID, t.Attempt)
	} else {
		tag, err = b.pool.Exec(ctx, `
		UPDATE wf_tasks SET visible_at = now() + $2::interval, worker_id = NULL WHERE id = $1`,
			t.ID, interval(delay))
	}
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
	var input, result, failure, searchAttrs, memo []byte
	var now time.Time
	err = tx.QueryRow(ctx, `
		SELECT id, name, queue, status, input, result, failure, next_seq, COALESCE(parent_id, ''), COALESCE(parent_seq, 0),
		       COALESCE(search_attributes, '{}'::jsonb), COALESCE(memo, '{}'::jsonb), now()
		FROM wf_instances WHERE id = $1`, instanceID).Scan(
		&st.Instance.ID, &st.Instance.Name, &st.Instance.Queue, &st.Instance.Status,
		&input, &result, &failure, &st.NextSeq, &st.Instance.ParentID, &st.Instance.ParentSeq, &searchAttrs, &memo, &now)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	st.Instance.Input, st.Instance.Result, st.Instance.Failure = input, result, failure
	st.Instance.SearchAttributes, _ = backend.SearchAttributesFromPayload(searchAttrs)
	st.Instance.Memo, _ = backend.SearchAttributesFromPayload(memo)
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
	if backend.HasSearchAttributesUpdate(adv.NewEvents) {
		_, err = tx.Exec(ctx, `
			UPDATE wf_instances SET search_attributes = $2::jsonb, updated_at = now() WHERE id = $1`,
			adv.InstanceID, backend.MarshalSearchAttributes(backend.LastSearchAttributesUpdate(adv.NewEvents)))
		if err != nil {
			return err
		}
	}
	if backend.HasMemoUpdate(adv.NewEvents) {
		_, err = tx.Exec(ctx, `
			UPDATE wf_instances SET memo = $2::jsonb, updated_at = now() WHERE id = $1`,
			adv.InstanceID, backend.MarshalSearchAttributes(backend.LastMemoUpdate(adv.NewEvents)))
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
			INSERT INTO wf_instances (id, name, queue, status, input, next_seq, parent_id, parent_seq, search_attributes, memo)
			VALUES ($1, $2, $3, 'running', $4::jsonb, 2, $5, $6, $7::jsonb, $8::jsonb)`,
			ch.ID, ch.Name, q, jsonbOrNull(ch.Input), ch.ParentID, ch.ParentSeq,
			backend.MarshalSearchAttributes(ch.SearchAttributes),
			backend.MarshalSearchAttributes(ch.Memo))
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
	if adv.EnsureWorkflowTask {
		// Truncated fanout: force a follow-up tick even though remaining
		// work is not yet in the inbox (it replays).
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_tasks (kind, instance_id, queue)
			SELECT 'workflow', $1, i.queue
			FROM wf_instances i
			WHERE i.id = $1 AND i.status = 'running'
			ON CONFLICT (instance_id) WHERE kind = 'workflow' DO NOTHING`, adv.InstanceID)
		if err != nil {
			return err
		}
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

func (b *Backend) RetryActivity(ctx context.Context, taskID int64, delay time.Duration) error {
	tag, err := b.pool.Exec(ctx, `
		UPDATE wf_tasks SET visible_at = now() + $2::interval, worker_id = NULL
		WHERE id = $1 AND kind = 'activity'`,
		taskID, interval(delay))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return backend.ErrNotFound
	}
	return nil
}

// PurgeInstances deletes terminal instances and all dependent rows in a single
// atomic statement. SKIP LOCKED lets concurrent purge jobs make progress
// without blocking each other.
func (b *Backend) PurgeInstances(ctx context.Context, olderThan time.Duration, statuses []string, limit int) (int, error) {
	sts, lim, err := backend.ValidatePurgeArgs(olderThan, statuses, limit)
	if err != nil {
		return 0, err
	}
	tag, err := b.pool.Exec(ctx, `
		WITH victims AS (
			SELECT id FROM wf_instances
			WHERE status = ANY($1)
			  AND completed_at IS NOT NULL
			  AND completed_at <= now() - $2::interval
			ORDER BY completed_at, id
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		del_journal AS (
			DELETE FROM wf_journal j USING victims v WHERE j.instance_id = v.id
		),
		del_inbox AS (
			DELETE FROM wf_inbox x USING victims v WHERE x.instance_id = v.id
		),
		del_tasks AS (
			DELETE FROM wf_tasks t USING victims v WHERE t.instance_id = v.id
		),
		del_timers AS (
			DELETE FROM wf_timers m USING victims v WHERE m.instance_id = v.id
		),
		del_dedupe AS (
			DELETE FROM wf_signal_dedupe d USING victims v WHERE d.instance_id = v.id
		)
		DELETE FROM wf_instances i USING victims v WHERE i.id = v.id`,
		sts, interval(olderThan), lim)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
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
	return b.SendToInboxBatch(ctx, instanceID, []backend.InboxItem{{Event: ev, DedupeID: dedupeID}})
}

func (b *Backend) SendToInboxBatch(ctx context.Context, instanceID string, items []backend.InboxItem) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) > backend.InboxBatchLimit(b.Capabilities()) {
		return backend.ErrBatchTooLarge
	}
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM wf_instances WHERE id = $1 FOR UPDATE`, instanceID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return backend.ErrNotFound
		}
		return err
	}
	inserted := 0
	for _, it := range items {
		if it.DedupeID != "" {
			var got string
			err = tx.QueryRow(ctx, `
				INSERT INTO wf_signal_dedupe (instance_id, dedupe_id) VALUES ($1, $2)
				ON CONFLICT DO NOTHING RETURNING dedupe_id`, instanceID, it.DedupeID).Scan(&got)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
		}
		payload := inboxPayload(it.Event)
		_, err = tx.Exec(ctx, `
			INSERT INTO wf_inbox (instance_id, type, ref_seq, payload)
			VALUES ($1, $2, NULLIF($3, 0), $4::jsonb)`,
			instanceID, string(it.Event.Type), it.Event.RefSeq, jsonbOrNull(payload))
		if err != nil {
			return err
		}
		inserted++
	}
	if inserted > 0 && status == "running" {
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
	if inserted > 0 && status == "running" {
		b.notifyTasks(ctx)
	}
	return nil
}

type activityPayload struct {
	Name                    string          `json:"name"`
	Input                   json.RawMessage `json:"input"`
	Retry                   retryJSON       `json:"retry"`
	StartToCloseTimeoutMs   int64           `json:"start_to_close_timeout_ms,omitempty"`
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
