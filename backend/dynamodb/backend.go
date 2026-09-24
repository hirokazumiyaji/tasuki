package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{MaxAdvancementEffects: 80, FairDispatch: true, CleansTerminalState: true, SupportsBulkCleanup: true}
}

// dynamoTxnItemLimit is the DynamoDB TransactWriteItems item cap. A combined
// CommitAdvancements batch must fit in a single transaction to stay atomic;
// anything larger is rejected before applying (see CommitAdvancements).
const dynamoTxnItemLimit = 100

// advancementItemCount predicts the TransactWriteItems operations
// buildAdvancementItems will emit for adv: 1 instance CAS + journal/activity/
// timer puts + inbox deletes + 3 per child + 1 parent inbox when the instance
// has a parent + 1 task delete/refresh. It must stay in lockstep with
// buildAdvancementItems — the combined-batch preflight in CommitAdvancements
// relies on the count being exact (a drift that undercounts would push the
// overflow into the defensive in-loop guard; a drift that overcounts only
// rejects a batch that would have fit).
//
// Terminal advancements skip activity and timer effects (buildAdvancementItems
// breaks out of both loops when adv.Terminal != nil), so the count mirrors
// those skips: terminal counts exclude ActivityTasks and Timers.
func advancementItemCount(adv backend.Advancement, hasParent bool) int {
	n := 2 + len(adv.NewEvents) + len(adv.DrainedInbox) + 3*len(adv.Children)
	if adv.Terminal == nil {
		n += len(adv.ActivityTasks) + len(adv.Timers)
	}
	if adv.ParentNotify != nil && hasParent {
		n++
	}
	return n
}

func (b *Backend) CreateInstance(ctx context.Context, inst backend.NewInstance) error {
	queue := inst.Queue
	if queue == "" {
		queue = "default"
	}
	now := nowUTC()
	taskID := newID()
	_, err := b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		put(b.table("wf_instances"), instanceItem(inst, queue, now), "attribute_not_exists(id)"),
		put(b.table("wf_journal"), journalItem(inst.ID, 1, journal.Event{Type: journal.TypeWorkflowStarted, Name: inst.Name, Payload: inst.Input}, now), "attribute_not_exists(instance_id) AND attribute_not_exists(seq)"),
		put(b.table("wf_tasks"), workflowTaskItem(inst.ID, queue, taskID, now), "attribute_not_exists(task_pk)"),
	}})
	if conditional(err) {
		return backend.ErrAlreadyExists
	}
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func instanceItem(inst backend.NewInstance, queue string, now time.Time) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{
		"id": avS(inst.ID), "name": avS(inst.Name), "queue": avS(queue), "status": avS("running"),
		"input": avJSON(inst.Input), "next_seq": avN(2), "created_at": avN(timeToN(now)), "updated_at": avN(timeToN(now)),
		"search_attributes": avJSON(backend.MarshalSearchAttributes(inst.SearchAttributes)),
		"memo":              avJSON(backend.MarshalSearchAttributes(inst.Memo)),
	}
	if inst.ParentID != "" {
		m["parent_id"] = avS(inst.ParentID)
	}
	if inst.ParentSeq != 0 {
		m["parent_seq"] = avN(inst.ParentSeq)
	}
	return m
}

func workflowTaskItem(instanceID, queue string, id int64, now time.Time) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"task_pk": avS(wfTaskPK(instanceID)), "id": avN(id), "kind": avS("workflow"), "queue": avS(queue),
		"gsi_pk": avS(claimGSI("workflow", queue)), "instance_id": avS(instanceID), "attempt": avN(0),
		"visible_at": avN(timeToN(now)), "created_at": avN(timeToN(now)),
	}
}

func journalItem(instanceID string, seq int64, ev journal.Event, now time.Time) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{
		"instance_id": avS(instanceID), "seq": avN(seq), "type": avS(string(ev.Type)),
		"name": avS(ev.Name), "payload": avJSON(ev.Payload), "recorded_at": avN(timeToN(now)),
	}
	if ev.RefSeq != 0 {
		m["ref_seq"] = avN(ev.RefSeq)
	}
	return m
}

func (b *Backend) GetInstance(ctx context.Context, id string) (*backend.Instance, error) {
	out, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(b.table("wf_instances")), Key: map[string]types.AttributeValue{"id": avS(id)}, ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, backend.ErrNotFound
	}
	return decodeInstance(out.Item), nil
}

func decodeInstance(m map[string]types.AttributeValue) *backend.Instance {
	attrs, _ := backend.SearchAttributesFromPayload(fromJSON(m["search_attributes"]))
	memo, _ := backend.SearchAttributesFromPayload(fromJSON(m["memo"]))
	return &backend.Instance{ID: fromS(m["id"]), Name: fromS(m["name"]), Queue: fromS(m["queue"]), Status: fromS(m["status"]),
		Input: fromJSON(m["input"]), Result: fromJSON(m["result"]), Failure: fromJSON(m["failure"]), NextSeq: fromN(m["next_seq"]),
		ParentID: fromS(m["parent_id"]), ParentSeq: fromN(m["parent_seq"]), SearchAttributes: attrs, Memo: memo}
}

func (b *Backend) GetJournal(ctx context.Context, id string, afterSeq int64) ([]journal.Event, error) {
	var events []journal.Event
	var start map[string]types.AttributeValue
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		out, err := b.client.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(b.table("wf_journal")),
			KeyConditionExpression:    aws.String("instance_id = :id AND seq > :after"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(id), ":after": avN(afterSeq)}, ScanIndexForward: aws.Bool(true),
			ConsistentRead: aws.Bool(true), ExclusiveStartKey: start})
		if err != nil {
			return nil, err
		}
		for _, m := range out.Items {
			events = append(events, journal.Event{Seq: fromN(m["seq"]), Type: journal.Type(fromS(m["type"])), Name: fromS(m["name"]), RefSeq: fromN(m["ref_seq"]), Payload: fromJSON(m["payload"])})
		}
		if out.LastEvaluatedKey == nil {
			break
		}
		start = out.LastEvaluatedKey
	}
	return events, nil
}

func (b *Backend) ListInstances(ctx context.Context, f backend.InstanceFilter) ([]backend.Instance, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	// Keep only what we need for ordering + filtering to avoid holding full
	// DynamoDB items for large scans. Full decode happens after Offset/Limit.
	type cand struct {
		created int64
		id      string
		item    map[string]types.AttributeValue
	}
	var cands []cand
	var start map[string]types.AttributeValue
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		out, err := b.client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(b.table("wf_instances")), ExclusiveStartKey: start})
		if err != nil {
			return nil, err
		}
		for _, m := range out.Items {
			if (f.Status == "" || fromS(m["status"]) == f.Status) && (f.Name == "" || fromS(m["name"]) == f.Name) {
				inst := decodeInstance(m)
				if backend.MatchesSearchAttributes(inst.SearchAttributes, f.SearchAttributes) {
					cands = append(cands, cand{created: fromN(m["created_at"]), id: fromS(m["id"]), item: m})
				}
			}
		}
		if out.LastEvaluatedKey == nil {
			break
		}
		start = out.LastEvaluatedKey
	}
	// Stable order mirrors SQL backends; timestamps are numeric and IDs break ties.
	// O(M log M) standard sort replaces the former O(M²) double loop.
	sort.Slice(cands, func(i, j int) bool {
		return lessInstance(cands[i].created, cands[i].id, cands[j].created, cands[j].id)
	})
	if f.Offset >= len(cands) {
		return nil, nil
	}
	cands = cands[f.Offset:]
	if len(cands) > limit {
		cands = cands[:limit]
	}
	result := make([]backend.Instance, 0, len(cands))
	for _, c := range cands {
		result = append(result, *decodeInstance(c.item))
	}
	return result, nil
}

func (b *Backend) TerminateInstance(ctx context.Context, id string) error {
	now := nowUTC()
	_, err := b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(b.table("wf_instances")), Key: map[string]types.AttributeValue{"id": avS(id)},
		UpdateExpression: aws.String("SET #s = :s, updated_at = :now, completed_at = :now"), ConditionExpression: aws.String("attribute_exists(id)"),
		ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":s": avS("terminated"), ":now": avN(timeToN(now))}})
	if conditional(err) {
		return backend.ErrNotFound
	}
	if err != nil {
		return err
	}
	// Terminate always runs the full task cleanup (not the bounded
	// hot-path sweep): an explicit termination must leave no claimable
	// rows behind for the terminated ID.
	if err := b.deleteTasksForInstanceFull(ctx, id); err != nil {
		return err
	}
	if err := b.deleteTimersForInstance(ctx, id); err != nil {
		return err
	}
	if err := b.deleteSignalDedupeForInstance(ctx, id); err != nil {
		return err
	}
	if err := b.deleteInboxForInstance(ctx, id); err != nil {
		return err
	}
	b.notifyTerminal(id)
	return nil
}

func (b *Backend) deleteTasksForInstance(ctx context.Context, id string) error {
	if err := b.deleteTasksForInstanceByGSI(ctx, id); err != nil {
		if !isMissingIndexError(err) {
			return err
		}
		// Backward compat: tables created before instance_gsi existed (or
		// still backfilling it) fall back to a strongly-consistent
		// full-table Scan; Migrate backfills the index on existing tables.
		return b.deleteTasksForInstanceByScan(ctx, id)
	}
	// The GSI is eventually consistent: a sweep can report a partial match
	// while lagging rows are still invisible to the index. Confirm with one
	// bounded strongly-consistent Scan page (see
	// verifyTasksFirstPageByScan): the common case stays cheap and lagging
	// rows in the page are reaped synchronously.
	return b.verifyTasksFirstPageByScan(ctx, id)
}

// deleteTasksForInstanceFull removes one instance's tasks with no bound on
// verification cost: the GSI sweep first, then a fully-paginated
// strongly-consistent Scan that reaps every lagging row anywhere in the
// table. Only the rare single-instance path that must leave nothing behind
// uses it — TerminateInstance — never the per-completion hot path (see
// deleteTasksForInstance for why the hot path stays bounded) and never a
// batch purge (see deleteTasksForInstancesFull for why the purge shares one
// scan across all its victims instead of paying one per instance).
func (b *Backend) deleteTasksForInstanceFull(ctx context.Context, id string) error {
	if err := b.deleteTasksForInstanceByGSI(ctx, id); err != nil {
		if !isMissingIndexError(err) {
			return err
		}
		// Without a queryable index the Scan below is the whole cleanup.
	}
	return b.deleteTasksForInstanceByScan(ctx, id)
}

// deleteTasksForInstancesFull removes the task rows of every listed instance
// with ONE shared fleet scan: a per-instance GSI sweep first (cheap,
// instance-keyed, no fleet read), then a single fully-paginated
// strongly-consistent Scan attributing rows to their instances (see
// deleteTasksForInstancesByScan).
//
// COST MODEL (Codex round 10 on #328): the previous purge loop ran the
// single-instance full cleanup per victim, so every victim paid its own
// fully-paginated Scan — a purge batch of K instances cost up to K fleet
// scans (O(K × table)). Sharing one scan per PurgeInstances CALL costs
// O(instance rows + table) regardless of victim count: K GSI sweeps plus
// exactly one Scan. TerminateInstance keeps the single-instance variant for
// the common one-ID path.
func (b *Backend) deleteTasksForInstancesFull(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	for _, id := range ids {
		if err := b.deleteTasksForInstanceByGSI(ctx, id); err != nil {
			if !isMissingIndexError(err) {
				return err
			}
			// The index is table-wide, so a missing index fails identically
			// for every ID: stop probing (further Queries fail the same way)
			// and let the shared Scan below perform the whole cleanup.
			break
		}
	}
	return b.deleteTasksForInstancesByScan(ctx, ids)
}

// deleteTasksForInstancesByScan performs one fully-paginated
// strongly-consistent Scan over wf_tasks, deleting every row whose
// instance_id belongs to ids. A single scan covers the whole purge batch no
// matter how many victims it holds; rows of live instances are never
// touched (see purgeTaskKeyForTargets).
func (b *Backend) deleteTasksForInstancesByScan(ctx context.Context, ids []string) error {
	targets := purgeTaskTargets(ids)
	var start map[string]types.AttributeValue
	for {
		out, err := b.client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(b.table("wf_tasks")), ConsistentRead: aws.Bool(true), ExclusiveStartKey: start})
		if err != nil {
			return err
		}
		for _, m := range out.Items {
			pk, ok := purgeTaskKeyForTargets(m, targets)
			if !ok {
				continue
			}
			if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(b.table("wf_tasks")), Key: map[string]types.AttributeValue{"task_pk": pk}}); err != nil {
				return err
			}
		}
		if out.LastEvaluatedKey == nil {
			return nil
		}
		start = out.LastEvaluatedKey
	}
}

// purgeTaskTargets builds the membership set for one shared purge scan from
// the victim IDs of a single PurgeInstances call.
func purgeTaskTargets(ids []string) map[string]struct{} {
	targets := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		targets[id] = struct{}{}
	}
	return targets
}

// purgeTaskKeyForTargets returns the task_pk of a scanned task row iff the
// row belongs to one of the purge targets. Rows of live instances, rows
// without an instance_id, and rows without a task_pk (undeletable by key —
// every real task row carries its HASH key) report false and are skipped.
func purgeTaskKeyForTargets(m map[string]types.AttributeValue, targets map[string]struct{}) (types.AttributeValue, bool) {
	inst, ok := m["instance_id"]
	if !ok {
		return nil, false
	}
	if _, ok := targets[fromS(inst)]; !ok {
		return nil, false
	}
	pk, ok := m["task_pk"]
	if !ok || pk == nil {
		return nil, false
	}
	return pk, true
}

// gsiVerifyScanLimit bounds the strongly-consistent verification Scan page
// on the terminal hot path. Only one page is read per terminal
// advancement: verification cost stays O(instance rows + one page) instead
// of scaling with fleet work (see verifyTasksFirstPageByScan).
const gsiVerifyScanLimit = 1000

// verifyTasksFirstPageByScan deletes the instance's tasks visible to a
// single strongly-consistent Scan page.
//
// DESIGN (Codex round 7 on #328): the previous fully-paginated
// verification Scan ran after every terminal advancement and scaled with
// the fleet's queued work — every completion paid a full
// strongly-consistent table Scan, and throttling mid-scan errored the
// cleanup after the terminal status had already committed. Scoping the
// verification to the instance is not directly possible — the base table's
// partition key is the task ID, so no strongly-consistent instance-keyed
// read exists — and the GSI rows already returned prove nothing about
// lagging rows the index has not caught up with. The honest trade-off:
//
//   - The GSI sweep (instance-keyed Query, fully paginated over the
//     instance's own rows) removes everything the index has observed, and
//     this single strong page reaps lagging rows visible to consistent
//     state near the head of the table.
//   - Correctness never depends on the sweep: a lagging row that survives
//     cannot execute — ClaimTasks gates the lease on instance status in
//     the same transaction (claimTaskItem) plus a post-claim re-check, so
//     residue is inert.
//   - The leak lifetime is bounded by the paths that always run the full
//     cleanup: TerminateInstance (deleteTasksForInstanceFull) and retention
//     PurgeInstances, plus best-effort deletion when residue is met by a
//     later claim.
//
// A lagging task beyond this page therefore waits for one of those
// backstops instead of forcing every completion to scan the fleet.
func (b *Backend) verifyTasksFirstPageByScan(ctx context.Context, id string) error {
	out, err := b.client.Scan(ctx, &dynamodb.ScanInput{
		TableName:      aws.String(b.table("wf_tasks")),
		Limit:          aws.Int32(gsiVerifyScanLimit),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return err
	}
	for _, m := range out.Items {
		if fromS(m["instance_id"]) != id {
			continue
		}
		pk, ok := m["task_pk"]
		if !ok {
			continue
		}
		if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(b.table("wf_tasks")), Key: map[string]types.AttributeValue{"task_pk": pk}}); err != nil {
			return err
		}
	}
	return nil
}

// deleteTasksForInstanceByGSI removes one instance's tasks via the
// instance_gsi Query (no full-table Scan, no RCU on unrelated tasks).
func (b *Backend) deleteTasksForInstanceByGSI(ctx context.Context, id string) error {
	var start map[string]types.AttributeValue
	for {
		out, err := b.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(b.table("wf_tasks")),
			IndexName:                 aws.String(instanceGSIName),
			KeyConditionExpression:    aws.String("instance_id = :id"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(id)},
			ExclusiveStartKey:         start,
		})
		if err != nil {
			return err
		}
		for _, m := range out.Items {
			pk, ok := m["task_pk"]
			if !ok {
				continue
			}
			if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(b.table("wf_tasks")), Key: map[string]types.AttributeValue{"task_pk": pk}}); err != nil {
				return err
			}
		}
		if out.LastEvaluatedKey == nil {
			return nil
		}
		start = out.LastEvaluatedKey
	}
}

func isMissingIndexError(err error) bool {
	if err == nil {
		return false
	}
	// Typed nonexistent-index failure: unambiguous, always falls back to
	// the full-table Scan.
	var infe *types.IndexNotFoundException
	if errors.As(err, &infe) {
		return true
	}
	var rnfe *types.ResourceNotFoundException
	if errors.As(err, &rnfe) {
		return strings.Contains(err.Error(), instanceGSIName)
	}
	// Pre-typed (DynamoDB Local / branch-era) failures carry the
	// missing-index phrasing without a ValidationException code. The index
	// name must appear alongside so unrelated failures (e.g. an IAM denial
	// quoting the index ARN) still surface instead of silently degrading to
	// fleet-wide Scans.
	lower := strings.ToLower(err.Error())
	if strings.Contains(err.Error(), instanceGSIName) {
		if strings.Contains(lower, "unknown index") ||
			strings.Contains(lower, "being created") ||
			strings.Contains(lower, "is creating") ||
			strings.Contains(lower, "not active") ||
			strings.Contains(lower, "backfill") {
			return true
		}
	}
	// Anything else must carry an explicit missing-index code AND phrasing.
	// In particular a bare index-name mention (e.g. an IAM AccessDenied
	// quoting the index ARN) must NOT fall back: that would silently turn
	// every Terminate into perpetual full scans while hiding the config
	// error, so unrecognized failures return false and surface.
	if !strings.Contains(lower, "validationexception") && !strings.Contains(lower, "indexnotfoundexception") {
		return false
	}
	return strings.Contains(lower, "specified index") ||
		strings.Contains(lower, "no such index") ||
		strings.Contains(lower, "unknown index") ||
		strings.Contains(lower, "backfill") ||
		strings.Contains(lower, "being created") ||
		strings.Contains(lower, "is creating") ||
		strings.Contains(lower, "not active")
}

func (b *Backend) deleteTasksForInstanceByScan(ctx context.Context, id string) error {
	var start map[string]types.AttributeValue
	for {
		out, err := b.client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(b.table("wf_tasks")), ConsistentRead: aws.Bool(true), ExclusiveStartKey: start})
		if err != nil {
			return err
		}
		for _, m := range out.Items {
			if fromS(m["instance_id"]) == id {
				if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(b.table("wf_tasks")), Key: map[string]types.AttributeValue{"task_pk": m["task_pk"]}}); err != nil {
					return err
				}
			}
		}
		if out.LastEvaluatedKey == nil {
			return nil
		}
		start = out.LastEvaluatedKey
	}
}

// deleteTimersForInstance pages through the instance's timers (Query results
// larger than 1 MB arrive in pages via LastEvaluatedKey) and removes each one.
// The read is strongly consistent so a timer committed just before the
// terminal transition is not missed (same gap as the terminal inbox query).
func (b *Backend) deleteTimersForInstance(ctx context.Context, id string) error {
	var start map[string]types.AttributeValue
	for {
		out, err := b.client.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(b.table("wf_timers")), KeyConditionExpression: aws.String("instance_id = :id"), ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(id)}, ConsistentRead: aws.Bool(true), ExclusiveStartKey: start})
		if err != nil {
			return err
		}
		for _, m := range out.Items {
			if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(b.table("wf_timers")), Key: timerKey(id, fromN(m["seq"]))}); err != nil {
				return err
			}
		}
		if out.LastEvaluatedKey == nil {
			return nil
		}
		start = out.LastEvaluatedKey
	}
}

func (b *Backend) CountClaimableTasks(ctx context.Context, kind string, queues []string) (map[string]int64, error) {
	if len(queues) == 0 {
		return map[string]int64{}, nil
	}
	now := nowUTC()
	out := map[string]int64{}
	for _, queue := range queues {
		var n int64
		var startKey map[string]types.AttributeValue
		for {
			in := &dynamodb.QueryInput{
				TableName:              aws.String(b.table("wf_tasks")),
				IndexName:              aws.String("claim_gsi"),
				KeyConditionExpression: aws.String("gsi_pk = :g AND visible_at <= :now"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":g": avS(claimGSI(kind, queue)), ":now": avN(timeToN(now)),
				},
				Select:            types.SelectCount,
				ExclusiveStartKey: startKey,
			}
			res, err := b.client.Query(ctx, in)
			if err != nil {
				return nil, err
			}
			n += int64(res.Count)
			if res.LastEvaluatedKey == nil {
				break
			}
			startKey = res.LastEvaluatedKey
		}
		if n > 0 {
			out[queue] = n
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
	now, visible := nowUTC(), nowUTC().Add(req.Lease)
	result := []backend.Task{}
	// Share one picker across queues so MaxPerInstance caps the whole claim
	// batch, not each queue independently.
	var picker *backend.FairPicker
	if req.MaxPerInstance > 0 {
		picker = backend.NewFairPicker(req.Limit, req.MaxPerInstance)
	}
	for _, queue := range req.Queues {
		if len(result) >= req.Limit {
			break
		}
		if picker != nil && picker.Full() {
			break
		}
		// skip holds IDs already attempted from this queue. A conflicted
		// candidate's stale GSI image can resurface on a re-query before
		// the index catches up, so refills must exclude attempted IDs
		// instead of reselecting the same stale entry. It stays small:
		// only attempted (picked) IDs are recorded, never every examined
		// row, and entries behind the committed cursor are pruned on each
		// refill (see below), so it is bounded by the current page's
		// attempts rather than the whole stale backlog.
		skip := map[int64]struct{}{}
		// cursor carries the claim_gsi scan position across conflict
		// refills within this queue: each page is read once per
		// ClaimTasks call instead of restarting from the head on every
		// refill (which re-reads the whole prefix per conflict and turns
		// long stale runs quadratic). exhausted marks the partition end
		// so a refill never restarts from nil and the loop terminates.
		var cursor map[string]types.AttributeValue
		exhausted := false
		for len(result) < req.Limit && (picker == nil || !picker.Full()) && !exhausted {
			cands, next, done, err := b.listClaimCandidates(ctx, req.Kind, queue, now, req.Limit-len(result), picker, skip, cursor)
			if err != nil {
				// The batch may already hold committed leases from earlier
				// candidates/queues: release them best-effort (fenced by
				// the claim token) instead of abandoning the whole batch
				// hidden for a full lease (round-17 P2 on #291).
				b.releaseClaimedLeases(ctx, result)
				return nil, err
			}
			cursor, exhausted = next, done
			// Prune attempted IDs behind the committed cursor: the GSI scan
			// is forward-only, so rows before the resume point cannot recur
			// on the next refill. Every existing entry sorts before next —
			// picks come from rows at or before the resume key and earlier
			// pages are further behind (this holds for the pageStart
			// re-fetch too: it still sits ahead of every previous page) —
			// so dropping them cannot reselect, and the next fetch starts
			// at/after next. Only the current page's attempts are re-added
			// below, bounding skip to O(batch) under prolonged GSI lag
			// instead of O(stale backlog). Termination is unchanged:
			// exhausted plus the empty/release breaks below.
			clear(skip)
			if len(cands) == 0 {
				break
			}
			released := false
			for _, item := range cands {
				id := fromN(item["id"])
				skip[id] = struct{}{}
				old := fromN(item["visible_at"])
				// The lease commits atomically with an instance-status
				// gate (see claimTaskItem): a terminal commit landing
				// between the GSI Query and the claim aborts the claim
				// instead of delivering the task to the worker.
				t, claimed, err := b.claimTaskItem(ctx, item, old, visible, req.WorkerID)
				if err != nil {
					// The transactional lease for THIS candidate did not
					// commit (TransactWriteItems errors never leave a
					// half-committed lease), but earlier candidates in
					// result already hold committed leases: release them
					// best-effort instead of hiding the batch for a full
					// lease (round-17 P2 on #291).
					b.releaseClaimedLeases(ctx, result)
					return nil, err
				}
				if !claimed {
					// Lost the lease race, or the instance already left
					// running (terminal residue is deleted best-effort
					// inside claimTaskItem): free its picker slot so Full
					// below does not stop later candidates and queues
					// from filling the batch, then re-query this queue
					// for a replacement (the loop above) instead of
					// moving on.
					if picker != nil {
						picker.Release(backend.FairTaskRef{ID: id, InstanceID: fromS(item["instance_id"])})
						released = true
					}
					continue
				}
				// The terminal transition commits the instance update
				// before its residual rows are swept, so a terminal commit
				// landing after the gated claim still needs a fence:
				// deleting the row afterwards cannot recall it
				// (tickActivities invokes user code immediately), so
				// verify the owning instance is still running before
				// handing the task out. A residual task of a terminal
				// instance is dropped best-effort here; the terminal sweep
				// removes whatever remains. The transact gate in
				// claimTaskItem already covers a terminal commit landing
				// BEFORE the claim; this read fences one landing AFTER the
				// claim commit, so it stays (round-17 P2 on #291) — but a
				// throttled/transient failure here must not abandon the
				// already-committed leases below.
				running, err := b.instanceRunning(ctx, t.InstanceID)
				if err != nil {
					b.releaseClaimedLeases(ctx, append(append([]backend.Task(nil), result...), t))
					return nil, err
				}
				if !running {
					_, _ = b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(b.table("wf_tasks")), Key: map[string]types.AttributeValue{"task_pk": item["task_pk"]}})
					if picker != nil {
						picker.Release(backend.FairTaskRef{ID: id, InstanceID: t.InstanceID})
						released = true
					}
					continue
				}
				result = append(result, t)
				if len(result) >= req.Limit {
					break
				}
			}
			if !released {
				break
			}
		}
	}
	return result, nil
}

// claimTaskItem leases one task row previously read from the claim GSI,
// gating on the owning instance's status in the SAME transaction: the lease
// Update and a ConditionCheck on the instance row (status = "running")
// commit atomically, so a terminal commit landing between the GSI Query and
// the claim aborts the claim instead of delivering the task to the worker.
// It reports claimed=false when the claim lost — either a lease race with
// another worker or a terminal (or vanished) instance — in which case the
// task must never be handed out. Terminal residue is deleted best-effort;
// the terminal sweep owns whatever remains.
func (b *Backend) claimTaskItem(ctx context.Context, item map[string]types.AttributeValue, oldVisible int64, visible time.Time, workerID string) (backend.Task, bool, error) {
	instanceID := fromS(item["instance_id"])
	_, err := b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: claimTransactItems(b.table("wf_tasks"), b.table("wf_instances"), item["task_pk"], avS(instanceID), oldVisible, visible, workerID),
	})
	if err != nil {
		if isTransactionUnsupported(err) {
			// Stores without transaction support (some DynamoDB-compatible
			// endpoints) fall back to the separate lease update; the
			// post-claim status gate in ClaimTasks still fences terminal
			// residue, only without atomicity.
			return b.claimTaskItemLegacy(ctx, item, oldVisible, visible, workerID)
		}
		if conditional(err) {
			// Either the lease moved (another worker won) or the instance
			// left running (or was reaped). Neither case may deliver the
			// task; the extra read only decides delete-vs-skip.
			if running, rerr := b.instanceRunning(ctx, instanceID); rerr == nil && !running {
				_, _ = b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(b.table("wf_tasks")), Key: map[string]types.AttributeValue{"task_pk": item["task_pk"]}})
			}
			return backend.Task{}, false, nil
		}
		return backend.Task{}, false, err
	}
	// TransactWriteItems returns no updated image, so apply the claimed
	// lease to the queried row locally (visible_at/worker_id set verbatim,
	// attempt incremented by the ADD :one update).
	t := decodeTask(item)
	t.VisibleAt = visible
	t.WorkerID = workerID
	t.Attempt++
	return t, true, nil
}

// claimTransactItems builds the atomic claim transaction: a ConditionCheck
// that the instance row still carries status "running" (a missing row fails
// the check, matching instanceRunning's terminal treatment) plus the lease
// Update guarded on the observed visible_at.
func claimTransactItems(tasksTable, instancesTable string, taskPK, instanceID types.AttributeValue, oldVisible int64, visible time.Time, workerID string) []types.TransactWriteItem {
	return []types.TransactWriteItem{
		{ConditionCheck: &types.ConditionCheck{
			TableName:           aws.String(instancesTable),
			Key:                 map[string]types.AttributeValue{"id": instanceID},
			ConditionExpression: aws.String("#s = :running"),
			ExpressionAttributeNames: map[string]string{
				"#s": "status",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":running": avS("running"),
			},
		}},
		{Update: &types.Update{
			TableName:           aws.String(tasksTable),
			Key:                 map[string]types.AttributeValue{"task_pk": taskPK},
			UpdateExpression:    aws.String("SET visible_at = :v, worker_id = :w ADD attempt :one"),
			ConditionExpression: aws.String("visible_at = :old"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":v":   avN(timeToN(visible)),
				":w":   avS(workerID),
				":one": avN(1),
				":old": avN(oldVisible),
			},
		}},
	}
}

// claimTaskItemLegacy performs the pre-transaction lease update for stores
// without TransactWriteItems support. The caller still applies the
// post-claim status gate as defense-in-depth.
func (b *Backend) claimTaskItemLegacy(ctx context.Context, item map[string]types.AttributeValue, oldVisible int64, visible time.Time, workerID string) (backend.Task, bool, error) {
	updated, err := b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(b.table("wf_tasks")), Key: map[string]types.AttributeValue{"task_pk": item["task_pk"]},
		UpdateExpression: aws.String("SET visible_at = :v, worker_id = :w ADD attempt :one"), ConditionExpression: aws.String("visible_at = :old"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": avN(timeToN(visible)), ":w": avS(workerID), ":one": avN(1), ":old": avN(oldVisible)}, ReturnValues: types.ReturnValueAllNew})
	if conditional(err) {
		return backend.Task{}, false, nil
	}
	if err != nil {
		return backend.Task{}, false, err
	}
	return decodeTask(updated.Attributes), true, nil
}

// isTransactionUnsupported reports whether err indicates the endpoint does
// not implement TransactWriteItems at all (as opposed to a transaction that
// executed and cancelled). Executed-then-cancelled errors must never take
// the legacy path: they already decided the claim.
func isTransactionUnsupported(err error) bool {
	if err == nil {
		return false
	}
	var cancelled *types.TransactionCanceledException
	if errors.As(err, &cancelled) {
		return false
	}
	var failed *types.ConditionalCheckFailedException
	if errors.As(err, &failed) {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"unknownoperationexception",
		"invalidaction",
		"unrecognized",
		"unsupported",
		"not supported",
		"not implemented",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// instanceRunning reports whether the instance still accepts work (status
// "running"). A missing instance is treated as terminal: its tasks are
// residue the terminal sweep owns.
func (b *Backend) instanceRunning(ctx context.Context, id string) (bool, error) {
	inst, err := b.GetInstance(ctx, id)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return inst.Status == "running", nil
}

// releaseClaimedLeases best-effort releases durable leases acquired during a
// ClaimTasks call that is about to fail (round-17 P2 on #291). Without this,
// a throttled/transient post-claim status read abandons every already-claimed
// candidate — each holds a committed lease hiding it for the full lease
// duration. Releases are fenced on the claim ownership token (see
// ReleaseLease), so a task reclaimed or refreshed since the claim matches
// nothing and is left alone; release errors are ignored because the original
// error is already being returned. Detached from cancellation so a cancelled
// claim still frees what it leased.
func (b *Backend) releaseClaimedLeases(ctx context.Context, tasks []backend.Task) {
	if len(tasks) == 0 {
		return
	}
	rctx := context.WithoutCancel(ctx)
	for _, t := range tasks {
		_ = b.ReleaseLease(rctx, t)
	}
}

// listClaimCandidates returns FIFO-ordered claim_gsi items for one queue.
// With a nil picker it returns the first remaining items; otherwise it pages
// through the GSI (page window FairOverfetch) feeding the shared picker, so a
// victim hidden behind a flooding instance is still found beyond the first
// page. The picker is shared across the outer queue loop in ClaimTasks so the
// per-instance cap applies to the whole claim batch, and only candidates
// picked during this call are returned (earlier queues' picks are not
// re-attempted). Callers claim the returned items with a visible_at re-check:
// items leased concurrently fail the conditional update, must be released
// from the picker via Release (so Full does not stop later queues), and are
// skipped; the caller then re-queries for replacements, passing attempted IDs
// in skip so a stale GSI image is never reselected.
//
// No unbounded dedup set is kept here: pagination via LastEvaluatedKey
// advances monotonically over the GSI partition, so each matching item is
// visited exactly once per scan and repeats are impossible without concurrent
// writes shifting page boundaries. The only cross-row state is byID, which
// retains payloads solely for picker-accepted candidates (bounded by the
// batch size) and doubles as a guard against double-offering an accepted ID
// if a concurrent update ever surfaces a duplicate within one scan.
//
// The caller threads cursor through conflict refills (it is both the resume
// point and, via exhausted, the termination signal), so refills continue past
// already-consumed pages instead of re-reading the prefix: every page is
// fetched once per ClaimTasks call and the loop ends when the partition is
// exhausted. When the batch fills mid-page the cursor points after the last
// examined item (not the page end), so a refill re-examines the unexamined
// page suffix instead of skipping it. Attempted IDs stay in skip so a stale
// GSI image of a released candidate is never reselected after its slot is
// freed; only the current page's attempts are retained, entries behind the
// committed cursor being pruned on each refill (they cannot recur past the
// forward-only resume point), which bounds skip to O(batch). Trade-off: rows rejected by the fair cap before a conflict freed a
// slot are picked up on a later poll rather than in the same call; liveness
// holds because they stay claimable.
// claimResumeKey rebuilds the ExclusiveStartKey that resumes a claim_gsi
// Query after item: the table HASH key (task_pk) plus the index HASH+RANGE
// keys (gsi_pk, visible_at). It lets a refill continue after the last
// examined item of a partially consumed page instead of skipping the
// unexamined page suffix. Nil when the item lacks those attributes (never
// for claim candidates, which always carry them); callers then re-fetch the
// page rather than skip rows.
func claimResumeKey(item map[string]types.AttributeValue) map[string]types.AttributeValue {
	tpk, ok1 := item["task_pk"]
	gpk, ok2 := item["gsi_pk"]
	vis, ok3 := item["visible_at"]
	if !ok1 || !ok2 || !ok3 || tpk == nil || gpk == nil || vis == nil {
		return nil
	}
	return map[string]types.AttributeValue{"task_pk": tpk, "gsi_pk": gpk, "visible_at": vis}
}

func (b *Backend) listClaimCandidates(ctx context.Context, kind, queue string, now time.Time, remaining int, picker *backend.FairPicker, skip map[int64]struct{}, cursor map[string]types.AttributeValue) ([]map[string]types.AttributeValue, map[string]types.AttributeValue, bool, error) {
	queryPage := func(start map[string]types.AttributeValue, pageLimit int32) (*dynamodb.QueryOutput, error) {
		return b.client.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(b.table("wf_tasks")), IndexName: aws.String("claim_gsi"),
			KeyConditionExpression: aws.String("gsi_pk = :g AND visible_at <= :now"), ExpressionAttributeValues: map[string]types.AttributeValue{":g": avS(claimGSI(kind, queue)), ":now": avN(timeToN(now))},
			Limit: aws.Int32(pageLimit), ExclusiveStartKey: start})
	}
	if picker == nil {
		// Single-pass path: the caller never refills without a picker
		// (refills follow a conflict Release), so cursor is always nil
		// here; it is threaded only for signature symmetry.
		out, err := queryPage(cursor, int32(remaining))
		if err != nil {
			return nil, cursor, false, err
		}
		if len(skip) == 0 {
			return out.Items, out.LastEvaluatedKey, false, nil
		}
		items := out.Items[:0]
		for _, item := range out.Items {
			if _, ok := skip[fromN(item["id"])]; !ok {
				items = append(items, item)
			}
		}
		return items, out.LastEvaluatedKey, false, nil
	}
	pageSize := backend.FairOverfetch(remaining)
	// byID retains full payloads only for picker-accepted candidates so
	// rejected backlog scanned past an over-quota flood does not accumulate
	// in memory.
	// fresh counts the picks made during this call: the shared picker may
	// already hold earlier queues' picks, which must not be re-attempted
	// (a re-attempt would fail its own conditional update and wrongly
	// release an already-successful claim).
	byID := map[int64]map[string]types.AttributeValue{}
	fresh := len(picker.Picked())
	start := cursor
	exhausted := false
	for !picker.Full() {
		pageStart := start
		out, err := queryPage(start, int32(pageSize))
		if err != nil {
			return nil, start, false, err
		}
		if len(out.Items) == 0 {
			start = out.LastEvaluatedKey
			exhausted = start == nil
			break
		}
		examined := -1
		for i, item := range out.Items {
			examined = i
			id := fromN(item["id"])
			if _, ok := skip[id]; ok {
				continue
			}
			if _, ok := byID[id]; ok {
				continue
			}
			before := len(picker.Picked())
			full := picker.Offer(backend.FairTaskRef{ID: id, InstanceID: fromS(item["instance_id"])})
			if len(picker.Picked()) > before {
				byID[id] = item
			}
			if full {
				break
			}
		}
		if picker.Full() {
			// Batch filled: the refill after a claim conflict resumes
			// after the last EXAMINED item, not after the whole fetched
			// page, so the unexamined page suffix is re-examined instead
			// of skipped. The resume key carries the table HASH key plus
			// the claim_gsi HASH+RANGE keys of that item.
			if examined >= 0 && examined < len(out.Items)-1 {
				if key := claimResumeKey(out.Items[examined]); key != nil {
					start = key
				} else {
					// Unreachable: claim items always carry these
					// attributes. Re-fetch the page rather than skip the
					// suffix; the skip set dedups the re-examined prefix
					// and attempted IDs keep growing, so the loop still
					// terminates.
					start = pageStart
				}
				break
			}
			start = out.LastEvaluatedKey
			// Batch filled exactly at the page end, or the page is the
			// partition tail: unscanned rows may remain behind.
			break
		}
		start = out.LastEvaluatedKey
		if start == nil {
			exhausted = true
			break
		}
	}
	picked := picker.Picked()[fresh:]
	items := make([]map[string]types.AttributeValue, 0, len(picked))
	for _, r := range picked {
		if item, ok := byID[r.ID]; ok {
			items = append(items, item)
		}
	}
	return items, start, exhausted, nil
}

func decodeTask(m map[string]types.AttributeValue) backend.Task {
	t := backend.Task{ID: fromN(m["id"]), Kind: fromS(m["kind"]), Queue: fromS(m["queue"]), InstanceID: fromS(m["instance_id"]), Seq: fromN(m["ref_seq"]), Attempt: int(fromN(m["attempt"])), MaxAttempts: int(fromN(m["max_attempts"])), VisibleAt: nToTime(fromN(m["visible_at"])), WorkerID: fromS(m["worker_id"]), HeartbeatDetails: fromJSON(m["heartbeat"])}
	if t.Kind == "activity" {
		var p activityPayload
		_ = json.Unmarshal(fromJSON(m["payload"]), &p)
		t.Name, t.Input, t.MaxAttempts = p.Name, p.Input, p.Retry.MaxAttempts
		t.Retry = backend.RetryPolicy{InitialInterval: time.Duration(p.Retry.InitialIntervalMs) * time.Millisecond, BackoffCoefficient: p.Retry.BackoffCoefficient, MaxInterval: time.Duration(p.Retry.MaxIntervalMs) * time.Millisecond, MaxAttempts: p.Retry.MaxAttempts}
		t.StartToCloseTimeout = time.Duration(p.StartToCloseTimeoutMs) * time.Millisecond
	}
	return t
}

func (b *Backend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	return b.updateTask(ctx, taskID, "SET visible_at = :v", map[string]types.AttributeValue{":v": avN(timeToN(nowUTC().Add(d)))}, "")
}

func (b *Backend) RecordHeartbeat(ctx context.Context, taskID int64, lease time.Duration, details []byte) error {
	values := map[string]types.AttributeValue{":v": avN(timeToN(nowUTC().Add(lease)))}
	update := "SET visible_at = :v"
	if details != nil {
		update += ", heartbeat = :h"
		values[":h"] = avJSON(details)
	}
	return b.updateTask(ctx, taskID, update, values, "")
}
func (b *Backend) ReleaseLease(ctx context.Context, t backend.Task) error {
	// Workflow tasks live under WF#<instanceID> (not ACT#<id>), so route by
	// kind like NackTask does. The release is fenced on the claim ownership
	// token (numeric id + worker + attempt): a stale worker whose task was
	// reclaimed or atomically refreshed matches nothing and reports
	// ErrNotFound instead of clearing the fresh lease.
	pk := actTaskPK(t.ID)
	if t.Kind == "workflow" && t.InstanceID != "" {
		pk = wfTaskPK(t.InstanceID)
	}
	values := map[string]types.AttributeValue{":v": avN(timeToN(nowUTC()))}
	cond := "attribute_exists(task_pk)"
	if t.Kind == "workflow" && t.InstanceID != "" && t.ID != 0 {
		cond += " AND id = :taskid"
		values[":taskid"] = avN(t.ID)
	}
	if t.WorkerID != "" {
		cond += " AND worker_id = :wid AND attempt = :attempt"
		values[":wid"] = avS(t.WorkerID)
		values[":attempt"] = avN(int64(t.Attempt))
	}
	_, err := b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(b.table("wf_tasks")),
		Key:                       map[string]types.AttributeValue{"task_pk": avS(pk)},
		UpdateExpression:          aws.String("SET visible_at = :v REMOVE worker_id"),
		ConditionExpression:       aws.String(cond),
		ExpressionAttributeValues: values,
	})
	if conditional(err) {
		return backend.ErrNotFound
	}
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}
func (b *Backend) NackTask(ctx context.Context, t backend.Task, delay time.Duration) error {
	pk := actTaskPK(t.ID)
	if t.Kind == "workflow" {
		pk = wfTaskPK(t.InstanceID)
	}
	// Fence the nack on the claim ownership token (numeric id on WF keys,
	// worker + attempt): a stale worker whose task was reclaimed or
	// atomically refreshed matches nothing and reports ErrNotFound instead
	// of clearing the fresh lease.
	values := map[string]types.AttributeValue{":v": avN(timeToN(nowUTC().Add(delay)))}
	cond := "attribute_exists(task_pk)"
	if t.Kind == "workflow" && t.InstanceID != "" && t.ID != 0 {
		cond += " AND id = :taskid"
		values[":taskid"] = avN(t.ID)
	}
	if t.WorkerID != "" {
		cond += " AND worker_id = :wid AND attempt = :attempt"
		values[":wid"] = avS(t.WorkerID)
		values[":attempt"] = avN(int64(t.Attempt))
	}
	_, err := b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(b.table("wf_tasks")),
		Key:                       map[string]types.AttributeValue{"task_pk": avS(pk)},
		UpdateExpression:          aws.String("SET visible_at = :v REMOVE worker_id"),
		ConditionExpression:       aws.String(cond),
		ExpressionAttributeValues: values,
	})
	if conditional(err) {
		return backend.ErrNotFound
	}
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}
func (b *Backend) RetryActivity(ctx context.Context, taskID int64, delay time.Duration) error {
	return b.updateTask(ctx, taskID, "SET visible_at = :v REMOVE worker_id", map[string]types.AttributeValue{":v": avN(timeToN(nowUTC().Add(delay)))}, "kind = :kind")
}
func (b *Backend) updateTask(ctx context.Context, id int64, update string, values map[string]types.AttributeValue, condition string) error {
	if condition != "" {
		values[":kind"] = avS("activity")
	}
	_, err := b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(b.table("wf_tasks")), Key: map[string]types.AttributeValue{"task_pk": avS(actTaskPK(id))}, UpdateExpression: aws.String(update), ConditionExpression: aws.String("attribute_exists(task_pk)" + condSuffix(condition)), ExpressionAttributeValues: values})
	if conditional(err) {
		return backend.ErrNotFound
	}
	return err
}
func condSuffix(s string) string {
	if s == "" {
		return ""
	}
	return " AND " + s
}

func (b *Backend) LoadWorkflowHead(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	inst, err := b.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	st := &backend.WorkflowState{Instance: *inst, NextSeq: inst.NextSeq, Now: nowUTC()}
	out, err := b.client.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(b.table("wf_inbox")), KeyConditionExpression: aws.String("instance_id = :id"), ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(instanceID)}, ScanIndexForward: aws.Bool(true)})
	if err != nil {
		return nil, err
	}
	items := make([]backend.InboxEntry, 0, len(out.Items))
	for _, m := range out.Items {
		name, payload := unwrapInboxPayload(fromJSON(m["payload"]))
		items = append(items, backend.InboxEntry{
			Seq:       fromN(m["seq"]),
			CreatedAt: fromN(m["created_at"]),
			ID:        fromN(m["id"]),
			Event:     journal.Event{Type: journal.Type(fromS(m["type"])), Name: name, RefSeq: fromN(m["ref_seq"]), Payload: payload},
		})
	}
	backend.SortInbox(items)
	for _, it := range items {
		st.Inbox = append(st.Inbox, backend.InboxEvent{ID: it.ID, Event: it.Event})
	}
	return st, nil
}

// allocInboxSeqs atomically reserves n per-instance inbox sequence numbers
// on the wf_inbox_seq item and returns the top of the reserved range [top-n+1, top].
func (b *Backend) allocInboxSeqs(ctx context.Context, instanceID string, n int64) (int64, error) {
	out, err := b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(b.table("wf_inbox_seq")),
		Key:              map[string]types.AttributeValue{"id": avS(instanceID)},
		UpdateExpression: aws.String("ADD seq :n"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":n": avN(n),
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	var tip *types.TransactionInProgressException
	if errors.As(err, &tip) {
		return 0, backend.ErrConflict
	}
	if err != nil {
		return 0, err
	}
	top := fromN(out.Attributes["seq"])
	if top < n {
		return 0, fmt.Errorf("dynamodb: invalid inbox_seq %d for %s", top, instanceID)
	}
	return top, nil
}

// InboxSeq reports the raw per-instance inbox sequence counter for tests
// (found=false when the counter does not exist).
func (b *Backend) InboxSeq(ctx context.Context, instanceID string) (int64, bool, error) {
	out, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(b.table("wf_inbox_seq")),
		Key:       map[string]types.AttributeValue{"id": avS(instanceID)},
	})
	if err != nil {
		return 0, false, err
	}
	if len(out.Item) == 0 {
		return 0, false, nil
	}
	return fromN(out.Item["seq"]), true, nil
}

func (b *Backend) LoadWorkflow(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	st, err := b.LoadWorkflowHead(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if st.Journal, err = b.GetJournal(ctx, instanceID, 0); err != nil {
		return nil, err
	}
	return st, nil
}

func (b *Backend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	return b.CommitAdvancements(ctx, []backend.Advancement{adv})
}

func (b *Backend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	if len(advs) == 0 {
		return nil
	}
	// Reject duplicate instances up front: the batch below builds one
	// TransactWriteItems with operations from every advancement, and
	// DynamoDB rejects multiple operations on the same item with a
	// ValidationException (not mapped to ErrConflict by conditional).
	// Preflight keeps the batch all-or-nothing with a conflict error
	// (see backendtest CommitAdvancementsAtomic).
	seen := make(map[string]struct{}, len(advs))
	for _, adv := range advs {
		if _, dup := seen[adv.InstanceID]; dup {
			return backend.ErrConflict
		}
		seen[adv.InstanceID] = struct{}{}
	}
	if len(advs) == 1 {
		if err := b.commitAdvancementOnce(ctx, advs[0]); err != nil {
			return err
		}
		return b.notifyAfterAdvancements(advs)
	}
	// Size the combined transaction BEFORE building anything: the batch
	// below commits as one TransactWriteItems (limit 100 items), and falling
	// back to sequential per-advancement commits when it overflows would
	// break atomicity — earlier advancements would stay committed while a
	// later stale ExpectedSeq returns ErrConflict (see backendtest
	// CommitAdvancementsAtomic case C). Oversized combined batches are
	// rejected with a sizing error while every instance is still untouched.
	// The preflight is read-only (instance reads for the parent-notify term)
	// so a rejection mutates nothing, not even inbox sequence counters
	// (which buildAdvancementItems would bump as a side effect).
	total := 0
	for _, adv := range advs {
		inst, err := b.GetInstance(ctx, adv.InstanceID)
		if err != nil {
			return backend.ErrConflict
		}
		total += advancementItemCount(adv, inst.ParentID != "")
		if total > dynamoTxnItemLimit {
			return fmt.Errorf("dynamodb: combined batch produces %d transaction operations (limit %d; split the batch or reduce fanout; see docs/09-limits.md)", total, dynamoTxnItemLimit)
		}
	}
	var all []types.TransactWriteItem
	var ensures []string
	// refreshed marks advancements whose workflow task was refreshed
	// atomically inside the transaction (truncated fanout): they need no
	// post-commit ensure.
	var refreshed map[string]bool
	var parentEnsures []string
	for _, adv := range advs {
		items, parentID, err := b.buildAdvancementItems(ctx, adv)
		if err != nil {
			return err
		}
		if len(all)+len(items) > dynamoTxnItemLimit {
			// Unreachable when the preflight above mirrors
			// buildAdvancementItems exactly: fail closed with a sizing
			// error rather than falling back to sequential commits that
			// would apply the batch partially.
			return fmt.Errorf("dynamodb: combined batch produces %d transaction operations (limit %d; split the batch or reduce fanout; see docs/09-limits.md)", len(all)+len(items), dynamoTxnItemLimit)
		}
		all = append(all, items...)
		ensures = append(ensures, adv.InstanceID)
		if adv.EnsureWorkflowTask {
			if refreshed == nil {
				refreshed = map[string]bool{}
			}
			refreshed[adv.InstanceID] = true
		}
		if parentID != "" {
			parentEnsures = append(parentEnsures, parentID)
		}
	}
	_, err := b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: all})
	if conditional(err) {
		return backend.ErrConflict
	}
	if err != nil {
		return err
	}
	// Sweep terminal residue before any fallible post-commit work: the
	// terminal status already committed, so a throttled ensure below would
	// otherwise skip cleanup with no recovery (retrying the advancement
	// conflicts on the consumed sequence and no later pass removes the
	// rows). Terminal instances take no follow-up task, so they are also
	// excluded from the ensure loop below: ensureWorkflowTask would be a
	// no-op status-gated read for them, and a transient failure must not
	// fail terminal success after the rows are already gone.
	if err := b.cleanupTerminalAdvancements(context.Background(), advs); err != nil {
		return err
	}
	terminal := make(map[string]bool, len(advs))
	for _, adv := range advs {
		if adv.Terminal != nil {
			terminal[adv.InstanceID] = true
		}
	}
	for _, id := range parentEnsures {
		if err := b.ensureWorkflowTask(ctx, id); err != nil {
			return err
		}
	}
	for _, id := range ensures {
		// Truncated advancements refreshed their task atomically inside
		// the transaction; only inbox-backed ensures remain here.
		if refreshed[id] {
			continue
		}
		if terminal[id] {
			continue
		}
		if err := b.ensureWorkflowTask(ctx, id); err != nil {
			return err
		}
	}
	return b.notifyAfterAdvancements(advs)
}

func (b *Backend) notifyAfterAdvancements(advs []backend.Advancement) error {
	// Terminal residue was swept before the fallible post-commit ensures
	// (see commitAdvancementOnce and CommitAdvancements), so only wake
	// hints remain here: task waiters first, then terminal watchers.
	b.notifyTasks()
	for _, adv := range advs {
		if adv.Terminal != nil {
			b.notifyTerminal(adv.InstanceID)
		}
	}
	return nil
}

// cleanupTerminalAdvancements sweeps residual rows for every terminal
// advancement. Callers run it immediately after the commit and before any
// fallible post-commit work, so a throttled ensure cannot strand claimable
// rows behind.
func (b *Backend) cleanupTerminalAdvancements(ctx context.Context, advs []backend.Advancement) error {
	for _, adv := range advs {
		if adv.Terminal != nil {
			if err := b.cleanupTerminalInstance(ctx, adv.InstanceID); err != nil {
				return err
			}
		}
	}
	return nil
}

// cleanupTerminalInstance removes residual tasks, timers, inbox entries and
// signal dedupe rows for a terminal instance, retrying transient failures
// (throttling, timeouts) before terminal success is reported.
func (b *Backend) cleanupTerminalInstance(ctx context.Context, id string) error {
	const attempts = 5
	var err error
	for i := 0; i < attempts; i++ {
		if err = b.cleanupTerminalInstanceOnce(ctx, id); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		time.Sleep(time.Duration(100*(1<<i)) * time.Millisecond)
	}
	return fmt.Errorf("dynamodb: terminal cleanup for %s failed after %d attempts: %w", id, attempts, err)
}

func (b *Backend) cleanupTerminalInstanceOnce(ctx context.Context, id string) error {
	if err := b.deleteSignalDedupeForInstance(ctx, id); err != nil {
		return err
	}
	if err := b.deleteTasksForInstance(ctx, id); err != nil {
		return err
	}
	if err := b.deleteTimersForInstance(ctx, id); err != nil {
		return err
	}
	if err := b.deleteInboxForInstance(ctx, id); err != nil {
		return err
	}
	return nil
}

func (b *Backend) commitAdvancementOnce(ctx context.Context, adv backend.Advancement) error {
	items, parentID, err := b.buildAdvancementItems(ctx, adv)
	if err != nil {
		return err
	}
	if len(items) > dynamoTxnItemLimit {
		return fmt.Errorf("dynamodb: advancement produces %d transaction operations (budget %d; see docs/09-limits.md)", len(items), b.Capabilities().MaxAdvancementEffects)
	}
	_, err = b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if conditional(err) {
		return backend.ErrConflict
	}
	if err != nil {
		return err
	}
	// Sweep terminal residue before the fallible ensures below: the
	// terminal status already committed, so a throttled GetItem here would
	// otherwise skip cleanup with no recovery (retrying the advancement
	// conflicts on the consumed sequence and no later pass removes the
	// rows).
	if adv.Terminal != nil {
		if err := b.cleanupTerminalInstance(context.Background(), adv.InstanceID); err != nil {
			return err
		}
	}
	if parentID != "" {
		if err := b.ensureWorkflowTask(ctx, parentID); err != nil {
			return err
		}
	}
	if adv.Terminal != nil {
		// The owned workflow task was deleted atomically in the
		// transaction and a terminal instance takes no follow-up, so the
		// self-ensure would be a no-op status-gated read. Skip the
		// fallible RPC instead of risking terminal success on throttling.
		// notifyAfterAdvancements (caller) still fires task wake hints.
		return nil
	}
	if adv.EnsureWorkflowTask {
		// Follow-up task was refreshed atomically inside the transaction;
		// notifyAfterAdvancements (caller) still fires task wake hints.
		return nil
	}
	return b.ensureWorkflowTask(ctx, adv.InstanceID)
}

func (b *Backend) buildAdvancementItems(ctx context.Context, adv backend.Advancement) ([]types.TransactWriteItem, string, error) {
	inst, err := b.GetInstance(ctx, adv.InstanceID)
	if err != nil {
		return nil, "", backend.ErrConflict
	}
	now := nowUTC()
	newSeq := adv.ExpectedSeq
	for _, e := range adv.NewEvents {
		if e.Seq >= newSeq {
			newSeq = e.Seq + 1
		}
	}
	names := map[string]string(nil)
	if adv.Terminal != nil {
		names = map[string]string{"#status": "status", "#result": "result"}
	}
	saUpdate := backend.HasSearchAttributesUpdate(adv.NewEvents)
	memoUpdate := backend.HasMemoUpdate(adv.NewEvents)
	items := []types.TransactWriteItem{{
		Update: &types.Update{TableName: aws.String(b.table("wf_instances")), Key: map[string]types.AttributeValue{"id": avS(adv.InstanceID)},
			UpdateExpression: aws.String(instanceAdvanceExpression(adv.Terminal, saUpdate, memoUpdate)), ConditionExpression: aws.String("next_seq = :expected"),
			ExpressionAttributeNames: names, ExpressionAttributeValues: instanceAdvanceValues(newSeq, adv.ExpectedSeq, now, adv.Terminal, adv.NewEvents)}},
	}
	for _, e := range adv.NewEvents {
		items = append(items, put(b.table("wf_journal"), journalItem(adv.InstanceID, e.Seq, e, now), "attribute_not_exists(instance_id) AND attribute_not_exists(seq)"))
	}
	for _, at := range adv.ActivityTasks {
		if adv.Terminal != nil {
			break
		}
		items = append(items, put(b.table("wf_tasks"), activityTaskItem(at, now), "attribute_not_exists(task_pk)"))
	}
	for _, tm := range adv.Timers {
		if adv.Terminal != nil {
			break
		}
		items = append(items, put(b.table("wf_timers"), timerItem(adv.InstanceID, tm, now), "attribute_not_exists(instance_id) AND attribute_not_exists(seq)"))
	}
	for _, id := range adv.DrainedInbox {
		items = append(items, del(b.table("wf_inbox"), inboxKey(adv.InstanceID, id), ""))
	}
	for _, child := range adv.Children {
		q := child.Queue
		if q == "" {
			q = "default"
		}
		items = append(items, put(b.table("wf_instances"), instanceItem(child, q, now), "attribute_not_exists(id)"), put(b.table("wf_journal"), journalItem(child.ID, 1, journal.Event{Type: journal.TypeWorkflowStarted, Name: child.Name, Payload: child.Input}, now), "attribute_not_exists(instance_id) AND attribute_not_exists(seq)"), put(b.table("wf_tasks"), workflowTaskItem(child.ID, q, newID(), now), "attribute_not_exists(task_pk)"))
	}
	parentID := ""
	if adv.ParentNotify != nil && inst.ParentID != "" {
		seq, err := b.allocInboxSeqs(ctx, inst.ParentID, 1)
		if err != nil {
			return nil, "", err
		}
		ev := *adv.ParentNotify
		if ev.RefSeq == 0 {
			ev.RefSeq = inst.ParentSeq
		}
		items = append(items, put(b.table("wf_inbox"), inboxItem(inst.ParentID, newID(), seq, ev, now), "attribute_not_exists(instance_id) AND attribute_not_exists(id)"))
		parentID = inst.ParentID
	}
	if adv.EnsureWorkflowTask && adv.Terminal == nil {
		// Truncated fanout: keep the singleton workflow task alive with an
		// in-place refresh instead of delete + post-commit ensure. The
		// follow-up is then part of the same atomic transaction, so no
		// crash gap can stall the remaining replayed commands (recovery
		// cannot detect them: they leave no inbox behind).
		items = append(items, b.refreshWorkflowTask(adv.InstanceID, adv.TaskID, inst.Queue, now))
	} else {
		items = append(items, delWithValues(b.table("wf_tasks"), map[string]types.AttributeValue{"task_pk": avS(wfTaskPK(adv.InstanceID))}, "id = :taskid AND kind = :workflow", map[string]types.AttributeValue{":taskid": avN(adv.TaskID), ":workflow": avS("workflow")}))
	}
	return items, parentID, nil
}

// refreshWorkflowTask atomically carries the singleton workflow task past a
// truncated advancement: one Update on the same key instead of Delete +
// post-commit Put. The fence (id/kind condition) is preserved so a zombie
// task that lost its lease still fails the transaction.
func (b *Backend) refreshWorkflowTask(instanceID string, taskID int64, queue string, now time.Time) types.TransactWriteItem {
	return types.TransactWriteItem{Update: &types.Update{
		TableName:           aws.String(b.table("wf_tasks")),
		Key:                 map[string]types.AttributeValue{"task_pk": avS(wfTaskPK(instanceID))},
		UpdateExpression:    aws.String("SET id = :newid, visible_at = :v, attempt = :zero, created_at = :now REMOVE worker_id"),
		ConditionExpression: aws.String("id = :taskid AND kind = :workflow"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newid": avN(newID()), ":v": avN(timeToN(now)), ":zero": avN(0), ":now": avN(timeToN(now)),
			":taskid": avN(taskID), ":workflow": avS("workflow"),
		},
	}}
}

func instanceAdvanceExpression(t *backend.TerminalUpdate, withSearchAttrs, withMemo bool) string {
	expr := "SET next_seq = :next, updated_at = :now"
	if withSearchAttrs {
		expr += ", search_attributes = :sa"
	}
	if withMemo {
		expr += ", memo = :memo"
	}
	if t == nil {
		return expr
	}
	return expr + ", #status = :status, #result = :result, failure = :failure, completed_at = :now"
}
func instanceAdvanceValues(next, expected int64, now time.Time, t *backend.TerminalUpdate, events []journal.Event) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{":next": avN(next), ":expected": avN(expected), ":now": avN(timeToN(now))}
	if backend.HasSearchAttributesUpdate(events) {
		m[":sa"] = avJSON(backend.MarshalSearchAttributes(backend.LastSearchAttributesUpdate(events)))
	}
	if backend.HasMemoUpdate(events) {
		m[":memo"] = avJSON(backend.MarshalSearchAttributes(backend.LastMemoUpdate(events)))
	}
	if t != nil {
		m[":status"], m[":result"], m[":failure"] = avS(t.Status), avJSON(t.Result), avJSON(t.Failure)
	}
	return m
}
func activityTaskItem(t backend.NewTask, now time.Time) map[string]types.AttributeValue {
	q := t.Queue
	if q == "" {
		q = "default"
	}
	id := newID()
	p, _ := json.Marshal(activityPayload{Name: t.Name, Input: t.Input, Retry: retryJSON{InitialIntervalMs: t.Retry.InitialInterval.Milliseconds(), BackoffCoefficient: t.Retry.BackoffCoefficient, MaxIntervalMs: t.Retry.MaxInterval.Milliseconds(), MaxAttempts: t.MaxAttempts}, StartToCloseTimeoutMs: t.StartToCloseTimeout.Milliseconds()})
	return map[string]types.AttributeValue{"task_pk": avS(actTaskPK(id)), "id": avN(id), "kind": avS("activity"), "queue": avS(q), "gsi_pk": avS(claimGSI("activity", q)), "instance_id": avS(t.InstanceID), "ref_seq": avN(t.Seq), "payload": avJSON(p), "attempt": avN(0), "max_attempts": avN(int64(t.MaxAttempts)), "visible_at": avN(timeToN(now)), "created_at": avN(timeToN(now))}
}
func timerItem(instanceID string, t backend.NewTimer, now time.Time) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"instance_id": avS(instanceID), "seq": avN(t.Seq), "gsi_pk": avS("TIMER"), "fire_at": avN(timeToN(t.FireAt)), "created_at": avN(timeToN(now))}
}
func inboxItem(instanceID string, id, seq int64, e journal.Event, now time.Time) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{"instance_id": avS(instanceID), "id": avN(id), "seq": avN(seq), "type": avS(string(e.Type)), "payload": avJSON(inboxPayload(e)), "created_at": avN(timeToN(now))}
	if e.RefSeq != 0 {
		m["ref_seq"] = avN(e.RefSeq)
	}
	return m
}

func (b *Backend) ensureWorkflowTask(ctx context.Context, instanceID string) error {
	inst, err := b.GetInstance(ctx, instanceID)
	if err != nil || inst.Status != "running" {
		return err
	}
	inbox, err := b.client.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(b.table("wf_inbox")), KeyConditionExpression: aws.String("instance_id = :id"), ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(instanceID)}, Limit: aws.Int32(1)})
	if err != nil || len(inbox.Items) == 0 {
		return err
	}
	item := workflowTaskItem(instanceID, inst.Queue, newID(), nowUTC())
	if terr := b.putWorkflowTaskIfRunning(ctx, instanceID, item); terr != nil {
		if isTransactionUnsupported(terr) {
			// Stores without TransactWriteItems support fall back to a
			// status re-check immediately before the Put (see
			// putWorkflowTaskLegacy): narrower race, documented residual.
			return b.putWorkflowTaskLegacy(ctx, instanceID, item)
		}
		return terr
	}
	return nil
}

// putWorkflowTaskIfRunning creates the singleton workflow task only while
// the instance is still running, in ONE transaction: a ConditionCheck on
// the instance row (status = "running"; a missing row fails the check,
// matching instanceRunning's terminal treatment) plus the Put guarded on
// attribute_not_exists(task_pk).
//
// This closes the recreate-after-cleanup race: CompleteActivity's inbox
// commit and the status read above can both serialize before a terminal
// transition whose sweep then finishes before the task Put executes. A
// bare PutItem has no status condition and would recreate the workflow
// task after the cleanup — the row lingers (the claim gate still prevents
// execution, but nothing reaps it). The transactional Put aborts instead,
// so a terminal sweep is never undone by a stale ensure.
//
// A conditional failure (instance left running, or the singleton already
// exists) is success: either the terminal sweep owns the task now or
// another ensure already created it.
func (b *Backend) putWorkflowTaskIfRunning(ctx context.Context, instanceID string, item map[string]types.AttributeValue) error {
	_, err := b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{ConditionCheck: &types.ConditionCheck{
			TableName:           aws.String(b.table("wf_instances")),
			Key:                 map[string]types.AttributeValue{"id": avS(instanceID)},
			ConditionExpression: aws.String("#s = :running"),
			ExpressionAttributeNames: map[string]string{
				"#s": "status",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":running": avS("running"),
			},
		}},
		{Put: &types.Put{
			TableName:           aws.String(b.table("wf_tasks")),
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(task_pk)"),
		}},
	}})
	if conditional(err) {
		return nil
	}
	return err
}

// putWorkflowTaskLegacy is the ensure fallback for stores without
// TransactWriteItems support: it re-reads the instance status immediately
// before the Put and skips a terminal instance, narrowing the
// read-then-Put gap to the minimum a bare PutItem allows. A residual race
// remains — a termination committing between the re-read and the Put still
// recreates the row — and is documented rather than hidden: the lingered
// row cannot execute (claimTaskItem's atomic status ConditionCheck plus
// the post-claim instanceRunning gate fence it) and is reaped by the next
// TerminateInstance full cleanup or retention purge.
func (b *Backend) putWorkflowTaskLegacy(ctx context.Context, instanceID string, item map[string]types.AttributeValue) error {
	running, err := b.instanceRunning(ctx, instanceID)
	if err != nil || !running {
		return err
	}
	_, err = b.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(b.table("wf_tasks")), Item: item, ConditionExpression: aws.String("attribute_not_exists(task_pk)")})
	if conditional(err) {
		return nil
	}
	return err
}

func (b *Backend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	key := map[string]types.AttributeValue{"task_pk": avS(actTaskPK(taskID))}
	task, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(b.table("wf_tasks")), Key: key, ConsistentRead: aws.Bool(true)})
	if err != nil || len(task.Item) == 0 || fromS(task.Item["kind"]) != "activity" {
		return backend.ErrSuperseded
	}
	inst, err := b.GetInstance(ctx, fromS(task.Item["instance_id"]))
	if err != nil {
		return err
	}
	if ev.RefSeq == 0 {
		ev.RefSeq = fromN(task.Item["ref_seq"])
	}
	now := nowUTC()
	items := []types.TransactWriteItem{del(b.table("wf_tasks"), key, "attribute_exists(task_pk)")}
	if inst.Status == "running" {
		seq, err := b.allocInboxSeqs(ctx, inst.ID, 1)
		if err != nil {
			return err
		}
		items = append(items, put(b.table("wf_inbox"), inboxItem(inst.ID, newID(), seq, ev, now), "attribute_not_exists(instance_id) AND attribute_not_exists(id)"))
	}
	_, err = b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if conditional(err) {
		return backend.ErrSuperseded
	}
	if err != nil {
		return err
	}
	if inst.Status == "running" {
		if err := b.ensureWorkflowTask(ctx, inst.ID); err != nil {
			return err
		}
		b.notifyTasks()
	}
	return nil
}

func (b *Backend) FireDueTimers(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 1
	}
	now := nowUTC()
	out, err := b.client.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(b.table("wf_timers")), IndexName: aws.String("fire_gsi"), KeyConditionExpression: aws.String("gsi_pk = :g AND fire_at <= :now"), ExpressionAttributeValues: map[string]types.AttributeValue{":g": avS("TIMER"), ":now": avN(timeToN(now))}, Limit: aws.Int32(int32(limit))})
	if err != nil {
		return 0, err
	}
	count := 0
	for _, timer := range out.Items {
		id, seq := fromS(timer["instance_id"]), fromN(timer["seq"])
		inst, err := b.GetInstance(ctx, id)
		if err != nil {
			continue
		}
		items := []types.TransactWriteItem{delWithValues(b.table("wf_timers"), timerKey(id, seq), "attribute_exists(instance_id)", nil)}
		if inst.Status == "running" {
			is, err := b.allocInboxSeqs(ctx, id, 1)
			if err != nil {
				continue
			}
			items = append(items, put(b.table("wf_inbox"), inboxItem(id, newID(), is, journal.Event{Type: journal.TypeTimerFired, RefSeq: seq}, now), "attribute_not_exists(instance_id) AND attribute_not_exists(id)"))
		}
		_, err = b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		if conditional(err) {
			continue
		}
		if err != nil {
			return count, err
		}
		if inst.Status != "running" {
			continue
		}
		count++
		if err := b.ensureWorkflowTask(ctx, id); err != nil {
			return count, err
		}
	}
	if count > 0 {
		b.notifyTasks()
	}
	return count, nil
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
	pending := append([]backend.InboxItem(nil), items...)
	wrote := false
	for len(pending) > 0 {
		now := nowUTC()
		top, err := b.allocInboxSeqs(ctx, instanceID, int64(len(pending)))
		if err != nil {
			return err
		}
		var twi []types.TransactWriteItem
		type meta struct {
			pidx   int
			dedupe bool
		}
		var metas []meta
		seq := top - int64(len(pending))
		// Skip same-batch DedupeID duplicates: TransactWriteItems rejects two
		// operations on the same key and would not ConditionalCheckFailed-retry cleanly.
		created := map[string]bool{}
		for pi, it := range pending {
			if it.DedupeID != "" {
				if created[it.DedupeID] {
					continue
				}
				created[it.DedupeID] = true
				twi = append(twi, put(b.table("wf_signal_dedupe"), map[string]types.AttributeValue{
					"instance_id": avS(instanceID),
					"dedupe_id":   avS(it.DedupeID),
					"created_at":  avN(timeToN(now)),
				}, "attribute_not_exists(instance_id) AND attribute_not_exists(dedupe_id)"))
				metas = append(metas, meta{pi, true})
			}
			seq++
			twi = append(twi, put(b.table("wf_inbox"), inboxItem(instanceID, newID(), seq, it.Event, now),
				"attribute_not_exists(instance_id) AND attribute_not_exists(id)"))
			metas = append(metas, meta{pi, false})
		}
		if len(twi) > dynamoTxnItemLimit {
			return backend.ErrBatchTooLarge
		}
		_, err = b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: twi})
		if err == nil {
			wrote = true
			break
		}
		var tce *types.TransactionCanceledException
		if !errors.As(err, &tce) {
			return err
		}
		skip := map[int]bool{}
		ok := true
		for i, r := range tce.CancellationReasons {
			if r.Code == nil || *r.Code == "None" {
				continue
			}
			if *r.Code != "ConditionalCheckFailed" || i >= len(metas) || !metas[i].dedupe {
				ok = false
				break
			}
			skip[metas[i].pidx] = true
		}
		if !ok || len(skip) == 0 {
			return err
		}
		next := make([]backend.InboxItem, 0, len(pending)-len(skip))
		for i, it := range pending {
			if !skip[i] {
				next = append(next, it)
			}
		}
		pending = next
	}
	if !wrote {
		// All items were dedupe hits: no new inbox, but a prior crash
		// between commit and ensure may have orphaned earlier inbox.
		// Ensure so a resend never permanently stalls the instance.
		if inst.Status == "running" {
			_ = b.ensureWorkflowTask(ctx, instanceID)
		}
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
// instances that hold inbox events but no workflow task. It closes the
// commit→ensure crash gap on inbox paths (activity completion, timers,
// signals: their follow-up ensure runs outside the transaction).
// Truncated fanout follow-ups need no recovery: they refresh the singleton
// task atomically inside the advancement transaction.
//
// The scan resumes from a persisted cursor on each pass and rotates through
// the fleet, so orphans beyond the per-call bound are eventually visited
// instead of starving behind the first page on every pass.
func (b *Backend) RecoverOrphanedWorkflowTasks(ctx context.Context) (int, error) {
	b.recoverMu.Lock()
	start := b.recoverCursor
	b.recoverMu.Unlock()
	recovered, next, exhausted, err := b.recoverOrphanedPass(ctx, b.client, start, recoverBound)
	if err != nil {
		return recovered, err
	}
	b.recoverMu.Lock()
	if exhausted {
		// Full fleet visited: restart from the beginning next pass.
		b.recoverCursor = nil
	} else {
		b.recoverCursor = next
	}
	b.recoverMu.Unlock()
	return recovered, nil
}

// recoverBound caps how many running instances a single recovery pass checks.
// Each check costs an inbox Query plus a task GetItem, so the bound keeps one
// pass from monopolizing DynamoDB when the fleet is large.
const recoverBound = 200

// recoverStore is the DynamoDB subset used by orphan recovery (seam for tests).
type recoverStore interface {
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// recoverTransactStore is the optional transact-capable extension of
// recoverStore: the production client implements TransactWriteItems, so
// orphan recovery can route its put through the same atomic
// status-conditioned transaction as ensureWorkflowTask (see
// putWorkflowTaskIfRunning). Test fakes that lack it fall back to a
// status re-check immediately before the Put (see recoverPutIfRunning).
type recoverTransactStore interface {
	recoverStore
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// recoverPutIfRunning recreates one orphaned workflow task only while its
// instance is still running (round-17 P2 on #291). The scan image may be
// stale — a terminal commit plus sweep can land between the scan and the put
// — and a bare PutItem would then recreate the workflow task after the
// cleanup, leaving a row that lingers unpolled (the claim gate still prevents
// execution, but nothing reaps it). When the store supports transactions the
// put rides the same atomic status-conditioned transaction as
// putWorkflowTaskIfRunning (a ConditionCheck on status="running" plus the Put
// guarded on attribute_not_exists); a terminal instance (or an existing
// singleton) aborts as success. Stores without transaction support re-read
// the instance status immediately before the Put and skip a terminal
// instance, narrowing the read-then-Put gap to the minimum a bare PutItem
// allows (same documented residual as putWorkflowTaskLegacy). It reports
// true when the put committed.
func (b *Backend) recoverPutIfRunning(ctx context.Context, store recoverStore, instanceID, queue string) (bool, error) {
	item := workflowTaskItem(instanceID, queue, newID(), nowUTC())
	if ts, ok := store.(recoverTransactStore); ok {
		_, err := ts.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
			{ConditionCheck: &types.ConditionCheck{
				TableName:           aws.String(b.table("wf_instances")),
				Key:                 map[string]types.AttributeValue{"id": avS(instanceID)},
				ConditionExpression: aws.String("#s = :running"),
				ExpressionAttributeNames: map[string]string{
					"#s": "status",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":running": avS("running"),
				},
			}},
			{Put: &types.Put{
				TableName:           aws.String(b.table("wf_tasks")),
				Item:                item,
				ConditionExpression: aws.String("attribute_not_exists(task_pk)"),
			}},
		}})
		if err == nil {
			return true, nil
		}
		if conditional(err) {
			return false, nil
		}
		if !isTransactionUnsupported(err) {
			return false, err
		}
		// Stores without TransactWriteItems support fall through to the
		// status re-check + Put below.
	}
	// Re-check the instance status immediately before the Put: the scan row
	// above may predate a terminal transition whose sweep already finished.
	// A missing instance reads as terminal (same as instanceRunning).
	got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(b.table("wf_instances")),
		Key:            map[string]types.AttributeValue{"id": avS(instanceID)},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return false, err
	}
	if len(got.Item) == 0 || fromS(got.Item["status"]) != "running" {
		return false, nil
	}
	if q := fromS(got.Item["queue"]); q != "" && q != queue {
		// Prefer the fresh queue for the recreated task over the
		// potentially stale scan image.
		item = workflowTaskItem(instanceID, q, newID(), nowUTC())
	}
	_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(b.table("wf_tasks")),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(task_pk)"),
	})
	if err != nil {
		if conditional(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// recoverOrphanedPass checks up to bound running instances starting from start
// and reports where the next pass should resume. exhausted is true only when
// the full table was visited (caller resets the cursor to nil).
//
// On bound overflow the returned cursor is the key of the last checked
// instance in the current page, so the next pass resumes mid-page. Saving the
// page's start key instead would re-scan the same bound instances forever
// when a single page holds more than bound running instances; saving
// LastEvaluatedKey instead would skip the unchecked tail of the page.
func (b *Backend) recoverOrphanedPass(ctx context.Context, store recoverStore, start map[string]types.AttributeValue, bound int) (recovered int, next map[string]types.AttributeValue, exhausted bool, err error) {
	checked := 0
	for {
		select {
		case <-ctx.Done():
			return recovered, nil, false, ctx.Err()
		default:
		}
		out, err := store.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(b.table("wf_instances")),
			ExclusiveStartKey: start,
		})
		if err != nil {
			return recovered, nil, false, err
		}
		var pageLast map[string]types.AttributeValue
		for _, m := range out.Items {
			if fromS(m["status"]) != "running" {
				continue
			}
			if checked >= bound {
				next := start
				if pageLast != nil {
					next = pageLast
				}
				return recovered, next, false, nil
			}
			checked++
			if idAv, ok := m["id"]; ok && idAv != nil {
				pageLast = map[string]types.AttributeValue{"id": idAv}
			}
			id := fromS(m["id"])
			inbox, err := store.Query(ctx, &dynamodb.QueryInput{
				TableName:                 aws.String(b.table("wf_inbox")),
				KeyConditionExpression:    aws.String("instance_id = :id"),
				ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(id)},
				Limit:                     aws.Int32(1),
			})
			if err != nil || len(inbox.Items) == 0 {
				continue
			}
			tout, err := store.GetItem(ctx, &dynamodb.GetItemInput{
				TableName: aws.String(b.table("wf_tasks")),
				Key:       map[string]types.AttributeValue{"task_pk": avS(wfTaskPK(id))},
			})
			if err != nil {
				continue
			}
			if len(tout.Item) != 0 {
				continue
			}
			inst := decodeInstance(m)
			// Gate the recreate on the live instance status (round-17 P2
			// on #291): the scan row above may predate a terminal commit
			// whose sweep already finished, and a bare PutItem would then
			// recreate the workflow task after the cleanup. The gated put
			// aborts on a terminal instance (or an existing singleton) as
			// success; only a committed put counts as recovered.
			ok, err := b.recoverPutIfRunning(ctx, store, id, inst.Queue)
			if err != nil {
				// A failed status re-read (throttling, transient) must not
				// fail the whole pass: skip this instance like the inbox
				// and task reads above do. It stays orphaned for the next
				// pass instead of aborting recovery for the fleet.
				continue
			}
			if ok {
				recovered++
			}
		}
		if out.LastEvaluatedKey == nil {
			return recovered, nil, true, nil
		}
		start = out.LastEvaluatedKey
	}
}

// deleteSignalDedupeForInstance pages through the instance's dedupe entries
// (Query results larger than 1 MB arrive in pages via LastEvaluatedKey) and
// removes each one. The read is strongly consistent so a dedupe key committed
// just before the terminal transition is not missed (same gap as the
// terminal inbox query).
func (b *Backend) deleteSignalDedupeForInstance(ctx context.Context, id string) error {
	var start map[string]types.AttributeValue
	for {
		out, err := b.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(b.table("wf_signal_dedupe")),
			KeyConditionExpression:    aws.String("instance_id = :id"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(id)},
			ConsistentRead:            aws.Bool(true),
			ExclusiveStartKey:         start,
		})
		if err != nil {
			return err
		}
		for _, m := range out.Items {
			if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(b.table("wf_signal_dedupe")),
				Key: map[string]types.AttributeValue{
					"instance_id": m["instance_id"],
					"dedupe_id":   m["dedupe_id"],
				},
			}); err != nil {
				return err
			}
		}
		if out.LastEvaluatedKey == nil {
			return nil
		}
		start = out.LastEvaluatedKey
	}
}

func put(table string, item map[string]types.AttributeValue, condition string) types.TransactWriteItem {
	p := &types.Put{TableName: aws.String(table), Item: item}
	if condition != "" {
		p.ConditionExpression = aws.String(condition)
	}
	return types.TransactWriteItem{Put: p}
}
func del(table string, key map[string]types.AttributeValue, condition string) types.TransactWriteItem {
	d := &types.Delete{TableName: aws.String(table), Key: key}
	if condition != "" {
		d.ConditionExpression = aws.String(condition)
	}
	return types.TransactWriteItem{Delete: d}
}
func delWithValues(table string, key map[string]types.AttributeValue, condition string, values map[string]types.AttributeValue) types.TransactWriteItem {
	return types.TransactWriteItem{Delete: &types.Delete{TableName: aws.String(table), Key: key, ConditionExpression: aws.String(condition), ExpressionAttributeValues: values}}
}
func inboxKey(instanceID string, id int64) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"instance_id": avS(instanceID), "id": avN(id)}
}

// lessInstance orders ListInstances results by (created_at, id), mirroring
// SQL backends. Extracted for unit testing the sort contract.
func lessInstance(aCreated int64, aID string, bCreated int64, bID string) bool {
	if aCreated != bCreated {
		return aCreated < bCreated
	}
	return aID < bID
}
func timerKey(instanceID string, seq int64) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"instance_id": avS(instanceID), "seq": avN(seq)}
}
func conditional(err error) bool {
	var c *types.ConditionalCheckFailedException
	if errors.As(err, &c) {
		return true
	}
	var t *types.TransactionCanceledException
	if errors.As(err, &t) {
		for _, r := range t.CancellationReasons {
			if r.Code != nil && (*r.Code == "ConditionalCheckFailed" || *r.Code == "TransactionConflict") {
				return true
			}
		}
	}
	return false
}
