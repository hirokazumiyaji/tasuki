package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{MaxAdvancementEffects: 80}
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
	out, err := b.client.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(b.table("wf_journal")),
		KeyConditionExpression:    aws.String("instance_id = :id AND seq > :after"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(id), ":after": avN(afterSeq)}, ScanIndexForward: aws.Bool(true)})
	if err != nil {
		return nil, err
	}
	events := make([]journal.Event, 0, len(out.Items))
	for _, m := range out.Items {
		events = append(events, journal.Event{Seq: fromN(m["seq"]), Type: journal.Type(fromS(m["type"])), Name: fromS(m["name"]), RefSeq: fromN(m["ref_seq"]), Payload: fromJSON(m["payload"])})
	}
	return events, nil
}

func (b *Backend) ListInstances(ctx context.Context, f backend.InstanceFilter) ([]backend.Instance, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	var items []map[string]types.AttributeValue
	var start map[string]types.AttributeValue
	for {
		out, err := b.client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(b.table("wf_instances")), ExclusiveStartKey: start})
		if err != nil {
			return nil, err
		}
		for _, m := range out.Items {
			if (f.Status == "" || fromS(m["status"]) == f.Status) && (f.Name == "" || fromS(m["name"]) == f.Name) {
				inst := decodeInstance(m)
				if backend.MatchesSearchAttributes(inst.SearchAttributes, f.SearchAttributes) {
					items = append(items, m)
				}
			}
		}
		if out.LastEvaluatedKey == nil {
			break
		}
		start = out.LastEvaluatedKey
	}
	// Stable order mirrors SQL backends; timestamps are numeric and IDs break ties.
	for i := range items {
		for j := i + 1; j < len(items); j++ {
			if fromN(items[j]["created_at"]) < fromN(items[i]["created_at"]) || (fromN(items[j]["created_at"]) == fromN(items[i]["created_at"]) && fromS(items[j]["id"]) < fromS(items[i]["id"])) {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
	if f.Offset >= len(items) {
		return nil, nil
	}
	items = items[f.Offset:]
	if len(items) > limit {
		items = items[:limit]
	}
	result := make([]backend.Instance, 0, len(items))
	for _, m := range items {
		result = append(result, *decodeInstance(m))
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
	if err := b.deleteTasksForInstance(ctx, id); err != nil {
		return err
	}
	if err := b.deleteTimersForInstance(ctx, id); err != nil {
		return err
	}
	if err := b.deleteSignalDedupeForInstance(ctx, id); err != nil {
		return err
	}
	b.notifyTerminal(id)
	return nil
}

func (b *Backend) deleteTasksForInstance(ctx context.Context, id string) error {
	var start map[string]types.AttributeValue
	for {
		out, err := b.client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(b.table("wf_tasks")), ExclusiveStartKey: start})
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

func (b *Backend) deleteTimersForInstance(ctx context.Context, id string) error {
	out, err := b.client.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(b.table("wf_timers")), KeyConditionExpression: aws.String("instance_id = :id"), ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(id)}})
	if err != nil {
		return err
	}
	for _, m := range out.Items {
		if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(b.table("wf_timers")), Key: timerKey(id, fromN(m["seq"]))}); err != nil {
			return err
		}
	}
	return nil
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
	for _, queue := range req.Queues {
		if len(result) >= req.Limit {
			break
		}
		out, err := b.client.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(b.table("wf_tasks")), IndexName: aws.String("claim_gsi"),
			KeyConditionExpression: aws.String("gsi_pk = :g AND visible_at <= :now"), ExpressionAttributeValues: map[string]types.AttributeValue{":g": avS(claimGSI(req.Kind, queue)), ":now": avN(timeToN(now))},
			Limit: aws.Int32(int32(req.Limit - len(result)))})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			old := fromN(item["visible_at"])
			updated, err := b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(b.table("wf_tasks")), Key: map[string]types.AttributeValue{"task_pk": item["task_pk"]},
				UpdateExpression: aws.String("SET visible_at = :v, worker_id = :w ADD attempt :one"), ConditionExpression: aws.String("visible_at = :old"),
				ExpressionAttributeValues: map[string]types.AttributeValue{":v": avN(timeToN(visible)), ":w": avS(req.WorkerID), ":one": avN(1), ":old": avN(old)}, ReturnValues: types.ReturnValueAllNew})
			if conditional(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			result = append(result, decodeTask(updated.Attributes))
		}
	}
	return result, nil
}

func decodeTask(m map[string]types.AttributeValue) backend.Task {
	t := backend.Task{ID: fromN(m["id"]), Kind: fromS(m["kind"]), Queue: fromS(m["queue"]), InstanceID: fromS(m["instance_id"]), Seq: fromN(m["ref_seq"]), Attempt: int(fromN(m["attempt"])), MaxAttempts: int(fromN(m["max_attempts"])), VisibleAt: nToTime(fromN(m["visible_at"])), WorkerID: fromS(m["worker_id"]), HeartbeatDetails: fromJSON(m["heartbeat"])}
	if t.Kind == "activity" {
		var p activityPayload
		_ = json.Unmarshal(fromJSON(m["payload"]), &p)
		t.Name, t.Input, t.MaxAttempts = p.Name, p.Input, p.Retry.MaxAttempts
		t.Retry = backend.RetryPolicy{InitialInterval: time.Duration(p.Retry.InitialIntervalMs) * time.Millisecond, BackoffCoefficient: p.Retry.BackoffCoefficient, MaxInterval: time.Duration(p.Retry.MaxIntervalMs) * time.Millisecond, MaxAttempts: p.Retry.MaxAttempts}
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
func (b *Backend) ReleaseLease(ctx context.Context, taskID int64) error {
	if err := b.updateTask(ctx, taskID, "SET visible_at = :v REMOVE worker_id", map[string]types.AttributeValue{":v": avN(timeToN(nowUTC()))}, ""); err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}
func (b *Backend) NackTask(ctx context.Context, t backend.Task, visibleAt time.Time) error {
	pk := actTaskPK(t.ID)
	if t.Kind == "workflow" {
		pk = wfTaskPK(t.InstanceID)
	}
	_, err := b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(b.table("wf_tasks")),
		Key:       map[string]types.AttributeValue{"task_pk": avS(pk)},
		UpdateExpression: aws.String("SET visible_at = :v REMOVE worker_id"),
		ConditionExpression: aws.String("attribute_exists(task_pk)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": avN(timeToN(visibleAt.UTC()))},
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
func (b *Backend) RetryActivity(ctx context.Context, taskID int64, at time.Time) error {
	return b.updateTask(ctx, taskID, "SET visible_at = :v REMOVE worker_id", map[string]types.AttributeValue{":v": avN(timeToN(at))}, "kind = :kind")
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
	for _, m := range out.Items {
		name, payload := unwrapInboxPayload(fromJSON(m["payload"]))
		st.Inbox = append(st.Inbox, backend.InboxEvent{ID: fromN(m["id"]), Event: journal.Event{Type: journal.Type(fromS(m["type"])), Name: name, RefSeq: fromN(m["ref_seq"]), Payload: payload}})
	}
	return st, nil
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
	if len(advs) == 1 {
		if err := b.commitAdvancementOnce(ctx, advs[0]); err != nil {
			return err
		}
		b.notifyAfterAdvancements(advs)
		return nil
	}
	var all []types.TransactWriteItem
	var ensures []string
	var parentEnsures []string
	for _, adv := range advs {
		items, parentID, err := b.buildAdvancementItems(ctx, adv)
		if err != nil {
			return err
		}
		if len(all)+len(items) > 100 {
			for _, a := range advs {
				if err := b.commitAdvancementOnce(ctx, a); err != nil {
					return err
				}
			}
			b.notifyAfterAdvancements(advs)
			return nil
		}
		all = append(all, items...)
		ensures = append(ensures, adv.InstanceID)
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
	for _, id := range parentEnsures {
		if err := b.ensureWorkflowTask(ctx, id); err != nil {
			return err
		}
	}
	for _, id := range ensures {
		if err := b.ensureWorkflowTask(ctx, id); err != nil {
			return err
		}
	}
	b.notifyAfterAdvancements(advs)
	return nil
}

func (b *Backend) notifyAfterAdvancements(advs []backend.Advancement) {
	b.notifyTasks()
	for _, adv := range advs {
		if adv.Terminal != nil {
			_ = b.deleteSignalDedupeForInstance(context.Background(), adv.InstanceID)
			b.notifyTerminal(adv.InstanceID)
		}
	}
}

func (b *Backend) commitAdvancementOnce(ctx context.Context, adv backend.Advancement) error {
	items, parentID, err := b.buildAdvancementItems(ctx, adv)
	if err != nil {
		return err
	}
	if len(items) > 100 {
		return fmt.Errorf("dynamodb: advancement produces %d transaction operations", len(items))
	}
	_, err = b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if conditional(err) {
		return backend.ErrConflict
	}
	if err != nil {
		return err
	}
	if parentID != "" {
		if err := b.ensureWorkflowTask(ctx, parentID); err != nil {
			return err
		}
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
		items = append(items, put(b.table("wf_tasks"), activityTaskItem(at, now), "attribute_not_exists(task_pk)"))
	}
	for _, tm := range adv.Timers {
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
		ev := *adv.ParentNotify
		if ev.RefSeq == 0 {
			ev.RefSeq = inst.ParentSeq
		}
		items = append(items, put(b.table("wf_inbox"), inboxItem(inst.ParentID, newID(), ev, now), "attribute_not_exists(instance_id) AND attribute_not_exists(id)"))
		parentID = inst.ParentID
	}
	items = append(items, delWithValues(b.table("wf_tasks"), map[string]types.AttributeValue{"task_pk": avS(wfTaskPK(adv.InstanceID))}, "id = :taskid AND kind = :workflow", map[string]types.AttributeValue{":taskid": avN(adv.TaskID), ":workflow": avS("workflow")}))
	return items, parentID, nil
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
	p, _ := json.Marshal(activityPayload{Name: t.Name, Input: t.Input, Retry: retryJSON{InitialIntervalMs: t.Retry.InitialInterval.Milliseconds(), BackoffCoefficient: t.Retry.BackoffCoefficient, MaxIntervalMs: t.Retry.MaxInterval.Milliseconds(), MaxAttempts: t.MaxAttempts}})
	return map[string]types.AttributeValue{"task_pk": avS(actTaskPK(id)), "id": avN(id), "kind": avS("activity"), "queue": avS(q), "gsi_pk": avS(claimGSI("activity", q)), "instance_id": avS(t.InstanceID), "ref_seq": avN(t.Seq), "payload": avJSON(p), "attempt": avN(0), "max_attempts": avN(int64(t.MaxAttempts)), "visible_at": avN(timeToN(now)), "created_at": avN(timeToN(now))}
}
func timerItem(instanceID string, t backend.NewTimer, now time.Time) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"instance_id": avS(instanceID), "seq": avN(t.Seq), "gsi_pk": avS("TIMER"), "fire_at": avN(timeToN(t.FireAt)), "created_at": avN(timeToN(now))}
}
func inboxItem(instanceID string, id int64, e journal.Event, now time.Time) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{"instance_id": avS(instanceID), "id": avN(id), "type": avS(string(e.Type)), "payload": avJSON(inboxPayload(e)), "created_at": avN(timeToN(now))}
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
	_, err = b.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(b.table("wf_tasks")), Item: workflowTaskItem(instanceID, inst.Queue, newID(), nowUTC()), ConditionExpression: aws.String("attribute_not_exists(task_pk)")})
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
		items = append(items, put(b.table("wf_inbox"), inboxItem(inst.ID, newID(), ev, now), "attribute_not_exists(instance_id) AND attribute_not_exists(id)"))
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
			items = append(items, put(b.table("wf_inbox"), inboxItem(id, newID(), journal.Event{Type: journal.TypeTimerFired, RefSeq: seq}, now), "attribute_not_exists(instance_id) AND attribute_not_exists(id)"))
		}
		_, err = b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		if conditional(err) {
			continue
		}
		if err != nil {
			return count, err
		}
		count++
		if inst.Status == "running" {
			if err := b.ensureWorkflowTask(ctx, id); err != nil {
				return count, err
			}
		}
	}
	if count > 0 {
		b.notifyTasks()
	}
	return count, nil
}

func (b *Backend) SendToInbox(ctx context.Context, instanceID string, ev journal.Event, dedupeID string) error {
	inst, err := b.GetInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	now := nowUTC()
	inbox := inboxItem(instanceID, newID(), ev, now)
	if dedupeID != "" {
		items := []types.TransactWriteItem{
			put(b.table("wf_signal_dedupe"), map[string]types.AttributeValue{
				"instance_id": avS(instanceID),
				"dedupe_id":   avS(dedupeID),
				"created_at":  avN(timeToN(now)),
			}, "attribute_not_exists(instance_id) AND attribute_not_exists(dedupe_id)"),
			put(b.table("wf_inbox"), inbox, "attribute_not_exists(instance_id) AND attribute_not_exists(id)"),
		}
		_, err = b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		if err != nil {
			if isDedupeConflict(err) {
				return nil
			}
			return err
		}
	} else {
		_, err = b.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:           aws.String(b.table("wf_inbox")),
			Item:                inbox,
			ConditionExpression: aws.String("attribute_not_exists(instance_id) AND attribute_not_exists(id)"),
		})
		if err != nil {
			return err
		}
	}
	if inst.Status == "running" {
		if err := b.ensureWorkflowTask(ctx, instanceID); err != nil {
			return err
		}
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) deleteSignalDedupeForInstance(ctx context.Context, id string) error {
	out, err := b.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(b.table("wf_signal_dedupe")),
		KeyConditionExpression: aws.String("instance_id = :id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id": avS(id),
		},
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
	return nil
}

func isDedupeConflict(err error) bool {
	var t *types.TransactionCanceledException
	if !errors.As(err, &t) || len(t.CancellationReasons) == 0 {
		return false
	}
	r := t.CancellationReasons[0]
	return r.Code != nil && *r.Code == "ConditionalCheckFailed"
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
