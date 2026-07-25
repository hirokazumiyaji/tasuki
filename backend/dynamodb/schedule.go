package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func (b *Backend) UpsertSchedule(ctx context.Context, s backend.NewSchedule) error {
	if s.ID == "" || s.Cron == "" || s.Workflow == "" {
		return fmt.Errorf("schedule id, cron, and workflow are required")
	}
	queue := s.Queue
	if queue == "" {
		queue = "default"
	}
	now := nowUTC()
	next, err := backend.NextCronTime(s.Cron, now)
	if err != nil {
		return err
	}
	_, err = b.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(b.table("wf_schedules")),
		Item: map[string]types.AttributeValue{
			"id": avS(s.ID), "cron": avS(s.Cron), "workflow": avS(s.Workflow), "queue": avS(queue),
			"input": avJSON(s.Input), "gsi_pk": avS("SCHED"), "next_run_at": avN(timeToN(next)),
			"paused": avBOOL(s.Paused), "created_at": avN(timeToN(now)), "updated_at": avN(timeToN(now)),
		},
	})
	return err
}

func (b *Backend) GetSchedule(ctx context.Context, id string) (*backend.Schedule, error) {
	out, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(b.table("wf_schedules")), Key: map[string]types.AttributeValue{"id": avS(id)}, ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, backend.ErrNotFound
	}
	return decodeSchedule(out.Item), nil
}

func decodeSchedule(m map[string]types.AttributeValue) *backend.Schedule {
	return &backend.Schedule{
		ID: fromS(m["id"]), Cron: fromS(m["cron"]), Workflow: fromS(m["workflow"]), Queue: fromS(m["queue"]),
		Input: fromJSON(m["input"]), NextRunAt: nToTime(fromN(m["next_run_at"])), Paused: fromBOOL(m["paused"]),
	}
}

func (b *Backend) PauseSchedule(ctx context.Context, id string, paused bool) error {
	_, err := b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(b.table("wf_schedules")), Key: map[string]types.AttributeValue{"id": avS(id)},
		UpdateExpression:          aws.String("SET paused = :paused, updated_at = :now"),
		ConditionExpression:       aws.String("attribute_exists(id)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":paused": avBOOL(paused), ":now": avN(timeToN(nowUTC()))},
	})
	if conditional(err) {
		return backend.ErrNotFound
	}
	return err
}

func (b *Backend) ClaimDueSchedules(ctx context.Context, limit int) ([]backend.DueSchedule, error) {
	if limit <= 0 {
		limit = 1
	}
	now := nowUTC()
	out, err := b.client.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(b.table("wf_schedules")), IndexName: aws.String("due_gsi"),
		KeyConditionExpression: aws.String("gsi_pk = :g AND next_run_at <= :now"),
		FilterExpression:       aws.String("paused = :false"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":g": avS("SCHED"), ":now": avN(timeToN(now)), ":false": avBOOL(false),
		},
		Limit: aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	result := make([]backend.DueSchedule, 0, len(out.Items))
	for _, item := range out.Items {
		s := decodeSchedule(item)
		scheduledAt := s.NextRunAt
		next, err := backend.NextCronTime(s.Cron, scheduledAt)
		if err != nil {
			return nil, err
		}
		instanceID := backend.ScheduleInstanceID(s.ID, scheduledAt)
		inst := backend.NewInstance{ID: instanceID, Name: s.Workflow, Queue: s.Queue, Input: s.Input}
		items := []types.TransactWriteItem{
			{Update: &types.Update{
				TableName: aws.String(b.table("wf_schedules")), Key: map[string]types.AttributeValue{"id": avS(s.ID)},
				UpdateExpression:    aws.String("SET next_run_at = :next, updated_at = :now"),
				ConditionExpression: aws.String("next_run_at = :old AND paused = :false"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":next": avN(timeToN(next)), ":now": avN(timeToN(now)), ":old": avN(timeToN(scheduledAt)), ":false": avBOOL(false),
				},
			}},
			put(b.table("wf_instances"), instanceItem(inst, s.Queue, now), "attribute_not_exists(id)"),
			put(b.table("wf_journal"), journalItem(instanceID, 1, journal.Event{Type: journal.TypeWorkflowStarted, Name: s.Workflow, Payload: s.Input}, now), "attribute_not_exists(instance_id) AND attribute_not_exists(seq)"),
			put(b.table("wf_tasks"), workflowTaskItem(instanceID, s.Queue, newID(), now), "attribute_not_exists(task_pk)"),
		}
		_, err = b.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		if err == nil {
			result = append(result, backend.DueSchedule{Schedule: backend.Schedule{ID: s.ID, Cron: s.Cron, Workflow: s.Workflow, Queue: s.Queue, Input: s.Input, NextRunAt: next, Paused: false}, InstanceID: instanceID, ScheduledAt: scheduledAt, Created: true})
			continue
		}
		// An existing deterministic instance is a successful deduplicated fire.
		if conditional(err) {
			current, getErr := b.GetSchedule(ctx, s.ID)
			if getErr != nil || current.NextRunAt.Equal(scheduledAt) {
				continue
			}
			if _, getErr = b.GetInstance(ctx, instanceID); getErr == nil {
				result = append(result, backend.DueSchedule{Schedule: *current, InstanceID: instanceID, ScheduledAt: scheduledAt, Created: false})
				continue
			}
			continue
		}
		return result, err
	}
	if len(result) > 0 {
		b.notifyTasks()
	}
	return result, nil
}
