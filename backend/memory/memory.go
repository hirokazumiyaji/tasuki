package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/hub"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// Backend is an in-memory store for M0 tests and local demos.
type Backend struct {
	mu sync.Mutex

	now       time.Time
	nextTask  int64
	nextInbox int64

	instances    map[string]*instance
	tasks        map[int64]*task
	timers       map[timerKey]*timer
	inbox        map[string][]*inboxItem // instanceID → ordered
	signalDedupe map[string]map[string]struct{} // instanceID → dedupeID
	schedules    map[string]*schedule

	hub *hub.Hub
}

type instance struct {
	id               string
	name             string
	queue            string
	status           string
	input            []byte
	result           []byte
	failure          []byte
	nextSeq          int64
	journal          []journal.Event
	parentID         string
	parentSeq        int64
	searchAttributes map[string]string
	memo             map[string]string
}

type task struct {
	id                  int64
	kind                string
	queue               string
	instanceID          string
	name                string
	seq                 int64
	input               []byte
	attempt             int
	maxAttempts         int
	retry               backend.RetryPolicy
	startToCloseTimeout time.Duration
	visibleAt           time.Time
	workerID            string
	heartbeat           []byte
}

type timerKey struct {
	instanceID string
	seq        int64
}

type timer struct {
	instanceID string
	seq        int64
	fireAt     time.Time
}

type inboxItem struct {
	id    int64
	event journal.Event
}

func New() *Backend {
	return &Backend{
		now:          time.Now().UTC(),
		instances:    map[string]*instance{},
		tasks:        map[int64]*task{},
		timers:       map[timerKey]*timer{},
		inbox:        map[string][]*inboxItem{},
		signalDedupe: map[string]map[string]struct{}{},
		schedules:    map[string]*schedule{},
		hub:          hub.New(),
	}
}

func (b *Backend) Migrate(context.Context) error { return nil }

func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{}
}

func (b *Backend) SetNow(t time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.now = t.UTC()
}

func (b *Backend) Now() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.now
}

func (b *Backend) CreateInstance(_ context.Context, inst backend.NewInstance) error {
	b.mu.Lock()
	err := b.createInstanceLocked(inst)
	b.mu.Unlock()
	if err != nil {
		return err
	}
	b.notifyTasks()
	return nil
}

func (b *Backend) GetInstance(_ context.Context, id string) (*backend.Instance, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	inst, ok := b.instances[id]
	if !ok {
		return nil, backend.ErrNotFound
	}
	return &backend.Instance{
		ID:               inst.id,
		Name:             inst.name,
		Queue:            inst.queue,
		Status:           inst.status,
		Input:            append([]byte(nil), inst.input...),
		Result:           append([]byte(nil), inst.result...),
		Failure:          append([]byte(nil), inst.failure...),
		NextSeq:          inst.nextSeq,
		ParentID:         inst.parentID,
		ParentSeq:        inst.parentSeq,
		SearchAttributes: backend.CloneSearchAttributes(inst.searchAttributes),
		Memo:             backend.CloneSearchAttributes(inst.memo),
	}, nil
}

func (b *Backend) GetJournal(_ context.Context, id string, afterSeq int64) ([]journal.Event, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	inst, ok := b.instances[id]
	if !ok {
		return nil, backend.ErrNotFound
	}
	out := make([]journal.Event, 0)
	for _, e := range inst.journal {
		if e.Seq > afterSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

func (b *Backend) ListInstances(_ context.Context, f backend.InstanceFilter) ([]backend.Instance, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	ids := make([]string, 0, len(b.instances))
	for id, inst := range b.instances {
		if f.Status != "" && inst.status != f.Status {
			continue
		}
		if f.Name != "" && inst.name != f.Name {
			continue
		}
		if !backend.MatchesSearchAttributes(inst.searchAttributes, f.SearchAttributes) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if f.Offset >= len(ids) {
		return nil, nil
	}
	ids = ids[f.Offset:]
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]backend.Instance, 0, len(ids))
	for _, id := range ids {
		inst := b.instances[id]
		out = append(out, backend.Instance{
			ID: inst.id, Name: inst.name, Queue: inst.queue, Status: inst.status,
			Input: append([]byte(nil), inst.input...), Result: append([]byte(nil), inst.result...),
			Failure: append([]byte(nil), inst.failure...), NextSeq: inst.nextSeq,
			ParentID: inst.parentID, ParentSeq: inst.parentSeq,
			SearchAttributes: backend.CloneSearchAttributes(inst.searchAttributes),
			Memo:             backend.CloneSearchAttributes(inst.memo),
		})
	}
	return out, nil
}

func (b *Backend) TerminateInstance(_ context.Context, id string) error {
	b.mu.Lock()
	inst, ok := b.instances[id]
	if !ok {
		b.mu.Unlock()
		return backend.ErrNotFound
	}
	inst.status = "terminated"
	delete(b.signalDedupe, id)
	for tid, t := range b.tasks {
		if t.instanceID == id {
			delete(b.tasks, tid)
		}
	}
	for k := range b.timers {
		if k.instanceID == id {
			delete(b.timers, k)
		}
	}
	b.mu.Unlock()
	b.notifyTerminal(id)
	return nil
}

func (b *Backend) ExtendLease(_ context.Context, taskID int64, d time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.tasks[taskID]
	if !ok {
		return backend.ErrNotFound
	}
	t.visibleAt = b.now.Add(d)
	return nil
}

func (b *Backend) RecordHeartbeat(_ context.Context, taskID int64, lease time.Duration, details []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.tasks[taskID]
	if !ok {
		return backend.ErrNotFound
	}
	t.visibleAt = b.now.Add(lease)
	if details != nil {
		t.heartbeat = append([]byte(nil), details...)
	}
	return nil
}

func (b *Backend) ReleaseLease(_ context.Context, taskID int64) error {
	b.mu.Lock()
	t, ok := b.tasks[taskID]
	if !ok {
		b.mu.Unlock()
		return backend.ErrNotFound
	}
	t.visibleAt = b.now
	t.workerID = ""
	b.mu.Unlock()
	b.notifyTasks()
	return nil
}

func (b *Backend) NackTask(_ context.Context, task backend.Task, visibleAt time.Time) error {
	b.mu.Lock()
	t, ok := b.tasks[task.ID]
	if !ok {
		b.mu.Unlock()
		return backend.ErrNotFound
	}
	t.visibleAt = visibleAt.UTC()
	t.workerID = ""
	b.mu.Unlock()
	b.notifyTasks()
	return nil
}

func (b *Backend) RetryActivity(ctx context.Context, taskID int64, visibleAt time.Time) error {
	b.mu.Lock()
	t, ok := b.tasks[taskID]
	if !ok || t.kind != "activity" {
		b.mu.Unlock()
		return backend.ErrNotFound
	}
	b.mu.Unlock()
	return b.NackTask(ctx, backend.Task{ID: taskID, Kind: "activity"}, visibleAt)
}

func (b *Backend) LoadWorkflowHead(_ context.Context, instanceID string) (*backend.WorkflowState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.loadWorkflowHeadLocked(instanceID)
}

func (b *Backend) loadWorkflowHeadLocked(instanceID string) (*backend.WorkflowState, error) {
	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, backend.ErrNotFound
	}
	inbox := make([]backend.InboxEvent, 0, len(b.inbox[instanceID]))
	for _, item := range b.inbox[instanceID] {
		inbox = append(inbox, backend.InboxEvent{ID: item.id, Event: item.event})
	}
	return &backend.WorkflowState{
		Instance: backend.Instance{
			ID:               inst.id,
			Name:             inst.name,
			Queue:            inst.queue,
			Status:           inst.status,
			Input:            append([]byte(nil), inst.input...),
			Result:           append([]byte(nil), inst.result...),
			Failure:          append([]byte(nil), inst.failure...),
			NextSeq:          inst.nextSeq,
			ParentID:         inst.parentID,
			ParentSeq:        inst.parentSeq,
			SearchAttributes: backend.CloneSearchAttributes(inst.searchAttributes),
			Memo:             backend.CloneSearchAttributes(inst.memo),
		},
		Inbox:   inbox,
		NextSeq: inst.nextSeq,
		Now:     b.now,
	}, nil
}

func (b *Backend) LoadWorkflow(_ context.Context, instanceID string) (*backend.WorkflowState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, err := b.loadWorkflowHeadLocked(instanceID)
	if err != nil {
		return nil, err
	}
	st.Journal = append([]journal.Event(nil), b.instances[instanceID].journal...)
	return st, nil
}

func (b *Backend) CountClaimableTasks(_ context.Context, kind string, queues []string) (map[string]int64, error) {
	if len(queues) == 0 {
		return map[string]int64{}, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	queueSet := map[string]struct{}{}
	for _, q := range queues {
		queueSet[q] = struct{}{}
	}
	out := map[string]int64{}
	for _, t := range b.tasks {
		if t.kind != kind {
			continue
		}
		if _, ok := queueSet[t.queue]; !ok {
			continue
		}
		if t.visibleAt.After(b.now) {
			continue
		}
		out[t.queue]++
	}
	return out, nil
}

func (b *Backend) ClaimTasks(_ context.Context, req backend.ClaimRequest) ([]backend.Task, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	queueSet := map[string]struct{}{}
	for _, q := range req.Queues {
		queueSet[q] = struct{}{}
	}
	type cand struct {
		id int64
		t  *task
	}
	var cands []cand
	for id, t := range b.tasks {
		if t.kind != req.Kind {
			continue
		}
		if _, ok := queueSet[t.queue]; !ok {
			continue
		}
		if t.visibleAt.After(b.now) {
			continue
		}
		cands = append(cands, cand{id: id, t: t})
	}
	// stable-ish: pick by lowest id
	for i := 0; i < len(cands); i++ {
		for j := i + 1; j < len(cands); j++ {
			if cands[j].id < cands[i].id {
				cands[i], cands[j] = cands[j], cands[i]
			}
		}
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 1
	}
	if len(cands) > limit {
		cands = cands[:limit]
	}
	out := make([]backend.Task, 0, len(cands))
	for _, c := range cands {
		c.t.visibleAt = b.now.Add(req.Lease)
		c.t.attempt++
		c.t.workerID = req.WorkerID
		out = append(out, toTask(c.t))
	}
	return out, nil
}

func (b *Backend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	return b.CommitAdvancements(ctx, []backend.Advancement{adv})
}

func (b *Backend) CommitAdvancements(_ context.Context, advs []backend.Advancement) error {
	if len(advs) == 0 {
		return nil
	}
	b.mu.Lock()
	for _, adv := range advs {
		if err := b.preflightAdvancementLocked(adv); err != nil {
			b.mu.Unlock()
			return err
		}
	}
	var terminals []string
	for _, adv := range advs {
		if err := b.commitAdvancementLocked(adv); err != nil {
			b.mu.Unlock()
			return err
		}
		if adv.Terminal != nil {
			terminals = append(terminals, adv.InstanceID)
		}
	}
	b.mu.Unlock()
	b.notifyTasks()
	for _, id := range terminals {
		b.notifyTerminal(id)
	}
	return nil
}

func (b *Backend) preflightAdvancementLocked(adv backend.Advancement) error {
	inst, ok := b.instances[adv.InstanceID]
	if !ok {
		return backend.ErrNotFound
	}
	if inst.nextSeq != adv.ExpectedSeq {
		return backend.ErrConflict
	}
	own, ok := b.tasks[adv.TaskID]
	if !ok || own.instanceID != adv.InstanceID || own.kind != "workflow" {
		return backend.ErrConflict
	}
	return nil
}

func (b *Backend) commitAdvancementLocked(adv backend.Advancement) error {
	inst, ok := b.instances[adv.InstanceID]
	if !ok {
		return backend.ErrNotFound
	}
	if inst.nextSeq != adv.ExpectedSeq {
		return backend.ErrConflict
	}
	own, ok := b.tasks[adv.TaskID]
	if !ok || own.instanceID != adv.InstanceID || own.kind != "workflow" {
		return backend.ErrConflict
	}

	// Apply drained inbox
	if len(adv.DrainedInbox) > 0 {
		drain := map[int64]struct{}{}
		for _, id := range adv.DrainedInbox {
			drain[id] = struct{}{}
		}
		kept := b.inbox[adv.InstanceID][:0]
		for _, item := range b.inbox[adv.InstanceID] {
			if _, drop := drain[item.id]; drop {
				continue
			}
			kept = append(kept, item)
		}
		b.inbox[adv.InstanceID] = kept
	}

	for _, ev := range adv.NewEvents {
		inst.journal = append(inst.journal, ev)
		if ev.Seq+1 > inst.nextSeq {
			inst.nextSeq = ev.Seq + 1
		}
	}
	if len(adv.NewEvents) > 0 {
		last := adv.NewEvents[len(adv.NewEvents)-1]
		if last.Seq >= inst.nextSeq {
			inst.nextSeq = last.Seq + 1
		}
	}
	if updated := backend.LastSearchAttributesUpdate(adv.NewEvents); updated != nil || backend.HasSearchAttributesUpdate(adv.NewEvents) {
		inst.searchAttributes = updated
	}
	if updated := backend.LastMemoUpdate(adv.NewEvents); updated != nil || backend.HasMemoUpdate(adv.NewEvents) {
		inst.memo = updated
	}

	for _, at := range adv.ActivityTasks {
		b.nextTask++
		b.tasks[b.nextTask] = &task{
			id:                  b.nextTask,
			kind:                "activity",
			queue:               at.Queue,
			instanceID:          at.InstanceID,
			name:                at.Name,
			seq:                 at.Seq,
			input:               append([]byte(nil), at.Input...),
			maxAttempts:         at.MaxAttempts,
			retry:               at.Retry,
			startToCloseTimeout: at.StartToCloseTimeout,
			visibleAt:           b.now,
		}
	}
	for _, tm := range adv.Timers {
		b.timers[timerKey{instanceID: adv.InstanceID, seq: tm.Seq}] = &timer{
			instanceID: adv.InstanceID,
			seq:        tm.Seq,
			fireAt:     tm.FireAt.UTC(),
		}
	}
	if adv.Terminal != nil {
		inst.status = adv.Terminal.Status
		inst.result = append([]byte(nil), adv.Terminal.Result...)
		inst.failure = append([]byte(nil), adv.Terminal.Failure...)
		delete(b.signalDedupe, adv.InstanceID)
	}
	for _, ch := range adv.Children {
		if err := b.createInstanceLocked(ch); err != nil {
			return err
		}
	}
	if adv.ParentNotify != nil && inst.parentID != "" {
		b.nextInbox++
		b.inbox[inst.parentID] = append(b.inbox[inst.parentID], &inboxItem{id: b.nextInbox, event: *adv.ParentNotify})
		if p, ok := b.instances[inst.parentID]; ok && p.status == "running" {
			b.enqueueWorkflowTaskLocked(inst.parentID, p.queue)
		}
	}

	delete(b.tasks, adv.TaskID)

	// Ensure workflow task if inbox remains and still running
	if inst.status == "running" && len(b.inbox[adv.InstanceID]) > 0 {
		b.enqueueWorkflowTaskLocked(adv.InstanceID, inst.queue)
	}
	return nil
}

func (b *Backend) CompleteActivity(_ context.Context, taskID int64, ev journal.Event) error {
	b.mu.Lock()
	t, ok := b.tasks[taskID]
	if !ok || t.kind != "activity" {
		b.mu.Unlock()
		return backend.ErrSuperseded
	}
	inst, ok := b.instances[t.instanceID]
	if !ok {
		b.mu.Unlock()
		return backend.ErrNotFound
	}
	delete(b.tasks, taskID)
	if inst.status != "running" {
		// Terminated/completed instances ignore late completions.
		b.mu.Unlock()
		return nil
	}
	ev.RefSeq = t.seq
	b.nextInbox++
	b.inbox[t.instanceID] = append(b.inbox[t.instanceID], &inboxItem{id: b.nextInbox, event: ev})
	b.enqueueWorkflowTaskLocked(t.instanceID, inst.queue)
	b.mu.Unlock()
	b.notifyTasks()
	return nil
}

func (b *Backend) FireDueTimers(_ context.Context, limit int) (int, error) {
	b.mu.Lock()
	if limit <= 0 {
		limit = 1
	}
	type due struct {
		key timerKey
		tm  *timer
	}
	var dues []due
	for k, tm := range b.timers {
		if !tm.fireAt.After(b.now) {
			dues = append(dues, due{key: k, tm: tm})
		}
	}
	for i := 0; i < len(dues); i++ {
		for j := i + 1; j < len(dues); j++ {
			if dues[j].tm.seq < dues[i].tm.seq {
				dues[i], dues[j] = dues[j], dues[i]
			}
		}
	}
	if len(dues) > limit {
		dues = dues[:limit]
	}
	n := 0
	for _, d := range dues {
		inst, ok := b.instances[d.tm.instanceID]
		if !ok {
			delete(b.timers, d.key)
			continue
		}
		delete(b.timers, d.key)
		b.nextInbox++
		b.inbox[d.tm.instanceID] = append(b.inbox[d.tm.instanceID], &inboxItem{
			id: b.nextInbox,
			event: journal.Event{
				Type:   journal.TypeTimerFired,
				RefSeq: d.tm.seq,
			},
		})
		if inst.status == "running" {
			b.enqueueWorkflowTaskLocked(d.tm.instanceID, inst.queue)
		}
		n++
	}
	b.mu.Unlock()
	if n > 0 {
		b.notifyTasks()
	}
	return n, nil
}

func (b *Backend) NextTimerFireAt() (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var (
		found bool
		earliest time.Time
	)
	for _, tm := range b.timers {
		if !found || tm.fireAt.Before(earliest) {
			earliest = tm.fireAt
			found = true
		}
	}
	return earliest, found
}

func (b *Backend) HasRunnableTasks() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range b.tasks {
		if !t.visibleAt.After(b.now) {
			return true
		}
	}
	return false
}

func (b *Backend) enqueueWorkflowTaskLocked(instanceID, queue string) {
	for _, t := range b.tasks {
		if t.kind == "workflow" && t.instanceID == instanceID {
			return // singleton
		}
	}
	b.nextTask++
	b.tasks[b.nextTask] = &task{
		id:         b.nextTask,
		kind:       "workflow",
		queue:      queue,
		instanceID: instanceID,
		visibleAt:  b.now,
	}
}

func toTask(t *task) backend.Task {
	return backend.Task{
		ID:                  t.id,
		Kind:                t.kind,
		Queue:               t.queue,
		InstanceID:          t.instanceID,
		Name:                t.name,
		Seq:                 t.seq,
		Input:               append([]byte(nil), t.input...),
		Attempt:             t.attempt,
		MaxAttempts:         t.maxAttempts,
		Retry:               t.retry,
		StartToCloseTimeout: t.startToCloseTimeout,
		VisibleAt:           t.visibleAt,
		WorkerID:            t.workerID,
		HeartbeatDetails:    append([]byte(nil), t.heartbeat...),
	}
}

func (b *Backend) SendToInbox(_ context.Context, instanceID string, ev journal.Event, dedupeID string) error {
	b.mu.Lock()
	inst, ok := b.instances[instanceID]
	if !ok {
		b.mu.Unlock()
		return backend.ErrNotFound
	}
	if dedupeID != "" {
		seen := b.signalDedupe[instanceID]
		if seen == nil {
			seen = map[string]struct{}{}
			b.signalDedupe[instanceID] = seen
		}
		if _, dup := seen[dedupeID]; dup {
			b.mu.Unlock()
			return nil
		}
		seen[dedupeID] = struct{}{}
	}
	b.nextInbox++
	b.inbox[instanceID] = append(b.inbox[instanceID], &inboxItem{id: b.nextInbox, event: ev})
	wake := false
	if inst.status == "running" {
		b.enqueueWorkflowTaskLocked(instanceID, inst.queue)
		wake = true
	}
	b.mu.Unlock()
	if wake {
		b.notifyTasks()
	}
	return nil
}

func (b *Backend) createInstanceLocked(inst backend.NewInstance) error {
	if _, ok := b.instances[inst.ID]; ok {
		return backend.ErrAlreadyExists
	}
	queue := inst.Queue
	if queue == "" {
		queue = "default"
	}
	ev := journal.Event{Seq: 1, Type: journal.TypeWorkflowStarted, Name: inst.Name, Payload: inst.Input}
	b.instances[inst.ID] = &instance{
		id: inst.ID, name: inst.Name, queue: queue, status: "running",
		input: inst.Input, nextSeq: 2, journal: []journal.Event{ev},
		parentID: inst.ParentID, parentSeq: inst.ParentSeq,
		searchAttributes: backend.CloneSearchAttributes(inst.SearchAttributes),
		memo:             backend.CloneSearchAttributes(inst.Memo),
	}
	b.enqueueWorkflowTaskLocked(inst.ID, queue)
	return nil
}
