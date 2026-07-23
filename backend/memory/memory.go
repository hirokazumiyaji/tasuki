package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// Backend is an in-memory store for M0 tests and local demos.
type Backend struct {
	mu sync.Mutex

	now       time.Time
	nextTask  int64
	nextInbox int64

	instances map[string]*instance
	tasks     map[int64]*task
	timers    map[timerKey]*timer
	inbox     map[string][]*inboxItem // instanceID → ordered
}

type instance struct {
	id        string
	name      string
	queue     string
	status    string
	input     []byte
	result    []byte
	failure   []byte
	nextSeq   int64
	journal   []journal.Event
	parentID  string
	parentSeq int64
}

type task struct {
	id          int64
	kind        string
	queue       string
	instanceID  string
	name        string
	seq         int64
	input       []byte
	attempt     int
	maxAttempts int
	retry       backend.RetryPolicy
	visibleAt   time.Time
	workerID    string
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
		now:       time.Now().UTC(),
		instances: map[string]*instance{},
		tasks:     map[int64]*task{},
		timers:    map[timerKey]*timer{},
		inbox:     map[string][]*inboxItem{},
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
	defer b.mu.Unlock()
	if _, ok := b.instances[inst.ID]; ok {
		return backend.ErrAlreadyExists
	}
	queue := inst.Queue
	if queue == "" {
		queue = "default"
	}
	ev := journal.Event{
		Seq:     1,
		Type:    journal.TypeWorkflowStarted,
		Name:    inst.Name,
		Payload: inst.Input,
	}
	b.instances[inst.ID] = &instance{
		id: inst.ID, name: inst.Name, queue: queue, status: "running",
		input: inst.Input, nextSeq: 2, journal: []journal.Event{ev},
		parentID: inst.ParentID, parentSeq: inst.ParentSeq,
	}
	b.enqueueWorkflowTaskLocked(inst.ID, queue)
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
		ID:      inst.id,
		Name:    inst.name,
		Queue:   inst.queue,
		Status:  inst.status,
		Input:   append([]byte(nil), inst.input...),
		Result:  append([]byte(nil), inst.result...),
		Failure: append([]byte(nil), inst.failure...),
		NextSeq: inst.nextSeq, ParentID: inst.parentID, ParentSeq: inst.parentSeq,
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
		})
	}
	return out, nil
}

func (b *Backend) TerminateInstance(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	inst, ok := b.instances[id]
	if !ok {
		return backend.ErrNotFound
	}
	inst.status = "terminated"
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

func (b *Backend) ReleaseLease(_ context.Context, taskID int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.tasks[taskID]
	if !ok {
		return backend.ErrNotFound
	}
	t.visibleAt = b.now
	t.workerID = ""
	return nil
}

func (b *Backend) RetryActivity(_ context.Context, taskID int64, visibleAt time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.tasks[taskID]
	if !ok || t.kind != "activity" {
		return backend.ErrNotFound
	}
	t.visibleAt = visibleAt.UTC()
	t.workerID = ""
	return nil
}

func (b *Backend) LoadWorkflow(_ context.Context, instanceID string) (*backend.WorkflowState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, backend.ErrNotFound
	}
	inbox := make([]backend.InboxEvent, 0, len(b.inbox[instanceID]))
	for _, item := range b.inbox[instanceID] {
		inbox = append(inbox, backend.InboxEvent{ID: item.id, Event: item.event})
	}
	journalCopy := append([]journal.Event(nil), inst.journal...)
	return &backend.WorkflowState{
		Instance: backend.Instance{
			ID:      inst.id,
			Name:    inst.name,
			Queue:   inst.queue,
			Status:  inst.status,
			Input:   append([]byte(nil), inst.input...),
			Result:  append([]byte(nil), inst.result...),
			Failure: append([]byte(nil), inst.failure...),
			NextSeq: inst.nextSeq, ParentID: inst.parentID, ParentSeq: inst.parentSeq,
		},
		Journal: journalCopy,
		Inbox:   inbox,
		NextSeq: inst.nextSeq,
		Now:     b.now,
	}, nil
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

func (b *Backend) CommitAdvancement(_ context.Context, adv backend.Advancement) error {
	b.mu.Lock()
	defer b.mu.Unlock()
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

	for _, at := range adv.ActivityTasks {
		b.nextTask++
		b.tasks[b.nextTask] = &task{
			id:          b.nextTask,
			kind:        "activity",
			queue:       at.Queue,
			instanceID:  at.InstanceID,
			name:        at.Name,
			seq:         at.Seq,
			input:       append([]byte(nil), at.Input...),
			maxAttempts: at.MaxAttempts,
			retry:       at.Retry,
			visibleAt:   b.now,
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
	defer b.mu.Unlock()
	t, ok := b.tasks[taskID]
	if !ok || t.kind != "activity" {
		return backend.ErrSuperseded
	}
	inst, ok := b.instances[t.instanceID]
	if !ok {
		return backend.ErrNotFound
	}
	delete(b.tasks, taskID)
	if inst.status != "running" {
		// Terminated/completed instances ignore late completions.
		return nil
	}
	ev.RefSeq = t.seq
	b.nextInbox++
	b.inbox[t.instanceID] = append(b.inbox[t.instanceID], &inboxItem{id: b.nextInbox, event: ev})
	b.enqueueWorkflowTaskLocked(t.instanceID, inst.queue)
	return nil
}

func (b *Backend) FireDueTimers(_ context.Context, limit int) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
		ID:          t.id,
		Kind:        t.kind,
		Queue:       t.queue,
		InstanceID:  t.instanceID,
		Name:        t.name,
		Seq:         t.seq,
		Input:       append([]byte(nil), t.input...),
		Attempt:     t.attempt,
		MaxAttempts: t.maxAttempts,
		Retry:       t.retry,
		VisibleAt:   t.visibleAt,
		WorkerID:    t.workerID,
	}
}

func (b *Backend) SendToInbox(_ context.Context, instanceID string, ev journal.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	inst, ok := b.instances[instanceID]
	if !ok {
		return backend.ErrNotFound
	}
	b.nextInbox++
	b.inbox[instanceID] = append(b.inbox[instanceID], &inboxItem{id: b.nextInbox, event: ev})
	if inst.status == "running" {
		b.enqueueWorkflowTaskLocked(instanceID, inst.queue)
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
	}
	b.enqueueWorkflowTaskLocked(inst.ID, queue)
	return nil
}
