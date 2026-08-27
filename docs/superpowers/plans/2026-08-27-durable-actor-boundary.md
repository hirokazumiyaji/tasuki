# Durable Actor Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Represent each workflow instance as a durable actor boundary without introducing resident goroutines or changing the persistence protocol.

**Architecture:** The backend remains the durable mailbox and source of inter-process correctness. A `workflowActor` owns the process-local serialization gate and idle-eviction timestamp, while the Worker continues to perform load, replay, and atomic commit exactly as before.

**Tech Stack:** Go 1.24+, standard-library `sync`, existing in-memory backend tests, Markdown design documentation.

**Spec:** `docs/superpowers/specs/2026-08-27-durable-actor-boundary-design.md`

## Global Constraints

- Do not add a public Actor API.
- Do not add a resident goroutine or in-memory mailbox per workflow instance.
- Preserve `next_seq` optimistic concurrency and Backend task lease semantics.
- Preserve workflow determinism and the existing Future/Update execution model.
- Do not add explanatory comments; comments may document intent only.

---

### Task 1: Record the approved design and task state

**Files:**
- Create: `docs/superpowers/specs/2026-08-27-durable-actor-boundary-design.md`
- Create: `docs/superpowers/plans/2026-08-27-durable-actor-boundary.md`
- Modify: `.claude/tasks/todo.md`

**Interfaces:**
- Consumes: existing `docs/02-architecture.md`, `worker.go`, and `worker_instance_lock.go` behavior.
- Produces: the durable actor invariants used by Tasks 2–4.

- [x] **Step 1: Write the design and implementation plan**

The design defines `instance_id` as the actor identity, `wf_inbox` as the durable mailbox, `workflowActor` as the process-local serialization gate, and the existing `CommitAdvancement` protocol as the durable turn commit.

- [x] **Step 2: Record checkable work items**

Add a new task section to `.claude/tasks/todo.md` covering the failing test, implementation, documentation, verification, and review.

### Task 2: Define the workflow actor serialization boundary

**Files:**
- Modify: `worker.go`
- Modify: `worker_instance_lock.go`
- Modify: `worker_instance_lock_test.go`

**Interfaces:**
- Consumes: existing per-instance lock registry and idle eviction behavior.
- Produces: `workflowActor` with `dispatch(func())`, `lastUsed`, and the same lock eviction semantics; `Worker.actorFor` is the single lookup path.

- [x] **Step 1: Write the failing serialization test**

Add a test that starts one actor turn, blocks it until released, starts a second turn, then releases the first turn. The observed callback order must be first turn followed by second turn.

```go
func TestWorkflowActorDispatchesTurnsSequentially(t *testing.T) {
	var actor workflowActor
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	order := make(chan int, 2)

	go actor.dispatch(func() {
		close(firstStarted)
		<-releaseFirst
		order <- 1
	})
	<-firstStarted
	go actor.dispatch(func() { order <- 2 })

	close(releaseFirst)
	if got := <-order; got != 1 {
		t.Fatalf("first callback order = %d", got)
	}
	if got := <-order; got != 2 {
		t.Fatalf("second callback order = %d", got)
	}
}
```

- [x] **Step 2: Run the focused test to verify it fails**

Run: `go test . -run TestWorkflowActorDispatchesTurnsSequentially -count=1`

Expected: FAIL because `workflowActor` and `dispatch` do not exist.

- [x] **Step 3: Implement the minimal actor boundary**

Rename the process-local lock type to `workflowActor`, update `Worker.instLock` to `map[string]*workflowActor`, add `Worker.actorFor`, and add:

```go
func (a *workflowActor) dispatch(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fn()
}
```

Update the Worker registry and eviction code to store `*workflowActor` values while preserving the existing idle eviction behavior.

- [x] **Step 4: Run the focused test and lock tests**

Run: `go test . -run 'TestWorkflowActorDispatchesTurnsSequentially|TestEvictIdleInstanceLocks' -count=1`

Expected: PASS.

### Task 3: Route workflow task turns through the actor boundary

**Files:**
- Modify: `worker.go:173-200`

**Interfaces:**
- Consumes: `workflowActor.dispatch`, `handleWorkflow`, and `pendingWorkflowCommit`.
- Produces: the same workflow task behavior with an explicit actor turn boundary.

- [x] **Step 1: Confirm existing workflow behavior coverage**

Run: `go test . -run 'TestWorker_ReplayResumesFromPartialJournal|TestFlushWorkflowCommits_Batch' -count=1`

Expected: PASS before the wiring change; these tests cover workflow execution and advancement commits that must remain unchanged.

- [x] **Step 2: Wrap the existing workflow task critical section**

Replace the direct `mu.Lock`/`mu.Unlock` pair in `Worker.tick` with `actor := w.actorFor(t.InstanceID)` followed by `actor.dispatch(func() { ... })`. Keep semaphore acquisition, metrics, task tracking, error logging, pending commit collection, and the post-batch flush in their current order.

- [x] **Step 3: Run focused Worker tests**

Run: `go test . -run 'TestWorkflowActorDispatchesTurnsSequentially|TestWorker_ReplayResumesFromPartialJournal|TestFlushWorkflowCommits_Batch' -count=1`

Expected: PASS.

### Task 4: Document the mapping and verify the repository

**Files:**
- Modify: `docs/02-architecture.md`
- Modify: `README.md`
- Modify: `worker_journal_warn_test.go`

**Interfaces:**
- Consumes: the durable actor specification and the completed Worker boundary.
- Produces: user-facing documentation that distinguishes durable actor semantics from resident actors.

- [x] **Step 1: Add the architecture explanation**

Add a short subsection after the execution-model discussion describing the identity, mailbox, state, turn, and recovery mapping. State that waiting workflows consume no actor goroutine and that cross-process correctness remains in the Backend.

- [x] **Step 2: Add the README positioning note**

Add one sentence to the architecture/status description: tasuki は各 workflow instance を durable actor として扱い、journal replay モデルを維持する。

- [x] **Step 3: Run formatting and focused verification**

Run: `gofmt -w worker.go worker_instance_lock.go worker_instance_lock_test.go worker_journal_warn_test.go worker_incompatible_test.go`

Run: `go test . -count=1`

Expected: PASS with no test failures.

If the race test exposes the existing unsynchronized log sink in `worker_journal_warn_test.go`, protect that test-only buffer with a mutex before rerunning the command.

- [x] **Step 4: Run race verification**

Run: `go test . -race -count=1`

Expected: PASS with no race reports.

- [x] **Step 5: Review the diff against the spec**

Check that no public API, Backend method, lease behavior, journal format, or workflow concurrency semantics changed. Check that the docs do not imply a resident actor runtime.

### Task 5: Final review record

**Files:**
- Modify: `.claude/tasks/todo.md`

**Interfaces:**
- Consumes: verification output and the spec invariants.
- Produces: a review section listing changed files, commands run, and any remaining non-goals.

- [x] **Step 1: Record verification evidence**

Add the exact test commands and their observed results to the review section.

- [x] **Step 2: Record scope confirmation**

State that the change adds a process-local serialization boundary and documentation only; durable correctness remains provided by the existing Backend protocol.
