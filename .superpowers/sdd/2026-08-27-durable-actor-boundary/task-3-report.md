# Task 3 Report: Route workflow task turns through the actor boundary

## Scope

Modified `worker.go` only for implementation. The task report is included as explicitly required. Existing changes to `.claude/tasks/todo.md`, `worker_instance_lock.go`, `worker_instance_lock_test.go`, and the durable-boundary plan/spec were left untouched.

## Change

The workflow-task goroutine in `Worker.tick` now retrieves or creates the instance's `workflowActor` while holding `instMu`, refreshes its `lastUsed` timestamp, and runs the existing critical section through `actor.dispatch`.

The task semaphore remains acquired before the actor turn. Metrics, logging, in-flight tracking, workflow handling, error handling, pending-commit collection, the batch wait, commit flush, and idle eviction remain in their prior order.

## Test Results

Pre-change:

```text
go test . -run 'TestWorker_ReplayResumesFromPartialJournal|TestFlushWorkflowCommits_Batch' -count=1
ok github.com/hirokazumiyaji/tasuki 0.408s
```

Post-change:

```text
go test . -run 'TestWorkflowActorDispatchesTurnsSequentially|TestWorker_ReplayResumesFromPartialJournal|TestFlushWorkflowCommits_Batch' -count=1
ok github.com/hirokazumiyaji/tasuki 0.544s
```

`git diff --check` also completed without output.

## Note

The initial direct use of `instanceMutex(...).dispatch` did not compile because that accessor returns `*sync.Mutex`. The final worker-only wiring obtains the existing actor from `instLock` under `instMu`, preserving the accessor's creation and `lastUsed` behavior before dispatching the turn.
