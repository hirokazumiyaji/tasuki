-- 000003_tasks_instance_idx: index terminal task cleanup by instance.
-- TerminateInstance and terminal CommitAdvancements delete one instance's
-- tasks via DELETE FROM wf_tasks WHERE instance_id = ?, which scanned the
-- global table: the only index was (kind, queue, visible_at). A general
-- (instance_id) index keeps per-terminal cleanup logarithmic as task volume
-- grows. IF NOT EXISTS keeps the migration idempotent.
CREATE INDEX IF NOT EXISTS wf_tasks_instance_idx ON wf_tasks (instance_id);
