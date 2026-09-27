-- 000005_tasks_instance_idx: index terminal task cleanup by instance.
-- TerminateInstance and terminal CommitAdvancements delete one instance's
-- tasks via DELETE FROM wf_tasks WHERE instance_id = ?, which scanned the
-- global table: the only index was (kind, queue, visible_at). A general
-- (instance_id) index keeps per-terminal cleanup logarithmic as task volume
-- grows. Migrate tolerates duplicate-index errors (the index is already
-- there, e.g. created by a pre-versioned backfill) but fails on anything
-- else (permission denied, missing table, ...).
CREATE INDEX wf_tasks_instance_idx ON wf_tasks (instance_id);
