-- 000003_tasks_instance_idx: index terminal task cleanup by instance.
-- TerminateInstance and terminal CommitAdvancements delete tasks via
-- DELETE FROM wf_tasks WHERE instance_id = $1, which scanned the global
-- table: the only indexes were (kind, queue, visible_at) and the partial
-- workflow-singleton index. A general (instance_id) index keeps per-terminal
-- cleanup logarithmic as task volume grows.
CREATE INDEX IF NOT EXISTS wf_tasks_instance_idx ON wf_tasks (instance_id);
