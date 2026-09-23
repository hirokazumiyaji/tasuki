-- 000003_completed_at_idx: index for PurgeInstances retention scans.
-- Databases created before this index existed need an explicit CREATE INDEX
-- (MySQL has no CREATE INDEX IF NOT EXISTS); Migrate tolerates the
-- duplicate-index error when the index is already present but fails on
-- anything else (permission denied, missing table, ...).
CREATE INDEX wf_instances_completed_at_idx ON wf_instances (completed_at);
