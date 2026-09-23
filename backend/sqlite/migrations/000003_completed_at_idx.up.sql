-- 000003_completed_at_idx: PurgeInstances victim-scan index.
-- PurgeInstances victim scan: WHERE status IN (...) AND completed_at IS NOT NULL
-- AND completed_at <= ? ORDER BY completed_at, id. Leading status keeps the
-- equality filter seekable while the range + ordering stay index-backed.
-- A dedicated migration (rather than relying on 000001) so databases stamped
-- as legacy version 1 — which skip the baseline DDL — still gain the index.
CREATE INDEX IF NOT EXISTS wf_instances_completed_at_idx ON wf_instances (status, completed_at) WHERE completed_at IS NOT NULL;
