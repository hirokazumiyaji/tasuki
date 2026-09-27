-- 000004_fix_purge_index_order: lead the PurgeInstances victim-scan index with
-- the ordering columns (issue #294 round-19 P2).
-- 000001/000003 created wf_instances_completed_at_idx as (status,
-- completed_at): with the default 4-status purge filter matching nearly every
-- completed row, the status-leading index cannot serve ORDER BY completed_at,
-- id and the victim scan falls back to a TEMP B-TREE sort. The fixed
-- definition leads with (completed_at, id) so SQLite walks victims in order
-- and stops at LIMIT. Databases already at version >= 3 never re-run 000003,
-- so this migration rebuilds the index in place; DROP + CREATE is idempotent
-- (fresh databases, which gain the fixed definition from 000001, rebuild a
-- no-op equivalent).
DROP INDEX IF EXISTS wf_instances_completed_at_idx;
CREATE INDEX IF NOT EXISTS wf_instances_completed_at_idx ON wf_instances (completed_at, id) WHERE completed_at IS NOT NULL;
