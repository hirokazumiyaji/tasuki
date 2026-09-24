-- 000004_fix_purge_index_order: lead the PurgeInstances victim-scan index with
-- the ordering columns (issue #294 round-20 P2).
-- 000003 created wf_instances_completed_at_idx as (completed_at): with the
-- victim SELECT ordering by completed_at, id, the single-column index cannot
-- serve the ORDER BY and the scan falls back to a residual sort over
-- equal-timestamp groups. The fixed definition leads with (completed_at, id)
-- so the index walk returns victims in order. Databases already at version 3
-- never re-run 000003, so this migration rebuilds the index in place; DROP +
-- CREATE is idempotent (fresh databases, which gain the fixed definition from
-- 000003, rebuild a no-op equivalent).
DROP INDEX IF EXISTS wf_instances_completed_at_idx;
CREATE INDEX IF NOT EXISTS wf_instances_completed_at_idx
    ON wf_instances (completed_at, id) WHERE completed_at IS NOT NULL;
