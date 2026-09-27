-- Revert 000004_fix_purge_index_order: restore the 000003 single-column shape.
DROP INDEX IF EXISTS wf_instances_completed_at_idx;
CREATE INDEX IF NOT EXISTS wf_instances_completed_at_idx
    ON wf_instances (completed_at) WHERE completed_at IS NOT NULL;
