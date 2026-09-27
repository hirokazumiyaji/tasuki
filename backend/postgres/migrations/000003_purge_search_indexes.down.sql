-- Revert 000003_purge_search_indexes.
DROP INDEX IF EXISTS wf_instances_search_attributes_gin_idx;
DROP INDEX IF EXISTS wf_instances_completed_at_idx;
