-- 000005_purge_index_default_statuses: restrict the PurgeInstances
-- victim-scan index to the default purge status set (issue #294 round-23 P2).
-- The (completed_at, id) partial index admitted every completed row,
-- including `continued` instances that the default purge never deletes
-- (DefaultPurgeStatuses omits `continued`): the forced ordered scan then
-- walked old continued rows on every default batch. The predicate below
-- covers exactly backend.DefaultPurgeStatuses, so continued rows are not in
-- the index and the forced scan (see purgeUsesOrderingHint, which forces it
-- only when the requested statuses equal the default set) walks victims
-- only. Non-default purges (e.g. statuses=["continued"]) run unhinted and
-- never touch this index. Databases already at version >= 1 never re-run
-- 000001/000003/000004, so this migration rebuilds the index in place;
-- DROP + CREATE is idempotent (fresh databases, which gain the restricted
-- definition from 000001, rebuild a no-op equivalent).
DROP INDEX IF EXISTS wf_instances_completed_at_idx;
CREATE INDEX IF NOT EXISTS wf_instances_completed_at_idx ON wf_instances (completed_at, id) WHERE completed_at IS NOT NULL AND status IN ('completed', 'failed', 'terminated', 'canceled');
