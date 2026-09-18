-- 000003_purge_search_indexes: indexes for retention purge and search_attributes listing (issue #294)
--
-- PurgeInstances selects victims with
--   WHERE status = ANY($1) AND completed_at IS NOT NULL AND completed_at <= now() - $2
--   ORDER BY completed_at, id LIMIT $3
-- Without an index this degrades to a sequential scan. The partial B-tree
-- below covers the range + ordering (`completed_at <= $x` implies the
-- partial predicate `completed_at IS NOT NULL`, so the planner can use it).
CREATE INDEX IF NOT EXISTS wf_instances_completed_at_idx
    ON wf_instances (completed_at) WHERE completed_at IS NOT NULL;

-- ListInstances filters with `search_attributes @> $1::jsonb`. A GIN index
-- with jsonb_path_ops accelerates containment queries.
CREATE INDEX IF NOT EXISTS wf_instances_search_attributes_gin_idx
    ON wf_instances USING gin (search_attributes jsonb_path_ops);
