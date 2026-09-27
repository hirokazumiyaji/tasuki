-- 000004_fix_purge_index_order: lead the PurgeInstances victim-scan index with
-- the ordering columns (issue #294 round-20 P2).
-- Older 000001/000003 created wf_instances_completed_at_idx as
-- (completed_at): with the victim SELECT ordering by completed_at, id, the
-- single-column index cannot serve the ORDER BY and the scan falls back to a
-- filesort over equal-timestamp groups. The fixed definition leads with
-- (completed_at, id) so the scan walks victims in order and stops at LIMIT.
-- Databases already at version 3 never re-run 000003, so this migration
-- rebuilds the index in place. 000003 always runs first in the same Migrate
-- pass, so the index exists when the DROP executes (000003 creates it,
-- tolerating the duplicate-index error when the baseline already did); a
-- retry after a crash between DROP and CREATE re-runs this version's DDL
-- because the version row is recorded only after all statements complete.
-- (MySQL DDL autocommits per statement, so that retry's DROP meets an
-- already-gone index: applyMigration tolerates error 1091 for DROP INDEX
-- statements and proceeds to the CREATE. See isMissingIndexError.)
ALTER TABLE wf_instances DROP INDEX wf_instances_completed_at_idx;
CREATE INDEX wf_instances_completed_at_idx ON wf_instances (completed_at, id);
