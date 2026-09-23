-- 000002_backfill_columns: columns introduced after the initial schema, for
-- databases created before they existed. Migrate tolerates duplicate-column
-- errors (the column is already there) but fails on anything else
-- (permission denied, missing table, ...).
ALTER TABLE wf_tasks ADD COLUMN heartbeat TEXT;
ALTER TABLE wf_instances ADD COLUMN search_attributes TEXT NOT NULL DEFAULT '{}';
ALTER TABLE wf_instances ADD COLUMN memo TEXT NOT NULL DEFAULT '{}';
