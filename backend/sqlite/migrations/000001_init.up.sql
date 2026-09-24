-- 000001_init: baseline tasuki schema (tables, indexes, current columns)
-- SQLite schema for tasuki
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS wf_instances (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    queue        TEXT NOT NULL DEFAULT 'default',
    status       TEXT NOT NULL DEFAULT 'running',
    input        TEXT,
    result       TEXT,
    failure      TEXT,
    parent_id    TEXT,
    parent_seq   INTEGER,
    next_seq     INTEGER NOT NULL DEFAULT 1,
    search_attributes TEXT NOT NULL DEFAULT '{}',
    memo         TEXT NOT NULL DEFAULT '{}',
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS wf_instances_visibility_idx ON wf_instances (status, name, created_at);
-- PurgeInstances victim scan: WHERE status IN (...) AND completed_at IS NOT NULL
-- AND completed_at <= ? ORDER BY completed_at, id. The index leads with the
-- ordering columns (completed_at, id): the default 4-status purge filter
-- matches nearly every completed row, so a status-leading index cannot serve
-- the ORDER BY and the scan falls back to a TEMP B-TREE sort. Leading with
-- the range + ordering lets SQLite walk victims in order and stop at LIMIT;
-- the status IN (...) filter applies per row off the ordered scan, which
-- stays bounded because completed rows are overwhelmingly purge-eligible.
-- The partial predicate covers exactly the default purge status set
-- (backend.DefaultPurgeStatuses): `continued` instances are never deleted by
-- a default purge, so admitting them (round-23 P2 on #294) would make the
-- forced ordered scan walk old continued rows on every batch. Purges whose
-- filter is not exactly the default set run unhinted (see
-- purgeUsesOrderingHint) and never need this index.
-- Kept here for fresh databases; pre-existing databases gain it through
-- migrations 000003 (legacy-stamped version-1 databases skip the baseline),
-- 000004 (ordering shape) and 000005 (default-status predicate).
CREATE INDEX IF NOT EXISTS wf_instances_completed_at_idx ON wf_instances (completed_at, id) WHERE completed_at IS NOT NULL AND status IN ('completed', 'failed', 'terminated', 'canceled');

CREATE TABLE IF NOT EXISTS wf_journal (
    instance_id TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    type        TEXT    NOT NULL,
    name        TEXT    NOT NULL DEFAULT '',
    ref_seq     INTEGER,
    payload     TEXT,
    recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (instance_id, seq)
);

CREATE TABLE IF NOT EXISTS wf_inbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    type        TEXT NOT NULL,
    ref_seq     INTEGER,
    payload     TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS wf_inbox_instance_idx ON wf_inbox (instance_id, id);

CREATE TABLE IF NOT EXISTS wf_signal_dedupe (
    instance_id TEXT NOT NULL,
    dedupe_id   TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (instance_id, dedupe_id)
);

CREATE TABLE IF NOT EXISTS wf_tasks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL,
    queue        TEXT NOT NULL DEFAULT 'default',
    instance_id  TEXT NOT NULL,
    ref_seq      INTEGER,
    payload      TEXT,
    attempt      INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER,
    visible_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    worker_id    TEXT,
    heartbeat    TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS wf_tasks_claim_idx ON wf_tasks (kind, queue, visible_at);
CREATE UNIQUE INDEX IF NOT EXISTS wf_tasks_wf_singleton ON wf_tasks (instance_id) WHERE kind = 'workflow';

CREATE TABLE IF NOT EXISTS wf_timers (
    instance_id TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    fire_at     TEXT    NOT NULL,
    PRIMARY KEY (instance_id, seq)
);
CREATE INDEX IF NOT EXISTS wf_timers_fire_idx ON wf_timers (fire_at);

CREATE TABLE IF NOT EXISTS wf_schedules (
    id          TEXT PRIMARY KEY,
    cron        TEXT NOT NULL,
    workflow    TEXT NOT NULL,
    queue       TEXT NOT NULL DEFAULT 'default',
    input       TEXT,
    next_run_at TEXT NOT NULL,
    paused      INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS wf_schedules_due_idx ON wf_schedules (next_run_at) WHERE paused = 0;
