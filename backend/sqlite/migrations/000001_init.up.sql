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
CREATE INDEX IF NOT EXISTS wf_tasks_instance_idx ON wf_tasks (instance_id);
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
