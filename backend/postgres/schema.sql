-- M1 schema for tasuki PostgreSQL backend
CREATE TABLE IF NOT EXISTS wf_instances (
    id           text PRIMARY KEY,
    name         text NOT NULL,
    queue        text NOT NULL DEFAULT 'default',
    status       text NOT NULL DEFAULT 'running',
    input        jsonb,
    result       jsonb,
    failure      jsonb,
    parent_id    text,
    parent_seq   bigint,
    next_seq     bigint NOT NULL DEFAULT 1,
    search_attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    memo         jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);
CREATE INDEX IF NOT EXISTS wf_instances_visibility_idx ON wf_instances (status, name, created_at);

CREATE TABLE IF NOT EXISTS wf_journal (
    instance_id text   NOT NULL,
    seq         bigint NOT NULL,
    type        text   NOT NULL,
    name        text   NOT NULL DEFAULT '',
    ref_seq     bigint,
    payload     jsonb,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, seq)
);

CREATE TABLE IF NOT EXISTS wf_inbox (
    id          bigserial PRIMARY KEY,
    instance_id text NOT NULL,
    type        text NOT NULL,
    ref_seq     bigint,
    payload     jsonb,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS wf_inbox_instance_idx ON wf_inbox (instance_id, id);

CREATE TABLE IF NOT EXISTS wf_signal_dedupe (
    instance_id text NOT NULL,
    dedupe_id   text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, dedupe_id)
);

CREATE TABLE IF NOT EXISTS wf_tasks (
    id           bigserial PRIMARY KEY,
    kind         text NOT NULL,
    queue        text NOT NULL DEFAULT 'default',
    instance_id  text NOT NULL,
    ref_seq      bigint,
    payload      jsonb,
    attempt      int  NOT NULL DEFAULT 0,
    max_attempts int,
    visible_at   timestamptz NOT NULL DEFAULT now(),
    worker_id    text,
    heartbeat    bytea,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS wf_tasks_claim_idx ON wf_tasks (kind, queue, visible_at);
CREATE UNIQUE INDEX IF NOT EXISTS wf_tasks_wf_singleton ON wf_tasks (instance_id) WHERE kind = 'workflow';

CREATE TABLE IF NOT EXISTS wf_timers (
    instance_id text   NOT NULL,
    seq         bigint NOT NULL,
    fire_at     timestamptz NOT NULL,
    PRIMARY KEY (instance_id, seq)
);
CREATE INDEX IF NOT EXISTS wf_timers_fire_idx ON wf_timers (fire_at);

CREATE TABLE IF NOT EXISTS wf_schedules (
    id          text PRIMARY KEY,
    cron        text NOT NULL,
    workflow    text NOT NULL,
    queue       text NOT NULL DEFAULT 'default',
    input       jsonb,
    next_run_at timestamptz NOT NULL,
    paused      boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS wf_schedules_due_idx ON wf_schedules (next_run_at) WHERE NOT paused;


ALTER TABLE wf_tasks ADD COLUMN IF NOT EXISTS heartbeat bytea;
ALTER TABLE wf_instances ADD COLUMN IF NOT EXISTS search_attributes jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE wf_instances ADD COLUMN IF NOT EXISTS memo jsonb NOT NULL DEFAULT '{}'::jsonb;
