CREATE TABLE wf_instances (
  id STRING(255) NOT NULL,
  name STRING(255) NOT NULL,
  queue STRING(255) NOT NULL DEFAULT ('default'),
  status STRING(64) NOT NULL DEFAULT ('running'),
  input JSON,
  result JSON,
  failure JSON,
  parent_id STRING(255),
  parent_seq INT64,
  next_seq INT64 NOT NULL DEFAULT (1),
  search_attributes JSON,
  memo JSON,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP
) PRIMARY KEY (id);

CREATE INDEX wf_instances_visibility_idx ON wf_instances(status, name, created_at);

CREATE TABLE wf_journal (
  instance_id STRING(255) NOT NULL,
  seq INT64 NOT NULL,
  type STRING(64) NOT NULL,
  name STRING(255) NOT NULL DEFAULT (''),
  ref_seq INT64,
  payload JSON,
  recorded_at TIMESTAMP NOT NULL
) PRIMARY KEY (instance_id, seq);

CREATE TABLE wf_inbox (
  id INT64 NOT NULL,
  instance_id STRING(255) NOT NULL,
  type STRING(64) NOT NULL,
  ref_seq INT64,
  payload JSON,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (id);

CREATE INDEX wf_inbox_instance_idx ON wf_inbox(instance_id, id);

CREATE TABLE wf_signal_dedupe (
  instance_id STRING(255) NOT NULL,
  dedupe_id STRING(255) NOT NULL,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (instance_id, dedupe_id);

CREATE TABLE wf_tasks (
  id INT64 NOT NULL,
  kind STRING(32) NOT NULL,
  queue STRING(255) NOT NULL DEFAULT ('default'),
  instance_id STRING(255) NOT NULL,
  ref_seq INT64,
  payload JSON,
  attempt INT64 NOT NULL DEFAULT (0),
  max_attempts INT64,
  visible_at TIMESTAMP NOT NULL,
  worker_id STRING(255),
  heartbeat BYTES(MAX),
  created_at TIMESTAMP NOT NULL,
  wf_singleton STRING(255) AS (IF(kind = 'workflow', instance_id, NULL)) STORED
) PRIMARY KEY (id);

CREATE UNIQUE NULL_FILTERED INDEX wf_tasks_wf_singleton ON wf_tasks(wf_singleton);

CREATE INDEX wf_tasks_claim_idx ON wf_tasks(kind, queue, visible_at);

CREATE TABLE wf_timers (
  instance_id STRING(255) NOT NULL,
  seq INT64 NOT NULL,
  fire_at TIMESTAMP NOT NULL
) PRIMARY KEY (instance_id, seq);

CREATE INDEX wf_timers_fire_idx ON wf_timers(fire_at);

CREATE TABLE wf_schedules (
  id STRING(255) NOT NULL,
  cron STRING(255) NOT NULL,
  workflow STRING(255) NOT NULL,
  queue STRING(255) NOT NULL DEFAULT ('default'),
  input JSON,
  next_run_at TIMESTAMP NOT NULL,
  paused BOOL NOT NULL DEFAULT (FALSE),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
) PRIMARY KEY (id);

CREATE INDEX wf_schedules_due_idx ON wf_schedules(next_run_at, paused);
