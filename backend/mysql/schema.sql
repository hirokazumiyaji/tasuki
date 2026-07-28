-- MySQL 8 schema for tasuki
CREATE TABLE IF NOT EXISTS wf_instances (
    id           VARCHAR(255) PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    queue        VARCHAR(255) NOT NULL DEFAULT 'default',
    status       VARCHAR(64)  NOT NULL DEFAULT 'running',
    input        JSON,
    result       JSON,
    failure      JSON,
    parent_id    VARCHAR(255) NULL,
    parent_seq   BIGINT NULL,
    next_seq     BIGINT NOT NULL DEFAULT 1,
    search_attributes JSON NOT NULL DEFAULT (JSON_OBJECT()),
    memo         JSON NOT NULL DEFAULT (JSON_OBJECT()),
    created_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    completed_at DATETIME(6) NULL,
    INDEX wf_instances_visibility_idx (status, name, created_at)
);

CREATE TABLE IF NOT EXISTS wf_journal (
    instance_id VARCHAR(255) NOT NULL,
    seq         BIGINT NOT NULL,
    type        VARCHAR(64) NOT NULL,
    name        VARCHAR(255) NOT NULL DEFAULT '',
    ref_seq     BIGINT NULL,
    payload     JSON,
    recorded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (instance_id, seq)
);

CREATE TABLE IF NOT EXISTS wf_inbox (
    id          BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    instance_id VARCHAR(255) NOT NULL,
    type        VARCHAR(64) NOT NULL,
    ref_seq     BIGINT NULL,
    payload     JSON,
    created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX wf_inbox_instance_idx (instance_id, id)
);

CREATE TABLE IF NOT EXISTS wf_signal_dedupe (
    instance_id VARCHAR(255) NOT NULL,
    dedupe_id   VARCHAR(255) NOT NULL,
    created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (instance_id, dedupe_id)
);

CREATE TABLE IF NOT EXISTS wf_tasks (
    id           BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    kind         VARCHAR(32) NOT NULL,
    queue        VARCHAR(255) NOT NULL DEFAULT 'default',
    instance_id  VARCHAR(255) NOT NULL,
    ref_seq      BIGINT NULL,
    payload      JSON,
    attempt      INT NOT NULL DEFAULT 0,
    max_attempts INT NULL,
    visible_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    worker_id    VARCHAR(255) NULL,
    heartbeat    BLOB NULL,
    created_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    wf_singleton VARCHAR(255)
        GENERATED ALWAYS AS (CASE WHEN kind = 'workflow' THEN instance_id ELSE NULL END) STORED,
    UNIQUE KEY wf_tasks_wf_singleton (wf_singleton),
    INDEX wf_tasks_claim_idx (kind, queue, visible_at)
);

CREATE TABLE IF NOT EXISTS wf_timers (
    instance_id VARCHAR(255) NOT NULL,
    seq         BIGINT NOT NULL,
    fire_at     DATETIME(6) NOT NULL,
    PRIMARY KEY (instance_id, seq),
    INDEX wf_timers_fire_idx (fire_at)
);

CREATE TABLE IF NOT EXISTS wf_schedules (
    id          VARCHAR(255) PRIMARY KEY,
    cron        VARCHAR(255) NOT NULL,
    workflow    VARCHAR(255) NOT NULL,
    queue       VARCHAR(255) NOT NULL DEFAULT 'default',
    input       JSON,
    next_run_at DATETIME(6) NOT NULL,
    paused      TINYINT(1) NOT NULL DEFAULT 0,
    created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    INDEX wf_schedules_due_idx (next_run_at, paused)
);
