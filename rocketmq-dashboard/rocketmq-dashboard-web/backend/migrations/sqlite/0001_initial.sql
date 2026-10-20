-- Initial schema and upgrades through version 4.
-- Version sections preserve existing databases and migration records.

-- migration: 1
CREATE TABLE IF NOT EXISTS dashboard_schema_migration (
    version INTEGER PRIMARY KEY,
    applied_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS dashboard_environment (
    environment_id TEXT PRIMARY KEY,
    name VARCHAR(128) UNIQUE NOT NULL,
    use_vip_channel INTEGER NOT NULL,
    use_tls INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    updated_by TEXT
);

CREATE TABLE IF NOT EXISTS dashboard_endpoint (
    endpoint_id TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL,
    endpoint_type TEXT NOT NULL,
    address TEXT NOT NULL,
    is_active INTEGER NOT NULL,
    sort_order INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(environment_id, endpoint_type, address),
    FOREIGN KEY(environment_id) REFERENCES dashboard_environment(environment_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS dashboard_endpoint_environment_type_idx
    ON dashboard_endpoint(environment_id, endpoint_type, sort_order);

CREATE TABLE IF NOT EXISTS consumer_monitor_rule (
    environment_id TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    min_count INTEGER NOT NULL,
    max_diff_total INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY(environment_id, consumer_group),
    FOREIGN KEY(environment_id) REFERENCES dashboard_environment(environment_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dashboard_metric_sample (
    environment_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    resource_name TEXT NOT NULL,
    bucket_ms INTEGER NOT NULL,
    value REAL NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY(environment_id, metric_name, resource_name, bucket_ms),
    FOREIGN KEY(environment_id) REFERENCES dashboard_environment(environment_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS dashboard_metric_sample_query_idx
    ON dashboard_metric_sample(environment_id, metric_name, resource_name, bucket_ms DESC);
CREATE INDEX IF NOT EXISTS dashboard_metric_sample_retention_idx
    ON dashboard_metric_sample(bucket_ms);

CREATE TABLE IF NOT EXISTS dashboard_session (
    session_id_hash TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    last_seen_at_ms INTEGER NOT NULL,
    revoked_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS dashboard_session_expiry_idx ON dashboard_session(expires_at_ms);

CREATE TABLE IF NOT EXISTS dashboard_audit_event (
    event_id TEXT PRIMARY KEY,
    environment_id TEXT,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_name TEXT,
    before_payload TEXT,
    after_payload TEXT,
    request_id TEXT,
    created_at_ms INTEGER NOT NULL,
    FOREIGN KEY(environment_id) REFERENCES dashboard_environment(environment_id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS dashboard_audit_event_created_idx ON dashboard_audit_event(created_at_ms DESC);

CREATE TABLE IF NOT EXISTS dashboard_task_lease (
    lease_name TEXT PRIMARY KEY,
    holder_id TEXT NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    fencing_token INTEGER NOT NULL
);
INSERT OR IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (1, 0);

-- migration: 2
ALTER TABLE dashboard_endpoint ADD COLUMN role TEXT NOT NULL DEFAULT 'secondary';
UPDATE dashboard_endpoint SET role = 'primary' WHERE is_active = 1;
ALTER TABLE dashboard_endpoint ADD COLUMN is_enabled INTEGER NOT NULL DEFAULT 1;
CREATE UNIQUE INDEX IF NOT EXISTS dashboard_endpoint_one_active_per_type_uq
    ON dashboard_endpoint(environment_id, endpoint_type)
    WHERE is_active = 1;
INSERT OR IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (2, 0);

-- migration: 3
CREATE TABLE IF NOT EXISTS dashboard_history_sample (
    environment_id TEXT COLLATE BINARY NOT NULL,
    metric_name TEXT COLLATE BINARY NOT NULL,
    bucket_ms INTEGER NOT NULL,
    dimensions_json TEXT COLLATE BINARY NOT NULL,
    value REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS dashboard_history_sample_query_idx
    ON dashboard_history_sample(environment_id, metric_name, dimensions_json, bucket_ms);
CREATE INDEX IF NOT EXISTS dashboard_history_sample_retention_idx
    ON dashboard_history_sample(environment_id, bucket_ms);
INSERT OR IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (3, 0);

-- migration: 4
DROP TABLE IF EXISTS dashboard_audit_event;
DROP TABLE IF EXISTS dashboard_session;
CREATE TABLE dashboard_session (
    session_id TEXT COLLATE BINARY NOT NULL UNIQUE,
    token_hash BLOB NOT NULL PRIMARY KEY CHECK (length(token_hash) = 32),
    username TEXT COLLATE BINARY NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    last_seen_at_ms INTEGER NOT NULL,
    revoked_at_ms INTEGER NULL
);
CREATE INDEX IF NOT EXISTS dashboard_session_username_active_idx
    ON dashboard_session(username, revoked_at_ms, expires_at_ms, created_at_ms DESC, session_id DESC);
CREATE INDEX IF NOT EXISTS dashboard_session_keyset_idx
    ON dashboard_session(created_at_ms DESC, session_id DESC);
CREATE INDEX IF NOT EXISTS dashboard_session_cleanup_idx
    ON dashboard_session(expires_at_ms, revoked_at_ms);

CREATE TABLE dashboard_audit_event (
    event_id TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
    request_id TEXT COLLATE BINARY NOT NULL,
    actor_kind TEXT COLLATE BINARY NOT NULL,
    actor_username TEXT COLLATE BINARY NULL,
    action TEXT COLLATE BINARY NOT NULL,
    resource_type TEXT COLLATE BINARY NOT NULL,
    resource_name TEXT COLLATE BINARY NULL,
    environment_id TEXT COLLATE BINARY NULL,
    outcome TEXT COLLATE BINARY NOT NULL,
    detail_json TEXT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS dashboard_audit_event_keyset_idx
    ON dashboard_audit_event(created_at_ms DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS dashboard_audit_event_actor_idx
    ON dashboard_audit_event(actor_username, created_at_ms DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS dashboard_audit_event_retention_idx
    ON dashboard_audit_event(created_at_ms);
CREATE INDEX IF NOT EXISTS dashboard_audit_event_action_time_idx
    ON dashboard_audit_event(action, created_at_ms DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS dashboard_audit_event_outcome_time_idx
    ON dashboard_audit_event(outcome, created_at_ms DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS dashboard_audit_event_environment_time_idx
    ON dashboard_audit_event(environment_id, created_at_ms DESC, event_id DESC);
INSERT OR IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (4, 0);
