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
