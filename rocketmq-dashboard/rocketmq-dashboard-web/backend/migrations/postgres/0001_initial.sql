CREATE TABLE IF NOT EXISTS dashboard_schema_migration (
    version BIGINT PRIMARY KEY,
    applied_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS dashboard_environment (
    environment_id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(128) UNIQUE NOT NULL,
    use_vip_channel BOOLEAN NOT NULL,
    use_tls BOOLEAN NOT NULL,
    revision BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    updated_by VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS dashboard_endpoint (
    endpoint_id VARCHAR(36) PRIMARY KEY,
    environment_id VARCHAR(36) NOT NULL REFERENCES dashboard_environment(environment_id) ON DELETE CASCADE,
    endpoint_type VARCHAR(32) NOT NULL,
    address VARCHAR(512) NOT NULL,
    is_active BOOLEAN NOT NULL,
    sort_order INTEGER NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE(environment_id, endpoint_type, address)
);
CREATE INDEX IF NOT EXISTS dashboard_endpoint_environment_type_idx
    ON dashboard_endpoint(environment_id, endpoint_type, sort_order);

CREATE TABLE IF NOT EXISTS consumer_monitor_rule (
    environment_id VARCHAR(36) NOT NULL REFERENCES dashboard_environment(environment_id) ON DELETE CASCADE,
    consumer_group VARCHAR(255) NOT NULL,
    min_count INTEGER NOT NULL,
    max_diff_total BIGINT NOT NULL,
    revision BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY(environment_id, consumer_group)
);

CREATE TABLE IF NOT EXISTS dashboard_metric_sample (
    environment_id VARCHAR(36) NOT NULL REFERENCES dashboard_environment(environment_id) ON DELETE CASCADE,
    metric_name VARCHAR(64) NOT NULL,
    resource_name VARCHAR(255) NOT NULL,
    bucket_ms BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY(environment_id, metric_name, resource_name, bucket_ms)
);
CREATE INDEX IF NOT EXISTS dashboard_metric_sample_query_idx
    ON dashboard_metric_sample(environment_id, metric_name, resource_name, bucket_ms DESC);
CREATE INDEX IF NOT EXISTS dashboard_metric_sample_retention_idx
    ON dashboard_metric_sample(bucket_ms);

CREATE TABLE IF NOT EXISTS dashboard_session (
    session_id_hash VARCHAR(128) PRIMARY KEY,
    username VARCHAR(128) NOT NULL,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    last_seen_at_ms BIGINT NOT NULL,
    revoked_at_ms BIGINT
);
CREATE INDEX IF NOT EXISTS dashboard_session_expiry_idx ON dashboard_session(expires_at_ms);

CREATE TABLE IF NOT EXISTS dashboard_audit_event (
    event_id VARCHAR(36) PRIMARY KEY,
    environment_id VARCHAR(36) REFERENCES dashboard_environment(environment_id) ON DELETE SET NULL,
    actor VARCHAR(128) NOT NULL,
    action VARCHAR(128) NOT NULL,
    resource_type VARCHAR(64) NOT NULL,
    resource_name VARCHAR(255),
    before_payload TEXT,
    after_payload TEXT,
    request_id VARCHAR(64),
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS dashboard_audit_event_created_idx ON dashboard_audit_event(created_at_ms DESC);

CREATE TABLE IF NOT EXISTS dashboard_task_lease (
    lease_name VARCHAR(128) PRIMARY KEY,
    holder_id VARCHAR(128) NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    fencing_token BIGINT NOT NULL
);
INSERT INTO dashboard_schema_migration (version, applied_at_ms) VALUES (1, 0)
ON CONFLICT (version) DO NOTHING;
