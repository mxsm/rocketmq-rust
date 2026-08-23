CREATE TABLE IF NOT EXISTS dashboard_schema_migration (
    version BIGINT PRIMARY KEY,
    applied_at_ms BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS dashboard_environment (
    environment_id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(128) UNIQUE NOT NULL,
    use_vip_channel BOOLEAN NOT NULL,
    use_tls BOOLEAN NOT NULL,
    revision BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    updated_by VARCHAR(128) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dashboard_endpoint (
    endpoint_id VARCHAR(36) PRIMARY KEY,
    environment_id VARCHAR(36) NOT NULL,
    endpoint_type VARCHAR(32) NOT NULL,
    address VARCHAR(512) NOT NULL,
    is_active BOOLEAN NOT NULL,
    sort_order INT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CONSTRAINT dashboard_endpoint_environment_fk FOREIGN KEY(environment_id)
        REFERENCES dashboard_environment(environment_id) ON DELETE CASCADE,
    UNIQUE KEY dashboard_endpoint_address_uq(environment_id, endpoint_type, address),
    KEY dashboard_endpoint_environment_type_idx(environment_id, endpoint_type, sort_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS consumer_monitor_rule (
    environment_id VARCHAR(36) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    min_count INT NOT NULL,
    max_diff_total BIGINT NOT NULL,
    revision BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY(environment_id, consumer_group),
    CONSTRAINT consumer_monitor_rule_environment_fk FOREIGN KEY(environment_id)
        REFERENCES dashboard_environment(environment_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dashboard_metric_sample (
    environment_id VARCHAR(36) NOT NULL,
    metric_name VARCHAR(64) NOT NULL,
    resource_name VARCHAR(255) NOT NULL,
    bucket_ms BIGINT NOT NULL,
    value DOUBLE NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY(environment_id, metric_name, resource_name, bucket_ms),
    CONSTRAINT dashboard_metric_sample_environment_fk FOREIGN KEY(environment_id)
        REFERENCES dashboard_environment(environment_id) ON DELETE CASCADE,
    KEY dashboard_metric_sample_query_idx(environment_id, metric_name, resource_name, bucket_ms),
    KEY dashboard_metric_sample_retention_idx(bucket_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dashboard_session (
    session_id_hash VARCHAR(128) PRIMARY KEY,
    username VARCHAR(128) NOT NULL,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    last_seen_at_ms BIGINT NOT NULL,
    revoked_at_ms BIGINT NULL,
    KEY dashboard_session_expiry_idx(expires_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dashboard_audit_event (
    event_id VARCHAR(36) PRIMARY KEY,
    environment_id VARCHAR(36) NULL,
    actor VARCHAR(128) NOT NULL,
    action VARCHAR(128) NOT NULL,
    resource_type VARCHAR(64) NOT NULL,
    resource_name VARCHAR(255) NULL,
    before_payload TEXT NULL,
    after_payload TEXT NULL,
    request_id VARCHAR(64) NULL,
    created_at_ms BIGINT NOT NULL,
    CONSTRAINT dashboard_audit_event_environment_fk FOREIGN KEY(environment_id)
        REFERENCES dashboard_environment(environment_id) ON DELETE SET NULL,
    KEY dashboard_audit_event_created_idx(created_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dashboard_task_lease (
    lease_name VARCHAR(128) PRIMARY KEY,
    holder_id VARCHAR(128) NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    fencing_token BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (1, 0);
