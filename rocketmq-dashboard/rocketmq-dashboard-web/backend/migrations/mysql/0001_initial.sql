-- Initial schema and upgrades through version 4.
-- Version sections preserve existing databases and migration records.

-- migration: 1
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
    lease_name VARBINARY(128) PRIMARY KEY,
    holder_id VARBINARY(128) NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    fencing_token BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (1, 0);

-- migration: 2
ALTER TABLE dashboard_endpoint ADD COLUMN role VARCHAR(32) NOT NULL DEFAULT 'secondary';
UPDATE dashboard_endpoint SET role = 'primary' WHERE is_active = TRUE;
ALTER TABLE dashboard_endpoint ADD COLUMN is_enabled BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE dashboard_endpoint
    ADD COLUMN active_endpoint_type VARCHAR(32)
    GENERATED ALWAYS AS (CASE WHEN is_active THEN endpoint_type ELSE NULL END) STORED;
CREATE UNIQUE INDEX dashboard_endpoint_one_active_per_type_uq
    ON dashboard_endpoint(environment_id, active_endpoint_type);
INSERT IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (2, 0);

-- migration: 3
CREATE TABLE IF NOT EXISTS dashboard_history_sample (
    environment_id VARBINARY(36) NOT NULL,
    metric_name VARBINARY(64) NOT NULL,
    bucket_ms BIGINT NOT NULL,
    dimensions_json VARBINARY(512) NOT NULL,
    value DOUBLE NOT NULL,
    UNIQUE KEY dashboard_history_sample_query_idx(environment_id, metric_name, dimensions_json, bucket_ms),
    KEY dashboard_history_sample_retention_idx(environment_id, bucket_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (3, 0);

-- migration: 4
DROP TABLE IF EXISTS dashboard_audit_event;
DROP TABLE IF EXISTS dashboard_session;
CREATE TABLE dashboard_session (
    session_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
    token_hash BINARY(32) NOT NULL PRIMARY KEY,
    username VARBINARY(128) NOT NULL,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    last_seen_at_ms BIGINT NOT NULL,
    revoked_at_ms BIGINT NULL,
    KEY dashboard_session_username_active_idx(username, revoked_at_ms, expires_at_ms, created_at_ms, session_id),
    KEY dashboard_session_keyset_idx(created_at_ms DESC, session_id DESC),
    KEY dashboard_session_cleanup_idx(expires_at_ms, revoked_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE dashboard_audit_event (
    event_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    request_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor_kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor_username VARBINARY(128) NULL,
    action VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    resource_type VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    resource_name VARBINARY(255) NULL,
    environment_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    outcome VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    detail_json TEXT NULL,
    created_at_ms BIGINT NOT NULL,
    KEY dashboard_audit_event_keyset_idx(created_at_ms DESC, event_id DESC),
    KEY dashboard_audit_event_actor_idx(actor_username, created_at_ms DESC, event_id DESC),
    KEY dashboard_audit_event_retention_idx(created_at_ms),
    KEY dashboard_audit_event_action_time_idx(action, created_at_ms DESC, event_id DESC),
    KEY dashboard_audit_event_outcome_time_idx(outcome, created_at_ms DESC, event_id DESC),
    KEY dashboard_audit_event_environment_time_idx(environment_id, created_at_ms DESC, event_id DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (4, 0);
