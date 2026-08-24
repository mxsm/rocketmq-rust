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
