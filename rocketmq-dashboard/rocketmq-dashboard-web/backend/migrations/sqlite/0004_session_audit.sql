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
