-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Connector sessions are mutable liveness records. They intentionally retain
-- identity subjects and issuers only; bearer tokens, client secrets, and TLS
-- key material are never persisted.
CREATE TABLE connector_channel_sessions (
    session_id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    connector_subject TEXT NOT NULL CHECK (char_length(connector_subject) BETWEEN 1 AND 512),
    connector_issuer TEXT NOT NULL CHECK (char_length(connector_issuer) BETWEEN 1 AND 1024),
    capability JSONB NOT NULL
        CHECK (capability @> '{"mutation_supported": false}'::JSONB),
    connector_observed_at TIMESTAMPTZ NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX connector_channel_sessions_cluster_liveness
    ON connector_channel_sessions (
        tenant_id,
        cluster_id,
        last_heartbeat_at DESC,
        registered_at DESC
    );

-- Commands and responses form the durable, replayable reverse-channel log.
-- Their rows are append-only; liveness and delivery state belong to sessions.
CREATE TABLE connector_channel_commands (
    session_id UUID NOT NULL REFERENCES connector_channel_sessions(session_id),
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    correlation_id UUID NOT NULL,
    command_kind TEXT NOT NULL CHECK (command_kind IN ('query', 'cancel')),
    command_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, sequence)
);

CREATE INDEX connector_channel_commands_correlation
    ON connector_channel_commands (session_id, correlation_id, sequence);

CREATE TABLE connector_channel_responses (
    session_id UUID NOT NULL,
    sequence BIGINT NOT NULL,
    correlation_id UUID NOT NULL,
    response_payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, sequence),
    FOREIGN KEY (session_id, sequence)
        REFERENCES connector_channel_commands(session_id, sequence)
);

CREATE INDEX connector_channel_responses_correlation
    ON connector_channel_responses (session_id, correlation_id, sequence);

CREATE FUNCTION reject_connector_channel_log_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'connector channel command and response logs are append-only';
END;
$$;

CREATE TRIGGER connector_channel_commands_append_only
    BEFORE UPDATE OR DELETE ON connector_channel_commands
    FOR EACH ROW EXECUTE FUNCTION reject_connector_channel_log_mutation();

CREATE TRIGGER connector_channel_responses_append_only
    BEFORE UPDATE OR DELETE ON connector_channel_responses
    FOR EACH ROW EXECUTE FUNCTION reject_connector_channel_log_mutation();
