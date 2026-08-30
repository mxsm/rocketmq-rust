-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Sequence allocation must remain monotonic after completed command/response
-- payloads are compacted out of the live reverse-channel log.
ALTER TABLE connector_channel_sessions
    ADD COLUMN next_sequence BIGINT NOT NULL DEFAULT 1
        CHECK (next_sequence > 0),
    ADD COLUMN compacted_through_sequence BIGINT NOT NULL DEFAULT 0
        CHECK (compacted_through_sequence >= 0),
    ADD COLUMN last_compacted_at TIMESTAMPTZ;

UPDATE connector_channel_sessions AS session
SET next_sequence = state.highest_sequence + 1
FROM (
    SELECT session_id, COALESCE(MAX(sequence), 0) AS highest_sequence
    FROM connector_channel_commands
    GROUP BY session_id
) AS state
WHERE state.session_id = session.session_id;

ALTER TABLE connector_channel_sessions
    ADD CONSTRAINT connector_channel_sequence_frontier_valid
    CHECK (compacted_through_sequence < next_sequence);

-- A compaction receipt preserves the bounded audit material for every
-- contiguous prefix removed from the operational command/response tables.
-- It never contains credentials or raw message bodies.
CREATE TABLE connector_channel_compaction_receipts (
    receipt_id UUID PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES connector_channel_sessions(session_id),
    from_sequence BIGINT NOT NULL CHECK (from_sequence > 0),
    through_sequence BIGINT NOT NULL CHECK (through_sequence >= from_sequence),
    command_count BIGINT NOT NULL CHECK (command_count > 0),
    response_count BIGINT NOT NULL CHECK (response_count = command_count),
    correlation_count BIGINT NOT NULL
        CHECK (correlation_count > 0 AND correlation_count <= command_count),
    material_hash TEXT NOT NULL
        CHECK (material_hash ~ '^sha256:[0-9a-f]{64}$'),
    retention_reason TEXT NOT NULL
        CHECK (retention_reason IN ('age', 'pressure', 'inactive_session')),
    compacted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (session_id, from_sequence, through_sequence)
);

CREATE INDEX connector_channel_compaction_receipts_session
    ON connector_channel_compaction_receipts (
        session_id,
        through_sequence DESC,
        compacted_at DESC
    );

-- Command and response payloads remain immutable during normal operation.
-- DELETE is allowed only for a range already covered by an append-only
-- receipt and the transaction-local session guard set by the repository's
-- bounded retention path.
CREATE OR REPLACE FUNCTION reject_connector_channel_log_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
       AND TG_TABLE_NAME IN (
           'connector_channel_commands',
           'connector_channel_responses'
       )
       AND current_setting(
           'rocketmq_sre.connector_retention_session',
           TRUE
       ) = OLD.session_id::TEXT
       AND EXISTS (
           SELECT 1
           FROM connector_channel_sessions AS session
           JOIN connector_channel_compaction_receipts AS receipt
             ON receipt.session_id = session.session_id
           WHERE session.session_id = OLD.session_id
             AND session.compacted_through_sequence >= OLD.sequence
             AND receipt.from_sequence <= OLD.sequence
             AND receipt.through_sequence >= OLD.sequence
       )
    THEN
        RETURN OLD;
    END IF;

    RAISE EXCEPTION 'connector channel command, response, and compaction logs are append-only';
END;
$$;

CREATE TRIGGER connector_channel_compaction_receipts_append_only
    BEFORE UPDATE OR DELETE ON connector_channel_compaction_receipts
    FOR EACH ROW EXECUTE FUNCTION reject_connector_channel_log_mutation();
