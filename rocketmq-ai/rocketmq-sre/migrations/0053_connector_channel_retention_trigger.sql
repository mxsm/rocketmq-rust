-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- The shared append-only trigger must not inspect command-only columns when
-- PostgreSQL invokes it for a compaction receipt.
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
    THEN
        IF current_setting(
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
    END IF;

    RAISE EXCEPTION 'connector channel command, response, and compaction logs are append-only';
END;
$$;
