-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Phase 01 replaced the early connector session sketch with the durable
-- reverse-channel log. Keep the legacy nullable reference for migration
-- compatibility and add the authoritative channel session reference.
ALTER TABLE source_capability_history
    ADD COLUMN connector_channel_session_id UUID
        REFERENCES connector_channel_sessions(session_id);

CREATE INDEX source_capability_history_channel_session
    ON source_capability_history (
        connector_channel_session_id,
        observed_at DESC,
        sequence_id DESC
    );
