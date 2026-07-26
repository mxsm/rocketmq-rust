-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE cluster_capability_snapshots
    ADD COLUMN tool_surface_digest TEXT;

UPDATE cluster_capability_snapshots
SET tool_surface_digest = COALESCE(
    manifest ->> 'tool_surface_digest',
    -- Legacy snapshots did not persist this value. A reserved digest forces
    -- the next handshake to refresh the capability surface instead of
    -- treating an inferred value as compatible.
    'sha256:0000000000000000000000000000000000000000000000000000000000000000'
)
WHERE tool_surface_digest IS NULL;

ALTER TABLE cluster_capability_snapshots
    ALTER COLUMN tool_surface_digest SET NOT NULL;

ALTER TABLE cluster_capability_snapshots
    ADD CONSTRAINT cluster_capability_snapshots_tool_surface_digest_format
    CHECK (tool_surface_digest ~ '^sha256:[0-9A-Fa-f]{64}$');

CREATE INDEX cluster_capability_snapshots_surface_pin
    ON cluster_capability_snapshots (cluster_id, created_at ASC, id ASC);
