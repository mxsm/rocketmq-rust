-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE autonomy_lifecycle_events
    ADD COLUMN owner_approval_ref TEXT;

ALTER TABLE autonomy_lifecycle_events
    ADD CONSTRAINT autonomy_lifecycle_events_owner_approval_ref_format
    CHECK (
        owner_approval_ref IS NULL
        OR (
            char_length(owner_approval_ref) BETWEEN 14 AND 160
            AND owner_approval_ref ~ '^approval://[a-z0-9][a-z0-9._/-]*[a-z0-9]$'
            AND position('..' IN substring(owner_approval_ref FROM 12)) = 0
            AND position('//' IN substring(owner_approval_ref FROM 12)) = 0
        )
    );
