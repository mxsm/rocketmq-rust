-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE knowledge_items
    ADD COLUMN valid_from TIMESTAMPTZ,
    ADD COLUMN valid_until TIMESTAMPTZ,
    ADD CONSTRAINT knowledge_items_valid_range
        CHECK (valid_until IS NULL OR valid_from IS NULL OR valid_until > valid_from);

CREATE INDEX knowledge_items_validity
    ON knowledge_items (tenant_id, valid_from, valid_until, review_due_at);
