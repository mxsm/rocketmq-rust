-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Preserve every existing identity and reference. Separate partial indexes keep
-- tenant-wide NULL scope unique without raising the supported PostgreSQL syntax.
ALTER TABLE knowledge_items
    DROP CONSTRAINT knowledge_items_tenant_id_source_uri_source_version_content_key;

CREATE UNIQUE INDEX knowledge_items_cluster_source_content
    ON knowledge_items (tenant_id, cluster_id, source_uri, source_version, content_hash)
    WHERE cluster_id IS NOT NULL;

CREATE UNIQUE INDEX knowledge_items_global_source_content
    ON knowledge_items (tenant_id, source_uri, source_version, content_hash)
    WHERE cluster_id IS NULL;

-- Old free-text reasons do not establish a typed reviewer identity. Do not
-- backfill them by parsing names or by treating the document owner as reviewer.
ALTER TABLE knowledge_review_tasks
    ADD COLUMN event_kind TEXT CHECK (event_kind IN ('import', 'review', 'feedback')),
    ADD COLUMN actor_subject TEXT,
    ADD COLUMN from_review_status TEXT CHECK (from_review_status IN ('draft', 'in_review', 'validated', 'deprecated', 'expired')),
    ADD COLUMN to_review_status TEXT CHECK (to_review_status IN ('draft', 'in_review', 'validated', 'deprecated', 'expired')),
    ADD COLUMN item_content_hash TEXT CHECK (item_content_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    ADD COLUMN human_confirmed BOOLEAN,
    ADD COLUMN ai_generated BOOLEAN;

CREATE INDEX knowledge_review_tasks_history
    ON knowledge_review_tasks (knowledge_item_id, created_at DESC, id DESC);

CREATE INDEX knowledge_feedback_history
    ON knowledge_feedback (tenant_id, knowledge_item_id, created_at DESC, id DESC);
