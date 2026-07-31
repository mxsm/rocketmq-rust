-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE workflow_event_entries (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    source_kind TEXT NOT NULL CHECK (
        source_kind IN (
            'alert',
            'manual_issue',
            'scheduled_inspection',
            'change_event',
            'external_integration'
        )
    ),
    idempotency_key TEXT NOT NULL
        CHECK (char_length(idempotency_key) BETWEEN 1 AND 256),
    request_hash TEXT NOT NULL
        CHECK (request_hash ~ '^sha256:[0-9a-f]{64}$'),
    target_kind TEXT NOT NULL CHECK (
        target_kind IN ('investigation', 'incident', 'inspection_run')
    ),
    target_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    actor_subject TEXT NOT NULL
        CHECK (char_length(actor_subject) BETWEEN 1 AND 512),
    occurred_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, cluster_id, source_kind, idempotency_key)
);

CREATE INDEX workflow_event_entries_target
    ON workflow_event_entries (
        tenant_id,
        cluster_id,
        target_kind,
        target_id,
        accepted_at DESC
    );

CREATE INDEX workflow_event_entries_recent
    ON workflow_event_entries (
        tenant_id,
        cluster_id,
        source_kind,
        accepted_at DESC,
        id
    );
