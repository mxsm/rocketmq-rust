-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE postmortem_revisions
    ADD COLUMN conclusions JSONB NOT NULL DEFAULT '[]'::JSONB;

ALTER TABLE postmortems
    ADD COLUMN fingerprint TEXT,
    ADD COLUMN root_cause_code TEXT,
    ADD COLUMN affected_component TEXT;

CREATE INDEX postmortems_recurrence_lookup
    ON postmortems (
        tenant_id,
        cluster_id,
        fingerprint,
        root_cause_code,
        affected_component,
        updated_at DESC
    )
    WHERE status = 'published';

CREATE TABLE action_item_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    action_item_id UUID NOT NULL REFERENCES action_items(id),
    tenant_id UUID NOT NULL,
    previous_status TEXT NOT NULL,
    next_status TEXT NOT NULL,
    actor TEXT NOT NULL,
    verification TEXT,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX action_item_events_history
    ON action_item_events (action_item_id, sequence_id);

CREATE TABLE operator_todos (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    kind TEXT NOT NULL CHECK (kind IN ('action_item_due', 'knowledge_review_due')),
    aggregate_id UUID NOT NULL,
    title TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 1024),
    due_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'completed', 'dismissed')),
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (kind, aggregate_id)
);

CREATE INDEX operator_todos_open
    ON operator_todos (tenant_id, cluster_id, due_at, id)
    WHERE status = 'open';
