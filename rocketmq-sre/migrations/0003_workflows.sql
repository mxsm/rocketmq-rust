-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE conversations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    question TEXT NOT NULL CHECK (char_length(question) BETWEEN 1 AND 8192),
    resource TEXT,
    status TEXT NOT NULL CHECK (status IN ('active', 'promoted', 'closed')),
    investigation_id UUID,
    created_by_subject TEXT NOT NULL,
    created_by_display_name TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX conversations_scope_created
    ON conversations (tenant_id, cluster_id, created_at DESC, id);

CREATE TABLE investigations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    conversation_id UUID REFERENCES conversations(id),
    incident_id UUID,
    title TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 512),
    resource TEXT,
    symptom_family TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('open', 'collecting', 'diagnosing', 'needs_evidence', 'monitoring', 'promoted', 'closed')
    ),
    workflow_checkpoint JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_by_subject TEXT NOT NULL,
    created_by_display_name TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX investigations_scope_updated
    ON investigations (tenant_id, cluster_id, updated_at DESC, id);
CREATE INDEX investigations_fingerprint
    ON investigations (tenant_id, cluster_id, fingerprint, updated_at DESC);

CREATE TABLE sre_incidents (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    investigation_id UUID REFERENCES investigations(id),
    title TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 512),
    resource TEXT,
    symptom_family TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('new', 'collecting', 'diagnosing', 'needs_evidence', 'monitoring', 'resolved', 'escalated')
    ),
    workflow_checkpoint JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_by_subject TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX sre_incidents_scope_updated
    ON sre_incidents (tenant_id, cluster_id, updated_at DESC, id);
CREATE INDEX sre_incidents_fingerprint
    ON sre_incidents (tenant_id, cluster_id, fingerprint, updated_at DESC);

ALTER TABLE investigations
    ADD CONSTRAINT investigations_incident_fk
    FOREIGN KEY (incident_id) REFERENCES sre_incidents(id);
ALTER TABLE conversations
    ADD CONSTRAINT conversations_investigation_fk
    FOREIGN KEY (investigation_id) REFERENCES investigations(id);

CREATE TABLE incident_timeline (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    investigation_id UUID REFERENCES investigations(id),
    incident_id UUID REFERENCES sre_incidents(id),
    event_type TEXT NOT NULL,
    summary TEXT NOT NULL CHECK (char_length(summary) <= 2048),
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    correlation_id UUID NOT NULL,
    actor_subject TEXT NOT NULL,
    actor_display_name TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    CHECK (investigation_id IS NOT NULL OR incident_id IS NOT NULL)
);

CREATE INDEX incident_timeline_investigation
    ON incident_timeline (tenant_id, cluster_id, investigation_id, sequence_id);
CREATE INDEX incident_timeline_incident
    ON incident_timeline (tenant_id, cluster_id, incident_id, sequence_id);

CREATE TABLE workflow_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_payload JSONB NOT NULL,
    correlation_id UUID NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX workflow_events_stream
    ON workflow_events (tenant_id, cluster_id, sequence_id);
