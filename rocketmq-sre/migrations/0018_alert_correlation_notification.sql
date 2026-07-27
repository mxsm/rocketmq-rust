-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE alert_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    source TEXT NOT NULL CHECK (
        source IN (
            'alertmanager',
            'kubernetes_event',
            'health_probe',
            'operator_query',
            'inspection',
            'deployment',
            'synthetic_probe'
        )
    ),
    source_event_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    correlation_key JSONB NOT NULL,
    affected_resource JSONB NOT NULL,
    symptom_family TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    status TEXT NOT NULL CHECK (status IN ('firing', 'resolved')),
    summary TEXT NOT NULL CHECK (char_length(summary) BETWEEN 1 AND 2048),
    labels JSONB NOT NULL DEFAULT '{}'::JSONB,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    occurrence_count INTEGER NOT NULL DEFAULT 0 CHECK (occurrence_count >= 0),
    last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0),
    first_occurred_at TIMESTAMPTZ NOT NULL,
    last_occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, cluster_id, source, source_event_id)
);

CREATE INDEX alert_events_correlation
    ON alert_events (
        tenant_id,
        cluster_id,
        symptom_family,
        last_occurred_at DESC,
        id
    );
CREATE INDEX alert_events_fingerprint
    ON alert_events (tenant_id, cluster_id, fingerprint, last_occurred_at DESC);

CREATE TABLE alert_occurrences (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_id UUID NOT NULL REFERENCES alert_events(id),
    source_occurrence_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('firing', 'resolved')),
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    UNIQUE (alert_id, source_occurrence_id)
);

CREATE INDEX alert_occurrences_alert
    ON alert_occurrences (alert_id, sequence_id);

CREATE TABLE incident_alerts (
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    alert_id UUID NOT NULL REFERENCES alert_events(id),
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    linked_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (incident_id, alert_id)
);

CREATE INDEX incident_alerts_scope
    ON incident_alerts (tenant_id, cluster_id, incident_id, linked_at);

CREATE TABLE incident_relations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    from_incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    to_incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    relation_kind TEXT NOT NULL CHECK (
        relation_kind IN (
            'duplicate',
            'same_root_cause',
            'parent',
            'child',
            'recurrence',
            'change_regression'
        )
    ),
    reason_code TEXT NOT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (from_incident_id <> to_incident_id),
    UNIQUE (tenant_id, cluster_id, from_incident_id, to_incident_id, relation_kind)
);

CREATE INDEX incident_relations_from
    ON incident_relations (tenant_id, cluster_id, from_incident_id, created_at DESC);
CREATE INDEX incident_relations_to
    ON incident_relations (tenant_id, cluster_id, to_incident_id, created_at DESC);

CREATE TABLE notification_targets (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    name TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('signed_webhook', 'email', 'pager')),
    endpoint TEXT NOT NULL,
    secret_reference TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, cluster_id, name)
);

CREATE TABLE on_call_owners (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    resource_selector TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    target_ids UUID[] NOT NULL DEFAULT '{}',
    source TEXT NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ,
    CHECK (valid_until IS NULL OR valid_until > valid_from)
);

CREATE INDEX on_call_owners_scope
    ON on_call_owners (tenant_id, cluster_id, valid_from DESC);

CREATE TABLE notification_outbox (
    id UUID PRIMARY KEY,
    target_id UUID NOT NULL REFERENCES notification_targets(id),
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    delivery_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'delivering', 'delivered', 'retry_scheduled', 'failed')
    ),
    sanitized_summary TEXT NOT NULL CHECK (char_length(sanitized_summary) <= 2048),
    deep_link TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ,
    last_error_code TEXT,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, delivery_key)
);

CREATE INDEX notification_outbox_pending
    ON notification_outbox (next_attempt_at, created_at, id)
    WHERE status IN ('pending', 'retry_scheduled');
