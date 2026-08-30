-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE postmortems (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    status TEXT NOT NULL CHECK (
        status IN ('draft', 'in_review', 'confirmed', 'published', 'archived')
    ),
    current_revision INTEGER NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
    confirmed_by TEXT,
    confirmed_at TIMESTAMPTZ,
    published_knowledge_item_id UUID REFERENCES knowledge_items(id),
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, incident_id),
    CHECK (
        (confirmed_by IS NULL AND confirmed_at IS NULL)
        OR (confirmed_by IS NOT NULL AND confirmed_at IS NOT NULL)
    )
);

CREATE INDEX postmortems_scope
    ON postmortems (tenant_id, cluster_id, status, updated_at DESC, id);

CREATE TABLE postmortem_revisions (
    id UUID PRIMARY KEY,
    postmortem_id UUID NOT NULL REFERENCES postmortems(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    summary TEXT NOT NULL,
    impact TEXT NOT NULL,
    detection TEXT NOT NULL,
    timeline JSONB NOT NULL,
    root_causes JSONB NOT NULL,
    contributing_factors JSONB NOT NULL DEFAULT '[]'::JSONB,
    recovery TEXT NOT NULL,
    effective_actions JSONB NOT NULL DEFAULT '[]'::JSONB,
    ineffective_actions JSONB NOT NULL DEFAULT '[]'::JSONB,
    evidence_ids UUID[] NOT NULL,
    model_invocation_id UUID REFERENCES model_invocations(id),
    edited_by TEXT NOT NULL,
    human_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (postmortem_id, revision)
);

CREATE INDEX postmortem_revisions_history
    ON postmortem_revisions (postmortem_id, revision DESC);

CREATE TABLE action_items (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    postmortem_id UUID NOT NULL REFERENCES postmortems(id),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    title TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 1024),
    owner_name TEXT,
    due_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (
        status IN ('open', 'assigned', 'in_progress', 'blocked', 'completed', 'reopened', 'cancelled')
    ),
    verification TEXT,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    execution_journal JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    CHECK (
        status <> 'completed'
        OR verification IS NOT NULL
        OR cardinality(evidence_ids) > 0
    )
);

CREATE INDEX action_items_scope
    ON action_items (tenant_id, cluster_id, status, due_at, id);

CREATE TABLE incident_recurrences (
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    previous_incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    postmortem_id UUID NOT NULL REFERENCES postmortems(id),
    fingerprint TEXT NOT NULL,
    root_cause_code TEXT NOT NULL,
    affected_component TEXT NOT NULL,
    matched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (incident_id, previous_incident_id, postmortem_id),
    CHECK (incident_id <> previous_incident_id)
);

CREATE INDEX incident_recurrences_previous
    ON incident_recurrences (previous_incident_id, matched_at DESC);
