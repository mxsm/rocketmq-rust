-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE diagnosis_revisions (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    status TEXT NOT NULL,
    rule_result JSONB NOT NULL,
    hypotheses JSONB NOT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    primary_model_invocation_id UUID,
    execution_eligible BOOLEAN NOT NULL DEFAULT FALSE
        CHECK (execution_eligible = FALSE),
    partial BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (incident_id, revision)
);

CREATE TABLE inspection_runs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    template TEXT NOT NULL CHECK (template IN ('cluster_health', 'consumer', 'broker', 'telemetry')),
    status TEXT NOT NULL CHECK (
        status IN ('scheduled', 'running', 'needs_evidence', 'completed', 'failed', 'cancelled')
    ),
    schedule TEXT,
    scope JSONB NOT NULL DEFAULT '{}'::JSONB,
    workflow_checkpoint JSONB NOT NULL DEFAULT '{}'::JSONB,
    finding_count INTEGER NOT NULL DEFAULT 0 CHECK (finding_count >= 0),
    partial BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX inspection_runs_scope
    ON inspection_runs (tenant_id, cluster_id, created_at DESC, id);
CREATE UNIQUE INDEX inspection_runs_nonoverlap
    ON inspection_runs (tenant_id, cluster_id, template, COALESCE(schedule, 'immediate'))
    WHERE status IN ('scheduled', 'running');

CREATE TABLE recommendations (
    id UUID PRIMARY KEY,
    inspection_run_id UUID NOT NULL REFERENCES inspection_runs(id),
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    rationale TEXT NOT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    status TEXT NOT NULL CHECK (
        status IN ('open', 'acknowledged', 'assigned', 'dismissed', 'resolved', 'promoted')
    ),
    assignee TEXT,
    investigation_id UUID REFERENCES investigations(id),
    incident_id UUID REFERENCES sre_incidents(id),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX recommendations_scope
    ON recommendations (tenant_id, cluster_id, status, updated_at DESC);

CREATE TABLE diagnostic_pack_runs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    incident_id UUID REFERENCES sre_incidents(id),
    inspection_run_id UUID REFERENCES inspection_runs(id),
    pack_id TEXT NOT NULL,
    pack_version TEXT NOT NULL,
    input_evidence_ids UUID[] NOT NULL,
    output JSONB NOT NULL,
    partial BOOLEAN NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    CHECK (incident_id IS NOT NULL OR inspection_run_id IS NOT NULL)
);

CREATE INDEX diagnostic_pack_runs_scope
    ON diagnostic_pack_runs (tenant_id, cluster_id, pack_id, completed_at DESC);
