-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE model_profiles (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    profile_name TEXT NOT NULL,
    provider_family TEXT NOT NULL,
    protocol_family TEXT NOT NULL,
    model_family TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_revision TEXT NOT NULL,
    endpoint_instance TEXT NOT NULL,
    region TEXT NOT NULL,
    data_residency TEXT NOT NULL,
    data_classes JSONB NOT NULL,
    capabilities JSONB NOT NULL,
    priority INTEGER NOT NULL,
    credential_ref TEXT NOT NULL,
    credential_owner TEXT NOT NULL CHECK (credential_owner IN ('gateway', 'adapter')),
    credential_version_fingerprint TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    health TEXT NOT NULL CHECK (health IN ('unknown', 'healthy', 'degraded', 'quarantined', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, profile_name)
);

CREATE INDEX model_profiles_route
    ON model_profiles (tenant_id, enabled, health, priority, id);

CREATE TABLE model_invocations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    incident_id UUID REFERENCES sre_incidents(id),
    diagnosis_revision_id UUID REFERENCES diagnosis_revisions(id),
    parent_invocation_id UUID REFERENCES model_invocations(id),
    purpose TEXT NOT NULL CHECK (purpose IN ('primary_diagnosis', 'critic', 'planner', 'summary', 'eval')),
    requested_profile_id UUID NOT NULL REFERENCES model_profiles(id),
    actual_profile_id UUID NOT NULL REFERENCES model_profiles(id),
    provider_family TEXT NOT NULL,
    model_family TEXT NOT NULL,
    model_revision TEXT NOT NULL,
    endpoint_instance TEXT NOT NULL,
    fallback_chain UUID[] NOT NULL DEFAULT '{}',
    prompt_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cost_micros BIGINT,
    rationale TEXT NOT NULL,
    error_code TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX model_invocations_incident
    ON model_invocations (tenant_id, cluster_id, incident_id, started_at DESC);

ALTER TABLE diagnosis_revisions
    ADD CONSTRAINT diagnosis_revisions_primary_model_invocation_fk
    FOREIGN KEY (primary_model_invocation_id) REFERENCES model_invocations(id);

CREATE TABLE provider_health_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id UUID NOT NULL,
    profile_id UUID NOT NULL REFERENCES model_profiles(id),
    health TEXT NOT NULL,
    capability JSONB NOT NULL,
    credential_version_fingerprint TEXT,
    observed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX provider_health_events_profile
    ON provider_health_events (tenant_id, profile_id, observed_at DESC);
