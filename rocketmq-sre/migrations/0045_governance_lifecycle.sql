-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE governance_artifacts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    object_kind TEXT NOT NULL
        CHECK (
            object_kind IN (
                'data_policy',
                'evidence_policy',
                'prompt',
                'knowledge',
                'model_profile',
                'provider_profile',
                'diagnostic_pack',
                'policy_bundle',
                'action_descriptor',
                'runbook',
                'integration_adapter'
            )
        ),
    logical_key TEXT NOT NULL CHECK (char_length(logical_key) BETWEEN 1 AND 256),
    owner_name TEXT NOT NULL CHECK (char_length(owner_name) BETWEEN 1 AND 256),
    reviewer_name TEXT NOT NULL CHECK (char_length(reviewer_name) BETWEEN 1 AND 256),
    current_version_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, object_kind, logical_key),
    CHECK (owner_name <> reviewer_name)
);

CREATE TABLE governance_versions (
    id UUID PRIMARY KEY,
    artifact_id UUID NOT NULL REFERENCES governance_artifacts(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    version_name TEXT NOT NULL CHECK (char_length(version_name) BETWEEN 1 AND 128),
    content_digest TEXT NOT NULL CHECK (content_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    signature_algorithm TEXT,
    signing_key_id TEXT,
    signature_value TEXT,
    lifecycle_state TEXT NOT NULL
        CHECK (
            lifecycle_state IN (
                'draft',
                'review',
                'active',
                'deprecated',
                'quarantined',
                'retired'
            )
        ),
    applicable_components JSONB NOT NULL CHECK (jsonb_typeof(applicable_components) = 'array'),
    applicable_version_range TEXT NOT NULL CHECK (char_length(applicable_version_range) BETWEEN 1 AND 256),
    dependencies JSONB NOT NULL CHECK (jsonb_typeof(dependencies) = 'array'),
    review_due_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    replacement_version_id UUID REFERENCES governance_versions(id),
    rollback_version_id UUID REFERENCES governance_versions(id),
    created_by TEXT NOT NULL CHECK (char_length(created_by) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (artifact_id, version_name),
    CHECK (
        lifecycle_state <> 'active'
        OR (
            signature_algorithm IS NOT NULL
            AND signing_key_id IS NOT NULL
            AND signature_value IS NOT NULL
        )
    ),
    CHECK (expires_at IS NULL OR expires_at > created_at),
    CHECK (replacement_version_id IS NULL OR replacement_version_id <> id),
    CHECK (rollback_version_id IS NULL OR rollback_version_id <> id)
);

ALTER TABLE governance_artifacts
    ADD CONSTRAINT governance_artifacts_current_version_fk
    FOREIGN KEY (current_version_id) REFERENCES governance_versions(id);

CREATE UNIQUE INDEX governance_versions_one_active
    ON governance_versions (artifact_id)
    WHERE lifecycle_state = 'active';

CREATE INDEX governance_versions_admission
    ON governance_versions (tenant_id, lifecycle_state, review_due_at, expires_at);

CREATE OR REPLACE FUNCTION enforce_governance_version_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.artifact_id <> OLD.artifact_id
        OR NEW.tenant_id <> OLD.tenant_id
        OR NEW.version_name <> OLD.version_name
        OR NEW.content_digest <> OLD.content_digest
        OR NEW.applicable_components <> OLD.applicable_components
        OR NEW.applicable_version_range <> OLD.applicable_version_range
        OR NEW.dependencies <> OLD.dependencies
        OR NEW.review_due_at <> OLD.review_due_at
        OR NEW.expires_at IS DISTINCT FROM OLD.expires_at
        OR NEW.created_by <> OLD.created_by
        OR NEW.created_at <> OLD.created_at
    THEN
        RAISE EXCEPTION 'governance version content is immutable';
    END IF;

    IF NOT (
        (OLD.lifecycle_state = 'draft' AND NEW.lifecycle_state IN ('review', 'retired'))
        OR (OLD.lifecycle_state = 'review' AND NEW.lifecycle_state IN ('draft', 'active', 'quarantined', 'retired'))
        OR (OLD.lifecycle_state = 'active' AND NEW.lifecycle_state IN ('deprecated', 'quarantined'))
        OR (OLD.lifecycle_state = 'deprecated' AND NEW.lifecycle_state IN ('review', 'quarantined', 'retired'))
        OR (OLD.lifecycle_state = 'quarantined' AND NEW.lifecycle_state IN ('review', 'retired'))
    ) THEN
        RAISE EXCEPTION 'invalid governance lifecycle transition';
    END IF;

    IF NEW.lifecycle_state = 'active'
        AND (
            NEW.signature_algorithm IS NULL
            OR NEW.signing_key_id IS NULL
            OR NEW.signature_value IS NULL
        )
    THEN
        RAISE EXCEPTION 'active governance versions require a signature';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER governance_version_update_guard
BEFORE UPDATE ON governance_versions
FOR EACH ROW
EXECUTE FUNCTION enforce_governance_version_update();

CREATE TABLE governance_impacts (
    version_id UUID NOT NULL REFERENCES governance_versions(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    cluster_id UUID REFERENCES clusters(id),
    impact_kind TEXT NOT NULL
        CHECK (
            impact_kind IN (
                'cluster',
                'diagnostic_pack',
                'action_plan',
                'action',
                'incident',
                'model_route',
                'integration'
            )
        ),
    reference_id TEXT NOT NULL CHECK (char_length(reference_id) BETWEEN 1 AND 256),
    label TEXT NOT NULL CHECK (char_length(label) BETWEEN 1 AND 512),
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (version_id, impact_kind, reference_id)
);

CREATE INDEX governance_impacts_scope
    ON governance_impacts (tenant_id, cluster_id, impact_kind, observed_at DESC);

CREATE TABLE governance_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    artifact_id UUID NOT NULL REFERENCES governance_artifacts(id),
    version_id UUID NOT NULL REFERENCES governance_versions(id),
    from_state TEXT
        CHECK (
            from_state IS NULL
            OR from_state IN ('draft', 'review', 'active', 'deprecated', 'quarantined', 'retired')
        ),
    to_state TEXT NOT NULL
        CHECK (to_state IN ('draft', 'review', 'active', 'deprecated', 'quarantined', 'retired')),
    actor_name TEXT NOT NULL CHECK (char_length(actor_name) BETWEEN 1 AND 256),
    actor_kind TEXT NOT NULL CHECK (actor_kind IN ('human', 'service', 'model')),
    reason TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 2048),
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX governance_events_export
    ON governance_events (tenant_id, artifact_id, version_id, occurred_at, sequence_id);

CREATE TABLE governance_admissions (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    cluster_id UUID REFERENCES clusters(id),
    access_path TEXT NOT NULL CHECK (access_path IN ('read_only', 'high_privilege')),
    required_version_ids UUID[] NOT NULL,
    allowed BOOLEAN NOT NULL,
    degraded BOOLEAN NOT NULL,
    reason_codes TEXT[] NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    CHECK (access_path <> 'high_privilege' OR NOT degraded)
);

CREATE INDEX governance_admissions_scope
    ON governance_admissions (tenant_id, cluster_id, access_path, evaluated_at DESC);
