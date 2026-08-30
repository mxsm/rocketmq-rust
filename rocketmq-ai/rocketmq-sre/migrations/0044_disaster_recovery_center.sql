-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE dr_plans (
    id UUID PRIMARY KEY,
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    cluster_id UUID REFERENCES clusters(id),
    subject TEXT NOT NULL CHECK (subject IN ('sre_control_plane', 'rocket_mq_cluster')),
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 256),
    plan_version INTEGER NOT NULL CHECK (plan_version > 0),
    owner_name TEXT NOT NULL CHECK (char_length(owner_name) BETWEEN 1 AND 256),
    rto_seconds BIGINT NOT NULL CHECK (rto_seconds > 0),
    rpo_seconds BIGINT NOT NULL CHECK (rpo_seconds >= 0),
    allowed_modes TEXT[] NOT NULL,
    required_sources TEXT[] NOT NULL,
    checkpoint_definitions JSONB NOT NULL CHECK (jsonb_typeof(checkpoint_definitions) = 'array'),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, name, plan_version),
    CHECK (
        (subject = 'rocket_mq_cluster' AND cluster_id IS NOT NULL)
        OR subject = 'sre_control_plane'
    ),
    CHECK (
        allowed_modes <@ ARRAY['readiness', 'tabletop', 'supervised_test']::TEXT[]
        AND cardinality(allowed_modes) > 0
    )
);

CREATE UNIQUE INDEX dr_plans_active_name
    ON dr_plans (tenant_id, name)
    WHERE active;

CREATE TABLE dr_backup_assets (
    id UUID PRIMARY KEY,
    plan_id UUID NOT NULL REFERENCES dr_plans(id),
    asset_kind TEXT NOT NULL
        CHECK (
            asset_kind IN (
                'postgre_sql',
                'object_storage',
                'oidc_configuration',
                'secret_references',
                'policy_bundle',
                'observability_backend',
                'control_plane_runtime',
                'connector_runtime',
                'executor_runtime',
                'execution_agent_runtime',
                'outbox_ledger',
                'effect_ledger',
                'quarantine_ledger',
                'audit_ledger',
                'rocket_mq_route',
                'rocket_mq_controller',
                'rocket_mq_broker_ha',
                'rocket_mq_store',
                'rocket_mq_rocks_db',
                'rocket_mq_tiered_store',
                'kubernetes_storage'
            )
        ),
    owner_name TEXT NOT NULL CHECK (char_length(owner_name) BETWEEN 1 AND 256),
    access_owner TEXT NOT NULL CHECK (char_length(access_owner) BETWEEN 1 AND 256),
    backup_locator_digest TEXT NOT NULL CHECK (backup_locator_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    encrypted BOOLEAN NOT NULL,
    last_backup_at TIMESTAMPTZ,
    restore_verified_at TIMESTAMPTZ,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (plan_id, asset_kind)
);

CREATE TABLE dr_exercises (
    id UUID PRIMARY KEY,
    plan_id UUID NOT NULL REFERENCES dr_plans(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    cluster_id UUID REFERENCES clusters(id),
    exercise_mode TEXT NOT NULL CHECK (exercise_mode IN ('readiness', 'tabletop', 'supervised_test')),
    execution_boundary TEXT NOT NULL CHECK (execution_boundary IN ('read_only', 'test_cluster_supervised')),
    exercise_state TEXT NOT NULL
        CHECK (
            exercise_state IN (
                'planned',
                'running',
                'awaiting_manual_confirmation',
                'completed',
                'failed',
                'cancelled'
            )
        ),
    target_rto_seconds BIGINT NOT NULL CHECK (target_rto_seconds > 0),
    target_rpo_seconds BIGINT NOT NULL CHECK (target_rpo_seconds >= 0),
    actual_rto_seconds BIGINT CHECK (actual_rto_seconds >= 0),
    actual_rpo_seconds BIGINT CHECK (actual_rpo_seconds >= 0),
    manual_checkpoint_count INTEGER NOT NULL DEFAULT 0 CHECK (manual_checkpoint_count >= 0),
    cleanup_complete BOOLEAN NOT NULL DEFAULT FALSE,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL CHECK (char_length(created_by) BETWEEN 1 AND 256),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (exercise_mode IN ('readiness', 'tabletop') AND execution_boundary = 'read_only')
        OR
        (exercise_mode = 'supervised_test' AND execution_boundary = 'test_cluster_supervised')
    ),
    CHECK (
        exercise_state NOT IN ('completed', 'failed', 'cancelled')
        OR completed_at IS NOT NULL
    )
);

CREATE INDEX dr_exercises_scope
    ON dr_exercises (tenant_id, region_id, cluster_id, exercise_state, created_at DESC);

CREATE TABLE dr_recovery_checkpoints (
    id UUID PRIMARY KEY,
    exercise_id UUID NOT NULL REFERENCES dr_exercises(id),
    sequence_number INTEGER NOT NULL CHECK (sequence_number >= 0),
    checkpoint_key TEXT NOT NULL CHECK (char_length(checkpoint_key) BETWEEN 1 AND 128),
    title TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 256),
    checkpoint_status TEXT NOT NULL
        CHECK (
            checkpoint_status IN (
                'pending',
                'running',
                'passed',
                'failed',
                'manual_confirmation_required',
                'skipped'
            )
        ),
    expected_duration_seconds BIGINT NOT NULL CHECK (expected_duration_seconds >= 0),
    actual_duration_seconds BIGINT CHECK (actual_duration_seconds >= 0),
    observed_rpo_seconds BIGINT CHECK (observed_rpo_seconds >= 0),
    manual_confirmation_required BOOLEAN NOT NULL,
    confirmed_by TEXT,
    cleanup_required BOOLEAN NOT NULL,
    cleanup_complete BOOLEAN NOT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    finding_codes TEXT[] NOT NULL DEFAULT '{}',
    note TEXT CHECK (note IS NULL OR char_length(note) <= 2048),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL,
    CHECK (
        checkpoint_status NOT IN ('passed', 'failed', 'skipped')
        OR completed_at IS NOT NULL
    ),
    CHECK (
        NOT manual_confirmation_required
        OR checkpoint_status = 'manual_confirmation_required'
        OR confirmed_by IS NOT NULL
    )
);

CREATE INDEX dr_recovery_checkpoints_exercise
    ON dr_recovery_checkpoints (exercise_id, sequence_number, observed_at);

CREATE TABLE dr_findings (
    id UUID PRIMARY KEY,
    exercise_id UUID NOT NULL REFERENCES dr_exercises(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    cluster_id UUID REFERENCES clusters(id),
    finding_code TEXT NOT NULL CHECK (char_length(finding_code) BETWEEN 1 AND 128),
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'blocker')),
    summary TEXT NOT NULL CHECK (char_length(summary) BETWEEN 1 AND 1024),
    remediation TEXT NOT NULL CHECK (char_length(remediation) BETWEEN 1 AND 2048),
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    finding_status TEXT NOT NULL CHECK (finding_status IN ('open', 'accepted', 'resolved')),
    created_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    UNIQUE (exercise_id, finding_code)
);

CREATE TABLE dr_action_items (
    id UUID PRIMARY KEY,
    finding_id UUID NOT NULL UNIQUE REFERENCES dr_findings(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    cluster_id UUID REFERENCES clusters(id),
    title TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 1024),
    owner_name TEXT,
    due_at TIMESTAMPTZ,
    action_status TEXT NOT NULL
        CHECK (
            action_status IN (
                'open',
                'assigned',
                'in_progress',
                'blocked',
                'completed',
                'reopened',
                'cancelled'
            )
        ),
    verification TEXT,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    CHECK (
        action_status <> 'completed'
        OR verification IS NOT NULL
        OR cardinality(evidence_ids) > 0
    )
);

CREATE INDEX dr_action_items_scope
    ON dr_action_items (tenant_id, cluster_id, action_status, due_at, id);
