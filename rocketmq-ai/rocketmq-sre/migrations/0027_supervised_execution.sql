-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Phase 01 deliberately forced every diagnosis to be read-only. Phase 03
-- permits execution eligibility only when the immutable diagnosis revision is
-- bound to a real primary model invocation.
ALTER TABLE diagnosis_revisions
    DROP CONSTRAINT IF EXISTS diagnosis_revisions_execution_eligible_check;

ALTER TABLE diagnosis_revisions
    ADD CONSTRAINT diagnosis_revisions_execution_eligible_requires_model
    CHECK (NOT execution_eligible OR primary_model_invocation_id IS NOT NULL);

CREATE TABLE action_plans (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    diagnosis_revision_id UUID NOT NULL REFERENCES diagnosis_revisions(id),
    primary_model_invocation_id UUID NOT NULL REFERENCES model_invocations(id),
    version INTEGER NOT NULL CHECK (version > 0),
    plan_hash TEXT NOT NULL UNIQUE CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    evidence_hash TEXT NOT NULL CHECK (evidence_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    risk TEXT NOT NULL CHECK (risk IN ('r1', 'r2')),
    status TEXT NOT NULL CHECK (
        status IN (
            'draft',
            'needs_critic',
            'ready_for_approval',
            'in_review',
            'approved',
            'rejected',
            'expired',
            'superseded'
        )
    ),
    request_snapshot JSONB NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    submitted_at TIMESTAMPTZ,
    CHECK (expires_at > created_at),
    CHECK (submitted_at IS NULL OR submitted_at >= created_at),
    UNIQUE (incident_id, version)
);

CREATE INDEX action_plans_scope_status
    ON action_plans (tenant_id, cluster_id, status, created_at DESC, id);

CREATE TABLE policy_decisions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    policy_version TEXT NOT NULL,
    input_hash TEXT NOT NULL CHECK (input_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    effect TEXT NOT NULL CHECK (effect IN ('allow', 'deny', 'require_approval')),
    reason_codes TEXT[] NOT NULL DEFAULT '{}',
    evaluated_by TEXT NOT NULL,
    decision_snapshot JSONB NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX policy_decisions_plan
    ON policy_decisions (tenant_id, cluster_id, plan_id, evaluated_at DESC);

CREATE TABLE approvals (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    requester_subject TEXT NOT NULL,
    approver_subject TEXT NOT NULL,
    approver_role TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected')),
    reason TEXT NOT NULL,
    approval_snapshot JSONB NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > decided_at),
    CHECK (requester_subject <> approver_subject)
);

CREATE INDEX approvals_plan
    ON approvals (tenant_id, cluster_id, plan_id, decided_at DESC);

CREATE TABLE critic_reviews (
    id UUID PRIMARY KEY,
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    primary_invocation_id UUID NOT NULL REFERENCES model_invocations(id),
    critic_invocation_id UUID NOT NULL REFERENCES model_invocations(id),
    primary_model_family TEXT NOT NULL,
    critic_model_family TEXT NOT NULL,
    critic_provider TEXT NOT NULL,
    critic_profile TEXT NOT NULL,
    critic_model_revision TEXT NOT NULL,
    endpoint_instance TEXT NOT NULL,
    conclusion TEXT NOT NULL CHECK (conclusion IN ('accept', 'needs_revision', 'reject')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'valid', 'invalid', 'unavailable', 'conflict')),
    review_hash TEXT NOT NULL UNIQUE CHECK (review_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    review_snapshot JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (primary_invocation_id <> critic_invocation_id)
);

CREATE INDEX critic_reviews_plan
    ON critic_reviews (plan_id, created_at DESC);

CREATE TABLE executions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    correlation_id UUID NOT NULL,
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    resource_key TEXT NOT NULL,
    action_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (
        state IN (
            'pending',
            'prechecking',
            'intent_persisted',
            'applying',
            'unknown',
            'reconciling',
            'verifying',
            'compensating',
            'succeeded',
            'rolled_back',
            'escalated'
        )
    ),
    request_snapshot JSONB NOT NULL,
    requested_by TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (completed_at IS NULL OR completed_at >= started_at)
);

CREATE INDEX executions_scope_state
    ON executions (tenant_id, cluster_id, state, updated_at DESC, id);

CREATE TABLE resource_locks (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    resource_key TEXT NOT NULL,
    action_id TEXT NOT NULL,
    holder_execution_id UUID NOT NULL REFERENCES executions(id),
    acquired_at TIMESTAMPTZ NOT NULL,
    renewed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ,
    release_reason TEXT,
    CHECK (expires_at > acquired_at),
    CHECK (
        (released_at IS NULL AND release_reason IS NULL)
        OR (released_at IS NOT NULL AND release_reason IS NOT NULL)
    )
);

CREATE UNIQUE INDEX resource_locks_active
    ON resource_locks (tenant_id, cluster_id, resource_key, action_id)
    WHERE released_at IS NULL;

CREATE INDEX resource_locks_expiry
    ON resource_locks (expires_at)
    WHERE released_at IS NULL;

CREATE TABLE resource_quarantines (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    resource_key TEXT NOT NULL,
    action_id TEXT,
    reason_code TEXT NOT NULL,
    source_execution_id UUID REFERENCES executions(id),
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    cleared_by TEXT,
    clear_reason TEXT,
    clear_evidence_ids UUID[] NOT NULL DEFAULT '{}',
    cleared_at TIMESTAMPTZ,
    CHECK (
        (cleared_at IS NULL AND cleared_by IS NULL AND clear_reason IS NULL)
        OR (
            cleared_at IS NOT NULL
            AND cleared_by IS NOT NULL
            AND clear_reason IS NOT NULL
            AND cardinality(clear_evidence_ids) > 0
        )
    )
);

CREATE UNIQUE INDEX resource_quarantines_active
    ON resource_quarantines (tenant_id, cluster_id, resource_key, COALESCE(action_id, '*'))
    WHERE cleared_at IS NULL;

CREATE INDEX resource_quarantines_scope
    ON resource_quarantines (tenant_id, cluster_id, cleared_at, created_at DESC);

CREATE TABLE executor_leases (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    owner TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending_fence', 'active', 'expired')),
    pending_nonce TEXT NOT NULL,
    fence_ack_snapshot JSONB,
    acquired_at TIMESTAMPTZ NOT NULL,
    activated_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > acquired_at),
    CHECK (
        (state = 'pending_fence' AND activated_at IS NULL AND fence_ack_snapshot IS NULL)
        OR (state = 'active' AND activated_at IS NOT NULL AND fence_ack_snapshot IS NOT NULL)
        OR state = 'expired'
    ),
    UNIQUE (cluster_id, epoch)
);

CREATE UNIQUE INDEX executor_leases_single_owner
    ON executor_leases (cluster_id)
    WHERE state IN ('pending_fence', 'active');

CREATE INDEX executor_leases_expiry
    ON executor_leases (expires_at)
    WHERE state IN ('pending_fence', 'active');

CREATE TABLE execution_agent_fences (
    cluster_id UUID PRIMARY KEY REFERENCES clusters(id),
    tenant_id UUID NOT NULL,
    highest_epoch BIGINT NOT NULL CHECK (highest_epoch > 0),
    lease_id UUID NOT NULL REFERENCES executor_leases(id),
    agent_subject TEXT NOT NULL,
    fence_ack_snapshot JSONB NOT NULL,
    acknowledged_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE execution_steps (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES executions(id),
    step_id UUID NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    record_kind TEXT NOT NULL CHECK (record_kind IN ('intent', 'result')),
    lease_id UUID REFERENCES executor_leases(id),
    lease_epoch BIGINT,
    compensation BOOLEAN NOT NULL DEFAULT FALSE,
    intent_snapshot JSONB,
    result_snapshot JSONB,
    reason_code TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (
            record_kind = 'intent'
            AND lease_id IS NOT NULL
            AND lease_epoch IS NOT NULL
            AND intent_snapshot IS NOT NULL
            AND result_snapshot IS NULL
        )
        OR (
            record_kind = 'result'
            AND intent_snapshot IS NULL
            AND result_snapshot IS NOT NULL
        )
    ),
    UNIQUE (execution_id, step_id, attempt, record_kind)
);

CREATE INDEX execution_steps_recovery
    ON execution_steps (execution_id, record_kind, sequence_id);

CREATE TABLE audit_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    correlation_id UUID NOT NULL,
    event_kind TEXT NOT NULL,
    actor_subject TEXT NOT NULL,
    actor_role TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    details JSONB NOT NULL,
    event_snapshot JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX audit_events_timeline
    ON audit_events (tenant_id, cluster_id, correlation_id, sequence_id);

CREATE TABLE execution_agent_effects (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    execution_id UUID NOT NULL REFERENCES executions(id),
    step_id UUID NOT NULL,
    lease_id UUID NOT NULL REFERENCES executor_leases(id),
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    idempotency_key TEXT NOT NULL UNIQUE,
    action_id TEXT NOT NULL,
    target TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('prepared', 'dispatched', 'confirmed', 'unknown')),
    request_snapshot JSONB NOT NULL,
    operation_id TEXT,
    outcome_code TEXT,
    sanitized_summary TEXT,
    prepared_at TIMESTAMPTZ NOT NULL,
    dispatched_at TIMESTAMPTZ,
    confirmed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (execution_id, step_id, idempotency_key),
    CHECK (
        (state = 'prepared' AND dispatched_at IS NULL AND confirmed_at IS NULL)
        OR (state IN ('dispatched', 'unknown') AND confirmed_at IS NULL)
        OR (state = 'confirmed' AND confirmed_at IS NOT NULL)
    )
);

CREATE INDEX execution_agent_effects_recovery
    ON execution_agent_effects (cluster_id, state, updated_at, id);

CREATE OR REPLACE FUNCTION rocketmq_sre_reject_append_only_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER policy_decisions_append_only
    BEFORE UPDATE OR DELETE ON policy_decisions
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER approvals_append_only
    BEFORE UPDATE OR DELETE ON approvals
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER critic_reviews_append_only
    BEFORE UPDATE OR DELETE ON critic_reviews
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER execution_steps_append_only
    BEFORE UPDATE OR DELETE ON execution_steps
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER audit_events_append_only
    BEFORE UPDATE OR DELETE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_action_plan_snapshot()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'action plans cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.id,
        OLD.tenant_id,
        OLD.cluster_id,
        OLD.incident_id,
        OLD.diagnosis_revision_id,
        OLD.primary_model_invocation_id,
        OLD.version,
        OLD.plan_hash,
        OLD.evidence_hash,
        OLD.risk,
        OLD.request_snapshot,
        OLD.created_by,
        OLD.created_at,
        OLD.expires_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.incident_id,
        NEW.diagnosis_revision_id,
        NEW.primary_model_invocation_id,
        NEW.version,
        NEW.plan_hash,
        NEW.evidence_hash,
        NEW.risk,
        NEW.request_snapshot,
        NEW.created_by,
        NEW.created_at,
        NEW.expires_at
    ) THEN
        RAISE EXCEPTION 'action plan protected fields are immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER action_plans_protected_snapshot
    BEFORE UPDATE OR DELETE ON action_plans
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_action_plan_snapshot();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_execution_snapshot()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'executions cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.id,
        OLD.tenant_id,
        OLD.cluster_id,
        OLD.correlation_id,
        OLD.plan_id,
        OLD.plan_hash,
        OLD.resource_key,
        OLD.action_id,
        OLD.idempotency_key,
        OLD.request_snapshot,
        OLD.requested_by,
        OLD.started_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.correlation_id,
        NEW.plan_id,
        NEW.plan_hash,
        NEW.resource_key,
        NEW.action_id,
        NEW.idempotency_key,
        NEW.request_snapshot,
        NEW.requested_by,
        NEW.started_at
    ) THEN
        RAISE EXCEPTION 'execution request snapshot is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state = NEW.state
        AND ROW(OLD.completed_at, OLD.updated_at)
            IS DISTINCT FROM ROW(NEW.completed_at, NEW.updated_at)
    THEN
        RAISE EXCEPTION 'execution updates require a state transition'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state <> NEW.state AND NOT (
        (OLD.state = 'pending' AND NEW.state = 'prechecking')
        OR (OLD.state = 'prechecking' AND NEW.state IN ('intent_persisted', 'compensating'))
        OR (OLD.state = 'intent_persisted' AND NEW.state = 'applying')
        OR (OLD.state = 'applying' AND NEW.state IN ('verifying', 'unknown', 'compensating'))
        OR (OLD.state = 'unknown' AND NEW.state = 'reconciling')
        OR (OLD.state = 'reconciling' AND NEW.state IN ('verifying', 'compensating', 'escalated'))
        OR (OLD.state = 'verifying' AND NEW.state IN ('succeeded', 'compensating'))
        OR (OLD.state = 'compensating' AND NEW.state IN ('rolled_back', 'escalated'))
    ) THEN
        RAISE EXCEPTION 'invalid execution state transition'
            USING ERRCODE = '55000';
    END IF;
    IF (
        NEW.state IN ('succeeded', 'rolled_back', 'escalated')
        AND NEW.completed_at IS NULL
    ) OR (
        NEW.state NOT IN ('succeeded', 'rolled_back', 'escalated')
        AND NEW.completed_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'execution completion timestamp does not match state'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER executions_protected_snapshot
    BEFORE UPDATE OR DELETE ON executions
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_execution_snapshot();

CREATE OR REPLACE FUNCTION rocketmq_sre_prevent_quarantined_lock()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    execution_tenant UUID;
    execution_cluster UUID;
BEGIN
    SELECT tenant_id, cluster_id
    INTO execution_tenant, execution_cluster
    FROM executions
    WHERE id = NEW.holder_execution_id;
    IF execution_tenant IS NULL
        OR execution_tenant <> NEW.tenant_id
        OR execution_cluster <> NEW.cluster_id
    THEN
        RAISE EXCEPTION 'invalid_resource_lock_scope' USING ERRCODE = 'P0001';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM resource_quarantines quarantine
        WHERE quarantine.tenant_id = NEW.tenant_id
          AND quarantine.cluster_id = NEW.cluster_id
          AND quarantine.resource_key = NEW.resource_key
          AND quarantine.cleared_at IS NULL
          AND (quarantine.action_id IS NULL OR quarantine.action_id = NEW.action_id)
    ) THEN
        RAISE EXCEPTION 'resource_quarantined' USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER resource_locks_reject_quarantine
    BEFORE INSERT ON resource_locks
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_prevent_quarantined_lock();

CREATE OR REPLACE FUNCTION rocketmq_sre_validate_quarantine_source()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.source_execution_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM executions execution
        WHERE execution.id = NEW.source_execution_id
          AND execution.tenant_id = NEW.tenant_id
          AND execution.cluster_id = NEW.cluster_id
    ) THEN
        RAISE EXCEPTION 'invalid_quarantine_source_scope' USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER resource_quarantines_validate_source
    BEFORE INSERT ON resource_quarantines
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_validate_quarantine_source();

CREATE OR REPLACE FUNCTION rocketmq_sre_validate_step_intent_lease()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    execution_cluster UUID;
BEGIN
    IF NEW.record_kind <> 'intent' THEN
        RETURN NEW;
    END IF;
    SELECT cluster_id INTO execution_cluster
    FROM executions
    WHERE id = NEW.execution_id;

    IF NOT EXISTS (
        SELECT 1
        FROM executor_leases lease
        WHERE lease.id = NEW.lease_id
          AND lease.cluster_id = execution_cluster
          AND lease.epoch = NEW.lease_epoch
          AND lease.state = 'active'
          AND lease.expires_at > NEW.occurred_at
          AND lease.epoch = (
              SELECT MAX(latest.epoch)
              FROM executor_leases latest
              WHERE latest.cluster_id = execution_cluster
          )
    ) THEN
        RAISE EXCEPTION 'invalid_executor_lease' USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_steps_validate_lease
    BEFORE INSERT ON execution_steps
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_validate_step_intent_lease();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_lease_epoch()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(OLD.id, OLD.tenant_id, OLD.cluster_id, OLD.epoch, OLD.owner, OLD.pending_nonce, OLD.acquired_at)
        IS DISTINCT FROM
       ROW(NEW.id, NEW.tenant_id, NEW.cluster_id, NEW.epoch, NEW.owner, NEW.pending_nonce, NEW.acquired_at)
    THEN
        RAISE EXCEPTION 'executor lease identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.state = NEW.state
        AND ROW(OLD.fence_ack_snapshot, OLD.activated_at)
            IS DISTINCT FROM ROW(NEW.fence_ack_snapshot, NEW.activated_at)
    THEN
        RAISE EXCEPTION 'executor lease acknowledgement is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NOT (
        OLD.state = NEW.state
        OR (OLD.state = 'pending_fence' AND NEW.state IN ('active', 'expired'))
        OR (OLD.state = 'active' AND NEW.state = 'expired')
    ) THEN
        RAISE EXCEPTION 'invalid executor lease transition' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER executor_leases_protected_epoch
    BEFORE UPDATE ON executor_leases
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_lease_epoch();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_agent_fence()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.highest_epoch < OLD.highest_epoch THEN
        RAISE EXCEPTION 'execution agent fence epoch cannot decrease'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_agent_fences_monotonic
    BEFORE UPDATE ON execution_agent_fences
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_agent_fence();

CREATE OR REPLACE FUNCTION rocketmq_sre_validate_agent_effect()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    accepted_epoch BIGINT;
    accepted_lease UUID;
    accepted_tenant UUID;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT highest_epoch, lease_id, tenant_id
        INTO accepted_epoch, accepted_lease, accepted_tenant
        FROM execution_agent_fences
        WHERE cluster_id = NEW.cluster_id;
        IF accepted_epoch IS NULL
            OR accepted_epoch <> NEW.epoch
            OR accepted_lease <> NEW.lease_id
            OR accepted_tenant <> NEW.tenant_id
            OR NEW.state <> 'prepared'
            OR NOT EXISTS (
                SELECT 1
                FROM executor_leases lease
                WHERE lease.id = NEW.lease_id
                  AND lease.cluster_id = NEW.cluster_id
                  AND lease.tenant_id = NEW.tenant_id
                  AND lease.epoch = NEW.epoch
                  AND lease.state = 'active'
                  AND lease.expires_at > NEW.prepared_at
            )
        THEN
            RAISE EXCEPTION 'agent_effect_fence_rejected' USING ERRCODE = 'P0001';
        END IF;
        RETURN NEW;
    END IF;

    IF ROW(
        OLD.id,
        OLD.tenant_id,
        OLD.cluster_id,
        OLD.execution_id,
        OLD.step_id,
        OLD.lease_id,
        OLD.epoch,
        OLD.idempotency_key,
        OLD.action_id,
        OLD.target,
        OLD.request_snapshot,
        OLD.prepared_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.execution_id,
        NEW.step_id,
        NEW.lease_id,
        NEW.epoch,
        NEW.idempotency_key,
        NEW.action_id,
        NEW.target,
        NEW.request_snapshot,
        NEW.prepared_at
    ) THEN
        RAISE EXCEPTION 'execution agent effect identity is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NOT (
        (OLD.state = 'prepared' AND NEW.state IN ('dispatched', 'unknown'))
        OR (OLD.state = 'dispatched' AND NEW.state IN ('confirmed', 'unknown'))
        OR (OLD.state = 'unknown' AND NEW.state = 'confirmed')
    ) THEN
        RAISE EXCEPTION 'invalid execution agent effect transition'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_agent_effects_fenced
    BEFORE INSERT OR UPDATE ON execution_agent_effects
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_validate_agent_effect();
