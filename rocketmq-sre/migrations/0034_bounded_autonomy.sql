-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Immutable autonomy policy definitions. Runtime mode, freeze, and kill-switch
-- state live in separate tables so routine safety changes never rewrite or
-- invalidate qualification history.
CREATE TABLE autonomy_policy_definitions (
    id UUID NOT NULL,
    definition_version BIGINT NOT NULL CHECK (definition_version > 0),
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    action_id TEXT NOT NULL CHECK (char_length(action_id) BETWEEN 1 AND 128),
    action_version TEXT NOT NULL CHECK (char_length(action_version) BETWEEN 1 AND 32),
    descriptor_digest TEXT NOT NULL CHECK (descriptor_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    diagnostic_pack_id TEXT NOT NULL CHECK (char_length(diagnostic_pack_id) BETWEEN 1 AND 128),
    diagnostic_pack_version TEXT NOT NULL CHECK (char_length(diagnostic_pack_version) BETWEEN 1 AND 32),
    owner TEXT NOT NULL CHECK (char_length(owner) BETWEEN 1 AND 128),
    minimum_evidence_freshness_seconds BIGINT NOT NULL CHECK (minimum_evidence_freshness_seconds > 0),
    required_evidence_sources TEXT[] NOT NULL CHECK (
        cardinality(required_evidence_sources) BETWEEN 1 AND 32
    ),
    min_shadow_samples INTEGER NOT NULL CHECK (min_shadow_samples > 0),
    min_supervised_successes INTEGER NOT NULL CHECK (min_supervised_successes > 0),
    observation_window_days INTEGER NOT NULL CHECK (observation_window_days BETWEEN 1 AND 365),
    max_unresolved_unknown INTEGER NOT NULL CHECK (max_unresolved_unknown >= 0),
    max_recent_rollbacks INTEGER NOT NULL CHECK (max_recent_rollbacks >= 0),
    max_executions_per_hour INTEGER NOT NULL CHECK (max_executions_per_hour BETWEEN 1 AND 100),
    cooldown_seconds BIGINT NOT NULL CHECK (cooldown_seconds > 0),
    max_concurrent_executions INTEGER NOT NULL CHECK (max_concurrent_executions BETWEEN 1 AND 16),
    stable_window_seconds BIGINT NOT NULL CHECK (stable_window_seconds > 0),
    definition_snapshot JSONB NOT NULL,
    created_by TEXT NOT NULL CHECK (char_length(created_by) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, definition_version),
    UNIQUE (tenant_id, cluster_id, action_id, action_version, definition_version),
    CHECK (jsonb_typeof(definition_snapshot) = 'object')
);

CREATE INDEX autonomy_policy_scope
    ON autonomy_policy_definitions (
        tenant_id, cluster_id, action_id, action_version, definition_version DESC
    );

-- Current operator-controlled state. New scopes are inserted as Disabled.
CREATE TABLE autonomy_lifecycle_states (
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    action_id TEXT NOT NULL CHECK (char_length(action_id) BETWEEN 1 AND 128),
    action_version TEXT NOT NULL CHECK (char_length(action_version) BETWEEN 1 AND 32),
    policy_id UUID NOT NULL,
    policy_definition_version BIGINT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'disabled' CHECK (
        mode IN ('disabled', 'shadow', 'supervised', 'autonomous', 'paused')
    ),
    previous_mode TEXT CHECK (
        previous_mode IS NULL
        OR previous_mode IN ('shadow', 'supervised', 'autonomous')
    ),
    owner TEXT NOT NULL CHECK (char_length(owner) BETWEEN 1 AND 128),
    owner_confirmed_at TIMESTAMPTZ,
    pause_reason TEXT CHECK (
        pause_reason IS NULL OR char_length(pause_reason) BETWEEN 1 AND 512
    ),
    lifecycle_revision BIGINT NOT NULL DEFAULT 1 CHECK (lifecycle_revision > 0),
    updated_by TEXT NOT NULL CHECK (char_length(updated_by) BETWEEN 1 AND 256),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, cluster_id, action_id, action_version),
    FOREIGN KEY (policy_id, policy_definition_version)
        REFERENCES autonomy_policy_definitions (id, definition_version),
    CHECK (
        (mode = 'paused' AND previous_mode IS NOT NULL AND pause_reason IS NOT NULL)
        OR (mode <> 'paused' AND previous_mode IS NULL AND pause_reason IS NULL)
    )
);

CREATE TABLE autonomy_lifecycle_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    action_id TEXT NOT NULL,
    action_version TEXT NOT NULL,
    from_mode TEXT,
    to_mode TEXT NOT NULL,
    previous_mode TEXT,
    lifecycle_revision BIGINT NOT NULL CHECK (lifecycle_revision > 0),
    reason_code TEXT NOT NULL CHECK (char_length(reason_code) BETWEEN 1 AND 128),
    actor_subject TEXT NOT NULL CHECK (char_length(actor_subject) BETWEEN 1 AND 256),
    event_snapshot JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    CHECK (jsonb_typeof(event_snapshot) = 'object')
);

CREATE INDEX autonomy_lifecycle_events_scope
    ON autonomy_lifecycle_events (
        tenant_id, cluster_id, action_id, action_version, sequence_id
    );

-- Shadow and Autonomous qualification keys are distinct. The Critic identity
-- is required only for the Autonomous key.
CREATE TABLE autonomy_qualification_cohorts (
    id UUID PRIMARY KEY,
    level TEXT NOT NULL CHECK (level IN ('shadow', 'autonomous')),
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    action_id TEXT NOT NULL,
    action_version TEXT NOT NULL,
    policy_id UUID NOT NULL,
    policy_definition_version BIGINT NOT NULL,
    descriptor_digest TEXT NOT NULL CHECK (descriptor_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    diagnostic_pack_id TEXT NOT NULL,
    diagnostic_pack_version TEXT NOT NULL,
    primary_actual_model_identity_hash TEXT NOT NULL CHECK (
        primary_actual_model_identity_hash ~ '^sha256:[0-9A-Fa-f]{64}$'
    ),
    critic_actual_model_identity_hash TEXT CHECK (
        critic_actual_model_identity_hash IS NULL
        OR critic_actual_model_identity_hash ~ '^sha256:[0-9A-Fa-f]{64}$'
    ),
    cohort_hash TEXT NOT NULL UNIQUE CHECK (cohort_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (policy_id, policy_definition_version)
        REFERENCES autonomy_policy_definitions (id, definition_version),
    CHECK (
        (level = 'shadow' AND critic_actual_model_identity_hash IS NULL)
        OR (level = 'autonomous' AND critic_actual_model_identity_hash IS NOT NULL)
    )
);

CREATE INDEX autonomy_cohort_scope
    ON autonomy_qualification_cohorts (
        tenant_id, cluster_id, action_id, action_version, level, created_at DESC
    );

CREATE TABLE autonomy_qualification_samples (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    cohort_id UUID NOT NULL REFERENCES autonomy_qualification_cohorts(id),
    sample_kind TEXT NOT NULL CHECK (
        sample_kind IN ('shadow_outcome', 'supervised_success')
    ),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    execution_id UUID REFERENCES executions(id),
    qualified BOOLEAN NOT NULL,
    reason_codes TEXT[] NOT NULL DEFAULT '{}',
    human_outcome_linked BOOLEAN NOT NULL,
    evidence_complete BOOLEAN NOT NULL,
    stable_window_passed BOOLEAN NOT NULL,
    sample_snapshot JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    reconciled_at TIMESTAMPTZ NOT NULL,
    UNIQUE (cohort_id, sample_kind, incident_id, plan_hash),
    CHECK (jsonb_typeof(sample_snapshot) = 'object'),
    CHECK (reconciled_at >= observed_at)
);

CREATE INDEX autonomy_samples_window
    ON autonomy_qualification_samples (
        cohort_id, qualified, observed_at DESC, sequence_id
    );

-- Shadow candidates are append-only and cannot be dispatched.
CREATE TABLE autonomy_shadow_outcomes (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    action_id TEXT NOT NULL,
    action_version TEXT NOT NULL,
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    diagnosis_revision_id UUID NOT NULL REFERENCES diagnosis_revisions(id),
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    cohort_id UUID NOT NULL REFERENCES autonomy_qualification_cohorts(id),
    eligibility_snapshot JSONB NOT NULL,
    expected_effect_snapshot JSONB NOT NULL,
    evidence_ids UUID[] NOT NULL,
    qualified BOOLEAN NOT NULL,
    reason_codes TEXT[] NOT NULL DEFAULT '{}',
    human_outcome_snapshot JSONB,
    stable_window_snapshot JSONB,
    observed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, cluster_id, action_id, action_version, incident_id, plan_hash),
    CHECK (jsonb_typeof(eligibility_snapshot) = 'object'),
    CHECK (jsonb_typeof(expected_effect_snapshot) = 'object'),
    CHECK (human_outcome_snapshot IS NULL OR jsonb_typeof(human_outcome_snapshot) = 'object'),
    CHECK (stable_window_snapshot IS NULL OR jsonb_typeof(stable_window_snapshot) = 'object')
);

-- Independent dynamic controls. Revision is monotonic within each scope.
CREATE TABLE autonomy_freezes (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    action_id TEXT,
    action_version TEXT,
    revision BIGINT NOT NULL CHECK (revision > 0),
    active BOOLEAN NOT NULL,
    reason TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 512),
    starts_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    updated_by TEXT NOT NULL CHECK (char_length(updated_by) BETWEEN 1 AND 256),
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, cluster_id, action_id, action_version),
    CHECK (
        (action_id IS NULL AND action_version IS NULL)
        OR (action_id IS NOT NULL AND action_version IS NOT NULL)
    ),
    CHECK (expires_at IS NULL OR expires_at > starts_at)
);

CREATE TABLE autonomy_kill_switches (
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    action_id TEXT NOT NULL,
    action_version TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    active BOOLEAN NOT NULL,
    reason TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 512),
    updated_by TEXT NOT NULL CHECK (char_length(updated_by) BETWEEN 1 AND 256),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, cluster_id, action_id, action_version)
);

CREATE TABLE autonomy_dynamic_safety_decisions (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    action_id TEXT NOT NULL,
    action_version TEXT NOT NULL,
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    execution_id UUID REFERENCES executions(id),
    execution_step_id UUID REFERENCES execution_steps(id),
    policy_definition_version BIGINT NOT NULL CHECK (policy_definition_version > 0),
    lifecycle_revision BIGINT NOT NULL CHECK (lifecycle_revision > 0),
    error_budget_available BOOLEAN NOT NULL,
    freeze_revision BIGINT NOT NULL CHECK (freeze_revision >= 0),
    kill_switch_revision BIGINT NOT NULL CHECK (kill_switch_revision >= 0),
    evidence_fresh BOOLEAN NOT NULL,
    allowed BOOLEAN NOT NULL,
    reason_codes TEXT[] NOT NULL DEFAULT '{}',
    decision_snapshot JSONB NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (jsonb_typeof(decision_snapshot) = 'object'),
    CHECK (expires_at > issued_at)
);

CREATE INDEX autonomy_safety_plan_timeline
    ON autonomy_dynamic_safety_decisions (plan_id, sequence_id);

-- Outcomes are idempotently reconciled from the execution journal. Failures
-- drive lifecycle pause in the same transaction as the outcome insert.
CREATE TABLE autonomy_outcomes (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    action_id TEXT NOT NULL,
    action_version TEXT NOT NULL,
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    execution_id UUID REFERENCES executions(id),
    cohort_id UUID REFERENCES autonomy_qualification_cohorts(id),
    outcome_class TEXT NOT NULL CHECK (
        outcome_class IN ('expected_deny', 'success', 'autonomous_execution_failure')
    ),
    failure_code TEXT,
    reason_codes TEXT[] NOT NULL DEFAULT '{}',
    first_positive_intent_persisted BOOLEAN NOT NULL,
    outcome_snapshot JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    reconciled_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, cluster_id, action_id, action_version, plan_id),
    CHECK (jsonb_typeof(outcome_snapshot) = 'object'),
    CHECK (
        (outcome_class = 'autonomous_execution_failure' AND failure_code IS NOT NULL)
        OR (outcome_class <> 'autonomous_execution_failure' AND failure_code IS NULL)
    ),
    CHECK (reconciled_at >= occurred_at)
);

CREATE TABLE autonomy_outbox (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    action_id TEXT NOT NULL,
    action_version TEXT NOT NULL,
    outcome_id UUID REFERENCES autonomy_outcomes(id),
    event_kind TEXT NOT NULL CHECK (
        event_kind IN (
            'lifecycle_changed',
            'autonomy_paused',
            'shadow_recorded',
            'autonomy_succeeded',
            'operator_feedback_requested',
            'provider_quarantined'
        )
    ),
    idempotency_key TEXT NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 256),
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'delivering', 'delivered', 'retry_scheduled', 'failed')
    ),
    event_snapshot JSONB NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ,
    last_error_code TEXT,
    claim_token UUID,
    claimed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, idempotency_key),
    CHECK (jsonb_typeof(event_snapshot) = 'object'),
    CHECK (
        (status = 'delivering' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL)
        OR (status <> 'delivering' AND claim_token IS NULL AND claimed_at IS NULL)
    )
);

CREATE INDEX autonomy_outbox_pending
    ON autonomy_outbox (next_attempt_at, created_at, id)
    WHERE status IN ('pending', 'retry_scheduled');

-- Bounded no-side-effect and preventive automation records. External
-- notifications still use an outbox and never authorize execution.
CREATE TABLE no_side_effect_automation_runs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    incident_id UUID REFERENCES sre_incidents(id),
    automation_kind TEXT NOT NULL CHECK (
        automation_kind IN (
            'alert_correlation',
            'severity_owner_suggestion',
            'evidence_collection',
            'shift_summary',
            'notification',
            'postmortem_draft'
        )
    ),
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'denied')),
    result_snapshot JSONB NOT NULL,
    model_invocation_id UUID REFERENCES model_invocations(id),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    UNIQUE (tenant_id, automation_kind, idempotency_key),
    CHECK (jsonb_typeof(result_snapshot) = 'object'),
    CHECK (completed_at IS NULL OR completed_at >= started_at)
);

CREATE TABLE preventive_automation_runs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    inspection_run_id UUID REFERENCES inspection_runs(id),
    risk_family TEXT NOT NULL CHECK (
        risk_family IN (
            'capacity', 'certificate', 'config', 'route', 'ha', 'upgrade'
        )
    ),
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'denied')),
    recommendation_id UUID REFERENCES recommendations(id),
    freeze_id UUID REFERENCES autonomy_freezes(id),
    result_snapshot JSONB NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    CHECK (jsonb_typeof(result_snapshot) = 'object'),
    CHECK (completed_at IS NULL OR completed_at >= started_at)
);

CREATE TABLE autonomy_operator_feedback (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    incident_id UUID REFERENCES sre_incidents(id),
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('severity', 'owner', 'summary', 'recommendation', 'plan')
    ),
    subject_id UUID,
    verdict TEXT NOT NULL CHECK (verdict IN ('correct', 'incorrect', 'useful', 'not_useful')),
    comment TEXT CHECK (comment IS NULL OR char_length(comment) <= 2000),
    actor_subject TEXT NOT NULL CHECK (char_length(actor_subject) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL
);

-- Provider automation lifecycle is an operator-controlled projection. Existing
-- model invocation rows preserve the actual provider/profile/family identity.
CREATE TABLE model_profile_lifecycle (
    profile_id UUID PRIMARY KEY REFERENCES model_profiles(id),
    tenant_id UUID NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('draft', 'certified', 'promoted', 'quarantined', 'retired')
    ),
    revision BIGINT NOT NULL CHECK (revision > 0),
    rollback_profile_id UUID REFERENCES model_profiles(id),
    reason_code TEXT NOT NULL CHECK (char_length(reason_code) BETWEEN 1 AND 128),
    operator_confirmed BOOLEAN NOT NULL,
    updated_by TEXT NOT NULL CHECK (char_length(updated_by) BETWEEN 1 AND 256),
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE provider_smoke_results (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    profile_id UUID NOT NULL REFERENCES model_profiles(id),
    connectivity_ok BOOLEAN NOT NULL,
    structured_output_ok BOOLEAN NOT NULL,
    tool_arguments_ok BOOLEAN NOT NULL,
    evidence_citation_ok BOOLEAN NOT NULL,
    latency_ms BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
    result_snapshot JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    CHECK (jsonb_typeof(result_snapshot) = 'object')
);

CREATE TABLE autonomy_operational_reports (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    period_kind TEXT NOT NULL CHECK (period_kind IN ('weekly', 'monthly')),
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    report_snapshot JSONB NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, period_kind, period_start, period_end),
    CHECK (period_end > period_start),
    CHECK (jsonb_typeof(report_snapshot) = 'object')
);

CREATE TRIGGER autonomy_policy_definitions_append_only
    BEFORE UPDATE OR DELETE ON autonomy_policy_definitions
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER autonomy_lifecycle_events_append_only
    BEFORE UPDATE OR DELETE ON autonomy_lifecycle_events
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER autonomy_qualification_cohorts_append_only
    BEFORE UPDATE OR DELETE ON autonomy_qualification_cohorts
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER autonomy_qualification_samples_append_only
    BEFORE UPDATE OR DELETE ON autonomy_qualification_samples
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER autonomy_shadow_outcomes_append_only
    BEFORE UPDATE OR DELETE ON autonomy_shadow_outcomes
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER autonomy_dynamic_safety_decisions_append_only
    BEFORE UPDATE OR DELETE ON autonomy_dynamic_safety_decisions
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER autonomy_outcomes_append_only
    BEFORE UPDATE OR DELETE ON autonomy_outcomes
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER no_side_effect_automation_runs_no_delete
    BEFORE DELETE ON no_side_effect_automation_runs
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER preventive_automation_runs_no_delete
    BEFORE DELETE ON preventive_automation_runs
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER autonomy_operator_feedback_append_only
    BEFORE UPDATE OR DELETE ON autonomy_operator_feedback
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER provider_smoke_results_append_only
    BEFORE UPDATE OR DELETE ON provider_smoke_results
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER autonomy_operational_reports_append_only
    BEFORE UPDATE OR DELETE ON autonomy_operational_reports
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_autonomy_lifecycle()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'autonomy lifecycle states cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.tenant_id,
        OLD.cluster_id,
        OLD.action_id,
        OLD.action_version,
        OLD.policy_id,
        OLD.policy_definition_version
    ) IS DISTINCT FROM ROW(
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.action_id,
        NEW.action_version,
        NEW.policy_id,
        NEW.policy_definition_version
    ) THEN
        RAISE EXCEPTION 'autonomy lifecycle scope and policy binding are immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.lifecycle_revision <> OLD.lifecycle_revision + 1 THEN
        RAISE EXCEPTION 'autonomy lifecycle revision must increase by exactly one'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.mode = 'paused' AND NEW.mode = 'autonomous' THEN
        RAISE EXCEPTION 'paused autonomy cannot recover directly to autonomous'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'autonomy lifecycle time cannot move backwards'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER autonomy_lifecycle_states_protected
    BEFORE UPDATE OR DELETE ON autonomy_lifecycle_states
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_autonomy_lifecycle();
