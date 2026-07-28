-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Versioned external integration targets. Secrets stay outside PostgreSQL;
-- only an environment-backed secret reference may be persisted.
CREATE TABLE integration_targets (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    descriptor_id TEXT NOT NULL,
    descriptor_version TEXT NOT NULL,
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 128),
    adapter_kind TEXT NOT NULL CHECK (
        adapter_kind IN (
            'mock_itsm',
            'signed_webhook_itsm',
            'chatops_webhook',
            'pager',
            'email'
        )
    ),
    endpoint TEXT NOT NULL CHECK (char_length(endpoint) BETWEEN 1 AND 2048),
    secret_reference TEXT,
    notification_target_id UUID REFERENCES notification_targets(id),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    inbound_approval BOOLEAN NOT NULL DEFAULT FALSE,
    outbound_events TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, cluster_id, name),
    CHECK (updated_at >= created_at),
    CHECK (
        (adapter_kind IN ('mock_itsm', 'signed_webhook_itsm') AND notification_target_id IS NULL)
        OR
        (adapter_kind IN ('chatops_webhook', 'pager', 'email') AND notification_target_id IS NOT NULL)
    )
);

CREATE INDEX integration_targets_scope
    ON integration_targets (tenant_id, cluster_id, enabled, adapter_kind, id);

-- One release binds an approved immutable execution plan and, when available,
-- a separately approved typed rollback plan. No target mutation credential is
-- stored here or exposed to an integration adapter.
CREATE TABLE release_workflows (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    correlation_id UUID NOT NULL,
    change_id TEXT NOT NULL CHECK (char_length(change_id) BETWEEN 1 AND 256),
    release_ref TEXT NOT NULL CHECK (char_length(release_ref) BETWEEN 1 AND 256),
    target_version TEXT NOT NULL CHECK (char_length(target_version) BETWEEN 1 AND 128),
    runbook_id UUID NOT NULL,
    runbook_version TEXT NOT NULL,
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    rollback_plan_id UUID REFERENCES action_plans(id),
    rollback_plan_hash TEXT CHECK (
        rollback_plan_hash IS NULL
        OR rollback_plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'
    ),
    readiness_snapshot JSONB,
    status TEXT NOT NULL CHECK (
        status IN (
            'planned',
            'readiness_checking',
            'ready',
            'canary_running',
            'paused',
            'verifying',
            'rolling_back',
            'rolled_back',
            'completed',
            'manual_takeover',
            'failed'
        )
    ),
    active_execution_id UUID REFERENCES executions(id),
    regression_detected BOOLEAN NOT NULL DEFAULT FALSE,
    pause_reason TEXT,
    workflow_snapshot JSONB NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (rollback_plan_id IS NULL AND rollback_plan_hash IS NULL)
        OR (rollback_plan_id IS NOT NULL AND rollback_plan_hash IS NOT NULL)
    ),
    CHECK (readiness_snapshot IS NULL OR jsonb_typeof(readiness_snapshot) = 'object'),
    CHECK (jsonb_typeof(workflow_snapshot) = 'object'),
    CHECK (updated_at >= created_at),
    FOREIGN KEY (tenant_id, cluster_id, runbook_id, runbook_version)
        REFERENCES runbook_definitions (tenant_id, cluster_id, id, version)
);

CREATE UNIQUE INDEX release_workflows_change
    ON release_workflows (tenant_id, cluster_id, change_id);
CREATE UNIQUE INDEX release_workflows_reference
    ON release_workflows (tenant_id, cluster_id, release_ref);
CREATE INDEX release_workflows_scope_status
    ON release_workflows (tenant_id, cluster_id, status, updated_at DESC, id);

-- Adapter-neutral transactional outbox. The target-scoped idempotency key
-- ensures delivery retry cannot duplicate a ticket, approval, or execution.
CREATE TABLE integration_outbox (
    id UUID PRIMARY KEY,
    target_id UUID NOT NULL REFERENCES integration_targets(id),
    descriptor_id TEXT NOT NULL,
    descriptor_version TEXT NOT NULL,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    plan_id UUID REFERENCES action_plans(id),
    release_id UUID REFERENCES release_workflows(id),
    event_kind TEXT NOT NULL CHECK (
        event_kind IN (
            'plan_submitted',
            'approval_changed',
            'release_started',
            'release_paused',
            'release_rolling_back',
            'release_completed',
            'manual_takeover_required'
        )
    ),
    idempotency_key TEXT NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 256),
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'delivering', 'delivered', 'retry_scheduled', 'failed')
    ),
    sanitized_summary TEXT NOT NULL CHECK (char_length(sanitized_summary) BETWEEN 1 AND 2048),
    deep_link TEXT NOT NULL CHECK (char_length(deep_link) BETWEEN 1 AND 2048),
    delivery_snapshot JSONB NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ,
    last_error_code TEXT,
    delivered_at TIMESTAMPTZ,
    claim_token UUID,
    claimed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (target_id, idempotency_key),
    CHECK (jsonb_typeof(delivery_snapshot) = 'object'),
    CHECK (
        (status = 'delivering' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL)
        OR (status <> 'delivering' AND claim_token IS NULL AND claimed_at IS NULL)
    )
);

CREATE INDEX integration_outbox_pending
    ON integration_outbox (next_attempt_at, created_at, id)
    WHERE status IN ('pending', 'retry_scheduled');

-- Mutable ITSM projection backed by immutable audit/outbox records.
CREATE TABLE itsm_ticket_links (
    target_id UUID NOT NULL REFERENCES integration_targets(id),
    external_ticket_key TEXT NOT NULL CHECK (char_length(external_ticket_key) BETWEEN 1 AND 256),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    approval_status TEXT NOT NULL,
    sre_url TEXT NOT NULL CHECK (char_length(sre_url) BETWEEN 1 AND 2048),
    sanitized_summary TEXT NOT NULL CHECK (char_length(sanitized_summary) BETWEEN 1 AND 2048),
    last_synced_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (target_id, external_ticket_key),
    UNIQUE (target_id, plan_id)
);

-- The receipt and resulting Approval are inserted in the same transaction.
-- This prevents webhook retries from producing more than one human decision.
CREATE TABLE external_approval_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_id UUID NOT NULL REFERENCES integration_targets(id),
    external_event_id TEXT NOT NULL CHECK (char_length(external_event_id) BETWEEN 1 AND 256),
    external_ticket_key TEXT NOT NULL CHECK (char_length(external_ticket_key) BETWEEN 1 AND 256),
    plan_id UUID NOT NULL REFERENCES action_plans(id),
    plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected')),
    subject TEXT NOT NULL,
    mfa_verified BOOLEAN NOT NULL,
    step_up_verified BOOLEAN NOT NULL,
    approval_id UUID NOT NULL UNIQUE REFERENCES approvals(id),
    input_snapshot JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    UNIQUE (target_id, external_event_id),
    CHECK (jsonb_typeof(input_snapshot) = 'object'),
    CHECK (mfa_verified AND step_up_verified)
);

-- SLO and synthetic-probe facts are append-only so regression decisions and
-- final before/during/after reports remain reproducible.
CREATE TABLE release_observations (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    observation_id UUID NOT NULL UNIQUE,
    release_id UUID NOT NULL REFERENCES release_workflows(id),
    phase TEXT NOT NULL CHECK (phase IN ('before', 'during', 'after')),
    observation_snapshot JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    CHECK (jsonb_typeof(observation_snapshot) = 'object')
);

CREATE INDEX release_observations_timeline
    ON release_observations (release_id, sequence_id);

CREATE TABLE release_reports (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    report_id UUID NOT NULL UNIQUE,
    release_id UUID NOT NULL REFERENCES release_workflows(id),
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    change_id TEXT NOT NULL,
    release_ref TEXT NOT NULL,
    final_status TEXT NOT NULL CHECK (
        final_status IN ('rolled_back', 'completed', 'manual_takeover', 'failed')
    ),
    report_snapshot JSONB NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (release_id),
    CHECK (jsonb_typeof(report_snapshot) = 'object')
);

CREATE TABLE release_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    release_id UUID NOT NULL REFERENCES release_workflows(id),
    correlation_id UUID NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    actor_subject TEXT NOT NULL,
    details JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX release_events_timeline
    ON release_events (release_id, sequence_id);

CREATE TRIGGER external_approval_events_append_only
    BEFORE UPDATE OR DELETE ON external_approval_events
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER release_observations_append_only
    BEFORE UPDATE OR DELETE ON release_observations
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER release_reports_append_only
    BEFORE UPDATE OR DELETE ON release_reports
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER release_events_append_only
    BEFORE UPDATE OR DELETE ON release_events
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_release_workflow()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'release workflows cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.id,
        OLD.tenant_id,
        OLD.cluster_id,
        OLD.incident_id,
        OLD.correlation_id,
        OLD.change_id,
        OLD.release_ref,
        OLD.target_version,
        OLD.runbook_id,
        OLD.runbook_version,
        OLD.plan_id,
        OLD.plan_hash,
        OLD.rollback_plan_id,
        OLD.rollback_plan_hash,
        OLD.created_by,
        OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.incident_id,
        NEW.correlation_id,
        NEW.change_id,
        NEW.release_ref,
        NEW.target_version,
        NEW.runbook_id,
        NEW.runbook_version,
        NEW.plan_id,
        NEW.plan_hash,
        NEW.rollback_plan_id,
        NEW.rollback_plan_hash,
        NEW.created_by,
        NEW.created_at
    ) THEN
        RAISE EXCEPTION 'release protected fields are immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.status IN ('rolled_back', 'completed', 'manual_takeover', 'failed') THEN
        RAISE EXCEPTION 'terminal release workflow is immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.status <> NEW.status AND NOT (
        (OLD.status = 'planned' AND NEW.status = 'readiness_checking')
        OR (OLD.status = 'readiness_checking' AND NEW.status IN ('ready', 'failed'))
        OR (OLD.status = 'ready' AND NEW.status IN ('canary_running', 'failed'))
        OR (OLD.status = 'canary_running' AND NEW.status IN (
            'paused', 'verifying', 'rolling_back', 'manual_takeover'
        ))
        OR (OLD.status = 'paused' AND NEW.status IN (
            'canary_running', 'rolling_back', 'manual_takeover'
        ))
        OR (OLD.status = 'verifying' AND NEW.status IN (
            'completed', 'paused', 'rolling_back', 'manual_takeover', 'failed'
        ))
        OR (OLD.status = 'rolling_back' AND NEW.status IN (
            'rolled_back', 'manual_takeover', 'failed'
        ))
    ) THEN
        RAISE EXCEPTION 'invalid release state transition' USING ERRCODE = '55000';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'release update time cannot move backwards' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER release_workflows_protected
    BEFORE UPDATE OR DELETE ON release_workflows
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_release_workflow();

CREATE OR REPLACE FUNCTION rocketmq_sre_validate_release_scope()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM action_plans plan
        WHERE plan.id = NEW.plan_id
          AND plan.tenant_id = NEW.tenant_id
          AND plan.cluster_id = NEW.cluster_id
          AND plan.incident_id = NEW.incident_id
          AND plan.plan_hash = NEW.plan_hash
    ) THEN
        RAISE EXCEPTION 'invalid_release_plan_scope' USING ERRCODE = 'P0001';
    END IF;
    IF NEW.rollback_plan_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM action_plans rollback
        WHERE rollback.id = NEW.rollback_plan_id
          AND rollback.tenant_id = NEW.tenant_id
          AND rollback.cluster_id = NEW.cluster_id
          AND rollback.plan_hash = NEW.rollback_plan_hash
    ) THEN
        RAISE EXCEPTION 'invalid_release_rollback_scope' USING ERRCODE = 'P0001';
    END IF;
    IF NEW.active_execution_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM executions execution
        WHERE execution.id = NEW.active_execution_id
          AND execution.tenant_id = NEW.tenant_id
          AND execution.cluster_id = NEW.cluster_id
          AND execution.plan_id IN (NEW.plan_id, NEW.rollback_plan_id)
    ) THEN
        RAISE EXCEPTION 'invalid_release_execution_scope' USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER release_workflows_validate_scope
    BEFORE INSERT OR UPDATE ON release_workflows
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_validate_release_scope();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_integration_target()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'integration targets cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.id,
        OLD.tenant_id,
        OLD.cluster_id,
        OLD.descriptor_id,
        OLD.descriptor_version,
        OLD.adapter_kind,
        OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.descriptor_id,
        NEW.descriptor_version,
        NEW.adapter_kind,
        NEW.created_at
    ) THEN
        RAISE EXCEPTION 'integration target identity and descriptor are immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'integration target update time cannot move backwards'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER integration_targets_protected
    BEFORE UPDATE OR DELETE ON integration_targets
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_integration_target();
