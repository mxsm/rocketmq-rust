-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Immutable tenant-scoped runbook versions. A definition is derived into
-- separately approved ActionPlans before any schedule can dispatch it.
CREATE TABLE runbook_definitions (
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    id UUID NOT NULL,
    version TEXT NOT NULL,
    risk TEXT NOT NULL CHECK (risk IN ('r1', 'r2')),
    definition_snapshot JSONB NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, cluster_id, id, version),
    CHECK (jsonb_typeof(definition_snapshot) = 'object')
);

CREATE INDEX runbook_definitions_tenant_created
    ON runbook_definitions (tenant_id, cluster_id, created_at DESC, id, version);

-- Windows are immutable calendar facts. Replacing a window creates a new ID
-- so conflict decisions remain explainable after an execution.
CREATE TABLE change_windows (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    kind TEXT NOT NULL CHECK (kind IN ('maintenance', 'freeze', 'blackout')),
    timezone TEXT NOT NULL,
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    resource_keys TEXT[] NOT NULL DEFAULT '{}',
    max_parallelism INTEGER NOT NULL CHECK (max_parallelism BETWEEN 1 AND 16),
    window_snapshot JSONB NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (ends_at > starts_at),
    CHECK (jsonb_typeof(window_snapshot) = 'object')
);

CREATE INDEX change_windows_scope_range
    ON change_windows (tenant_id, cluster_id, starts_at, ends_at, id);

-- One schedule binds every action step to a separately approved immutable
-- plan. The scheduler can therefore sequence/pause work without minting a
-- mutation path around Plan, Approval, Executor, Agent, or Audit.
CREATE TABLE change_schedules (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    correlation_id UUID NOT NULL,
    runbook_id UUID NOT NULL,
    runbook_version TEXT NOT NULL,
    plan_bindings JSONB NOT NULL,
    scheduled_start TIMESTAMPTZ NOT NULL,
    scheduled_end TIMESTAMPTZ NOT NULL,
    resource_keys TEXT[] NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'scheduled',
            'running',
            'awaiting_manual_gate',
            'paused',
            'safe_stopping',
            'reconciling',
            'completed',
            'cancelled',
            'rejected'
        )
    ),
    intent_persisted BOOLEAN NOT NULL DEFAULT FALSE,
    next_step_sequence INTEGER NOT NULL CHECK (next_step_sequence > 0),
    active_execution_id UUID REFERENCES executions(id),
    waiting_manual_gate UUID,
    completed_step_ids UUID[] NOT NULL DEFAULT '{}',
    pause_requested_at TIMESTAMPTZ,
    cancel_requested_at TIMESTAMPTZ,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (scheduled_end > scheduled_start),
    CHECK (cardinality(resource_keys) > 0),
    CHECK (jsonb_typeof(plan_bindings) = 'array'),
    CHECK (NOT (active_execution_id IS NOT NULL AND waiting_manual_gate IS NOT NULL)),
    FOREIGN KEY (tenant_id, cluster_id, runbook_id, runbook_version)
        REFERENCES runbook_definitions (tenant_id, cluster_id, id, version)
);

CREATE INDEX change_schedules_scope_status
    ON change_schedules (tenant_id, cluster_id, status, scheduled_start, id);

CREATE INDEX change_schedules_worker_due
    ON change_schedules (scheduled_start, updated_at, id)
    WHERE status IN ('scheduled', 'running', 'safe_stopping', 'reconciling');

-- Human gate decisions and lifecycle events are append-only. They complement
-- the shared audit_events timeline with scheduler-specific recovery detail.
CREATE TABLE runbook_manual_gate_decisions (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    decision_id UUID NOT NULL UNIQUE,
    schedule_id UUID NOT NULL REFERENCES change_schedules(id),
    step_id UUID NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected')),
    actor_subject TEXT NOT NULL,
    actor_role TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    UNIQUE (schedule_id, step_id)
);

CREATE TABLE change_schedule_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    schedule_id UUID NOT NULL REFERENCES change_schedules(id),
    correlation_id UUID NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    actor_subject TEXT NOT NULL,
    details JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX change_schedule_events_timeline
    ON change_schedule_events (schedule_id, sequence_id);

CREATE TRIGGER runbook_definitions_append_only
    BEFORE UPDATE OR DELETE ON runbook_definitions
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER change_windows_append_only
    BEFORE UPDATE OR DELETE ON change_windows
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER runbook_manual_gate_decisions_append_only
    BEFORE UPDATE OR DELETE ON runbook_manual_gate_decisions
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER change_schedule_events_append_only
    BEFORE UPDATE OR DELETE ON change_schedule_events
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_change_schedule()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'change schedules cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.id,
        OLD.tenant_id,
        OLD.cluster_id,
        OLD.correlation_id,
        OLD.runbook_id,
        OLD.runbook_version,
        OLD.plan_bindings,
        OLD.scheduled_start,
        OLD.scheduled_end,
        OLD.resource_keys,
        OLD.created_by,
        OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.correlation_id,
        NEW.runbook_id,
        NEW.runbook_version,
        NEW.plan_bindings,
        NEW.scheduled_start,
        NEW.scheduled_end,
        NEW.resource_keys,
        NEW.created_by,
        NEW.created_at
    ) THEN
        RAISE EXCEPTION 'change schedule protected fields are immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.status IN ('completed', 'cancelled', 'rejected') THEN
        RAISE EXCEPTION 'terminal change schedule is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.status <> NEW.status AND NOT (
        (OLD.status = 'scheduled' AND NEW.status IN (
            'running', 'awaiting_manual_gate', 'paused', 'cancelled', 'rejected'
        ))
        OR (OLD.status = 'running' AND NEW.status IN (
            'awaiting_manual_gate', 'paused', 'safe_stopping',
            'reconciling', 'completed', 'rejected'
        ))
        OR (OLD.status = 'awaiting_manual_gate' AND NEW.status IN (
            'running', 'paused', 'safe_stopping', 'completed', 'cancelled', 'rejected'
        ))
        OR (OLD.status = 'paused' AND NEW.status IN (
            'scheduled', 'running', 'awaiting_manual_gate',
            'safe_stopping', 'cancelled'
        ))
        OR (OLD.status = 'safe_stopping' AND NEW.status = 'reconciling')
        OR (OLD.status = 'reconciling' AND NEW.status IN ('completed', 'rejected'))
    ) THEN
        RAISE EXCEPTION 'invalid change schedule state transition'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'change schedule update time cannot move backwards'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER change_schedules_protected
    BEFORE UPDATE OR DELETE ON change_schedules
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_change_schedule();

CREATE OR REPLACE FUNCTION rocketmq_sre_validate_schedule_execution_scope()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.active_execution_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM executions execution
        WHERE execution.id = NEW.active_execution_id
          AND execution.tenant_id = NEW.tenant_id
          AND execution.cluster_id = NEW.cluster_id
    ) THEN
        RAISE EXCEPTION 'invalid_schedule_execution_scope' USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER change_schedules_validate_execution_scope
    BEFORE INSERT OR UPDATE ON change_schedules
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_validate_schedule_execution_scope();
