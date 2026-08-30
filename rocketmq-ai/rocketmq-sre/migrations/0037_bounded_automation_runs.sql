-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Preserve the exact bounded request and correlation identity. Defaults only
-- backfill deployments that created rows before this contract was wired.
ALTER TABLE no_side_effect_automation_runs
    ADD COLUMN correlation_id UUID NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN budget_snapshot JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN request_snapshot JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE no_side_effect_automation_runs
    ALTER COLUMN correlation_id DROP DEFAULT,
    ALTER COLUMN budget_snapshot DROP DEFAULT,
    ALTER COLUMN request_snapshot DROP DEFAULT,
    ALTER COLUMN updated_at DROP DEFAULT;

ALTER TABLE no_side_effect_automation_runs
    ADD CONSTRAINT no_side_effect_budget_object
    CHECK (jsonb_typeof(budget_snapshot) = 'object') NOT VALID,
    ADD CONSTRAINT no_side_effect_request_object
    CHECK (jsonb_typeof(request_snapshot) = 'object') NOT VALID,
    ADD CONSTRAINT no_side_effect_idempotency_bound
    CHECK (char_length(idempotency_key) BETWEEN 16 AND 200) NOT VALID;

ALTER TABLE preventive_automation_runs
    ADD COLUMN correlation_id UUID NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT 'legacy-preventive-run',
    ADD COLUMN budget_snapshot JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN request_snapshot JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE preventive_automation_runs
    ALTER COLUMN correlation_id DROP DEFAULT,
    ALTER COLUMN idempotency_key DROP DEFAULT,
    ALTER COLUMN budget_snapshot DROP DEFAULT,
    ALTER COLUMN request_snapshot DROP DEFAULT,
    ALTER COLUMN updated_at DROP DEFAULT;

ALTER TABLE preventive_automation_runs
    ADD CONSTRAINT preventive_budget_object
    CHECK (jsonb_typeof(budget_snapshot) = 'object') NOT VALID,
    ADD CONSTRAINT preventive_request_object
    CHECK (jsonb_typeof(request_snapshot) = 'object') NOT VALID,
    ADD CONSTRAINT preventive_idempotency_bound
    CHECK (char_length(idempotency_key) BETWEEN 16 AND 200) NOT VALID;

CREATE UNIQUE INDEX preventive_automation_idempotency
    ON preventive_automation_runs (tenant_id, risk_family, idempotency_key);

CREATE TABLE automation_run_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    run_id UUID NOT NULL,
    run_family TEXT NOT NULL CHECK (
        run_family IN ('no_side_effect', 'preventive')
    ),
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    correlation_id UUID NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL CHECK (
        to_status IN ('pending', 'running', 'succeeded', 'failed', 'denied')
    ),
    reason_code TEXT NOT NULL CHECK (char_length(reason_code) BETWEEN 1 AND 128),
    event_snapshot JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    CHECK (jsonb_typeof(event_snapshot) = 'object')
);

CREATE INDEX automation_run_events_timeline
    ON automation_run_events (tenant_id, correlation_id, sequence_id);

CREATE TRIGGER automation_run_events_append_only
    BEFORE UPDATE OR DELETE ON automation_run_events
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_automation_run()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'automation runs cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.id,
        OLD.tenant_id,
        OLD.cluster_id,
        OLD.incident_id,
        OLD.automation_kind,
        OLD.idempotency_key,
        OLD.correlation_id,
        OLD.budget_snapshot,
        OLD.request_snapshot,
        OLD.started_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.incident_id,
        NEW.automation_kind,
        NEW.idempotency_key,
        NEW.correlation_id,
        NEW.budget_snapshot,
        NEW.request_snapshot,
        NEW.started_at
    ) THEN
        RAISE EXCEPTION 'automation run identity and request are immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.status IN ('succeeded', 'failed', 'denied') THEN
        RAISE EXCEPTION 'terminal automation runs are immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NOT (
        (OLD.status = 'pending' AND NEW.status IN ('running', 'failed', 'denied'))
        OR (OLD.status = 'running' AND NEW.status IN ('succeeded', 'failed', 'denied'))
    ) THEN
        RAISE EXCEPTION 'invalid automation run transition' USING ERRCODE = '55000';
    END IF;
    IF NEW.updated_at < OLD.updated_at
        OR NEW.completed_at IS NOT NULL AND NEW.completed_at < NEW.started_at
    THEN
        RAISE EXCEPTION 'automation run time cannot move backwards' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER no_side_effect_automation_runs_no_delete
    ON no_side_effect_automation_runs;

CREATE TRIGGER no_side_effect_automation_runs_protected
    BEFORE UPDATE OR DELETE ON no_side_effect_automation_runs
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_automation_run();

CREATE OR REPLACE FUNCTION rocketmq_sre_protect_preventive_run()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'preventive automation runs cannot be deleted'
            USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.id,
        OLD.tenant_id,
        OLD.cluster_id,
        OLD.inspection_run_id,
        OLD.risk_family,
        OLD.idempotency_key,
        OLD.correlation_id,
        OLD.budget_snapshot,
        OLD.request_snapshot,
        OLD.started_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.inspection_run_id,
        NEW.risk_family,
        NEW.idempotency_key,
        NEW.correlation_id,
        NEW.budget_snapshot,
        NEW.request_snapshot,
        NEW.started_at
    ) THEN
        RAISE EXCEPTION 'preventive run identity and request are immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.status IN ('succeeded', 'failed', 'denied') THEN
        RAISE EXCEPTION 'terminal preventive runs are immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NOT (
        (OLD.status = 'pending' AND NEW.status IN ('running', 'failed', 'denied'))
        OR (OLD.status = 'running' AND NEW.status IN ('succeeded', 'failed', 'denied'))
    ) THEN
        RAISE EXCEPTION 'invalid preventive run transition' USING ERRCODE = '55000';
    END IF;
    IF NEW.updated_at < OLD.updated_at
        OR NEW.completed_at IS NOT NULL AND NEW.completed_at < NEW.started_at
    THEN
        RAISE EXCEPTION 'preventive run time cannot move backwards'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER preventive_automation_runs_no_delete
    ON preventive_automation_runs;

CREATE TRIGGER preventive_automation_runs_protected
    BEFORE UPDATE OR DELETE ON preventive_automation_runs
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_protect_preventive_run();
