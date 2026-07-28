-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- A preventive run is accepted before its read-only Inspection exists. Allow
-- that immutable artifact identity, the representative Recommendation, and an
-- optional one-way safety freeze to be bound exactly once at terminalization.
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
    IF OLD.inspection_run_id IS DISTINCT FROM NEW.inspection_run_id
        AND (
            OLD.inspection_run_id IS NOT NULL
            OR NEW.inspection_run_id IS NULL
            OR NEW.status NOT IN ('succeeded', 'failed', 'denied')
        )
    THEN
        RAISE EXCEPTION 'preventive inspection artifact can only be bound once at completion'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.recommendation_id IS DISTINCT FROM NEW.recommendation_id
        AND (
            OLD.recommendation_id IS NOT NULL
            OR NEW.recommendation_id IS NULL
            OR NEW.status NOT IN ('succeeded', 'failed', 'denied')
        )
    THEN
        RAISE EXCEPTION 'preventive recommendation artifact can only be bound once at completion'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.freeze_id IS DISTINCT FROM NEW.freeze_id
        AND (
            OLD.freeze_id IS NOT NULL
            OR NEW.freeze_id IS NULL
            OR NEW.status NOT IN ('succeeded', 'failed', 'denied')
        )
    THEN
        RAISE EXCEPTION 'preventive freeze artifact can only be bound once at completion'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.status = 'succeeded' AND NEW.inspection_run_id IS NULL THEN
        RAISE EXCEPTION 'successful preventive runs require an inspection artifact'
            USING ERRCODE = '23514';
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

ALTER TABLE preventive_automation_runs
    ADD CONSTRAINT preventive_success_requires_inspection
    CHECK (status <> 'succeeded' OR inspection_run_id IS NOT NULL) NOT VALID;

CREATE INDEX preventive_automation_runs_timeline
    ON preventive_automation_runs (tenant_id, started_at DESC, id DESC);

CREATE INDEX preventive_automation_runs_scope
    ON preventive_automation_runs (
        tenant_id,
        cluster_id,
        risk_family,
        started_at DESC
    );
