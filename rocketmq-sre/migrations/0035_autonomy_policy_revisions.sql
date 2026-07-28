-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- A lifecycle scope remains stable while its immutable policy definition
-- advances. The revision bump invalidates old grants without clearing cohort
-- history; the new policy version creates new qualification cohorts.
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
        OLD.policy_id
    ) IS DISTINCT FROM ROW(
        NEW.tenant_id,
        NEW.cluster_id,
        NEW.action_id,
        NEW.action_version,
        NEW.policy_id
    ) THEN
        RAISE EXCEPTION 'autonomy lifecycle scope and policy identity are immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.policy_definition_version < OLD.policy_definition_version
        OR NEW.policy_definition_version > OLD.policy_definition_version + 1
    THEN
        RAISE EXCEPTION 'autonomy policy definition version must be monotonic'
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
