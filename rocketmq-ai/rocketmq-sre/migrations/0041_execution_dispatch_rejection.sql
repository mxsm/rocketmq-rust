-- Copyright 2026 The RocketMQ Rust Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
        (OLD.state = 'pending' AND NEW.state IN ('prechecking', 'escalated'))
        OR (OLD.state = 'prechecking' AND NEW.state IN ('intent_persisted', 'compensating', 'escalated'))
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
