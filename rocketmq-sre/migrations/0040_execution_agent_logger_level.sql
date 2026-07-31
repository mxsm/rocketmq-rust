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

CREATE TABLE execution_agent_logger_level_before_states (
    id UUID PRIMARY KEY,
    execution_id UUID NOT NULL,
    plan_step_id UUID NOT NULL,
    component TEXT NOT NULL CHECK (component = 'broker'),
    broker_addr TEXT NOT NULL CHECK (char_length(broker_addr) BETWEEN 1 AND 512),
    logger TEXT NOT NULL CHECK (
        char_length(logger) BETWEEN 1 AND 128
        AND logger LIKE 'rocketmq_broker::%'
    ),
    before_level TEXT NOT NULL CHECK (before_level IN ('INFO', 'DEBUG')),
    requested_level TEXT NOT NULL CHECK (requested_level IN ('INFO', 'DEBUG')),
    forward_operation_id TEXT NOT NULL UNIQUE
        CHECK (char_length(forward_operation_id) BETWEEN 1 AND 128),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (execution_id, plan_step_id)
);

CREATE INDEX execution_agent_logger_level_before_resource
    ON execution_agent_logger_level_before_states (
        broker_addr,
        logger,
        created_at DESC,
        id
    );

CREATE TABLE execution_agent_logger_level_results (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL,
    plan_step_id UUID NOT NULL,
    component TEXT NOT NULL CHECK (component = 'broker'),
    broker_addr TEXT NOT NULL CHECK (char_length(broker_addr) BETWEEN 1 AND 512),
    logger TEXT NOT NULL CHECK (
        char_length(logger) BETWEEN 1 AND 128
        AND logger LIKE 'rocketmq_broker::%'
    ),
    operation_id TEXT NOT NULL UNIQUE
        CHECK (char_length(operation_id) BETWEEN 1 AND 128),
    direction TEXT NOT NULL CHECK (direction IN ('forward', 'compensation')),
    observed_level TEXT NOT NULL CHECK (observed_level IN ('INFO', 'DEBUG')),
    active_operation_id TEXT,
    last_completed_operation_id TEXT,
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX execution_agent_logger_level_results_reconcile
    ON execution_agent_logger_level_results (
        broker_addr,
        logger,
        sequence_id DESC
    );

CREATE TRIGGER execution_agent_logger_level_before_states_append_only
    BEFORE UPDATE OR DELETE ON execution_agent_logger_level_before_states
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE TRIGGER execution_agent_logger_level_results_append_only
    BEFORE UPDATE OR DELETE ON execution_agent_logger_level_results
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
