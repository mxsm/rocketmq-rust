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

CREATE TABLE execution_agent_topic_config_before_states (
    id UUID PRIMARY KEY,
    execution_id UUID NOT NULL,
    plan_step_id UUID NOT NULL,
    topic TEXT NOT NULL,
    operation_id TEXT NOT NULL UNIQUE,
    expected_version BIGINT NOT NULL CHECK (expected_version >= 0),
    broker_states_snapshot JSONB NOT NULL,
    forward_patch_snapshot JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (execution_id, plan_step_id)
);

CREATE INDEX execution_agent_topic_config_before_resource
    ON execution_agent_topic_config_before_states (topic, created_at DESC, id);

CREATE TABLE execution_agent_topic_config_results (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL,
    plan_step_id UUID NOT NULL,
    topic TEXT NOT NULL,
    broker_addr TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('forward', 'compensation')),
    outcome TEXT NOT NULL CHECK (outcome IN ('applied', 'version_conflict')),
    expected_version BIGINT NOT NULL CHECK (expected_version >= 0),
    observed_version BIGINT NOT NULL CHECK (observed_version >= 0),
    result_snapshot JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    UNIQUE (operation_id, broker_addr, direction)
);

CREATE INDEX execution_agent_topic_config_results_reconcile
    ON execution_agent_topic_config_results (
        topic,
        observed_version DESC,
        sequence_id DESC
    );

CREATE TRIGGER execution_agent_topic_config_before_states_append_only
    BEFORE UPDATE OR DELETE ON execution_agent_topic_config_before_states
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE TRIGGER execution_agent_topic_config_results_append_only
    BEFORE UPDATE OR DELETE ON execution_agent_topic_config_results
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
