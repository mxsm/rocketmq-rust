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

CREATE TABLE execution_agent_proxy_canary_before_states (
    id UUID PRIMARY KEY,
    execution_id UUID NOT NULL,
    plan_step_id UUID NOT NULL,
    namespace TEXT NOT NULL CHECK (char_length(namespace) BETWEEN 1 AND 63),
    workload TEXT NOT NULL CHECK (char_length(workload) BETWEEN 1 AND 253),
    container_name TEXT NOT NULL CHECK (char_length(container_name) BETWEEN 1 AND 128),
    operation_id TEXT NOT NULL UNIQUE CHECK (char_length(operation_id) BETWEEN 1 AND 128),
    base_generation BIGINT NOT NULL CHECK (base_generation >= 1),
    previous_image TEXT NOT NULL CHECK (char_length(previous_image) BETWEEN 1 AND 512),
    candidate_image_digest TEXT NOT NULL CHECK (
        candidate_image_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    original_replicas INTEGER NOT NULL CHECK (original_replicas >= 1),
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (execution_id, plan_step_id)
);

CREATE INDEX execution_agent_proxy_canary_before_resource
    ON execution_agent_proxy_canary_before_states (
        namespace,
        workload,
        created_at DESC,
        id
    );

CREATE TABLE execution_agent_proxy_canary_results (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL,
    plan_step_id UUID NOT NULL,
    namespace TEXT NOT NULL,
    workload TEXT NOT NULL,
    canary_name TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('forward', 'compensation')),
    canary_uid TEXT,
    image_digest TEXT NOT NULL CHECK (image_digest ~ '^sha256:[0-9a-f]{64}$'),
    ready BOOLEAN NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    UNIQUE (operation_id, direction)
);

CREATE INDEX execution_agent_proxy_canary_results_reconcile
    ON execution_agent_proxy_canary_results (
        namespace,
        workload,
        sequence_id DESC
    );

CREATE TRIGGER execution_agent_proxy_canary_before_states_append_only
    BEFORE UPDATE OR DELETE ON execution_agent_proxy_canary_before_states
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE TRIGGER execution_agent_proxy_canary_results_append_only
    BEFORE UPDATE OR DELETE ON execution_agent_proxy_canary_results
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
