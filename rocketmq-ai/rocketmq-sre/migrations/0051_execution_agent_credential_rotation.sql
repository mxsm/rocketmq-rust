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

CREATE TABLE execution_agent_credential_rotation_before_states (
    id UUID PRIMARY KEY,
    execution_id UUID NOT NULL,
    plan_step_id UUID NOT NULL,
    credential_set TEXT NOT NULL CHECK (char_length(credential_set) BETWEEN 1 AND 128),
    selector_namespace TEXT NOT NULL CHECK (char_length(selector_namespace) BETWEEN 1 AND 63),
    selector_name TEXT NOT NULL CHECK (char_length(selector_name) BETWEEN 1 AND 253),
    selector_uid TEXT NOT NULL CHECK (char_length(selector_uid) BETWEEN 1 AND 128),
    selector_resource_version TEXT NOT NULL CHECK (char_length(selector_resource_version) BETWEEN 1 AND 128),
    operation_id TEXT NOT NULL UNIQUE CHECK (char_length(operation_id) BETWEEN 1 AND 128),
    previous_active_version TEXT NOT NULL CHECK (char_length(previous_active_version) BETWEEN 1 AND 128),
    previous_active_secret_ref TEXT NOT NULL CHECK (char_length(previous_active_secret_ref) BETWEEN 1 AND 255),
    candidate_version TEXT NOT NULL CHECK (char_length(candidate_version) BETWEEN 1 AND 128),
    candidate_secret_ref_hash TEXT NOT NULL CHECK (
        candidate_secret_ref_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    validation_probe_topic TEXT NOT NULL CHECK (
        char_length(validation_probe_topic) BETWEEN 1 AND 255
        AND validation_probe_topic LIKE 'SRE\_PROBE\_%' ESCAPE '\'
    ),
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (execution_id, plan_step_id)
);

CREATE INDEX execution_agent_credential_rotation_before_resource
    ON execution_agent_credential_rotation_before_states (
        credential_set,
        created_at DESC,
        id
    );

CREATE TABLE execution_agent_credential_rotation_results (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL,
    plan_step_id UUID NOT NULL,
    credential_set TEXT NOT NULL,
    operation_id TEXT NOT NULL UNIQUE,
    direction TEXT NOT NULL CHECK (direction IN ('forward', 'compensation')),
    active_version TEXT NOT NULL,
    retiring_version TEXT,
    overlap_deadline TIMESTAMPTZ,
    candidate_probe_healthy BOOLEAN NOT NULL,
    selector_resource_version TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX execution_agent_credential_rotation_results_reconcile
    ON execution_agent_credential_rotation_results (
        credential_set,
        sequence_id DESC
    );

CREATE TRIGGER execution_agent_credential_rotation_before_states_append_only
    BEFORE UPDATE OR DELETE ON execution_agent_credential_rotation_before_states
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE TRIGGER execution_agent_credential_rotation_results_append_only
    BEFORE UPDATE OR DELETE ON execution_agent_credential_rotation_results
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
