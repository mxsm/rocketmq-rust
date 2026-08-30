// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use sqlx::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::AgentStoreError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CredentialBeforeState {
    pub(super) credential_set: String,
    pub(super) selector_namespace: String,
    pub(super) selector_name: String,
    pub(super) selector_uid: String,
    pub(super) selector_resource_version: String,
    pub(super) operation_id: String,
    pub(super) previous_active_version: String,
    pub(super) previous_active_secret_ref: String,
    pub(super) candidate_version: String,
    pub(super) candidate_secret_ref_hash: String,
    pub(super) validation_probe_topic: String,
}

#[derive(Clone, Copy)]
pub(super) enum OperationDirection {
    Forward,
    Compensation,
}

impl OperationDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Compensation => "compensation",
        }
    }
}

pub(super) struct CredentialResult<'a> {
    pub(super) credential_set: &'a str,
    pub(super) operation_id: &'a str,
    pub(super) direction: OperationDirection,
    pub(super) active_version: &'a str,
    pub(super) retiring_version: Option<&'a str>,
    pub(super) overlap_deadline: Option<DateTime<Utc>>,
    pub(super) candidate_probe_healthy: bool,
    pub(super) selector_resource_version: &'a str,
}

#[derive(Clone)]
pub(super) struct CredentialRotationJournal {
    pool: PgPool,
}

impl CredentialRotationJournal {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(super) async fn persist_before(
        &self,
        execution_id: ExecutionId,
        plan_step_id: PlanStepId,
        before: &CredentialBeforeState,
        created_at: DateTime<Utc>,
    ) -> Result<CredentialBeforeState, AgentStoreError> {
        sqlx::query(
            "INSERT INTO execution_agent_credential_rotation_before_states (
                 id, execution_id, plan_step_id, credential_set,
                 selector_namespace, selector_name, selector_uid,
                 selector_resource_version, operation_id,
                 previous_active_version, previous_active_secret_ref,
                 candidate_version, candidate_secret_ref_hash,
                 validation_probe_topic, created_at
             )
             VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                 $11, $12, $13, $14, $15
             )
             ON CONFLICT (execution_id, plan_step_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .bind(&before.credential_set)
        .bind(&before.selector_namespace)
        .bind(&before.selector_name)
        .bind(&before.selector_uid)
        .bind(&before.selector_resource_version)
        .bind(&before.operation_id)
        .bind(&before.previous_active_version)
        .bind(&before.previous_active_secret_ref)
        .bind(&before.candidate_version)
        .bind(&before.candidate_secret_ref_hash)
        .bind(&before.validation_probe_topic)
        .bind(created_at)
        .execute(&self.pool)
        .await?;

        let persisted = self.load_before(execution_id, plan_step_id).await?;
        if persisted == *before {
            Ok(persisted)
        } else {
            Err(AgentStoreError::IdempotencyConflict)
        }
    }

    pub(super) async fn load_before(
        &self,
        execution_id: ExecutionId,
        plan_step_id: PlanStepId,
    ) -> Result<CredentialBeforeState, AgentStoreError> {
        let row = sqlx::query(
            "SELECT credential_set, selector_namespace, selector_name,
                    selector_uid, selector_resource_version, operation_id,
                    previous_active_version, previous_active_secret_ref,
                    candidate_version, candidate_secret_ref_hash,
                    validation_probe_topic
             FROM execution_agent_credential_rotation_before_states
             WHERE execution_id = $1 AND plan_step_id = $2",
        )
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AgentStoreError::NotFound)?;
        Ok(CredentialBeforeState {
            credential_set: row.try_get("credential_set")?,
            selector_namespace: row.try_get("selector_namespace")?,
            selector_name: row.try_get("selector_name")?,
            selector_uid: row.try_get("selector_uid")?,
            selector_resource_version: row.try_get("selector_resource_version")?,
            operation_id: row.try_get("operation_id")?,
            previous_active_version: row.try_get("previous_active_version")?,
            previous_active_secret_ref: row.try_get("previous_active_secret_ref")?,
            candidate_version: row.try_get("candidate_version")?,
            candidate_secret_ref_hash: row.try_get("candidate_secret_ref_hash")?,
            validation_probe_topic: row.try_get("validation_probe_topic")?,
        })
    }

    pub(super) async fn append_result(
        &self,
        execution_id: ExecutionId,
        plan_step_id: PlanStepId,
        result: &CredentialResult<'_>,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), AgentStoreError> {
        let insert = sqlx::query(
            "INSERT INTO execution_agent_credential_rotation_results (
                 execution_id, plan_step_id, credential_set, operation_id,
                 direction, active_version, retiring_version,
                 overlap_deadline, candidate_probe_healthy,
                 selector_resource_version, recorded_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (operation_id) DO NOTHING",
        )
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .bind(result.credential_set)
        .bind(result.operation_id)
        .bind(result.direction.as_str())
        .bind(result.active_version)
        .bind(result.retiring_version)
        .bind(result.overlap_deadline)
        .bind(result.candidate_probe_healthy)
        .bind(result.selector_resource_version)
        .bind(recorded_at)
        .execute(&self.pool)
        .await?;
        if insert.rows_affected() == 1 {
            return Ok(());
        }
        let identical: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM execution_agent_credential_rotation_results
                 WHERE execution_id = $1
                   AND plan_step_id = $2
                   AND credential_set = $3
                   AND operation_id = $4
                   AND direction = $5
                   AND active_version = $6
                   AND retiring_version IS NOT DISTINCT FROM $7
                   AND overlap_deadline IS NOT DISTINCT FROM $8
                   AND candidate_probe_healthy = $9
                   AND selector_resource_version = $10
             )",
        )
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .bind(result.credential_set)
        .bind(result.operation_id)
        .bind(result.direction.as_str())
        .bind(result.active_version)
        .bind(result.retiring_version)
        .bind(result.overlap_deadline)
        .bind(result.candidate_probe_healthy)
        .bind(result.selector_resource_version)
        .fetch_one(&self.pool)
        .await?;
        if identical {
            Ok(())
        } else {
            Err(AgentStoreError::IdempotencyConflict)
        }
    }
}
