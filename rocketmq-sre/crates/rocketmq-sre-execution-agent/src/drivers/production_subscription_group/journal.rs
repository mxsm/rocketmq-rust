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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use sqlx::PgPool;
use sqlx::Row;
use uuid::Uuid;

use super::SubscriptionGroupPatch;
use super::SubscriptionGroupPatchApplyOutcome;
use super::SubscriptionGroupSafetyState;
use crate::AgentStoreError;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(super) struct SubscriptionGroupBeforeBroker {
    pub(super) broker_addr: String,
    pub(super) version: u64,
    pub(super) before: SubscriptionGroupPatch,
    pub(super) safety: SubscriptionGroupSafetyState,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(super) struct SubscriptionGroupBeforeState {
    pub(super) group: String,
    pub(super) operation_id: String,
    pub(super) expected_version: u64,
    pub(super) brokers: Vec<SubscriptionGroupBeforeBroker>,
    pub(super) forward_patch: SubscriptionGroupPatch,
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

#[derive(Clone)]
pub(super) struct SubscriptionGroupJournal {
    pool: PgPool,
}

impl SubscriptionGroupJournal {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(super) async fn persist_before(
        &self,
        execution_id: ExecutionId,
        plan_step_id: PlanStepId,
        before: &SubscriptionGroupBeforeState,
        created_at: DateTime<Utc>,
    ) -> Result<SubscriptionGroupBeforeState, AgentStoreError> {
        let broker_states = serde_json::to_value(&before.brokers).map_err(AgentStoreError::SnapshotEncoding)?;
        let forward_patch = serde_json::to_value(&before.forward_patch).map_err(AgentStoreError::SnapshotEncoding)?;
        sqlx::query(
            "INSERT INTO execution_agent_subscription_group_before_states (
                id, execution_id, plan_step_id, consumer_group, operation_id,
                expected_version, broker_states_snapshot, forward_patch_snapshot,
                created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (execution_id, plan_step_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .bind(&before.group)
        .bind(&before.operation_id)
        .bind(version_i64(before.expected_version)?)
        .bind(broker_states)
        .bind(forward_patch)
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
    ) -> Result<SubscriptionGroupBeforeState, AgentStoreError> {
        let row = sqlx::query(
            "SELECT consumer_group, operation_id, expected_version,
                    broker_states_snapshot, forward_patch_snapshot
             FROM execution_agent_subscription_group_before_states
             WHERE execution_id = $1 AND plan_step_id = $2",
        )
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AgentStoreError::NotFound)?;
        decode_before_row(&row)
    }

    pub(super) async fn load_before_by_operation(
        &self,
        operation_id: &str,
    ) -> Result<SubscriptionGroupBeforeState, AgentStoreError> {
        let row = sqlx::query(
            "SELECT consumer_group, operation_id, expected_version,
                    broker_states_snapshot, forward_patch_snapshot
             FROM execution_agent_subscription_group_before_states
             WHERE operation_id = $1",
        )
        .bind(operation_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AgentStoreError::NotFound)?;
        decode_before_row(&row)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the append-only row deliberately records the full effect identity"
    )]
    pub(super) async fn append_result(
        &self,
        execution_id: ExecutionId,
        plan_step_id: PlanStepId,
        group: &str,
        broker_addr: &str,
        operation_id: &str,
        direction: OperationDirection,
        expected_version: u64,
        outcome: SubscriptionGroupPatchApplyOutcome,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), AgentStoreError> {
        let (outcome_code, observed_version, result_snapshot) = match outcome {
            SubscriptionGroupPatchApplyOutcome::Applied {
                previous_version,
                version,
            } => (
                "applied",
                version,
                json!({
                    "previous_version": previous_version,
                    "version": version,
                }),
            ),
            SubscriptionGroupPatchApplyOutcome::VersionConflict {
                expected_version,
                actual_version,
            } => (
                "version_conflict",
                actual_version,
                json!({
                    "expected_version": expected_version,
                    "actual_version": actual_version,
                }),
            ),
        };
        let insert = sqlx::query(
            "INSERT INTO execution_agent_subscription_group_results (
                execution_id, plan_step_id, consumer_group, broker_addr,
                operation_id, direction, outcome, expected_version,
                observed_version, result_snapshot, recorded_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (operation_id, broker_addr, direction) DO NOTHING",
        )
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .bind(group)
        .bind(broker_addr)
        .bind(operation_id)
        .bind(direction.as_str())
        .bind(outcome_code)
        .bind(version_i64(expected_version)?)
        .bind(version_i64(observed_version)?)
        .bind(result_snapshot)
        .bind(recorded_at)
        .execute(&self.pool)
        .await?;
        if insert.rows_affected() == 1 {
            return Ok(());
        }
        let identical: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM execution_agent_subscription_group_results
                WHERE operation_id = $1
                  AND broker_addr = $2
                  AND direction = $3
                  AND execution_id = $4
                  AND plan_step_id = $5
                  AND consumer_group = $6
                  AND outcome = $7
                  AND expected_version = $8
                  AND observed_version = $9
            )",
        )
        .bind(operation_id)
        .bind(broker_addr)
        .bind(direction.as_str())
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .bind(group)
        .bind(outcome_code)
        .bind(version_i64(expected_version)?)
        .bind(version_i64(observed_version)?)
        .fetch_one(&self.pool)
        .await?;
        if identical {
            Ok(())
        } else {
            Err(AgentStoreError::IdempotencyConflict)
        }
    }

    pub(super) async fn last_applied_operation(
        &self,
        group: &str,
        version: u64,
        broker_addrs: &BTreeSet<String>,
    ) -> Result<Option<String>, AgentStoreError> {
        let rows = sqlx::query(
            "SELECT operation_id, broker_addr
             FROM execution_agent_subscription_group_results
             WHERE consumer_group = $1
               AND observed_version = $2
               AND outcome = 'applied'
             ORDER BY sequence_id DESC",
        )
        .bind(group)
        .bind(version_i64(version)?)
        .fetch_all(&self.pool)
        .await?;
        let mut operation_order = Vec::new();
        let mut brokers_by_operation = BTreeMap::<String, BTreeSet<String>>::new();
        for row in rows {
            let operation_id: String = row.try_get("operation_id")?;
            let broker_addr: String = row.try_get("broker_addr")?;
            if !brokers_by_operation.contains_key(&operation_id) {
                operation_order.push(operation_id.clone());
            }
            brokers_by_operation
                .entry(operation_id)
                .or_default()
                .insert(broker_addr);
        }
        Ok(operation_order.into_iter().find(|operation_id| {
            brokers_by_operation
                .get(operation_id)
                .is_some_and(|observed| observed == broker_addrs)
        }))
    }
}

fn decode_before_row(row: &sqlx::postgres::PgRow) -> Result<SubscriptionGroupBeforeState, AgentStoreError> {
    let expected_version = u64::try_from(row.try_get::<i64, _>("expected_version")?)
        .map_err(|_| AgentStoreError::InvalidInput("stored Subscription Group version is invalid".to_owned()))?;
    Ok(SubscriptionGroupBeforeState {
        group: row.try_get("consumer_group")?,
        operation_id: row.try_get("operation_id")?,
        expected_version,
        brokers: serde_json::from_value(row.try_get::<Value, _>("broker_states_snapshot")?)
            .map_err(AgentStoreError::SnapshotDecoding)?,
        forward_patch: serde_json::from_value(row.try_get::<Value, _>("forward_patch_snapshot")?)
            .map_err(AgentStoreError::SnapshotDecoding)?,
    })
}

fn version_i64(version: u64) -> Result<i64, AgentStoreError> {
    i64::try_from(version)
        .map_err(|_| AgentStoreError::InvalidInput("Subscription Group version exceeds PostgreSQL BIGINT".to_owned()))
}
