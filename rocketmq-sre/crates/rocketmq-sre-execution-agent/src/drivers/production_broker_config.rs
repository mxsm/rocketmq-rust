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
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_admin_core::core::broker::BrokerMutationAdmin;
use rocketmq_admin_core::core::broker::BrokerQueryAdmin;
use rocketmq_admin_core::core::broker::PatchBrokerConfigOutcome;
use rocketmq_admin_core::core::broker::PatchBrokerConfigRequest;
use rocketmq_admin_core::core::broker::QueryBrokerAllowlistedConfigRequest;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminBuilder;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminSession;
use rocketmq_admin_core::read_client_adapter::ClientRuntime;
use rocketmq_admin_core::read_client_adapter::ClientRuntimeConfig;
use rocketmq_admin_core::read_client_adapter::ReadAdminBuilder;
use rocketmq_admin_core::read_client_adapter::ReadAdminSession;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use sqlx::PgPool;
use sqlx::Row;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::BrokerConfigPatch;
use super::BrokerConfigPatchApplyOutcome;
use super::BrokerConfigPatchClient;
use super::BrokerConfigPatchRestore;
use super::BrokerConfigPatchState;
use super::BrokerConfigPatchWrite;
use super::DriverFuture;
use crate::AgentStoreError;
use crate::ExecutionAgentError;
use crate::config::BrokerAdminDriverConfig;

const SEND_THREADS: &str = "send_message_thread_pool_nums";
const PULL_THREADS: &str = "pull_message_thread_pool_nums";
const FLUSH_DELAY: &str = "flush_delay_offset_interval_ms";

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct BrokerBeforeState {
    broker_addr: String,
    operation_id: String,
    expected_generation: u64,
    before: BrokerConfigPatch,
    forward_patch: BrokerConfigPatch,
}

#[derive(Clone, Copy)]
enum OperationDirection {
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
struct BrokerConfigJournal {
    pool: PgPool,
}

impl BrokerConfigJournal {
    fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn persist_before(
        &self,
        execution_id: ExecutionId,
        plan_step_id: PlanStepId,
        before: &BrokerBeforeState,
        created_at: DateTime<Utc>,
    ) -> Result<BrokerBeforeState, AgentStoreError> {
        let before_snapshot = serde_json::to_value(&before.before).map_err(AgentStoreError::SnapshotEncoding)?;
        let forward_patch_snapshot =
            serde_json::to_value(&before.forward_patch).map_err(AgentStoreError::SnapshotEncoding)?;
        sqlx::query(
            "INSERT INTO execution_agent_broker_config_before_states (
                id, execution_id, plan_step_id, broker_addr, operation_id,
                expected_generation, before_snapshot, forward_patch_snapshot,
                created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (execution_id, plan_step_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .bind(&before.broker_addr)
        .bind(&before.operation_id)
        .bind(generation_i64(before.expected_generation)?)
        .bind(before_snapshot)
        .bind(forward_patch_snapshot)
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

    async fn load_before(
        &self,
        execution_id: ExecutionId,
        plan_step_id: PlanStepId,
    ) -> Result<BrokerBeforeState, AgentStoreError> {
        let row = sqlx::query(
            "SELECT broker_addr, operation_id, expected_generation,
                    before_snapshot, forward_patch_snapshot
             FROM execution_agent_broker_config_before_states
             WHERE execution_id = $1 AND plan_step_id = $2",
        )
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AgentStoreError::NotFound)?;
        let expected_generation = u64::try_from(row.try_get::<i64, _>("expected_generation")?)
            .map_err(|_| AgentStoreError::InvalidInput("stored Broker generation is invalid".to_owned()))?;
        Ok(BrokerBeforeState {
            broker_addr: row.try_get("broker_addr")?,
            operation_id: row.try_get("operation_id")?,
            expected_generation,
            before: serde_json::from_value(row.try_get::<Value, _>("before_snapshot")?)
                .map_err(AgentStoreError::SnapshotDecoding)?,
            forward_patch: serde_json::from_value(row.try_get::<Value, _>("forward_patch_snapshot")?)
                .map_err(AgentStoreError::SnapshotDecoding)?,
        })
    }

    async fn append_result(
        &self,
        execution_id: ExecutionId,
        plan_step_id: PlanStepId,
        broker_addr: &str,
        operation_id: &str,
        direction: OperationDirection,
        expected_generation: u64,
        outcome: BrokerConfigPatchApplyOutcome,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), AgentStoreError> {
        let (outcome_code, observed_generation, result_snapshot) = match outcome {
            BrokerConfigPatchApplyOutcome::Applied {
                previous_generation,
                generation,
            } => (
                "applied",
                generation,
                json!({
                    "previous_generation": previous_generation,
                    "generation": generation,
                }),
            ),
            BrokerConfigPatchApplyOutcome::GenerationConflict {
                expected_generation,
                actual_generation,
            } => (
                "generation_conflict",
                actual_generation,
                json!({
                    "expected_generation": expected_generation,
                    "actual_generation": actual_generation,
                }),
            ),
        };
        let insert = sqlx::query(
            "INSERT INTO execution_agent_broker_config_results (
                execution_id, plan_step_id, broker_addr, operation_id,
                direction, outcome, expected_generation, observed_generation,
                result_snapshot, recorded_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (operation_id) DO NOTHING",
        )
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .bind(broker_addr)
        .bind(operation_id)
        .bind(direction.as_str())
        .bind(outcome_code)
        .bind(generation_i64(expected_generation)?)
        .bind(generation_i64(observed_generation)?)
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
                FROM execution_agent_broker_config_results
                WHERE operation_id = $1
                  AND execution_id = $2
                  AND plan_step_id = $3
                  AND broker_addr = $4
                  AND direction = $5
                  AND outcome = $6
                  AND expected_generation = $7
                  AND observed_generation = $8
            )",
        )
        .bind(operation_id)
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .bind(broker_addr)
        .bind(direction.as_str())
        .bind(outcome_code)
        .bind(generation_i64(expected_generation)?)
        .bind(generation_i64(observed_generation)?)
        .fetch_one(&self.pool)
        .await?;
        if identical {
            Ok(())
        } else {
            Err(AgentStoreError::IdempotencyConflict)
        }
    }

    async fn last_applied_operation(
        &self,
        broker_addr: &str,
        generation: u64,
    ) -> Result<Option<String>, AgentStoreError> {
        sqlx::query_scalar(
            "SELECT operation_id
             FROM execution_agent_broker_config_results
             WHERE broker_addr = $1
               AND observed_generation = $2
               AND outcome = 'applied'
             ORDER BY sequence_id DESC
             LIMIT 1",
        )
        .bind(broker_addr)
        .bind(generation_i64(generation)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }
}

/// Production RocketMQ Admin Core adapter for generation-checked Broker
/// configuration changes.
pub(crate) struct ProductionBrokerConfigPatchClient {
    read_admin: Mutex<ReadAdminSession>,
    mutation_admin: Mutex<MutationAdminSession>,
    journal: BrokerConfigJournal,
    _client_runtime: Arc<ClientRuntime>,
}

impl ProductionBrokerConfigPatchClient {
    pub(crate) async fn start(
        config: &BrokerAdminDriverConfig,
        pool: PgPool,
        context: ChildServiceContext,
    ) -> Result<Self, ExecutionAgentError> {
        let client_runtime = ClientRuntime::new(
            context.child("broker-admin-client"),
            ClientRuntimeConfig {
                shutdown_timeout: config.shutdown_timeout,
            },
        );
        let timeout_millis = duration_millis(config.request_timeout)?;
        let mut read_builder = ReadAdminBuilder::new(Arc::clone(&client_runtime))
            .namesrv_addr(config.namesrv_addr.clone())
            .admin_group("rocketmq-sre-agent-broker-read")
            .instance_name("rocketmq-sre-execution-agent-read")
            .timeout_millis(timeout_millis)
            .use_tls(config.use_tls);
        if let Some(credentials) = &config.read_credentials {
            read_builder = read_builder.credentials(credentials.clone());
        }
        let mut read_admin = read_builder
            .build_and_start()
            .await
            .map_err(|_| ExecutionAgentError::Configuration)?;

        let mut mutation_builder = MutationAdminBuilder::new(Arc::clone(&client_runtime))
            .namesrv_addr(config.namesrv_addr.clone())
            .admin_group("rocketmq-sre-agent-broker-mutation")
            .instance_name("rocketmq-sre-execution-agent-mutation")
            .timeout_millis(timeout_millis)
            .use_tls(config.use_tls);
        if let Some(credentials) = &config.mutation_credentials {
            mutation_builder = mutation_builder.credentials(credentials.clone());
        }
        let mutation_admin = match mutation_builder.build_and_start().await {
            Ok(session) => session,
            Err(_) => {
                read_admin.shutdown().await;
                return Err(ExecutionAgentError::Configuration);
            }
        };
        Ok(Self {
            read_admin: Mutex::new(read_admin),
            mutation_admin: Mutex::new(mutation_admin),
            journal: BrokerConfigJournal::new(pool),
            _client_runtime: client_runtime,
        })
    }

    pub(crate) async fn shutdown(&self) {
        self.read_admin.lock().await.shutdown().await;
        self.mutation_admin.lock().await.shutdown().await;
    }

    async fn live_state(&self, broker_addr: &str) -> Result<BrokerConfigPatchState, ExecutionAgentError> {
        let request =
            QueryBrokerAllowlistedConfigRequest::try_new(broker_addr).map_err(|_| ExecutionAgentError::DriverFailed)?;
        let config = {
            let mut admin = self.read_admin.lock().await;
            admin
                .query_allowlisted_config(&request)
                .await
                .map_err(|_| ExecutionAgentError::DriverFailed)?
        };
        let values = BrokerConfigPatch {
            send_message_thread_pool_nums: config.send_message_thread_pool_nums,
            pull_message_thread_pool_nums: config.pull_message_thread_pool_nums,
            flush_delay_offset_interval_ms: config.flush_delay_offset_interval_ms,
        };
        let supported_fields = [
            values.send_message_thread_pool_nums.map(|_| SEND_THREADS.to_owned()),
            values.pull_message_thread_pool_nums.map(|_| PULL_THREADS.to_owned()),
            values.flush_delay_offset_interval_ms.map(|_| FLUSH_DELAY.to_owned()),
        ]
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
        let last_operation_id = self
            .journal
            .last_applied_operation(broker_addr, config.generation)
            .await?;
        Ok(BrokerConfigPatchState {
            generation: config.generation,
            values,
            supported_fields,
            restart_required_fields: BTreeSet::new(),
            last_operation_id,
        })
    }

    async fn apply_patch(
        &self,
        broker_addr: &str,
        expected_generation: u64,
        patch: &BrokerConfigPatch,
    ) -> Result<BrokerConfigPatchApplyOutcome, ExecutionAgentError> {
        let request = PatchBrokerConfigRequest {
            broker_addr: broker_addr.to_owned(),
            expected_generation,
            properties: broker_properties(patch)?,
        };
        let outcome = {
            let mut admin = self.mutation_admin.lock().await;
            admin
                .patch_config_if_generation(&request)
                .await
                .map_err(|_| ExecutionAgentError::DriverFailed)?
        };
        Ok(match outcome {
            PatchBrokerConfigOutcome::Applied {
                previous_generation,
                generation,
            } => BrokerConfigPatchApplyOutcome::Applied {
                previous_generation,
                generation,
            },
            PatchBrokerConfigOutcome::GenerationConflict {
                expected_generation,
                actual_generation,
            } => BrokerConfigPatchApplyOutcome::GenerationConflict {
                expected_generation,
                actual_generation,
            },
        })
    }
}

impl BrokerConfigPatchClient for ProductionBrokerConfigPatchClient {
    fn broker_config_patch_state<'a>(&'a self, broker_addr: &'a str) -> DriverFuture<'a, BrokerConfigPatchState> {
        Box::pin(async move { self.live_state(broker_addr).await })
    }

    fn patch_broker_config<'a>(
        &'a self,
        request: &'a BrokerConfigPatchWrite,
    ) -> DriverFuture<'a, BrokerConfigPatchApplyOutcome> {
        Box::pin(async move {
            let live = self.live_state(&request.broker_addr).await?;
            if live.generation != request.expected_generation {
                return Ok(BrokerConfigPatchApplyOutcome::GenerationConflict {
                    expected_generation: request.expected_generation,
                    actual_generation: live.generation,
                });
            }
            let before = BrokerBeforeState {
                broker_addr: request.broker_addr.clone(),
                operation_id: request.operation_id.clone(),
                expected_generation: request.expected_generation,
                before: select_before_values(&live.values, &request.patch)?,
                forward_patch: request.patch.clone(),
            };
            self.journal
                .persist_before(request.execution_id, request.plan_step_id, &before, Utc::now())
                .await?;
            let outcome = self
                .apply_patch(&request.broker_addr, request.expected_generation, &request.patch)
                .await?;
            if self
                .journal
                .append_result(
                    request.execution_id,
                    request.plan_step_id,
                    &request.broker_addr,
                    &request.operation_id,
                    OperationDirection::Forward,
                    request.expected_generation,
                    outcome,
                    Utc::now(),
                )
                .await
                .is_err()
            {
                return Err(ExecutionAgentError::DriverUnknown);
            }
            Ok(outcome)
        })
    }

    fn restore_broker_config<'a>(
        &'a self,
        request: &'a BrokerConfigPatchRestore,
    ) -> DriverFuture<'a, BrokerConfigPatchApplyOutcome> {
        Box::pin(async move {
            let before = self
                .journal
                .load_before(request.execution_id, request.plan_step_id)
                .await?;
            if before.broker_addr != request.broker_addr {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let live = self.live_state(&request.broker_addr).await?;
            if !patch_matches(&before.forward_patch, &live.values) {
                return Ok(BrokerConfigPatchApplyOutcome::GenerationConflict {
                    expected_generation: before.expected_generation.saturating_add(1),
                    actual_generation: live.generation,
                });
            }
            let outcome = self
                .apply_patch(&request.broker_addr, live.generation, &before.before)
                .await?;
            if self
                .journal
                .append_result(
                    request.execution_id,
                    request.plan_step_id,
                    &request.broker_addr,
                    &request.operation_id,
                    OperationDirection::Compensation,
                    live.generation,
                    outcome,
                    Utc::now(),
                )
                .await
                .is_err()
            {
                return Err(ExecutionAgentError::DriverUnknown);
            }
            Ok(outcome)
        })
    }
}

fn select_before_values(
    live: &BrokerConfigPatch,
    requested: &BrokerConfigPatch,
) -> Result<BrokerConfigPatch, ExecutionAgentError> {
    Ok(BrokerConfigPatch {
        send_message_thread_pool_nums: requested
            .send_message_thread_pool_nums
            .map(|_| live.send_message_thread_pool_nums)
            .transpose_required()?,
        pull_message_thread_pool_nums: requested
            .pull_message_thread_pool_nums
            .map(|_| live.pull_message_thread_pool_nums)
            .transpose_required()?,
        flush_delay_offset_interval_ms: requested
            .flush_delay_offset_interval_ms
            .map(|_| live.flush_delay_offset_interval_ms)
            .transpose_required()?,
    })
}

trait RequiredOption<T> {
    fn transpose_required(self) -> Result<Option<T>, ExecutionAgentError>;
}

impl<T> RequiredOption<T> for Option<Option<T>> {
    fn transpose_required(self) -> Result<Option<T>, ExecutionAgentError> {
        match self {
            Some(Some(value)) => Ok(Some(value)),
            Some(None) => Err(ExecutionAgentError::DriverFailed),
            None => Ok(None),
        }
    }
}

fn broker_properties(patch: &BrokerConfigPatch) -> Result<BTreeMap<String, String>, ExecutionAgentError> {
    let mut properties = BTreeMap::new();
    if let Some(value) = patch.send_message_thread_pool_nums {
        properties.insert("sendMessageThreadPoolNums".to_owned(), value.to_string());
    }
    if let Some(value) = patch.pull_message_thread_pool_nums {
        properties.insert("pullMessageThreadPoolNums".to_owned(), value.to_string());
    }
    if let Some(value) = patch.flush_delay_offset_interval_ms {
        properties.insert("flushDelayOffsetInterval".to_owned(), value.to_string());
    }
    if properties.is_empty() {
        Err(ExecutionAgentError::InvalidRequest)
    } else {
        Ok(properties)
    }
}

fn patch_matches(patch: &BrokerConfigPatch, state: &BrokerConfigPatch) -> bool {
    patch
        .send_message_thread_pool_nums
        .is_none_or(|value| state.send_message_thread_pool_nums == Some(value))
        && patch
            .pull_message_thread_pool_nums
            .is_none_or(|value| state.pull_message_thread_pool_nums == Some(value))
        && patch
            .flush_delay_offset_interval_ms
            .is_none_or(|value| state.flush_delay_offset_interval_ms == Some(value))
}

fn generation_i64(generation: u64) -> Result<i64, AgentStoreError> {
    i64::try_from(generation)
        .map_err(|_| AgentStoreError::InvalidInput("Broker generation exceeds PostgreSQL BIGINT".to_owned()))
}

fn duration_millis(duration: std::time::Duration) -> Result<u64, ExecutionAgentError> {
    u64::try_from(duration.as_millis()).map_err(|_| ExecutionAgentError::Configuration)
}

#[cfg(test)]
#[path = "production_broker_config_tests.rs"]
mod tests;
