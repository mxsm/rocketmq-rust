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
use rocketmq_admin_core::core::broker::BrokerLogFilterState;
use rocketmq_admin_core::core::broker::BrokerLogLevel;
use rocketmq_admin_core::core::broker::BrokerMutationAdmin;
use rocketmq_admin_core::core::broker::BrokerQueryAdmin;
use rocketmq_admin_core::core::broker::QueryBrokerLogFilterStateRequest;
use rocketmq_admin_core::core::broker::RestoreBrokerLogFilterRequest;
use rocketmq_admin_core::core::broker::SetBrokerLogFilterTtlRequest;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use sqlx::PgPool;
use sqlx::Row;
use uuid::Uuid;

use super::ProductionBrokerConfigPatchClient;
use crate::AgentStoreError;
use crate::ConfigWriteClient;
use crate::DriverFuture;
use crate::ExecutionAgentError;
use crate::LoggerLevelControlClient;
use crate::LoggerLevelState;
use crate::LoggerLevelTtlRestore;
use crate::LoggerLevelTtlWrite;

const COMPONENT: &str = "broker";
const MIN_TTL_SECONDS: u32 = 60;
const MAX_TTL_SECONDS: u32 = 900;

#[derive(Clone, Debug, Eq, PartialEq)]
struct LoggerBeforeState {
    execution_id: ExecutionId,
    plan_step_id: PlanStepId,
    component: String,
    broker_addr: String,
    logger: String,
    before_level: String,
    requested_level: String,
    forward_operation_id: String,
    expires_at: DateTime<Utc>,
}

impl LoggerBeforeState {
    fn matches_request(&self, request: &LoggerLevelTtlWrite) -> bool {
        self.execution_id == request.execution_id
            && self.plan_step_id == request.plan_step_id
            && self.component == request.component
            && self.broker_addr == request.broker_addr
            && self.logger == request.logger
            && self.requested_level == request.level
            && self.forward_operation_id == request.operation_id
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Forward,
    Compensation,
}

impl Direction {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Compensation => "compensation",
        }
    }
}

#[derive(Clone)]
struct LoggerLevelJournal {
    pool: PgPool,
}

impl LoggerLevelJournal {
    fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn load_before(
        &self,
        execution_id: ExecutionId,
        plan_step_id: PlanStepId,
    ) -> Result<Option<LoggerBeforeState>, AgentStoreError> {
        let row = sqlx::query(
            "SELECT component, broker_addr, logger, before_level,
                    requested_level, forward_operation_id, expires_at
             FROM execution_agent_logger_level_before_states
             WHERE execution_id = $1 AND plan_step_id = $2",
        )
        .bind(execution_id.as_uuid())
        .bind(plan_step_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(LoggerBeforeState {
                execution_id,
                plan_step_id,
                component: row.try_get("component")?,
                broker_addr: row.try_get("broker_addr")?,
                logger: row.try_get("logger")?,
                before_level: row.try_get("before_level")?,
                requested_level: row.try_get("requested_level")?,
                forward_operation_id: row.try_get("forward_operation_id")?,
                expires_at: row.try_get("expires_at")?,
            })
        })
        .transpose()
    }

    async fn persist_before(&self, state: &LoggerBeforeState) -> Result<LoggerBeforeState, AgentStoreError> {
        sqlx::query(
            "INSERT INTO execution_agent_logger_level_before_states (
                id, execution_id, plan_step_id, component, broker_addr,
                logger, before_level, requested_level, forward_operation_id,
                expires_at, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (execution_id, plan_step_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(state.execution_id.as_uuid())
        .bind(state.plan_step_id.as_uuid())
        .bind(&state.component)
        .bind(&state.broker_addr)
        .bind(&state.logger)
        .bind(&state.before_level)
        .bind(&state.requested_level)
        .bind(&state.forward_operation_id)
        .bind(state.expires_at)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        self.load_before(state.execution_id, state.plan_step_id)
            .await?
            .filter(|persisted| {
                persisted.component == state.component
                    && persisted.broker_addr == state.broker_addr
                    && persisted.logger == state.logger
                    && persisted.before_level == state.before_level
                    && persisted.requested_level == state.requested_level
                    && persisted.forward_operation_id == state.forward_operation_id
            })
            .ok_or(AgentStoreError::IdempotencyConflict)
    }

    async fn append_result(
        &self,
        before: &LoggerBeforeState,
        operation_id: &str,
        direction: Direction,
        observed: &LoggerLevelState,
    ) -> Result<(), AgentStoreError> {
        let insert = sqlx::query(
            "INSERT INTO execution_agent_logger_level_results (
                execution_id, plan_step_id, component, broker_addr, logger,
                operation_id, direction, observed_level, active_operation_id,
                last_completed_operation_id, recorded_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (operation_id) DO NOTHING",
        )
        .bind(before.execution_id.as_uuid())
        .bind(before.plan_step_id.as_uuid())
        .bind(&before.component)
        .bind(&before.broker_addr)
        .bind(&before.logger)
        .bind(operation_id)
        .bind(direction.as_str())
        .bind(&observed.level)
        .bind(&observed.active_operation_id)
        .bind(&observed.last_completed_operation_id)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        if insert.rows_affected() == 1 {
            return Ok(());
        }
        let identical: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM execution_agent_logger_level_results
                WHERE operation_id = $1
                  AND execution_id = $2
                  AND plan_step_id = $3
                  AND component = $4
                  AND broker_addr = $5
                  AND logger = $6
                  AND direction = $7
                  AND observed_level = $8
                  AND active_operation_id IS NOT DISTINCT FROM $9
                  AND last_completed_operation_id IS NOT DISTINCT FROM $10
            )",
        )
        .bind(operation_id)
        .bind(before.execution_id.as_uuid())
        .bind(before.plan_step_id.as_uuid())
        .bind(&before.component)
        .bind(&before.broker_addr)
        .bind(&before.logger)
        .bind(direction.as_str())
        .bind(&observed.level)
        .bind(&observed.active_operation_id)
        .bind(&observed.last_completed_operation_id)
        .fetch_one(&self.pool)
        .await?;
        if identical {
            Ok(())
        } else {
            Err(AgentStoreError::IdempotencyConflict)
        }
    }
}

impl ProductionBrokerConfigPatchClient {
    fn logger_journal(&self) -> LoggerLevelJournal {
        LoggerLevelJournal::new(self.journal.pool.clone())
    }

    async fn live_logger_state(
        &self,
        component: &str,
        broker_addr: &str,
        logger: &str,
    ) -> Result<LoggerLevelState, ExecutionAgentError> {
        require_scope(component, broker_addr, logger)?;
        let request = QueryBrokerLogFilterStateRequest::try_new(broker_addr, logger)
            .map_err(|_| ExecutionAgentError::InvalidRequest)?;
        let state = {
            let mut admin = self.read_admin.lock().await;
            admin
                .query_log_filter_state(&request)
                .await
                .map_err(|_| ExecutionAgentError::DriverFailed)?
        };
        logger_state(state)
    }
}

impl ConfigWriteClient for ProductionBrokerConfigPatchClient {
    fn set_logger_level_ttl<'a>(&'a self, request: &'a LoggerLevelTtlWrite) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            require_scope(&request.component, &request.broker_addr, &request.logger)?;
            let ttl_seconds = ttl_seconds(request.expires_at, Utc::now())?;
            let journal = self.logger_journal();
            let live = self
                .live_logger_state(&request.component, &request.broker_addr, &request.logger)
                .await?;
            if let Some(persisted) = journal.load_before(request.execution_id, request.plan_step_id).await? {
                if !persisted.matches_request(request) {
                    return Err(ExecutionAgentError::InvalidRequest);
                }
                if live.active_operation_id.as_deref() == Some(request.operation_id.as_str())
                    && live.level == request.level
                {
                    return Ok(());
                }
                if live.last_completed_operation_id.as_deref() == Some(request.operation_id.as_str())
                    && live.active_operation_id.is_none()
                    && live.level == persisted.before_level
                {
                    return Ok(());
                }
                return Err(ExecutionAgentError::DriverFailed);
            }
            if live.active_operation_id.is_some() {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let before = journal
                .persist_before(&LoggerBeforeState {
                    execution_id: request.execution_id,
                    plan_step_id: request.plan_step_id,
                    component: request.component.clone(),
                    broker_addr: request.broker_addr.clone(),
                    logger: request.logger.clone(),
                    before_level: live.level,
                    requested_level: request.level.clone(),
                    forward_operation_id: request.operation_id.clone(),
                    expires_at: request.expires_at,
                })
                .await?;
            let mutation = SetBrokerLogFilterTtlRequest::try_new(
                &request.broker_addr,
                &request.logger,
                parse_level(&request.level)?,
                ttl_seconds,
                &request.operation_id,
            )
            .map_err(|_| ExecutionAgentError::InvalidRequest)?;
            {
                let mut admin = self.mutation_admin.lock().await;
                admin
                    .set_log_filter_ttl(&mutation)
                    .await
                    .map_err(|_| ExecutionAgentError::DriverFailed)?;
            }
            let observed = self
                .live_logger_state(&request.component, &request.broker_addr, &request.logger)
                .await?;
            if observed.level != request.level
                || observed.active_operation_id.as_deref() != Some(request.operation_id.as_str())
            {
                return Err(ExecutionAgentError::DriverUnknown);
            }
            journal
                .append_result(&before, &request.operation_id, Direction::Forward, &observed)
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)
        })
    }
}

impl LoggerLevelControlClient for ProductionBrokerConfigPatchClient {
    fn logger_level_state<'a>(
        &'a self,
        component: &'a str,
        broker_addr: &'a str,
        logger: &'a str,
    ) -> DriverFuture<'a, LoggerLevelState> {
        Box::pin(async move { self.live_logger_state(component, broker_addr, logger).await })
    }

    fn restore_logger_level<'a>(&'a self, request: &'a LoggerLevelTtlRestore) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            require_scope(&request.component, &request.broker_addr, &request.logger)?;
            let journal = self.logger_journal();
            let before = journal
                .load_before(request.execution_id, request.plan_step_id)
                .await?
                .ok_or(AgentStoreError::NotFound)?;
            if before.component != request.component
                || before.broker_addr != request.broker_addr
                || before.logger != request.logger
            {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let live = self
                .live_logger_state(&request.component, &request.broker_addr, &request.logger)
                .await?;
            if live.active_operation_id.is_none()
                && live.last_completed_operation_id.as_deref() == Some(before.forward_operation_id.as_str())
                && live.level == before.before_level
            {
                journal
                    .append_result(&before, &request.operation_id, Direction::Compensation, &live)
                    .await?;
                return Ok(());
            }
            if live.active_operation_id.as_deref() != Some(before.forward_operation_id.as_str()) {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let mutation = RestoreBrokerLogFilterRequest::try_new(&request.broker_addr, &request.operation_id)
                .map_err(|_| ExecutionAgentError::InvalidRequest)?;
            {
                let mut admin = self.mutation_admin.lock().await;
                admin
                    .restore_log_filter(&mutation)
                    .await
                    .map_err(|_| ExecutionAgentError::DriverFailed)?;
            }
            let observed = self
                .live_logger_state(&request.component, &request.broker_addr, &request.logger)
                .await?;
            if observed.active_operation_id.is_some()
                || observed.last_completed_operation_id.as_deref() != Some(before.forward_operation_id.as_str())
                || observed.level != before.before_level
            {
                return Err(ExecutionAgentError::DriverUnknown);
            }
            journal
                .append_result(&before, &request.operation_id, Direction::Compensation, &observed)
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)
        })
    }
}

fn require_scope(component: &str, broker_addr: &str, logger: &str) -> Result<(), ExecutionAgentError> {
    if component != COMPONENT || broker_addr.trim() != broker_addr || broker_addr.is_empty() {
        return Err(ExecutionAgentError::InvalidRequest);
    }
    QueryBrokerLogFilterStateRequest::try_new(broker_addr, logger)
        .map(|_| ())
        .map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn logger_state(state: BrokerLogFilterState) -> Result<LoggerLevelState, ExecutionAgentError> {
    if !state.supported {
        return Err(ExecutionAgentError::DriverFailed);
    }
    Ok(LoggerLevelState {
        level: state
            .level
            .ok_or(ExecutionAgentError::DriverFailed)?
            .as_uppercase()
            .to_owned(),
        active_operation_id: state.active_operation_id,
        last_completed_operation_id: state.last_completed_operation_id,
    })
}

fn parse_level(level: &str) -> Result<BrokerLogLevel, ExecutionAgentError> {
    match level {
        "INFO" => Ok(BrokerLogLevel::Info),
        "DEBUG" => Ok(BrokerLogLevel::Debug),
        _ => Err(ExecutionAgentError::InvalidRequest),
    }
}

fn ttl_seconds(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> Result<u32, ExecutionAgentError> {
    let milliseconds = expires_at.signed_duration_since(now).num_milliseconds();
    if milliseconds <= 0 {
        return Err(ExecutionAgentError::InvalidRequest);
    }
    let rounded_seconds = milliseconds.saturating_add(999) / 1_000;
    let seconds = u32::try_from(rounded_seconds).map_err(|_| ExecutionAgentError::InvalidRequest)?;
    if (MIN_TTL_SECONDS..=MAX_TTL_SECONDS).contains(&seconds) {
        Ok(seconds)
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

#[cfg(test)]
#[path = "logger_level_tests.rs"]
mod tests;
