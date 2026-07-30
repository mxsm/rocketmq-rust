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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::Utc;
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::AgentStepResult;
use rocketmq_sre_contracts::EffectState;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::is_sha256_digest;
use tokio::time::timeout;
use uuid::Uuid;

use crate::AgentDriverRegistry;
use crate::AgentEffectRecord;
use crate::AgentEffectStore;
use crate::DispatchBarrier;
use crate::ExecutionAgentError;
use crate::FenceAckSigner;
use crate::LeaseAuthorityClient;
use rocketmq_sre_contracts::AdvanceFenceRequest;
use rocketmq_sre_contracts::AgentDispatchAuthorization;
use rocketmq_sre_contracts::AgentDispatchRequest;
use rocketmq_sre_contracts::AgentDispatchResponse;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::ReconcileEffectRequest;
use rocketmq_sre_contracts::ReconcileEffectResponse;
use rocketmq_sre_contracts::ReconcileEffectState;

const MAX_EFFECT_RECONCILE_SCAN: u32 = 10_000;
const MAX_SUMMARY_BYTES: usize = 2_048;

/// In-process low-cardinality Agent counters.
#[derive(Debug, Default)]
pub struct ExecutionAgentMetrics {
    active_dispatches: AtomicUsize,
    dispatch_total: AtomicU64,
    replay_total: AtomicU64,
    fence_rejections_total: AtomicU64,
    unknown_effects_total: AtomicU64,
}

impl ExecutionAgentMetrics {
    #[must_use]
    pub fn snapshot(&self) -> ExecutionAgentMetricsSnapshot {
        ExecutionAgentMetricsSnapshot {
            active_dispatches: self.active_dispatches.load(Ordering::Relaxed),
            dispatch_total: self.dispatch_total.load(Ordering::Relaxed),
            replay_total: self.replay_total.load(Ordering::Relaxed),
            fence_rejections_total: self.fence_rejections_total.load(Ordering::Relaxed),
            unknown_effects_total: self.unknown_effects_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionAgentMetricsSnapshot {
    pub active_dispatches: usize,
    pub dispatch_total: u64,
    pub replay_total: u64,
    pub fence_rejections_total: u64,
    pub unknown_effects_total: u64,
}

/// Fenced execution service. Every mutation crosses Authority verification,
/// a cluster dispatch barrier, and the durable effect ledger.
#[derive(Clone)]
pub struct ExecutionAgent {
    store: AgentEffectStore,
    barrier: DispatchBarrier,
    authority: Arc<dyn LeaseAuthorityClient>,
    registry: AgentDriverRegistry,
    ack_signer: FenceAckSigner,
    driver_timeout: Duration,
    metrics: Arc<ExecutionAgentMetrics>,
}

impl ExecutionAgent {
    #[must_use]
    pub fn new(
        store: AgentEffectStore,
        barrier: DispatchBarrier,
        authority: Arc<dyn LeaseAuthorityClient>,
        registry: AgentDriverRegistry,
        ack_signer: FenceAckSigner,
        driver_timeout: Duration,
    ) -> Self {
        Self {
            store,
            barrier,
            authority,
            registry,
            ack_signer,
            driver_timeout,
            metrics: Arc::new(ExecutionAgentMetrics::default()),
        }
    }

    pub async fn ready(&self) -> bool {
        self.store.ready().await.is_ok()
    }

    #[must_use]
    pub fn capabilities(&self) -> crate::ExecutionAgentCapabilities {
        self.registry.capabilities()
    }

    #[must_use]
    pub fn metrics(&self) -> ExecutionAgentMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Performs a typed, sanitized read for Executor precheck.
    ///
    /// # Errors
    ///
    /// Rejects unknown actions, incompatible schemas, and malformed driver
    /// responses.
    pub async fn read_state(&self, request: &AgentReadRequest) -> Result<AgentReadResult, ExecutionAgentError> {
        self.registry.validate_read(request)?;
        let handler = self.registry.handler(request.action)?;
        let result = timeout(self.driver_timeout, handler.read_state(request))
            .await
            .map_err(|_| ExecutionAgentError::DriverUnknown)??;
        if result.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
            || result.action != request.action
            || result.target != request.target
            || !is_sha256_digest(&result.precondition_hash)
            || result.reason_codes.len() > 32
            || result
                .reason_codes
                .iter()
                .any(|code| code.is_empty() || code.len() > 96)
        {
            return Err(ExecutionAgentError::InvalidRequest);
        }
        Ok(result)
    }

    /// Executes one idempotent, typed mutation under a shared dispatch barrier.
    ///
    /// # Errors
    ///
    /// Fails closed on identity, grant, barrier, persistence, registry, or
    /// driver uncertainty. An existing non-terminal effect is never retried.
    pub async fn dispatch(&self, request: &AgentDispatchRequest) -> Result<AgentDispatchResponse, ExecutionAgentError> {
        if request.schema_version != EXECUTION_AGENT_SCHEMA_VERSION {
            return Err(ExecutionAgentError::InvalidRequest);
        }
        self.registry.validate_dispatch(&request.request)?;
        self.verify_dispatch_safety(request).await?;
        self.authority
            .verify_fence_grant(request.tenant_id, &request.request.intent.fence_grant)
            .await
            .map_err(|error| self.record_fence_error(error))?;
        let guard = self
            .barrier
            .acquire_dispatch(request.request.intent.fence_grant.cluster_id)
            .await?;
        let result = self.dispatch_under_guard(request).await;
        let release = guard.release().await;
        match (result, release) {
            (Ok(response), Ok(())) => Ok(response),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn dispatch_under_guard(
        &self,
        request: &AgentDispatchRequest,
    ) -> Result<AgentDispatchResponse, ExecutionAgentError> {
        self.verify_dispatch_safety(request).await?;
        self.authority
            .verify_fence_grant(request.tenant_id, &request.request.intent.fence_grant)
            .await
            .map_err(|error| self.record_fence_error(error))?;
        let highest = self
            .store
            .highest_epoch(request.request.intent.fence_grant.cluster_id)
            .await?;
        if highest != Some(request.request.intent.fence_grant.epoch) {
            return Err(self.record_fence_error(ExecutionAgentError::AuthorityRejected));
        }
        let creation = self
            .store
            .prepare(request.tenant_id, &request.request, Utc::now())
            .await?;
        if !creation.created {
            if creation.effect.state == EffectState::Confirmed {
                self.metrics.replay_total.fetch_add(1, Ordering::Relaxed);
                return Ok(AgentDispatchResponse {
                    schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                    result: effect_result(&creation.effect)?,
                    replayed: true,
                });
            }
            self.metrics.unknown_effects_total.fetch_add(1, Ordering::Relaxed);
            return Err(ExecutionAgentError::UnresolvedEffect);
        }

        let operation_id = format!("sre-{}", creation.effect.id.simple());
        self.store
            .mark_dispatched(&creation.effect.idempotency_key, &operation_id, Utc::now())
            .await?;
        self.metrics.dispatch_total.fetch_add(1, Ordering::Relaxed);
        self.metrics.active_dispatches.fetch_add(1, Ordering::Relaxed);
        let handler = self.registry.handler(request.request.action)?;
        let driver = if request.request.intent.compensation {
            handler.compensate(&request.request, &operation_id)
        } else {
            handler.dispatch(&request.request, &operation_id)
        };
        let outcome = timeout(self.driver_timeout, driver).await;
        self.metrics.active_dispatches.fetch_sub(1, Ordering::Relaxed);
        match outcome {
            Ok(Ok(outcome))
                if outcome.operation_id == operation_id
                    && valid_bounded(&outcome.outcome_code, 96)
                    && valid_bounded(&outcome.sanitized_summary, MAX_SUMMARY_BYTES) =>
            {
                self.store
                    .confirm(
                        &creation.effect.idempotency_key,
                        &outcome.outcome_code,
                        &outcome.sanitized_summary,
                        Utc::now(),
                    )
                    .await?;
                let effect = self.store.effect(&creation.effect.idempotency_key).await?;
                Ok(AgentDispatchResponse {
                    schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                    result: effect_result(&effect)?,
                    replayed: false,
                })
            }
            Ok(Ok(_)) => {
                self.store
                    .mark_unknown(&creation.effect.idempotency_key, Utc::now())
                    .await?;
                self.metrics.unknown_effects_total.fetch_add(1, Ordering::Relaxed);
                Err(ExecutionAgentError::DriverUnknown)
            }
            Ok(Err(_)) | Err(_) => {
                self.store
                    .mark_unknown(&creation.effect.idempotency_key, Utc::now())
                    .await?;
                self.metrics.unknown_effects_total.fetch_add(1, Ordering::Relaxed);
                Err(ExecutionAgentError::DriverUnknown)
            }
        }
    }

    async fn verify_dispatch_safety(&self, request: &AgentDispatchRequest) -> Result<(), ExecutionAgentError> {
        match request.authorization {
            AgentDispatchAuthorization::HumanApproved => {
                if request.request.intent.dynamic_safety.is_some() {
                    return Err(ExecutionAgentError::InvalidRequest);
                }
                Ok(())
            }
            AgentDispatchAuthorization::Autonomous if request.request.intent.compensation => Ok(()),
            AgentDispatchAuthorization::Autonomous => {
                let decision = request
                    .request
                    .intent
                    .dynamic_safety
                    .as_ref()
                    .ok_or(ExecutionAgentError::AuthorityRejected)?;
                let plan_id = request.plan_id.ok_or(ExecutionAgentError::InvalidRequest)?;
                decision
                    .validate_allow_at(Utc::now())
                    .map_err(|_| ExecutionAgentError::AuthorityRejected)?;
                if decision.tenant_id != request.tenant_id
                    || decision.cluster_id != request.request.intent.fence_grant.cluster_id
                    || decision.action != request.request.action
                    || decision.action_version != request.request.descriptor_version
                    || decision.plan_id != plan_id
                    || decision.plan_hash != request.request.intent.plan_hash
                    || decision.execution_id != request.request.intent.execution_id
                    || decision.execution_step_id != request.request.intent.step_id
                {
                    return Err(ExecutionAgentError::AuthorityRejected);
                }
                self.authority
                    .verify_dynamic_safety(request.tenant_id, decision)
                    .await?;
                Ok(())
            }
        }
    }

    /// Reconciles one old effect using read-only live state and never retries
    /// the mutation.
    ///
    /// # Errors
    ///
    /// Rejects stale grants, cross-scope effects, and malformed driver output.
    pub async fn reconcile_effect(
        &self,
        request: &ReconcileEffectRequest,
    ) -> Result<ReconcileEffectResponse, ExecutionAgentError> {
        validate_reconcile_envelope(request)?;
        self.authority
            .verify_reconcile_grant(request.tenant_id, &request.reconcile_grant)
            .await
            .map_err(|error| self.record_fence_error(error))?;
        let effect = self.store.effect(&request.idempotency_key).await?;
        if effect.tenant_id != request.tenant_id
            || effect.cluster_id != request.reconcile_grant.cluster_id
            || effect.epoch >= request.reconcile_grant.pending_epoch
        {
            return Err(ExecutionAgentError::AuthorityRejected);
        }
        // A confirmed dispatch result proves what happened at dispatch time,
        // not what is live during takeover. TTL actions and external
        // operators can legitimately remove an effect later, so every
        // recovery reconcile must consult the typed read-only driver.
        let stored = self.store.request(&request.idempotency_key).await?;
        let read = read_request(request.tenant_id, &stored);
        self.registry.validate_read(&read)?;
        let handler = self.registry.handler(stored.action)?;
        let reconciled = timeout(
            self.driver_timeout,
            handler.reconcile(&read, effect.operation_id.as_deref()),
        )
        .await
        .map_err(|_| ExecutionAgentError::DriverUnknown)??;
        validate_reconcile_result(&reconciled)?;
        // Confirmed is the immutable dispatch-time journal terminal. Recovery
        // still returns the current live observation, but must not rewrite
        // that terminal record when a TTL or operator removed the effect.
        if reconciliation_requires_store_confirmation(effect.state, reconciled.state) {
            if effect.state == EffectState::Prepared {
                self.store.mark_unknown(&request.idempotency_key, Utc::now()).await?;
            }
            self.store
                .confirm(
                    &request.idempotency_key,
                    &reconciled.outcome_code,
                    &reconciled.sanitized_summary,
                    reconciled.observed_at,
                )
                .await?;
        }
        Ok(reconciled)
    }

    /// Advances the durable Agent epoch only after all old effects are
    /// terminal and all in-flight shared dispatch guards have drained.
    ///
    /// # Errors
    ///
    /// Fails closed when Authority, barrier, persistence, or reconciliation
    /// state is unavailable.
    pub async fn advance_fence(&self, request: &AdvanceFenceRequest) -> Result<FenceAck, ExecutionAgentError> {
        if request.schema_version != EXECUTION_AGENT_SCHEMA_VERSION {
            return Err(ExecutionAgentError::InvalidRequest);
        }
        self.authority
            .verify_reconcile_grant(request.tenant_id, &request.reconcile_grant)
            .await
            .map_err(|error| self.record_fence_error(error))?;
        let guard = self.barrier.acquire_fence(request.reconcile_grant.cluster_id).await?;
        let result = self.advance_fence_under_guard(request).await;
        let release = guard.release().await;
        match (result, release) {
            (Ok(ack), Ok(())) => Ok(ack),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn advance_fence_under_guard(&self, request: &AdvanceFenceRequest) -> Result<FenceAck, ExecutionAgentError> {
        self.authority
            .verify_reconcile_grant(request.tenant_id, &request.reconcile_grant)
            .await
            .map_err(|error| self.record_fence_error(error))?;
        let current = self.store.highest_epoch(request.reconcile_grant.cluster_id).await?;
        if current == Some(request.reconcile_grant.pending_epoch) {
            return self
                .store
                .fence_ack(request.reconcile_grant.cluster_id)
                .await
                .map_err(Into::into);
        }
        if current.is_some_and(|epoch| epoch > request.reconcile_grant.pending_epoch) {
            return Err(self.record_fence_error(ExecutionAgentError::AuthorityRejected));
        }
        if !self
            .store
            .unfinished_before_epoch(
                request.reconcile_grant.cluster_id,
                request.reconcile_grant.pending_epoch,
                MAX_EFFECT_RECONCILE_SCAN,
            )
            .await?
            .is_empty()
        {
            return Err(ExecutionAgentError::UnresolvedEffect);
        }
        let ack = self.ack_signer.sign(&request.reconcile_grant, Utc::now())?;
        self.store
            .accept_fence(request.tenant_id, request.reconcile_grant.lease_id, &ack)
            .await?;
        Ok(ack)
    }

    fn record_fence_error(&self, error: ExecutionAgentError) -> ExecutionAgentError {
        self.metrics.fence_rejections_total.fetch_add(1, Ordering::Relaxed);
        error
    }
}

fn read_request(tenant_id: rocketmq_sre_contracts::TenantId, request: &AgentStepRequest) -> AgentReadRequest {
    AgentReadRequest {
        schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id,
        cluster_id: request.intent.fence_grant.cluster_id,
        execution_id: request.intent.execution_id,
        plan_step_id: request.intent.step.id,
        action: request.action,
        descriptor_version: request.descriptor_version.clone(),
        target: request.target.clone(),
        parameters: request.parameters.clone(),
    }
}

fn effect_result(effect: &AgentEffectRecord) -> Result<AgentStepResult, ExecutionAgentError> {
    let operation_id = effect
        .operation_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ExecutionAgentError::UnresolvedEffect)?;
    let outcome_code = effect
        .outcome_code
        .clone()
        .filter(|value| valid_bounded(value, 96))
        .ok_or(ExecutionAgentError::UnresolvedEffect)?;
    let sanitized_summary = effect
        .sanitized_summary
        .clone()
        .filter(|value| valid_bounded(value, MAX_SUMMARY_BYTES))
        .ok_or(ExecutionAgentError::UnresolvedEffect)?;
    let completed_at = effect.confirmed_at.ok_or(ExecutionAgentError::UnresolvedEffect)?;
    Ok(AgentStepResult {
        execution_id: effect.execution_id,
        step_id: effect.step_id,
        state: effect.state,
        operation_id,
        outcome_code,
        sanitized_summary,
        completed_at,
    })
}

fn validate_reconcile_envelope(request: &ReconcileEffectRequest) -> Result<(), ExecutionAgentError> {
    if request.schema_version == EXECUTION_AGENT_SCHEMA_VERSION
        && !request.idempotency_key.trim().is_empty()
        && request.reconcile_grant.cluster_id.as_uuid() != Uuid::nil()
        && request.reconcile_grant.pending_epoch.0 > 0
    {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn validate_reconcile_result(result: &ReconcileEffectResponse) -> Result<(), ExecutionAgentError> {
    if result.schema_version == EXECUTION_AGENT_SCHEMA_VERSION
        && valid_bounded(&result.outcome_code, 96)
        && valid_bounded(&result.sanitized_summary, MAX_SUMMARY_BYTES)
    {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

const fn reconciliation_requires_store_confirmation(
    effect_state: EffectState,
    reconciliation_state: ReconcileEffectState,
) -> bool {
    !matches!(reconciliation_state, ReconcileEffectState::Unknown) && !matches!(effect_state, EffectState::Confirmed)
}

fn valid_bounded(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests;
