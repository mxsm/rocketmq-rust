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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::ActivateLeaseRequest;
use rocketmq_sre_contracts::AdvanceFenceRequest;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::AuditEventId;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::BeginLeaseTakeoverRequest;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::ExecutionTransition;
use rocketmq_sre_contracts::ExecutorLease;
use rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION;
use rocketmq_sre_contracts::LeaseState;
use rocketmq_sre_contracts::ReconcileEffectRequest;
use rocketmq_sre_contracts::ReconcileEffectState;
use rocketmq_sre_contracts::ReconcileGrant;
use rocketmq_sre_contracts::ResourceLockId;
use rocketmq_sre_contracts::StepIntent;
use rocketmq_sre_contracts::StepResult;
use rocketmq_sre_contracts::VerifyExecutionRequest;
use rocketmq_sre_contracts::is_sha256_digest;
use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;

use crate::ExecutionAgentClient;
use crate::ExecutionJournal;
use crate::ExecutionPrechecker;
use crate::ExecutionVerifier;
use crate::ExecutorAuthorityClient;
use crate::ExecutorError;
use crate::ResourceLock;
use crate::ResourceLockRequest;
use crate::ResourceSafetyStore;

const MAX_RECOVERY_INTENTS: u32 = 1_000;

/// Bounded result returned by the internal Executor API.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ExecuteOutcome {
    pub execution_id: ExecutionId,
    pub state: ExecutionState,
    pub replayed: bool,
    pub accepted_steps: usize,
}

/// Bounded startup recovery summary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct RecoverySweepOutcome {
    pub attempted: u32,
    pub recovered: u32,
    pub blocked: u32,
}

/// Low-cardinality process counters.
#[derive(Default)]
pub struct ExecutorMetrics {
    active_executions: AtomicUsize,
    execution_total: AtomicU64,
    replay_total: AtomicU64,
    precondition_rejections_total: AtomicU64,
    fence_rejections_total: AtomicU64,
    reconcile_blocks_total: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ExecutorMetricsSnapshot {
    pub active_executions: usize,
    pub execution_total: u64,
    pub replay_total: u64,
    pub precondition_rejections_total: u64,
    pub fence_rejections_total: u64,
    pub reconcile_blocks_total: u64,
}

impl ExecutorMetrics {
    #[must_use]
    pub fn snapshot(&self) -> ExecutorMetricsSnapshot {
        ExecutorMetricsSnapshot {
            active_executions: self.active_executions.load(Ordering::Relaxed),
            execution_total: self.execution_total.load(Ordering::Relaxed),
            replay_total: self.replay_total.load(Ordering::Relaxed),
            precondition_rejections_total: self.precondition_rejections_total.load(Ordering::Relaxed),
            fence_rejections_total: self.fence_rejections_total.load(Ordering::Relaxed),
            reconcile_blocks_total: self.reconcile_blocks_total.load(Ordering::Relaxed),
        }
    }
}

/// Single-active supervised execution engine.
#[derive(Clone)]
pub struct ChangeExecutor {
    journal: ExecutionJournal,
    safety: ResourceSafetyStore,
    authority: Arc<dyn ExecutorAuthorityClient>,
    agent: Arc<dyn ExecutionAgentClient>,
    prechecker: ExecutionPrechecker,
    verifier: Option<ExecutionVerifier>,
    executor_subject: Arc<str>,
    lease_ttl_seconds: u32,
    resource_lock_ttl: Duration,
    leases: Arc<Mutex<BTreeMap<ClusterId, ExecutorLease>>>,
    metrics: Arc<ExecutorMetrics>,
}

impl ChangeExecutor {
    #[allow(
        clippy::too_many_arguments,
        reason = "security boundaries are injected explicitly and are not optional"
    )]
    #[must_use]
    pub fn new(
        journal: ExecutionJournal,
        safety: ResourceSafetyStore,
        authority: Arc<dyn ExecutorAuthorityClient>,
        agent: Arc<dyn ExecutionAgentClient>,
        prechecker: ExecutionPrechecker,
        executor_subject: impl Into<Arc<str>>,
        lease_ttl_seconds: u32,
        resource_lock_ttl: Duration,
    ) -> Self {
        Self {
            journal,
            safety,
            authority,
            agent,
            prechecker,
            verifier: None,
            executor_subject: executor_subject.into(),
            lease_ttl_seconds,
            resource_lock_ttl,
            leases: Arc::new(Mutex::new(BTreeMap::new())),
            metrics: Arc::new(ExecutorMetrics::default()),
        }
    }

    /// Installs the read-only verification boundary required before any
    /// descriptor can execute.
    #[must_use]
    pub fn with_verifier(mut self, verifier: ExecutionVerifier) -> Self {
        self.verifier = Some(verifier);
        self
    }

    /// Returns true when PostgreSQL and the typed Agent boundary are ready.
    pub async fn ready(&self) -> bool {
        self.journal.ready().await.is_ok() && self.agent.capabilities().await.is_ok()
    }

    #[must_use]
    pub fn metrics(&self) -> ExecutorMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Captures a current, typed Agent precondition without entering the
    /// mutation or execution-journal paths.
    ///
    /// # Errors
    ///
    /// Rejects Agent transport failures, malformed hashes, non-ready state,
    /// and any non-empty reason-code set.
    pub async fn read_precondition(&self, request: &AgentReadRequest) -> Result<AgentReadResult, ExecutorError> {
        let result = self.agent.precheck(request).await?;
        if !result.ready || !result.reason_codes.is_empty() || !is_sha256_digest(&result.precondition_hash) {
            return Err(ExecutorError::PreconditionChanged);
        }
        Ok(result)
    }

    /// Executes every approved step in order and stops before Phase 3 generic
    /// verification.
    ///
    /// The method never talks to a target system directly. It verifies the
    /// signed request online, obtains an active fenced lease, rechecks live
    /// preconditions, writes each intent before dispatch, and persists every
    /// Agent result.
    ///
    /// # Errors
    ///
    /// Fails closed on identity, descriptor, precondition, quarantine, lock,
    /// Authority, Agent, journal, or reconciliation uncertainty.
    pub async fn execute(&self, request: &ExecutionRequest) -> Result<ExecuteOutcome, ExecutorError> {
        self.metrics.execution_total.fetch_add(1, Ordering::Relaxed);
        self.metrics.active_executions.fetch_add(1, Ordering::Relaxed);
        let result = self.execute_inner(request).await;
        self.metrics.active_executions.fetch_sub(1, Ordering::Relaxed);
        result
    }

    /// Recovers an interrupted compensating execution from its immutable
    /// journal snapshot.
    ///
    /// The original dispatch signature may have expired, so this path never
    /// authorizes a new mutation from that request. It obtains a fresh fenced
    /// takeover grant and performs only Agent reconciliation. The execution is
    /// closed as rolled back only when every recorded forward effect is
    /// confirmed absent; any applied, failed, or unknown effect remains
    /// fail-closed in `compensating`.
    ///
    /// # Errors
    ///
    /// Returns a typed failure when the execution is not recoverable, durable
    /// state cannot be loaded, fencing fails, or an effect is not proven
    /// absent.
    pub async fn recover_execution(&self, id: ExecutionId) -> Result<ExecuteOutcome, ExecutorError> {
        self.metrics.execution_total.fetch_add(1, Ordering::Relaxed);
        self.metrics.replay_total.fetch_add(1, Ordering::Relaxed);
        self.metrics.active_executions.fetch_add(1, Ordering::Relaxed);
        let result = self.recover_execution_inner(id).await;
        self.metrics.active_executions.fetch_sub(1, Ordering::Relaxed);
        result
    }

    /// Reconciles a bounded set of interrupted compensation records during
    /// service startup.
    ///
    /// Effects that cannot be proven absent remain blocked without preventing
    /// other records from being examined. Dependency and persistence failures
    /// stop the sweep so the caller can expose degraded readiness.
    ///
    /// # Errors
    ///
    /// Returns dependency, fencing, or journal failures other than the
    /// expected fail-closed unresolved-effect result.
    pub async fn recover_interrupted_executions(&self, limit: u32) -> Result<RecoverySweepOutcome, ExecutorError> {
        let ids = self.journal.compensating_execution_ids(limit).await?;
        let mut summary = RecoverySweepOutcome::default();
        for id in ids {
            summary.attempted = summary.attempted.saturating_add(1);
            match self.recover_execution(id).await {
                Ok(outcome) if outcome.state == ExecutionState::RolledBack => {
                    summary.recovered = summary.recovered.saturating_add(1);
                }
                Ok(_) | Err(ExecutorError::ReconcileBlocked) => {
                    summary.blocked = summary.blocked.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(summary)
    }

    async fn recover_execution_inner(&self, id: ExecutionId) -> Result<ExecuteOutcome, ExecutorError> {
        let request = self.journal.execution_request(id).await?;
        let state = self.journal.execution_state(id).await?;
        if matches!(
            state,
            ExecutionState::Succeeded | ExecutionState::RolledBack | ExecutionState::Escalated
        ) {
            return Ok(ExecuteOutcome {
                execution_id: id,
                state,
                replayed: true,
                accepted_steps: request.plan.steps.len(),
            });
        }
        if state != ExecutionState::Compensating {
            return Err(ExecutorError::ReconcileBlocked);
        }
        let forward_intents = self.journal.forward_intents_for_execution(id).await?;
        if forward_intents.is_empty() {
            return Err(ExecutorError::ReconcileBlocked);
        }

        let mut leases = self.leases.lock().await;
        leases.remove(&request.cluster_id);
        let takeover = self
            .authority
            .begin_takeover(&BeginLeaseTakeoverRequest {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                requested_ttl_seconds: self.lease_ttl_seconds,
            })
            .await?;
        self.reconcile_old_effects(&request, &takeover.reconcile_grant).await?;

        let mut every_effect_absent = true;
        for intent in &forward_intents {
            let response = self
                .agent
                .reconcile(&ReconcileEffectRequest {
                    schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                    tenant_id: request.tenant_id,
                    reconcile_grant: takeover.reconcile_grant.clone(),
                    idempotency_key: intent.idempotency_key.clone(),
                })
                .await?;
            every_effect_absent &= response.state == ReconcileEffectState::NotApplied;
        }
        let response = self
            .agent
            .advance_fence(&AdvanceFenceRequest {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                tenant_id: request.tenant_id,
                reconcile_grant: takeover.reconcile_grant,
            })
            .await?;
        let active = self
            .authority
            .activate(
                request.tenant_id,
                request.cluster_id,
                &ActivateLeaseRequest {
                    schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                    tenant_id: request.tenant_id,
                    lease_id: takeover.lease.id,
                    fence_ack: response.fence_ack,
                },
            )
            .await?;
        leases.insert(request.cluster_id, active);
        drop(leases);

        if !every_effect_absent || self.journal.execution_state(id).await? != ExecutionState::Compensating {
            self.metrics.reconcile_blocks_total.fetch_add(1, Ordering::Relaxed);
            return Err(ExecutorError::ReconcileBlocked);
        }
        self.transition(
            &request,
            ExecutionState::Compensating,
            ExecutionState::RolledBack,
            "recovery_confirmed_forward_effects_absent",
        )
        .await?;
        let locks = self.safety.unreleased_for_execution(id).await?;
        self.release_locks(&locks, id, "recovery_confirmed_rolled_back").await;
        Ok(ExecuteOutcome {
            execution_id: id,
            state: ExecutionState::RolledBack,
            replayed: true,
            accepted_steps: request.plan.steps.len(),
        })
    }

    async fn execute_inner(&self, request: &ExecutionRequest) -> Result<ExecuteOutcome, ExecutorError> {
        self.authority
            .verify_execution(&VerifyExecutionRequest {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                execution: request.clone(),
            })
            .await?;
        if request.plan.steps.is_empty() {
            return Err(ExecutorError::InvalidRequest);
        }
        for step in &request.plan.steps {
            self.prechecker
                .registry()
                .validate_step_authorization(step, request.is_autonomous())?;
        }
        let verifier = self.verifier.as_ref().ok_or(ExecutorError::Configuration)?;
        let (resource_key, action) = execution_projection(request)?;
        let creation = self
            .journal
            .create_execution(request, &resource_key, action, Utc::now())
            .await?;
        if !creation.created {
            self.metrics.replay_total.fetch_add(1, Ordering::Relaxed);
            let state = self.journal.execution_state(creation.id).await?;
            let force_takeover = !matches!(state, ExecutionState::Pending);
            self.ensure_active_lease(request, force_takeover).await?;
            let recovered = self.journal.execution_state(creation.id).await?;
            if !matches!(recovered, ExecutionState::Pending) {
                return Ok(ExecuteOutcome {
                    execution_id: creation.id,
                    state: recovered,
                    replayed: true,
                    accepted_steps: request.plan.steps.len(),
                });
            }
        } else {
            self.ensure_active_lease(request, false).await?;
        }

        self.transition(
            request,
            ExecutionState::Pending,
            ExecutionState::Prechecking,
            "execution_precheck_started",
        )
        .await?;
        if let Err(error) = self.prechecker.check(request).await {
            if matches!(error, ExecutorError::PreconditionChanged) {
                self.metrics
                    .precondition_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            let _ = self
                .transition(
                    request,
                    ExecutionState::Prechecking,
                    ExecutionState::Escalated,
                    "execution_precheck_rejected",
                )
                .await;
            return Err(error);
        }

        let locks = match self.acquire_locks(request).await {
            Ok(locks) => locks,
            Err(error) => {
                let _ = self
                    .transition(
                        request,
                        ExecutionState::Prechecking,
                        ExecutionState::Escalated,
                        "resource_lock_rejected",
                    )
                    .await;
                return Err(error);
            }
        };
        let execution_result = self.execute_supervised_flow(request, &locks, verifier).await;
        if execution_result.is_err() && !self.journal.has_intent(request.id).await.unwrap_or(true) {
            if matches!(
                self.journal.execution_state(request.id).await,
                Ok(ExecutionState::Prechecking)
            ) {
                let _ = self
                    .transition(
                        request,
                        ExecutionState::Prechecking,
                        ExecutionState::Escalated,
                        "execution_rejected_before_dispatch",
                    )
                    .await;
            }
            self.release_locks(&locks, request.id, "execution_rejected_before_dispatch")
                .await;
        }
        let state = execution_result?;
        Ok(ExecuteOutcome {
            execution_id: request.id,
            state,
            replayed: false,
            accepted_steps: request.plan.steps.len(),
        })
    }

    async fn ensure_active_lease(
        &self,
        request: &ExecutionRequest,
        force_takeover: bool,
    ) -> Result<ExecutorLease, ExecutorError> {
        let mut leases = self.leases.lock().await;
        if !force_takeover
            && let Some(lease) = leases.get(&request.cluster_id)
            && lease.state == LeaseState::Active
            && lease.expires_at > Utc::now() + TimeDelta::seconds(5)
        {
            return Ok(lease.clone());
        }
        leases.remove(&request.cluster_id);
        let takeover = self
            .authority
            .begin_takeover(&BeginLeaseTakeoverRequest {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                requested_ttl_seconds: self.lease_ttl_seconds,
            })
            .await?;
        self.reconcile_old_effects(request, &takeover.reconcile_grant).await?;
        let response = self
            .agent
            .advance_fence(&AdvanceFenceRequest {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                tenant_id: request.tenant_id,
                reconcile_grant: takeover.reconcile_grant,
            })
            .await?;
        let active = self
            .authority
            .activate(
                request.tenant_id,
                request.cluster_id,
                &ActivateLeaseRequest {
                    schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                    tenant_id: request.tenant_id,
                    lease_id: takeover.lease.id,
                    fence_ack: response.fence_ack,
                },
            )
            .await?;
        leases.insert(request.cluster_id, active.clone());
        Ok(active)
    }

    async fn reconcile_old_effects(
        &self,
        request: &ExecutionRequest,
        grant: &ReconcileGrant,
    ) -> Result<(), ExecutorError> {
        let pending = self
            .journal
            .pending_intents_for_cluster(request.cluster_id, MAX_RECOVERY_INTENTS)
            .await?;
        let mut outcomes: BTreeMap<ExecutionId, (ReconcileAggregate, ExecutionRequest)> = BTreeMap::new();
        for pending in pending {
            let recovery_scope = ExecutionRequest {
                id: pending.intent.execution_id,
                tenant_id: pending.tenant_id,
                cluster_id: pending.cluster_id,
                correlation_id: pending.correlation_id,
                ..request.clone()
            };
            self.enter_reconciling(pending.execution_state, &recovery_scope).await?;
            let response = self
                .agent
                .reconcile(&ReconcileEffectRequest {
                    schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                    tenant_id: pending.tenant_id,
                    reconcile_grant: grant.clone(),
                    idempotency_key: pending.intent.idempotency_key.clone(),
                })
                .await?;
            if response.state == ReconcileEffectState::Unknown {
                return Err(ExecutorError::ReconcileBlocked);
            }
            let reason_code = match response.state {
                ReconcileEffectState::Applied => "reconciled_applied",
                ReconcileEffectState::NotApplied => "reconciled_not_applied",
                ReconcileEffectState::Failed => "reconciled_failed",
                ReconcileEffectState::Unknown => return Err(ExecutorError::ReconcileBlocked),
            };
            let result = StepResult {
                step_id: pending.intent.step_id,
                state: if response.state == ReconcileEffectState::Applied {
                    ExecutionState::Verifying
                } else {
                    ExecutionState::Compensating
                },
                agent_result: None,
                verification: None,
                reason_code: reason_code.to_owned(),
                completed_at: response.observed_at,
            };
            self.journal
                .append_result_with_audit(
                    pending.intent.execution_id,
                    pending.intent.attempt,
                    &result,
                    &self.audit_for_intent(
                        &recovery_scope,
                        &pending.intent,
                        AuditEventKind::StepResultPersisted,
                        reason_code,
                        json!({"outcome_code": response.outcome_code}),
                        response.observed_at,
                    ),
                )
                .await?;
            outcomes
                .entry(pending.intent.execution_id)
                .or_insert_with(|| (ReconcileAggregate::default(), recovery_scope))
                .0
                .observe(response.state);
        }
        for (execution_id, (aggregate, recovery_scope)) in outcomes {
            let state = self.journal.execution_state(execution_id).await?;
            if state == ExecutionState::Reconciling {
                let to = if aggregate.requires_compensation {
                    ExecutionState::Compensating
                } else {
                    ExecutionState::Verifying
                };
                self.transition(
                    &recovery_scope,
                    ExecutionState::Reconciling,
                    to,
                    if to == ExecutionState::Verifying {
                        "reconcile_confirmed_applied"
                    } else {
                        "reconcile_requires_compensation"
                    },
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn enter_reconciling(&self, state: ExecutionState, scope: &ExecutionRequest) -> Result<(), ExecutorError> {
        let synthetic = scope.clone();
        match state {
            ExecutionState::IntentPersisted => {
                self.transition(
                    &synthetic,
                    ExecutionState::IntentPersisted,
                    ExecutionState::Applying,
                    "restart_found_persisted_intent",
                )
                .await?;
                self.transition(
                    &synthetic,
                    ExecutionState::Applying,
                    ExecutionState::Unknown,
                    "restart_effect_unknown",
                )
                .await?;
                self.transition(
                    &synthetic,
                    ExecutionState::Unknown,
                    ExecutionState::Reconciling,
                    "restart_reconcile_started",
                )
                .await?;
            }
            ExecutionState::Applying => {
                self.transition(
                    &synthetic,
                    ExecutionState::Applying,
                    ExecutionState::Unknown,
                    "restart_effect_unknown",
                )
                .await?;
                self.transition(
                    &synthetic,
                    ExecutionState::Unknown,
                    ExecutionState::Reconciling,
                    "restart_reconcile_started",
                )
                .await?;
            }
            ExecutionState::Unknown => {
                self.transition(
                    &synthetic,
                    ExecutionState::Unknown,
                    ExecutionState::Reconciling,
                    "restart_reconcile_started",
                )
                .await?;
            }
            ExecutionState::Reconciling => {}
            _ => return Err(ExecutorError::ReconcileBlocked),
        }
        Ok(())
    }

    async fn acquire_locks(&self, request: &ExecutionRequest) -> Result<Vec<ResourceLock>, ExecutorError> {
        let now = Utc::now();
        let ttl = TimeDelta::from_std(self.resource_lock_ttl).map_err(|_| ExecutorError::Configuration)?;
        let mut unique = BTreeSet::new();
        let mut locks = Vec::new();
        for step in &request.plan.steps {
            let key = (step.resource.clone(), step.action);
            if !unique.insert(key.clone()) {
                continue;
            }
            match self
                .safety
                .acquire(&ResourceLockRequest {
                    id: ResourceLockId::new(),
                    tenant_id: request.tenant_id,
                    cluster_id: request.cluster_id,
                    resource_key: key.0,
                    action: key.1,
                    holder_execution_id: request.id,
                    acquired_at: now,
                    expires_at: now + ttl,
                })
                .await
            {
                Ok(lock) => locks.push(lock),
                Err(error) => {
                    self.release_locks(&locks, request.id, "partial_lock_acquisition_failed")
                        .await;
                    return Err(error.into());
                }
            }
        }
        Ok(locks)
    }

    async fn release_locks(&self, locks: &[ResourceLock], execution_id: ExecutionId, reason: &str) {
        for lock in locks {
            if let Err(error) = self.safety.release(lock.id, execution_id, Utc::now(), reason).await {
                tracing::warn!(
                    execution_id = %execution_id,
                    resource_lock_id = %lock.id,
                    error = %error,
                    "failed to release Executor resource lock"
                );
            }
        }
    }

    async fn transition(
        &self,
        request: &ExecutionRequest,
        from: ExecutionState,
        to: ExecutionState,
        reason: &str,
    ) -> Result<(), ExecutorError> {
        let occurred_at = Utc::now();
        let transition = ExecutionTransition {
            from,
            to,
            reason_code: reason.to_owned(),
            occurred_at,
        };
        let audit = self.audit(
            request,
            AuditEventKind::StateChanged,
            "execution",
            request.id.to_string(),
            reason,
            json!({"from": from, "to": to}),
            occurred_at,
        );
        if self
            .journal
            .transition_with_audit(request.id, &transition, &audit)
            .await?
        {
            Ok(())
        } else {
            Err(ExecutorError::InvalidRequest)
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "audit scope and identity fields are intentionally explicit"
    )]
    fn audit(
        &self,
        request: &ExecutionRequest,
        event_kind: AuditEventKind,
        resource_kind: &str,
        resource_id: String,
        reason_code: &str,
        details: serde_json::Value,
        occurred_at: chrono::DateTime<Utc>,
    ) -> AuditEvent {
        AuditEvent {
            id: AuditEventId::new(),
            tenant_id: request.tenant_id,
            cluster_id: request.cluster_id,
            correlation_id: request.correlation_id,
            event_kind,
            actor_subject: self.executor_subject.to_string(),
            actor_role: "executor_service".to_owned(),
            resource_kind: resource_kind.to_owned(),
            resource_id,
            reason_code: reason_code.to_owned(),
            details,
            occurred_at,
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "recovery audit fields are intentionally explicit"
    )]
    fn audit_for_intent(
        &self,
        scope: &ExecutionRequest,
        intent: &StepIntent,
        event_kind: AuditEventKind,
        reason_code: &str,
        details: serde_json::Value,
        occurred_at: chrono::DateTime<Utc>,
    ) -> AuditEvent {
        AuditEvent {
            id: AuditEventId::new(),
            tenant_id: scope.tenant_id,
            cluster_id: scope.cluster_id,
            correlation_id: scope.correlation_id,
            event_kind,
            actor_subject: self.executor_subject.to_string(),
            actor_role: "executor_service".to_owned(),
            resource_kind: "step_result".to_owned(),
            resource_id: intent.step_id.to_string(),
            reason_code: reason_code.to_owned(),
            details,
            occurred_at,
        }
    }
}

#[derive(Default)]
struct ReconcileAggregate {
    requires_compensation: bool,
}

impl ReconcileAggregate {
    fn observe(&mut self, state: ReconcileEffectState) {
        self.requires_compensation |= matches!(state, ReconcileEffectState::NotApplied | ReconcileEffectState::Failed);
    }
}

fn execution_projection(
    request: &ExecutionRequest,
) -> Result<(String, rocketmq_sre_contracts::ExecutionAction), ExecutorError> {
    let first = request.plan.steps.first().ok_or(ExecutorError::InvalidRequest)?;
    let resource = if request.plan.steps.len() == 1 {
        first.resource.clone()
    } else {
        format!("plan/{}", request.plan.id)
    };
    Ok((resource, first.action))
}

#[path = "execution_flow.rs"]
mod execution_flow;
