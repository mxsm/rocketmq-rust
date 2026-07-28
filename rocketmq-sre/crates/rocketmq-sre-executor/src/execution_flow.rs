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

use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::Utc;
use rocketmq_sre_contracts::AUTONOMY_SCHEMA_VERSION;
use rocketmq_sre_contracts::AgentDispatchAuthorization;
use rocketmq_sre_contracts::AgentDispatchRequest;
use rocketmq_sre_contracts::AgentDispatchResponse;
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::CompensationMode;
use rocketmq_sre_contracts::DynamicSafetyDecision;
use rocketmq_sre_contracts::DynamicSafetyEvaluationRequest;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::ExecutionStepId;
use rocketmq_sre_contracts::ExecutionTransition;
use rocketmq_sre_contracts::IssueFenceGrantRequest;
use rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION;
use rocketmq_sre_contracts::PlanStep;
use rocketmq_sre_contracts::ResourceQuarantine;
use rocketmq_sre_contracts::ResourceQuarantineId;
use rocketmq_sre_contracts::StepIntent;
use rocketmq_sre_contracts::StepResult;
use rocketmq_sre_contracts::VerificationOutcome;
use serde_json::json;

use super::ChangeExecutor;
use crate::ExecutionVerifier;
use crate::ExecutorError;
use crate::ResourceLock;
use crate::VerificationCaptureRequest;
use crate::VerificationPhase;

const ATTEMPT: u16 = 1;

#[derive(Clone)]
struct AppliedStep {
    step: PlanStep,
    forward_step_id: ExecutionStepId,
    pre_evidence_ids: Vec<EvidenceId>,
    during_evidence_ids: Vec<EvidenceId>,
    post_evidence_ids: Vec<EvidenceId>,
}

enum ForwardDispatch {
    Applied(Vec<AppliedStep>),
    Replan(Vec<AppliedStep>),
    SafetyInvalidated(Vec<AppliedStep>),
    VerificationUnavailable(Vec<AppliedStep>),
}

impl ChangeExecutor {
    pub(super) async fn execute_supervised_flow(
        &self,
        request: &ExecutionRequest,
        locks: &[ResourceLock],
        verifier: &ExecutionVerifier,
    ) -> Result<ExecutionState, ExecutorError> {
        let dispatch = self.dispatch_forward_steps(request, verifier).await?;
        let mut applied = match dispatch {
            ForwardDispatch::Applied(applied) => {
                self.transition(
                    request,
                    ExecutionState::Applying,
                    ExecutionState::Verifying,
                    "all_agent_effects_confirmed",
                )
                .await?;
                applied
            }
            ForwardDispatch::Replan(applied) => {
                self.transition(
                    request,
                    ExecutionState::Applying,
                    ExecutionState::Compensating,
                    "agent_precondition_conflict_replan_required",
                )
                .await?;
                return self
                    .rollback_applied(request, locks, verifier, applied, "agent_precondition_conflict")
                    .await;
            }
            ForwardDispatch::VerificationUnavailable(applied) => {
                self.transition(
                    request,
                    ExecutionState::Applying,
                    ExecutionState::Compensating,
                    "during_verification_unavailable",
                )
                .await?;
                return self
                    .rollback_applied(request, locks, verifier, applied, "during_verification_unavailable")
                    .await;
            }
            ForwardDispatch::SafetyInvalidated(applied) => {
                self.transition(
                    request,
                    ExecutionState::Applying,
                    ExecutionState::Compensating,
                    "dynamic_safety_invalidated",
                )
                .await?;
                return self
                    .rollback_applied(request, locks, verifier, applied, "dynamic_safety_invalidated")
                    .await;
            }
        };

        let mut failed_index = None;
        for (index, applied_step) in applied.iter_mut().enumerate() {
            match self.verify_forward_step(request, applied_step, verifier).await {
                Ok(VerificationOutcome::Succeeded) => {}
                Ok(VerificationOutcome::Failed | VerificationOutcome::Inconclusive) | Err(_) => {
                    failed_index = Some(index);
                    break;
                }
            }
        }
        if failed_index.is_none() {
            self.transition(
                request,
                ExecutionState::Verifying,
                ExecutionState::Succeeded,
                "descriptor_verification_succeeded",
            )
            .await?;
            self.release_locks(locks, request.id, "execution_succeeded").await;
            return Ok(ExecutionState::Succeeded);
        }

        self.transition(
            request,
            ExecutionState::Verifying,
            ExecutionState::Compensating,
            "descriptor_verification_requires_rollback",
        )
        .await?;
        self.rollback_applied(
            request,
            locks,
            verifier,
            applied,
            "descriptor_verification_requires_rollback",
        )
        .await
    }

    async fn dispatch_forward_steps(
        &self,
        request: &ExecutionRequest,
        verifier: &ExecutionVerifier,
    ) -> Result<ForwardDispatch, ExecutorError> {
        let mut applied = Vec::with_capacity(request.plan.steps.len());
        for (index, step) in request.plan.steps.iter().enumerate() {
            let step_id = ExecutionStepId::new();
            let pre = self
                .capture_and_persist(request, step, step_id, VerificationPhase::Pre, verifier)
                .await?;
            let dynamic_safety = match self.evaluate_step_safety(request, step, step_id).await {
                Ok(decision) => decision,
                Err(error) if applied.is_empty() => {
                    self.transition(
                        request,
                        ExecutionState::Prechecking,
                        ExecutionState::Escalated,
                        "dynamic_safety_expected_deny",
                    )
                    .await?;
                    return Err(error);
                }
                Err(_) => return Ok(ForwardDispatch::SafetyInvalidated(applied)),
            };
            let intent = self
                .persist_intent(request, step, step_id, dynamic_safety, false, "forward")
                .await?;
            if index == 0 {
                self.transition(
                    request,
                    ExecutionState::Prechecking,
                    ExecutionState::IntentPersisted,
                    "first_step_intent_persisted",
                )
                .await?;
                self.transition(
                    request,
                    ExecutionState::IntentPersisted,
                    ExecutionState::Applying,
                    "agent_dispatch_started",
                )
                .await?;
            }
            let response = self.dispatch_agent(request, step, intent.clone()).await;
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    self.transition(
                        request,
                        ExecutionState::Applying,
                        ExecutionState::Unknown,
                        "agent_effect_unknown",
                    )
                    .await?;
                    self.transition(
                        request,
                        ExecutionState::Unknown,
                        ExecutionState::Reconciling,
                        "read_only_reconcile_required",
                    )
                    .await?;
                    let reconcile = self.ensure_active_lease(request, true).await;
                    return match reconcile {
                        Ok(_) => Err(error),
                        Err(ExecutorError::ReconcileBlocked) => {
                            self.metrics.reconcile_blocks_total.fetch_add(1, Ordering::Relaxed);
                            Err(ExecutorError::ReconcileBlocked)
                        }
                        Err(other) => Err(other),
                    };
                }
            };
            let outcome_code = response.result.outcome_code.clone();
            self.persist_agent_result(
                request,
                step,
                step_id,
                response,
                if forward_requires_replan(&outcome_code) {
                    ExecutionState::Compensating
                } else {
                    ExecutionState::Verifying
                },
            )
            .await?;
            let during = self
                .capture_and_persist(request, step, step_id, VerificationPhase::During, verifier)
                .await;
            if forward_requires_replan(&outcome_code) {
                return Ok(ForwardDispatch::Replan(applied));
            }
            let during = match during {
                Ok(during) => during,
                Err(_) => {
                    applied.push(AppliedStep {
                        step: step.clone(),
                        forward_step_id: step_id,
                        pre_evidence_ids: vec![pre],
                        during_evidence_ids: Vec::new(),
                        post_evidence_ids: Vec::new(),
                    });
                    return Ok(ForwardDispatch::VerificationUnavailable(applied));
                }
            };
            applied.push(AppliedStep {
                step: step.clone(),
                forward_step_id: step_id,
                pre_evidence_ids: vec![pre],
                during_evidence_ids: vec![during],
                post_evidence_ids: Vec::new(),
            });
        }
        Ok(ForwardDispatch::Applied(applied))
    }

    async fn verify_forward_step(
        &self,
        request: &ExecutionRequest,
        applied: &mut AppliedStep,
        verifier: &ExecutionVerifier,
    ) -> Result<VerificationOutcome, ExecutorError> {
        let capture = verification_request(request, &applied.step, applied.forward_step_id, VerificationPhase::Post);
        let run = verifier
            .verify_post(
                &capture,
                &applied.step.verification,
                Utc::now(),
                applied.pre_evidence_ids.clone(),
                applied.during_evidence_ids.clone(),
            )
            .await?;
        for evidence in &run.post_evidence {
            self.persist_evidence(request, applied.forward_step_id, VerificationPhase::Post, evidence)
                .await?;
        }
        applied.post_evidence_ids = run.post_evidence.iter().map(|evidence| evidence.evidence_id).collect();
        self.persist_verification(request, false, &run.result).await?;
        Ok(run.result.outcome)
    }

    async fn rollback_applied(
        &self,
        request: &ExecutionRequest,
        locks: &[ResourceLock],
        verifier: &ExecutionVerifier,
        applied: Vec<AppliedStep>,
        trigger: &str,
    ) -> Result<ExecutionState, ExecutorError> {
        let rollback_started_at = Utc::now();
        self.journal
            .append_audit_event(
                request.id,
                &self.audit(
                    request,
                    AuditEventKind::RollbackStarted,
                    "execution",
                    request.id.to_string(),
                    trigger,
                    json!({"step_count": applied.len()}),
                    rollback_started_at,
                ),
            )
            .await?;
        if applied.is_empty() {
            self.transition(
                request,
                ExecutionState::Compensating,
                ExecutionState::RolledBack,
                "no_external_effect_to_compensate",
            )
            .await?;
            self.release_locks(locks, request.id, "replan_required_without_effect")
                .await;
            return Ok(ExecutionState::RolledBack);
        }

        for applied_step in applied.iter().rev() {
            if applied_step.step.compensation.mode == CompensationMode::NotAvailable {
                return self
                    .manual_takeover(request, locks, applied_step, "compensation_not_available")
                    .await;
            }
            let compensation_step_id = ExecutionStepId::new();
            let intent = self
                .persist_intent(
                    request,
                    &applied_step.step,
                    compensation_step_id,
                    None,
                    true,
                    "compensation",
                )
                .await?;
            let timeout_seconds = applied_step.step.compensation.timeout_seconds;
            let response = tokio::time::timeout(
                Duration::from_secs(timeout_seconds),
                self.dispatch_agent(request, &applied_step.step, intent),
            )
            .await;
            let response = match response {
                Ok(Ok(response)) => response,
                Ok(Err(_)) | Err(_) => {
                    return self
                        .manual_takeover(request, locks, applied_step, "compensation_effect_unknown")
                        .await;
                }
            };
            let outcome_code = response.result.outcome_code.clone();
            self.persist_agent_result(
                request,
                &applied_step.step,
                compensation_step_id,
                response,
                ExecutionState::Compensating,
            )
            .await?;
            if compensation_requires_manual_takeover(&outcome_code) {
                return self.manual_takeover(request, locks, applied_step, &outcome_code).await;
            }
            let during = match self
                .capture_and_persist(
                    request,
                    &applied_step.step,
                    compensation_step_id,
                    VerificationPhase::During,
                    verifier,
                )
                .await
            {
                Ok(during) => during,
                Err(_) => {
                    return self
                        .manual_takeover(request, locks, applied_step, "rollback_verification_unavailable")
                        .await;
                }
            };
            let capture = verification_request(
                request,
                &applied_step.step,
                compensation_step_id,
                VerificationPhase::RollbackPost,
            );
            let run = match verifier
                .verify_post(
                    &capture,
                    &applied_step.step.verification,
                    Utc::now(),
                    applied_step.pre_evidence_ids.clone(),
                    vec![during],
                )
                .await
            {
                Ok(run) => run,
                Err(_) => {
                    return self
                        .manual_takeover(request, locks, applied_step, "rollback_verification_unavailable")
                        .await;
                }
            };
            for evidence in &run.post_evidence {
                self.persist_evidence(request, compensation_step_id, VerificationPhase::RollbackPost, evidence)
                    .await?;
            }
            self.persist_verification(request, true, &run.result).await?;
            if run.result.outcome != VerificationOutcome::Succeeded {
                let mut failed = applied_step.clone();
                failed
                    .post_evidence_ids
                    .extend(run.post_evidence.iter().map(|evidence| evidence.evidence_id));
                return self
                    .manual_takeover(request, locks, &failed, "rollback_verification_failed")
                    .await;
            }
        }

        self.transition(
            request,
            ExecutionState::Compensating,
            ExecutionState::RolledBack,
            "rollback_verification_succeeded",
        )
        .await?;
        self.release_locks(locks, request.id, "execution_rolled_back").await;
        Ok(ExecutionState::RolledBack)
    }

    async fn manual_takeover(
        &self,
        request: &ExecutionRequest,
        locks: &[ResourceLock],
        applied: &AppliedStep,
        reason: &str,
    ) -> Result<ExecutionState, ExecutorError> {
        let occurred_at = Utc::now();
        let mut evidence_ids = applied.pre_evidence_ids.clone();
        evidence_ids.extend(applied.during_evidence_ids.iter().copied());
        evidence_ids.extend(applied.post_evidence_ids.iter().copied());
        evidence_ids.sort_unstable();
        evidence_ids.dedup();
        let quarantine = ResourceQuarantine {
            id: ResourceQuarantineId::new(),
            tenant_id: request.tenant_id,
            cluster_id: request.cluster_id,
            resource_key: applied.step.resource.clone(),
            action_id: Some(applied.step.action.id().to_owned()),
            reason_code: reason.to_owned(),
            source_execution_id: Some(request.id),
            evidence_ids,
            created_by: self.executor_subject.to_string(),
            created_at: occurred_at,
            cleared_by: None,
            clear_reason: None,
            clear_evidence_ids: Vec::new(),
            cleared_at: None,
        };
        let transition = ExecutionTransition {
            from: ExecutionState::Compensating,
            to: ExecutionState::Escalated,
            reason_code: "manual_takeover_required".to_owned(),
            occurred_at,
        };
        self.journal
            .escalate_manual_takeover(
                request.id,
                &transition,
                &quarantine,
                &self.audit(
                    request,
                    AuditEventKind::StateChanged,
                    "execution",
                    request.id.to_string(),
                    "manual_takeover_required",
                    json!({"from": ExecutionState::Compensating, "to": ExecutionState::Escalated}),
                    occurred_at,
                ),
                &self.audit(
                    request,
                    AuditEventKind::QuarantineCreated,
                    "resource_quarantine",
                    quarantine.id.to_string(),
                    reason,
                    json!({"resource": applied.step.resource, "action": applied.step.action}),
                    occurred_at,
                ),
                &self.audit(
                    request,
                    AuditEventKind::ManualTakeoverRequired,
                    "execution",
                    request.id.to_string(),
                    reason,
                    json!({"quarantine_id": quarantine.id}),
                    occurred_at,
                ),
            )
            .await?;
        self.release_locks(locks, request.id, "manual_takeover_quarantine_committed")
            .await;
        Ok(ExecutionState::Escalated)
    }

    async fn persist_intent(
        &self,
        request: &ExecutionRequest,
        step: &PlanStep,
        step_id: ExecutionStepId,
        dynamic_safety: Option<DynamicSafetyDecision>,
        compensation: bool,
        direction: &str,
    ) -> Result<StepIntent, ExecutorError> {
        let lease = self.current_lease(request.cluster_id).await?;
        let grant = self
            .authority
            .issue_fence_grant(&IssueFenceGrantRequest {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                lease_id: lease.id,
                epoch: lease.epoch,
                execution_id: request.id,
                step_id,
                plan_step_id: step.id,
            })
            .await
            .inspect_err(|_| {
                self.metrics.fence_rejections_total.fetch_add(1, Ordering::Relaxed);
            })?;
        let intended_at = Utc::now();
        let intent = StepIntent {
            execution_id: request.id,
            step_id,
            plan_hash: request.plan.plan_hash.clone(),
            step: step.clone(),
            attempt: ATTEMPT,
            idempotency_key: format!("{}:{}:{direction}:{ATTEMPT}", request.idempotency_key, step.id),
            fence_grant: grant,
            dynamic_safety,
            intended_at,
            compensation,
        };
        self.journal
            .append_intent_with_audit(
                &intent,
                &self.audit(
                    request,
                    AuditEventKind::StepIntentPersisted,
                    "step_intent",
                    step_id.to_string(),
                    if compensation {
                        "compensation_intent_persisted"
                    } else {
                        "step_intent_persisted"
                    },
                    json!({
                        "plan_step_id": step.id,
                        "sequence": step.sequence,
                        "action": step.action,
                        "lease_epoch": intent.fence_grant.epoch,
                        "dynamic_safety_decision_id": intent.dynamic_safety.as_ref().map(|decision| decision.id),
                        "compensation": compensation,
                    }),
                    intended_at,
                ),
            )
            .await?;
        Ok(intent)
    }

    async fn dispatch_agent(
        &self,
        request: &ExecutionRequest,
        step: &PlanStep,
        intent: StepIntent,
    ) -> Result<AgentDispatchResponse, ExecutorError> {
        self.agent
            .dispatch(&AgentDispatchRequest {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                tenant_id: request.tenant_id,
                plan_id: Some(request.plan.id),
                authorization: if request.is_autonomous() {
                    AgentDispatchAuthorization::Autonomous
                } else {
                    AgentDispatchAuthorization::HumanApproved
                },
                request: AgentStepRequest {
                    intent,
                    action: step.action,
                    descriptor_version: step.descriptor_version.clone(),
                    target: step.resource.clone(),
                    parameters: step.parameters.clone(),
                },
            })
            .await
    }

    async fn evaluate_step_safety(
        &self,
        request: &ExecutionRequest,
        step: &PlanStep,
        step_id: ExecutionStepId,
    ) -> Result<Option<DynamicSafetyDecision>, ExecutorError> {
        let Some(grant) = request.autonomy_grant.as_ref() else {
            return Ok(None);
        };
        let decision = self
            .authority
            .evaluate_dynamic_safety(&DynamicSafetyEvaluationRequest {
                schema_version: AUTONOMY_SCHEMA_VERSION.to_owned(),
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                action: step.action,
                action_version: step.descriptor_version.clone(),
                plan_id: request.plan.id,
                plan_hash: request.plan.plan_hash.clone(),
                execution_id: request.id,
                execution_step_id: step_id,
                policy_definition_version: grant.policy_definition_version,
                lifecycle_revision: grant.lifecycle_revision,
            })
            .await?;
        Ok(Some(decision))
    }

    async fn persist_agent_result(
        &self,
        request: &ExecutionRequest,
        step: &PlanStep,
        step_id: ExecutionStepId,
        response: AgentDispatchResponse,
        state: ExecutionState,
    ) -> Result<(), ExecutorError> {
        let completed_at = Utc::now();
        let reason_code = if response.replayed {
            "agent_effect_replayed".to_owned()
        } else {
            response.result.outcome_code.clone()
        };
        let result = StepResult {
            step_id,
            state,
            agent_result: Some(response.result),
            verification: None,
            reason_code,
            completed_at,
        };
        self.journal
            .append_result_with_audit(
                request.id,
                ATTEMPT,
                &result,
                &self.audit(
                    request,
                    AuditEventKind::StepResultPersisted,
                    "step_result",
                    step_id.to_string(),
                    &result.reason_code,
                    json!({
                        "sequence": step.sequence,
                        "action": step.action,
                        "compensation": state == ExecutionState::Compensating,
                    }),
                    completed_at,
                ),
            )
            .await?;
        Ok(())
    }

    async fn capture_and_persist(
        &self,
        request: &ExecutionRequest,
        step: &PlanStep,
        step_id: ExecutionStepId,
        phase: VerificationPhase,
        verifier: &ExecutionVerifier,
    ) -> Result<EvidenceId, ExecutorError> {
        let observation = verifier
            .capture(&verification_request(request, step, step_id, phase))
            .await?;
        let evidence_id = observation.evidence.evidence_id;
        self.persist_evidence(request, step_id, phase, &observation.evidence)
            .await?;
        Ok(evidence_id)
    }

    async fn persist_evidence(
        &self,
        request: &ExecutionRequest,
        step_id: ExecutionStepId,
        phase: VerificationPhase,
        evidence: &rocketmq_sre_contracts::EvidenceSnapshot,
    ) -> Result<(), ExecutorError> {
        self.journal
            .append_verification_evidence_with_audit(
                request.id,
                step_id,
                ATTEMPT,
                phase,
                evidence,
                &self.audit(
                    request,
                    AuditEventKind::VerificationCaptured,
                    "verification_evidence",
                    evidence.evidence_id.to_string(),
                    verification_phase_reason(phase),
                    json!({
                        "step_id": step_id,
                        "phase": verification_phase_name(phase),
                        "content_hash": evidence.content_hash,
                    }),
                    evidence.observed_at,
                ),
            )
            .await?;
        Ok(())
    }

    async fn persist_verification(
        &self,
        request: &ExecutionRequest,
        compensation: bool,
        result: &rocketmq_sre_contracts::VerificationResult,
    ) -> Result<(), ExecutorError> {
        self.journal
            .append_verification_result_with_audit(
                request.id,
                ATTEMPT,
                compensation,
                result,
                &self.audit(
                    request,
                    AuditEventKind::VerificationCompleted,
                    "verification",
                    result.step_id.to_string(),
                    match result.outcome {
                        VerificationOutcome::Succeeded => "verification_succeeded",
                        VerificationOutcome::Failed => "verification_failed",
                        VerificationOutcome::Inconclusive => "verification_inconclusive",
                    },
                    json!({
                        "outcome": result.outcome,
                        "compensation": compensation,
                        "stable_window_seconds": result.stable_window_seconds,
                    }),
                    result.completed_at,
                ),
            )
            .await?;
        Ok(())
    }
}

fn verification_request(
    request: &ExecutionRequest,
    step: &PlanStep,
    step_id: ExecutionStepId,
    phase: VerificationPhase,
) -> VerificationCaptureRequest {
    VerificationCaptureRequest {
        tenant_id: request.tenant_id,
        cluster_id: request.cluster_id,
        correlation_id: request.correlation_id,
        execution_id: request.id,
        step_id,
        plan_step_id: step.id,
        action: step.action,
        descriptor_version: step.descriptor_version.clone(),
        target: step.resource.clone(),
        parameters: step.parameters.clone(),
        phase,
        resource_conditions: step.verification.resource_conditions.clone(),
        technical_slis: step.verification.technical_slis.clone(),
    }
}

fn forward_requires_replan(outcome_code: &str) -> bool {
    matches!(
        outcome_code,
        "broker_config_generation_conflict" | "topic_config_version_conflict"
    )
}

fn compensation_requires_manual_takeover(outcome_code: &str) -> bool {
    matches!(
        outcome_code,
        "broker_config_rollback_generation_conflict"
            | "topic_config_rollback_version_conflict"
            | "proxy_restart_manual_takeover_required"
    )
}

const fn verification_phase_name(phase: VerificationPhase) -> &'static str {
    match phase {
        VerificationPhase::Pre => "pre",
        VerificationPhase::During => "during",
        VerificationPhase::Post => "post",
        VerificationPhase::RollbackPost => "rollback_post",
    }
}

const fn verification_phase_reason(phase: VerificationPhase) -> &'static str {
    match phase {
        VerificationPhase::Pre => "verification_pre_captured",
        VerificationPhase::During => "verification_during_captured",
        VerificationPhase::Post => "verification_post_captured",
        VerificationPhase::RollbackPost => "verification_rollback_post_captured",
    }
}
