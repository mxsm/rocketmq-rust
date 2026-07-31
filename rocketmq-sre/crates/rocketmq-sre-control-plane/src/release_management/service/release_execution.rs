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

use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::IntegrationEventKind;
use rocketmq_sre_contracts::ReleaseId;
use rocketmq_sre_contracts::ReleaseObservation;
use rocketmq_sre_contracts::ReleaseObservationPhase;
use rocketmq_sre_contracts::ReleaseReport;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::ReleaseWorkflow;
use rocketmq_sre_contracts::is_sha256_digest;
use rocketmq_sre_core::ReleaseStateMachine;
use rocketmq_sre_core::ReleaseValidator;
use serde_json::json;
use uuid::Uuid;

use super::ReleaseManagementService;
use super::release_validation::validate_observation_phase;
use super::support::ReleaseTransition;
use super::support::audit_event;
use super::support::reject_sensitive;
use super::support::require_operator;
use super::support::transition_release;
use super::support::validate_bounded_text;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::release_management::model::QueuedIntegrationDelivery;
use crate::release_management::model::RecordReleaseObservationRequest;
use crate::release_management::model::ReleaseDetail;
use crate::release_management::model::ReleaseExecutionRequest;
use crate::release_management::model::ReleaseExecutionView;
use crate::release_management::model::ReleaseTransitionRequest;
use crate::supervised_execution::ExecutionSubmissionView;
use crate::supervised_execution::SubmitExecutionRequest;

impl ReleaseManagementService {
    pub(in crate::release_management) async fn start_release(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
        request: &ReleaseExecutionRequest,
    ) -> Result<ReleaseExecutionView, ControlPlaneError> {
        require_operator(auth)?;
        validate_execution_input(request)?;
        let current = self.load_release(auth, release_id).await?;
        if current.status == ReleaseStatus::CanaryRunning {
            return self
                .existing_release_execution(auth, &current, request, current.plan_id)
                .await;
        }
        if current.status != ReleaseStatus::Ready {
            return Err(ControlPlaneError::conflict_code(
                "release_state_invalid",
                "release canary may start only after readiness passes",
            ));
        }
        ReleaseValidator::require_ready(&current, self.now())
            .map_err(|error| ControlPlaneError::conflict_code("release_readiness_invalid", error.to_string()))?;
        let observations = self.repository.release_observations(auth.tenant_id, current.id).await?;
        require_healthy_phase(
            &observations,
            ReleaseObservationPhase::Before,
            "release requires a healthy before observation",
        )?;
        let submitted = self
            .supervised
            .submit_execution(
                auth,
                &SubmitExecutionRequest {
                    plan_id: current.plan_id,
                    plan_hash: current.plan_hash.clone(),
                    precondition_hash: request.precondition_hash.clone(),
                    idempotency_key: request.idempotency_key.clone(),
                },
                current.correlation_id,
            )
            .await?;
        let mut transition = transition_release(
            &current,
            ReleaseStatus::CanaryRunning,
            auth,
            "ReleaseCanaryStarted",
            "approved canary execution submitted",
            json!({
                "execution_id": submitted.execution.id,
                "plan_id": current.plan_id,
            }),
            self.now(),
        )?;
        transition.workflow.active_execution_id = Some(submitted.execution.id);
        let outbound = self
            .outbound_deliveries(
                &transition.workflow,
                IntegrationEventKind::ReleaseStarted,
                "Supervised release canary execution started",
                auth,
            )
            .await?;
        self.persist_transition(&current, &transition, &outbound).await?;
        Ok(ReleaseExecutionView {
            schema_version: "rocketmq-sre.release-execution.v1",
            workflow: transition.workflow,
            execution_id: submitted.execution.id,
        })
    }

    pub(in crate::release_management) async fn record_release_observation(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
        request: RecordReleaseObservationRequest,
    ) -> Result<ReleaseDetail, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.load_release(auth, release_id).await?;
        validate_observation_phase(current.status, request.phase)?;
        let observation = request.into_observation(self.now());
        ReleaseValidator::validate_observation(&observation)
            .map_err(|error| ControlPlaneError::validation("release_observation_invalid", error.to_string()))?;
        let observation_audit = observation_audit(auth, &current, &observation);
        let mut updated = None;
        let mut state_event = None;
        let mut audits = vec![observation_audit];
        let mut outbound = Vec::new();
        if observation.regression_detected {
            let next = match current.status {
                ReleaseStatus::Ready => ReleaseStatus::Failed,
                ReleaseStatus::CanaryRunning | ReleaseStatus::Verifying => {
                    ReleaseStateMachine::observe(current.status, &observation)
                        .map_err(|error| ControlPlaneError::conflict_code("release_state_invalid", error.to_string()))?
                }
                _ => {
                    return Err(ControlPlaneError::conflict_code(
                        "release_state_invalid",
                        "regression cannot be applied in the current release state",
                    ));
                }
            };
            let mut transition = transition_release(
                &current,
                next,
                auth,
                "ReleaseRegressionDetected",
                "SLO or synthetic probe regression detected",
                json!({"phase": observation.phase}),
                observation.observed_at,
            )?;
            transition.workflow.regression_detected = true;
            if next == ReleaseStatus::Paused {
                outbound = self
                    .outbound_deliveries(
                        &transition.workflow,
                        IntegrationEventKind::ReleasePaused,
                        "Release paused after an SLO or synthetic probe regression",
                        auth,
                    )
                    .await?;
            }
            audits.push(transition.audit);
            state_event = Some(transition.event);
            updated = Some(transition.workflow);
        }
        self.repository
            .insert_release_observation(
                Uuid::new_v4(),
                &current,
                updated.as_ref(),
                &observation,
                state_event.as_ref(),
                &audits,
                &outbound,
            )
            .await?;
        self.release(auth, current.id).await
    }

    pub(in crate::release_management) async fn pause_release(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
        request: &ReleaseTransitionRequest,
    ) -> Result<ReleaseDetail, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.load_release(auth, release_id).await?;
        let transition = transition_release(
            &current,
            ReleaseStatus::Paused,
            auth,
            "ReleasePausedByOperator",
            &request.reason,
            json!({}),
            self.now(),
        )?;
        let outbound = self
            .outbound_deliveries(
                &transition.workflow,
                IntegrationEventKind::ReleasePaused,
                "Release paused by an authenticated operator",
                auth,
            )
            .await?;
        self.persist_transition(&current, &transition, &outbound).await?;
        self.release(auth, release_id).await
    }

    pub(in crate::release_management) async fn resume_release(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
        request: &ReleaseTransitionRequest,
    ) -> Result<ReleaseDetail, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.load_release(auth, release_id).await?;
        if current.regression_detected {
            return Err(ControlPlaneError::conflict_code(
                "release_regression_unresolved",
                "a regressed release must roll back or enter manual takeover",
            ));
        }
        let transition = transition_release(
            &current,
            ReleaseStatus::CanaryRunning,
            auth,
            "ReleaseResumed",
            &request.reason,
            json!({"active_execution_id": current.active_execution_id}),
            self.now(),
        )?;
        let outbound = self
            .outbound_deliveries(
                &transition.workflow,
                IntegrationEventKind::ReleaseStarted,
                "Supervised release resumed after an operator pause",
                auth,
            )
            .await?;
        self.persist_transition(&current, &transition, &outbound).await?;
        self.release(auth, release_id).await
    }

    pub(in crate::release_management) async fn begin_release_verification(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
    ) -> Result<ReleaseDetail, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.load_release(auth, release_id).await?;
        if current.status != ReleaseStatus::CanaryRunning {
            return Err(ControlPlaneError::conflict_code(
                "release_state_invalid",
                "verification may begin only while the canary is running",
            ));
        }
        self.require_active_execution_succeeded(auth, &current).await?;
        let observations = self.repository.release_observations(auth.tenant_id, current.id).await?;
        require_healthy_phase(
            &observations,
            ReleaseObservationPhase::Before,
            "verification requires a healthy before observation",
        )?;
        require_healthy_phase(
            &observations,
            ReleaseObservationPhase::During,
            "verification requires a healthy during observation",
        )?;
        let transition = transition_release(
            &current,
            ReleaseStatus::Verifying,
            auth,
            "ReleaseVerificationStarted",
            "canary execution and during-release checks succeeded",
            json!({"active_execution_id": current.active_execution_id}),
            self.now(),
        )?;
        self.persist_transition(&current, &transition, &[]).await?;
        self.release(auth, release_id).await
    }

    pub(in crate::release_management) async fn complete_release(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
    ) -> Result<ReleaseDetail, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.load_release(auth, release_id).await?;
        if current.status == ReleaseStatus::Completed {
            self.ensure_release_report(auth, &current).await?;
            return self.release(auth, release_id).await;
        }
        if current.status != ReleaseStatus::Verifying || current.regression_detected {
            return Err(ControlPlaneError::conflict_code(
                "release_state_invalid",
                "only a healthy verifying release may complete",
            ));
        }
        self.require_active_execution_succeeded(auth, &current).await?;
        let observations = self.repository.release_observations(auth.tenant_id, current.id).await?;
        require_report_observations(&observations)?;
        let transition = transition_release(
            &current,
            ReleaseStatus::Completed,
            auth,
            "ReleaseCompleted",
            "release verification completed successfully",
            json!({"active_execution_id": current.active_execution_id}),
            self.now(),
        )?;
        let outbound = self
            .outbound_deliveries(
                &transition.workflow,
                IntegrationEventKind::ReleaseCompleted,
                "Release completed with healthy before, during, and after checks",
                auth,
            )
            .await?;
        self.persist_transition(&current, &transition, &outbound).await?;
        self.ensure_release_report(auth, &transition.workflow).await?;
        self.release(auth, release_id).await
    }

    pub(super) async fn persist_transition(
        &self,
        current: &ReleaseWorkflow,
        transition: &ReleaseTransition,
        outbound: &[QueuedIntegrationDelivery],
    ) -> Result<(), ControlPlaneError> {
        self.repository
            .update_release_workflow(
                &transition.workflow,
                current.status,
                current.updated_at,
                &transition.event,
                &transition.audit,
                outbound,
            )
            .await
    }

    pub(super) async fn existing_release_execution(
        &self,
        auth: &AuthContext,
        workflow: &ReleaseWorkflow,
        request: &ReleaseExecutionRequest,
        plan_id: ActionPlanId,
    ) -> Result<ReleaseExecutionView, ControlPlaneError> {
        let execution_id = workflow.active_execution_id.ok_or_else(|| {
            ControlPlaneError::conflict_code("release_execution_missing", "release state has no active execution")
        })?;
        let execution = self.supervised.execution(auth, execution_id).await?;
        if execution.execution.plan.id != plan_id
            || execution.execution.idempotency_key != request.idempotency_key
            || execution.execution.requested_by != auth.subject
        {
            return Err(ControlPlaneError::conflict_code(
                "idempotency_conflict",
                "release execution is bound to a different request",
            ));
        }
        Ok(ReleaseExecutionView {
            schema_version: "rocketmq-sre.release-execution.v1",
            workflow: workflow.clone(),
            execution_id,
        })
    }

    pub(super) async fn require_active_execution_succeeded(
        &self,
        auth: &AuthContext,
        workflow: &ReleaseWorkflow,
    ) -> Result<ExecutionSubmissionView, ControlPlaneError> {
        let execution_id = workflow.active_execution_id.ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "release_execution_missing",
                "release has no active supervised execution",
            )
        })?;
        let execution = self.supervised.execution(auth, execution_id).await?;
        if !matches!(execution.state, ExecutionState::Succeeded | ExecutionState::RolledBack) {
            return Err(ControlPlaneError::conflict_code(
                "release_execution_incomplete",
                "active supervised execution has not reached a successful terminal state",
            ));
        }
        Ok(execution)
    }

    pub(super) async fn ensure_release_report(
        &self,
        auth: &AuthContext,
        workflow: &ReleaseWorkflow,
    ) -> Result<ReleaseReport, ControlPlaneError> {
        if let Some(report) = self.repository.release_report(workflow.tenant_id, workflow.id).await? {
            return Ok(report);
        }
        let observations = self
            .repository
            .release_observations(workflow.tenant_id, workflow.id)
            .await?;
        let report = ReleaseValidator::build_report(workflow, &observations, self.now())
            .map_err(|error| ControlPlaneError::conflict_code("release_report_not_ready", error.to_string()))?;
        let audit = audit_event(
            auth,
            workflow.cluster_id,
            workflow.correlation_id,
            AuditEventKind::ReleaseReportGenerated,
            "release_report",
            report.id.to_string(),
            "ReleaseReportGenerated",
            json!({
                "release_id": workflow.id,
                "incident_id": workflow.incident_id,
                "change_id": &workflow.change_id,
                "release_ref": &workflow.release_ref,
                "final_status": workflow.status,
            }),
            report.generated_at,
        );
        self.repository.insert_release_report(&report, &audit).await
    }
}

pub(super) fn observation_audit(
    auth: &AuthContext,
    workflow: &ReleaseWorkflow,
    observation: &ReleaseObservation,
) -> AuditEvent {
    audit_event(
        auth,
        workflow.cluster_id,
        workflow.correlation_id,
        AuditEventKind::ReleaseObservationCaptured,
        "release",
        workflow.id.to_string(),
        "ReleaseObservationCaptured",
        json!({
            "phase": observation.phase,
            "slo_healthy": observation.slo_healthy,
            "synthetic_probe_healthy": observation.synthetic_probe_healthy,
            "regression_detected": observation.regression_detected,
            "evidence_ids": &observation.evidence_ids,
        }),
        observation.observed_at,
    )
}

pub(super) fn validate_execution_input(request: &ReleaseExecutionRequest) -> Result<(), ControlPlaneError> {
    if !is_sha256_digest(&request.precondition_hash) {
        return Err(ControlPlaneError::validation(
            "invalid_precondition_hash",
            "release execution precondition must be a SHA-256 digest",
        ));
    }
    validate_bounded_text("release execution idempotency key", &request.idempotency_key, 256)?;
    reject_sensitive(&request.idempotency_key)
}

fn require_healthy_phase(
    observations: &[ReleaseObservation],
    phase: ReleaseObservationPhase,
    message: &'static str,
) -> Result<(), ControlPlaneError> {
    if !observations.iter().any(|observation| {
        observation.phase == phase
            && !observation.regression_detected
            && observation.slo_healthy
            && observation.synthetic_probe_healthy
    }) {
        return Err(ControlPlaneError::conflict_code("release_observation_missing", message));
    }
    Ok(())
}

fn require_report_observations(observations: &[ReleaseObservation]) -> Result<(), ControlPlaneError> {
    for (phase, message) in [
        (
            ReleaseObservationPhase::Before,
            "release report requires a healthy before observation",
        ),
        (
            ReleaseObservationPhase::During,
            "release report requires a healthy during observation",
        ),
        (
            ReleaseObservationPhase::After,
            "release report requires a healthy after observation",
        ),
    ] {
        require_healthy_phase(observations, phase, message)?;
    }
    if observations.iter().any(|observation| observation.regression_detected) {
        return Err(ControlPlaneError::conflict_code(
            "release_regression_unresolved",
            "release observations contain an unresolved regression",
        ));
    }
    Ok(())
}
