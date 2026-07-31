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

use rocketmq_sre_contracts::IntegrationEventKind;
use rocketmq_sre_contracts::ReleaseId;
use rocketmq_sre_contracts::ReleaseObservationPhase;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::ReleaseWorkflow;
use rocketmq_sre_core::ReleaseValidator;
use serde_json::json;
use uuid::Uuid;

use super::ReleaseManagementService;
use super::release_execution::observation_audit;
use super::release_execution::validate_execution_input;
use super::release_validation::require_approved_release_plan;
use super::support::reject_sensitive;
use super::support::require_operator;
use super::support::transition_release;
use super::support::validate_bounded_text;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::release_management::model::CompleteRollbackRequest;
use crate::release_management::model::ReleaseDetail;
use crate::release_management::model::ReleaseExecutionRequest;
use crate::release_management::model::ReleaseExecutionView;
use crate::release_management::model::ReleaseTransitionRequest;
use crate::supervised_execution::SubmitExecutionRequest;

impl ReleaseManagementService {
    pub(in crate::release_management) async fn start_release_rollback(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
        request: &ReleaseExecutionRequest,
    ) -> Result<ReleaseExecutionView, ControlPlaneError> {
        require_operator(auth)?;
        validate_execution_input(request)?;
        let current = self.load_release(auth, release_id).await?;
        if current.status == ReleaseStatus::RollingBack {
            let rollback_id = current.rollback_plan_id.ok_or_else(|| {
                ControlPlaneError::conflict_code("rollback_unavailable", "rolling-back release has no rollback plan")
            })?;
            return self
                .existing_release_execution(auth, &current, request, rollback_id)
                .await;
        }
        if !matches!(
            current.status,
            ReleaseStatus::CanaryRunning | ReleaseStatus::Paused | ReleaseStatus::Verifying
        ) {
            return Err(ControlPlaneError::conflict_code(
                "release_state_invalid",
                "rollback may start only from an active, paused, or verifying release",
            ));
        }
        let rollback_id = match current.rollback_plan_id {
            Some(id) => id,
            None => {
                self.enter_manual_takeover(
                    auth,
                    &current,
                    "RollbackPlanUnavailable",
                    "typed rollback plan is unavailable",
                )
                .await?;
                return Err(ControlPlaneError::conflict_code(
                    "rollback_unavailable",
                    "release entered manual takeover because no typed rollback plan is available",
                ));
            }
        };
        ReleaseValidator::require_rollback(&current)
            .map_err(|error| ControlPlaneError::conflict_code("rollback_unavailable", error.to_string()))?;
        let rollback = self.supervised.plan(auth, rollback_id).await?;
        let approval = require_approved_release_plan(
            &rollback,
            auth.tenant_id,
            current.cluster_id,
            current.incident_id,
            current.rollback_plan_hash.as_deref().ok_or_else(|| {
                ControlPlaneError::conflict_code("rollback_unavailable", "rollback plan hash is unavailable")
            })?,
            self.now(),
        );
        if let Err(error) = approval {
            self.enter_manual_takeover(
                auth,
                &current,
                "RollbackApprovalUnavailable",
                "typed rollback approval is unavailable or expired",
            )
            .await?;
            return Err(error);
        }
        let submitted = self
            .supervised
            .submit_execution(
                auth,
                &SubmitExecutionRequest {
                    plan_id: rollback.plan.id,
                    plan_hash: rollback.plan.plan_hash.clone(),
                    precondition_hash: request.precondition_hash.clone(),
                    idempotency_key: request.idempotency_key.clone(),
                },
                current.correlation_id,
            )
            .await?;
        let mut transition = transition_release(
            &current,
            ReleaseStatus::RollingBack,
            auth,
            "ReleaseRollbackStarted",
            "approved typed rollback execution submitted",
            json!({
                "execution_id": submitted.execution.id,
                "rollback_plan_id": rollback.plan.id,
            }),
            self.now(),
        )?;
        transition.workflow.active_execution_id = Some(submitted.execution.id);
        let outbound = self
            .outbound_deliveries(
                &transition.workflow,
                IntegrationEventKind::ReleaseRollingBack,
                "Approved typed rollback execution started",
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

    pub(in crate::release_management) async fn complete_release_rollback(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
        request: CompleteRollbackRequest,
    ) -> Result<ReleaseDetail, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.load_release(auth, release_id).await?;
        if current.status == ReleaseStatus::RolledBack {
            self.ensure_release_report(auth, &current).await?;
            return self.release(auth, release_id).await;
        }
        if current.status != ReleaseStatus::RollingBack {
            return Err(ControlPlaneError::conflict_code(
                "release_state_invalid",
                "rollback completion requires a rolling-back release",
            ));
        }
        validate_bounded_text("rollback reason", &request.reason, 2_048)?;
        reject_sensitive(&request.reason)?;
        let observation = request.observation.into_observation(self.now());
        if observation.phase != ReleaseObservationPhase::After {
            return Err(ControlPlaneError::validation(
                "release_observation_phase_invalid",
                "rollback completion requires an after observation",
            ));
        }
        ReleaseValidator::validate_observation(&observation)
            .map_err(|error| ControlPlaneError::validation("release_observation_invalid", error.to_string()))?;
        let execution_succeeded = if request.succeeded {
            self.require_active_execution_succeeded(auth, &current).await?;
            true
        } else {
            false
        };
        let succeeded = execution_succeeded && !observation.regression_detected;
        let next = if succeeded {
            ReleaseStatus::RolledBack
        } else {
            ReleaseStatus::ManualTakeover
        };
        let mut transition = transition_release(
            &current,
            next,
            auth,
            if succeeded {
                "ReleaseRollbackCompleted"
            } else {
                "ReleaseRollbackFailed"
            },
            &request.reason,
            json!({
                "active_execution_id": current.active_execution_id,
                "execution_succeeded": execution_succeeded,
                "observation_healthy": !observation.regression_detected,
            }),
            observation.observed_at,
        )?;
        if !succeeded {
            transition.workflow.regression_detected = true;
        }
        let event_kind = if succeeded {
            IntegrationEventKind::ReleaseCompleted
        } else {
            IntegrationEventKind::ManualTakeoverRequired
        };
        let outbound = self
            .outbound_deliveries(
                &transition.workflow,
                event_kind,
                if succeeded {
                    "Release rollback completed with healthy after checks"
                } else {
                    "Release rollback requires manual operator takeover"
                },
                auth,
            )
            .await?;
        let audits = vec![observation_audit(auth, &current, &observation), transition.audit];
        self.repository
            .insert_release_observation(
                Uuid::new_v4(),
                &current,
                Some(&transition.workflow),
                &observation,
                Some(&transition.event),
                &audits,
                &outbound,
            )
            .await?;
        if succeeded {
            self.ensure_release_report(auth, &transition.workflow).await?;
        }
        self.release(auth, release_id).await
    }

    pub(in crate::release_management) async fn manual_release_takeover(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
        request: &ReleaseTransitionRequest,
    ) -> Result<ReleaseDetail, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.load_release(auth, release_id).await?;
        self.enter_manual_takeover(auth, &current, "ManualTakeoverRequested", &request.reason)
            .await?;
        self.release(auth, release_id).await
    }

    async fn enter_manual_takeover(
        &self,
        auth: &AuthContext,
        current: &ReleaseWorkflow,
        reason_code: &str,
        reason: &str,
    ) -> Result<(), ControlPlaneError> {
        let transition = transition_release(
            current,
            ReleaseStatus::ManualTakeover,
            auth,
            reason_code,
            reason,
            json!({"active_execution_id": current.active_execution_id}),
            self.now(),
        )?;
        let outbound = self
            .outbound_deliveries(
                &transition.workflow,
                IntegrationEventKind::ManualTakeoverRequired,
                "Release requires authenticated manual operator takeover",
                auth,
            )
            .await?;
        self.persist_transition(current, &transition, &outbound).await
    }
}
