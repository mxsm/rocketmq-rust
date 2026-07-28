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

use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IntegrationEventKind;
use rocketmq_sre_contracts::ReleaseId;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::ReleaseWorkflow;
use rocketmq_sre_contracts::SimulationKind;
use rocketmq_sre_contracts::WhatIfSimulationRequest;
use rocketmq_sre_core::ReleaseValidator;
use serde_json::json;
use uuid::Uuid;

use super::ReleaseManagementService;
use super::release_validation::bounded_release_page_size;
use super::release_validation::build_readiness_snapshot;
use super::release_validation::require_approved_release_plan;
use super::release_validation::validate_create_release;
use super::release_validation::validate_release_runbook;
use super::support::audit_event;
use super::support::require_cluster;
use super::support::require_operator;
use super::support::transition_release;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::release_management::model::CreateReleaseRequest;
use crate::release_management::model::PrepareReleaseRequest;
use crate::release_management::model::ReleaseDetail;
use crate::release_management::model::ReleaseEventRecord;
use crate::release_management::model::ReleaseListQuery;
use crate::release_management::model::ReleasePage;
use crate::release_management::model::ReleasePreparationView;

impl ReleaseManagementService {
    pub(crate) async fn create_release(
        &self,
        auth: &AuthContext,
        request: &CreateReleaseRequest,
        correlation_id: CorrelationId,
    ) -> Result<ReleaseDetail, ControlPlaneError> {
        require_operator(auth)?;
        require_cluster(auth, request.cluster_id)?;
        validate_create_release(request)?;
        let now = self.now();
        let primary = self.supervised.plan(auth, request.plan_id).await?;
        require_approved_release_plan(
            &primary,
            auth.tenant_id,
            request.cluster_id,
            request.incident_id,
            &request.plan_hash,
            now,
        )?;
        let runbook = self
            .repository
            .runbook_definition(
                auth.tenant_id,
                request.cluster_id,
                request.runbook_id,
                &request.runbook_version,
            )
            .await?;
        validate_release_runbook(&runbook, &primary.plan)?;
        if let (Some(rollback_id), Some(rollback_hash)) =
            (request.rollback_plan_id, request.rollback_plan_hash.as_deref())
        {
            let rollback = self.supervised.plan(auth, rollback_id).await?;
            require_approved_release_plan(
                &rollback,
                auth.tenant_id,
                request.cluster_id,
                request.incident_id,
                rollback_hash,
                now,
            )?;
        }
        let workflow = ReleaseWorkflow {
            schema_version: "rocketmq-sre.release-workflow.v1".to_owned(),
            id: ReleaseId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            incident_id: request.incident_id,
            correlation_id,
            change_id: request.change_id.trim().to_owned(),
            release_ref: request.release_ref.trim().to_owned(),
            target_version: request.target_version.trim().to_owned(),
            runbook_id: request.runbook_id,
            runbook_version: request.runbook_version.trim().to_owned(),
            plan_id: request.plan_id,
            plan_hash: request.plan_hash.clone(),
            rollback_plan_id: request.rollback_plan_id,
            rollback_plan_hash: request.rollback_plan_hash.clone(),
            readiness: None,
            status: ReleaseStatus::Planned,
            active_execution_id: None,
            regression_detected: false,
            pause_reason: None,
            created_by: auth.subject.clone(),
            created_at: now,
            updated_at: now,
        };
        ReleaseValidator::validate_workflow(&workflow)
            .map_err(|error| ControlPlaneError::validation("release_invalid", error.to_string()))?;
        let event = ReleaseEventRecord {
            id: Uuid::new_v4(),
            release_id: workflow.id,
            correlation_id,
            from_status: None,
            to_status: ReleaseStatus::Planned,
            reason_code: "ReleaseCreated".to_owned(),
            actor_subject: auth.subject.clone(),
            details: json!({
                "change_id": &workflow.change_id,
                "release_ref": &workflow.release_ref,
                "plan_id": workflow.plan_id,
                "plan_hash": &workflow.plan_hash,
                "rollback_plan_id": workflow.rollback_plan_id,
                "runbook_id": workflow.runbook_id,
                "runbook_version": &workflow.runbook_version,
            }),
            occurred_at: now,
        };
        let audit = audit_event(
            auth,
            workflow.cluster_id,
            correlation_id,
            AuditEventKind::ReleaseCreated,
            "release",
            workflow.id.to_string(),
            "ReleaseCreated",
            event.details.clone(),
            now,
        );
        let outbound = self
            .outbound_deliveries(
                &workflow,
                IntegrationEventKind::PlanSubmitted,
                "Approved release plan registered for supervised release escort",
                auth,
            )
            .await?;
        self.repository
            .insert_release_workflow(&workflow, &event, &audit, &outbound)
            .await?;
        self.release(auth, workflow.id).await
    }

    pub(crate) async fn release(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
    ) -> Result<ReleaseDetail, ControlPlaneError> {
        let workflow = self.load_release(auth, release_id).await?;
        self.release_detail(workflow).await
    }

    pub(crate) async fn releases(
        &self,
        auth: &AuthContext,
        query: &ReleaseListQuery,
    ) -> Result<ReleasePage, ControlPlaneError> {
        require_cluster(auth, query.cluster_id)?;
        let limit = bounded_release_page_size(query.limit);
        let mut items = self
            .repository
            .release_workflows(auth.tenant_id, query.cluster_id, query.status, i64::from(limit + 1))
            .await?;
        let partial = items.len() > limit as usize;
        items.truncate(limit as usize);
        Ok(ReleasePage {
            schema_version: "rocketmq-sre.release-page.v1",
            items,
            partial,
        })
    }

    pub(crate) async fn prepare_release(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
        request: &PrepareReleaseRequest,
    ) -> Result<ReleasePreparationView, ControlPlaneError> {
        require_operator(auth)?;
        let mut current = self.load_release(auth, release_id).await?;
        if current.status == ReleaseStatus::Planned {
            let transition = transition_release(
                &current,
                ReleaseStatus::ReadinessChecking,
                auth,
                "ReleaseReadinessStarted",
                "release readiness evaluation started",
                json!({}),
                self.now(),
            )?;
            self.repository
                .update_release_workflow(
                    &transition.workflow,
                    current.status,
                    current.updated_at,
                    &transition.event,
                    &transition.audit,
                    &[],
                )
                .await?;
            current = transition.workflow;
        } else if current.status != ReleaseStatus::ReadinessChecking {
            return Err(ControlPlaneError::conflict_code(
                "release_state_invalid",
                "release readiness may run only from planned or readiness-checking state",
            ));
        }
        let readiness = self
            .forecast
            .upgrade_readiness(auth, current.cluster_id, &current.target_version)
            .await?;
        let simulation = self
            .forecast
            .run_simulation(
                auth,
                WhatIfSimulationRequest {
                    cluster_id: current.cluster_id,
                    kind: SimulationKind::VersionUpgrade,
                    current_utilization: None,
                    current_instances: None,
                    traffic_increase_percent: None,
                    instance_delta: None,
                    current_queue_count: None,
                    queue_delta: None,
                    target_version: Some(current.target_version.clone()),
                    configuration_changes: request.configuration_changes.clone(),
                    affected_resource_keys: request.affected_resource_keys.clone(),
                    evidence_ids: request.evidence_ids.clone(),
                },
            )
            .await?;
        let snapshot = build_readiness_snapshot(request, &readiness, &simulation)?;
        let mut evaluated = current.clone();
        evaluated.readiness = Some(snapshot.clone());
        let next = if snapshot.ready() {
            ReleaseStatus::Ready
        } else {
            ReleaseStatus::Failed
        };
        let mut transition = transition_release(
            &evaluated,
            next,
            auth,
            if snapshot.ready() {
                "ReleaseReadinessPassed"
            } else {
                "ReleaseReadinessFailed"
            },
            if snapshot.ready() {
                "release readiness gates passed"
            } else {
                "one or more release readiness gates failed"
            },
            json!({
                "upgrade_readiness_id": readiness.id,
                "simulation_id": simulation.id,
                "pdb_ready": snapshot.pdb_ready,
                "capacity_ready": snapshot.capacity_ready,
                "quorum_ready": snapshot.quorum_ready,
                "store_recovery_ready": snapshot.store_recovery_ready,
                "synthetic_probe_ready": snapshot.synthetic_probe_ready,
            }),
            self.now(),
        )?;
        transition.audit.event_kind = AuditEventKind::ReleaseReadinessEvaluated;
        self.repository
            .update_release_workflow(
                &transition.workflow,
                current.status,
                current.updated_at,
                &transition.event,
                &transition.audit,
                &[],
            )
            .await?;
        Ok(ReleasePreparationView {
            schema_version: "rocketmq-sre.release-preparation.v1",
            workflow: transition.workflow,
            upgrade_readiness: readiness,
            simulation,
        })
    }

    pub(super) async fn load_release(
        &self,
        auth: &AuthContext,
        release_id: ReleaseId,
    ) -> Result<ReleaseWorkflow, ControlPlaneError> {
        let workflow = self.repository.release_workflow(auth.tenant_id, release_id).await?;
        require_cluster(auth, workflow.cluster_id)?;
        Ok(workflow)
    }

    async fn release_detail(&self, workflow: ReleaseWorkflow) -> Result<ReleaseDetail, ControlPlaneError> {
        let observations = self
            .repository
            .release_observations(workflow.tenant_id, workflow.id)
            .await?;
        let report = self.repository.release_report(workflow.tenant_id, workflow.id).await?;
        Ok(ReleaseDetail {
            schema_version: "rocketmq-sre.release-detail.v1",
            workflow,
            observations,
            report,
        })
    }
}
