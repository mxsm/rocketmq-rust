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

use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::ApprovalRecord;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExternalApprovalInput;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IntegrationAdapterKind;
use rocketmq_sre_contracts::IntegrationDelivery;
use rocketmq_sre_contracts::IntegrationEventKind;
use rocketmq_sre_contracts::IntegrationTarget;
use rocketmq_sre_contracts::IntegrationTargetId;
use rocketmq_sre_contracts::NotificationTargetId;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::ReleaseId;
use rocketmq_sre_contracts::ReleaseObservation;
use rocketmq_sre_contracts::ReleaseObservationPhase;
use rocketmq_sre_contracts::ReleaseReport;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::ReleaseWorkflow;
use rocketmq_sre_contracts::RunbookId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::UpgradeReadinessReport;
use rocketmq_sre_contracts::WhatIfSimulation;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RegisterIntegrationTargetRequest {
    pub(super) cluster_id: Option<ClusterId>,
    pub(super) descriptor_id: String,
    pub(super) descriptor_version: String,
    pub(super) name: String,
    pub(super) adapter_kind: IntegrationAdapterKind,
    pub(super) endpoint: String,
    pub(super) secret_reference: Option<String>,
    pub(super) notification_target_id: Option<NotificationTargetId>,
    #[serde(default = "default_enabled")]
    pub(super) enabled: bool,
    #[serde(default)]
    pub(super) inbound_approval: bool,
    #[serde(default)]
    pub(super) outbound_events: BTreeSet<IntegrationEventKind>,
}

const fn default_enabled() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SetIntegrationTargetStateRequest {
    pub(super) enabled: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct IntegrationTargetListQuery {
    pub(super) cluster_id: Option<ClusterId>,
    pub(super) adapter_kind: Option<IntegrationAdapterKind>,
    pub(super) enabled: Option<bool>,
    pub(super) limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct IntegrationTargetView {
    #[serde(flatten)]
    pub(super) target: IntegrationTarget,
    pub(super) notification_target_id: Option<NotificationTargetId>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct IntegrationTargetPage {
    pub(super) schema_version: &'static str,
    pub(super) items: Vec<IntegrationTargetView>,
    pub(super) partial: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct IntegrationDeliveryListQuery {
    pub(super) cluster_id: ClusterId,
    pub(super) target_id: Option<IntegrationTargetId>,
    pub(super) limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct IntegrationDeliveryPage {
    pub(super) schema_version: &'static str,
    pub(super) items: Vec<IntegrationDelivery>,
    pub(super) partial: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(transparent)]
pub(super) struct ExternalApprovalRequest {
    pub(super) input: ExternalApprovalInput,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ExternalApprovalView {
    pub(super) schema_version: &'static str,
    pub(super) duplicate: bool,
    pub(super) approval: ApprovalRecord,
    pub(super) plan_status: PlanStatus,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateReleaseRequest {
    pub(super) cluster_id: ClusterId,
    pub(super) incident_id: IncidentId,
    pub(super) change_id: String,
    pub(super) release_ref: String,
    pub(super) target_version: String,
    pub(super) runbook_id: RunbookId,
    pub(super) runbook_version: String,
    pub(super) plan_id: ActionPlanId,
    pub(super) plan_hash: String,
    pub(super) rollback_plan_id: Option<ActionPlanId>,
    pub(super) rollback_plan_hash: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PrepareReleaseRequest {
    pub(super) pdb_ready: bool,
    pub(super) synthetic_probe_ready: bool,
    #[serde(default)]
    pub(super) evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub(super) affected_resource_keys: Vec<String>,
    #[serde(default)]
    pub(super) configuration_changes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReleaseExecutionRequest {
    pub(super) precondition_hash: String,
    pub(super) idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecordReleaseObservationRequest {
    pub(super) phase: ReleaseObservationPhase,
    pub(super) slo_healthy: bool,
    pub(super) synthetic_probe_healthy: bool,
    #[serde(default)]
    pub(super) evidence_ids: Vec<EvidenceId>,
    pub(super) sanitized_summary: String,
}

impl RecordReleaseObservationRequest {
    pub(super) fn into_observation(self, observed_at: DateTime<Utc>) -> ReleaseObservation {
        ReleaseObservation {
            phase: self.phase,
            slo_healthy: self.slo_healthy,
            synthetic_probe_healthy: self.synthetic_probe_healthy,
            regression_detected: !self.slo_healthy || !self.synthetic_probe_healthy,
            evidence_ids: self.evidence_ids,
            sanitized_summary: self.sanitized_summary,
            observed_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReleaseTransitionRequest {
    pub(super) reason: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CompleteRollbackRequest {
    pub(super) succeeded: bool,
    pub(super) reason: String,
    pub(super) observation: RecordReleaseObservationRequest,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct ReleaseListQuery {
    pub(super) cluster_id: ClusterId,
    pub(super) status: Option<ReleaseStatus>,
    pub(super) limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ReleasePage {
    pub(super) schema_version: &'static str,
    pub(super) items: Vec<ReleaseWorkflow>,
    pub(super) partial: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ReleaseDetail {
    pub(super) schema_version: &'static str,
    pub(super) workflow: ReleaseWorkflow,
    pub(super) observations: Vec<ReleaseObservation>,
    pub(super) report: Option<ReleaseReport>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ReleasePreparationView {
    pub(super) schema_version: &'static str,
    pub(super) workflow: ReleaseWorkflow,
    pub(super) upgrade_readiness: UpgradeReadinessReport,
    pub(super) simulation: WhatIfSimulation,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ReleaseExecutionView {
    pub(super) schema_version: &'static str,
    pub(super) workflow: ReleaseWorkflow,
    pub(super) execution_id: ExecutionId,
}

#[derive(Clone, Debug)]
pub(super) struct IntegrationDeliveryClaim {
    pub(super) delivery: IntegrationDelivery,
    pub(super) claim_token: Uuid,
    pub(super) adapter_kind: IntegrationAdapterKind,
    pub(super) endpoint: String,
    pub(super) secret_reference: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct AdapterDeliveryReceipt {
    pub(super) external_ticket_key: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct ReleaseEventRecord {
    pub(super) id: Uuid,
    pub(super) release_id: ReleaseId,
    pub(super) correlation_id: CorrelationId,
    pub(super) from_status: Option<ReleaseStatus>,
    pub(super) to_status: ReleaseStatus,
    pub(super) reason_code: String,
    pub(super) actor_subject: String,
    pub(super) details: Value,
    pub(super) occurred_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(super) struct ExternalApprovalRecord {
    pub(super) target_id: IntegrationTargetId,
    pub(super) input: ExternalApprovalInput,
    pub(super) approval: ApprovalRecord,
    pub(super) received_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(super) struct ReleaseScope {
    pub(super) tenant_id: TenantId,
    pub(super) cluster_id: ClusterId,
    pub(super) incident_id: IncidentId,
}
