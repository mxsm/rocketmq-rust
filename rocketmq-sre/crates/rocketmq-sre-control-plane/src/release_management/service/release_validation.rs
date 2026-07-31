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
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ApprovalDecision;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::DescriptorVersion;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::ReadinessStatus;
use rocketmq_sre_contracts::ReleaseObservationPhase;
use rocketmq_sre_contracts::ReleaseReadinessSnapshot;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookStepBody;
use rocketmq_sre_contracts::SimulationStatus;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::UpgradeReadinessReport;
use rocketmq_sre_contracts::WhatIfSimulation;
use rocketmq_sre_contracts::is_sha256_digest;

use super::support::reject_sensitive;
use super::support::validate_bounded_text;
use crate::ControlPlaneError;
use crate::release_management::model::CreateReleaseRequest;
use crate::release_management::model::PrepareReleaseRequest;
use crate::supervised_execution::ActionPlanView;

pub(super) const DEFAULT_RELEASE_PAGE_SIZE: u32 = 50;
pub(super) const MAX_RELEASE_PAGE_SIZE: u32 = 200;

pub(super) fn validate_create_release(request: &CreateReleaseRequest) -> Result<(), ControlPlaneError> {
    validate_bounded_text("change id", &request.change_id, 256)?;
    validate_bounded_text("release reference", &request.release_ref, 256)?;
    validate_bounded_text("target version", &request.target_version, 128)?;
    validate_bounded_text("runbook version", &request.runbook_version, 64)?;
    reject_sensitive(&request.change_id)?;
    reject_sensitive(&request.release_ref)?;
    reject_sensitive(&request.target_version)?;
    DescriptorVersion::parse(&request.runbook_version).map_err(|error| {
        ControlPlaneError::validation(
            "runbook_version_invalid",
            format!("runbook version must be semantic: {error}"),
        )
    })?;
    if !is_sha256_digest(&request.plan_hash) {
        return Err(ControlPlaneError::validation(
            "plan_hash_invalid",
            "release plan hash must be a SHA-256 digest",
        ));
    }
    match (&request.rollback_plan_id, &request.rollback_plan_hash) {
        (None, None) => {}
        (Some(rollback_id), Some(rollback_hash))
            if *rollback_id != request.plan_id && is_sha256_digest(rollback_hash) => {}
        _ => {
            return Err(ControlPlaneError::validation(
                "rollback_plan_invalid",
                "rollback plan identity and digest must be present together and differ from the primary plan",
            ));
        }
    }
    Ok(())
}

pub(super) fn require_approved_release_plan(
    view: &ActionPlanView,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    expected_hash: &str,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    let plan = &view.plan;
    if plan.tenant_id != tenant_id || plan.cluster_id != cluster_id || plan.incident_id != incident_id {
        return Err(ControlPlaneError::forbidden(
            "release_plan_scope_mismatch",
            "release plan does not match the authenticated tenant, cluster, and incident",
        ));
    }
    if plan.plan_hash != expected_hash {
        return Err(ControlPlaneError::conflict_code(
            "plan_hash_mismatch",
            "release does not bind the current immutable plan hash",
        ));
    }
    if plan.status != PlanStatus::Approved || plan.expires_at <= now {
        return Err(ControlPlaneError::conflict_code(
            "approval_required",
            "release plan must have a current human approval",
        ));
    }
    let approval = view.latest_approval.as_ref().ok_or_else(|| {
        ControlPlaneError::conflict_code("approval_required", "release plan has no persisted approval")
    })?;
    if approval.plan_id != plan.id
        || approval.plan_hash != plan.plan_hash
        || approval.decision != ApprovalDecision::Approved
        || approval.expires_at <= now
    {
        return Err(ControlPlaneError::conflict_code(
            "approval_invalidated",
            "release plan approval is expired or no longer matches the plan",
        ));
    }
    validate_release_actions(plan)
}

pub(super) fn validate_release_runbook(
    runbook: &RunbookDefinition,
    plan: &ActionPlan,
) -> Result<(), ControlPlaneError> {
    if runbook.max_parallelism != 1 {
        return Err(ControlPlaneError::validation(
            "release_runbook_invalid",
            "release runbook must execute one mutation step at a time",
        ));
    }
    let actions = runbook
        .steps
        .iter()
        .filter_map(|step| match &step.body {
            RunbookStepBody::Action {
                action,
                descriptor_version,
                ..
            } => Some((*action, descriptor_version.as_str())),
            RunbookStepBody::ManualGate { .. } => None,
        })
        .collect::<Vec<_>>();
    if actions.len() != plan.steps.len()
        || plan
            .steps
            .iter()
            .zip(actions)
            .any(|(step, action)| step.action != action.0 || step.descriptor_version != action.1)
    {
        return Err(ControlPlaneError::conflict_code(
            "release_runbook_plan_mismatch",
            "runbook action sequence does not match the approved release plan",
        ));
    }
    Ok(())
}

pub(super) fn build_readiness_snapshot(
    request: &PrepareReleaseRequest,
    readiness: &UpgradeReadinessReport,
    simulation: &WhatIfSimulation,
) -> Result<ReleaseReadinessSnapshot, ControlPlaneError> {
    if request.evidence_ids.is_empty() || request.evidence_ids.len() > 64 {
        return Err(ControlPlaneError::validation(
            "release_evidence_invalid",
            "PDB and synthetic probe gates require between 1 and 64 evidence identifiers",
        ));
    }
    let mut evidence_ids = request.evidence_ids.iter().copied().collect::<BTreeSet<_>>();
    for finding in &readiness.findings {
        evidence_ids.extend(finding.evidence_ids.iter().copied());
    }
    evidence_ids.extend(simulation.evidence_ids.iter().copied());
    if evidence_ids.len() > 64 {
        return Err(ControlPlaneError::validation(
            "release_evidence_invalid",
            "combined readiness evidence exceeds the 64-item contract bound",
        ));
    }
    let readiness_complete = readiness.status == ReadinessStatus::Ready;
    let simulation_ready = simulation.status == SimulationStatus::Completed
        && simulation.bottlenecks.is_empty()
        && simulation.missing_assumptions.is_empty();
    let observed_at = readiness.observed_at.max(simulation.created_at);
    if readiness.expires_at <= observed_at {
        return Err(ControlPlaneError::conflict_code(
            "release_readiness_expired",
            "readiness evidence expired before release preparation completed",
        ));
    }
    Ok(ReleaseReadinessSnapshot {
        upgrade_readiness_id: readiness.id,
        simulation_id: simulation.id,
        pdb_ready: request.pdb_ready,
        capacity_ready: readiness_complete
            && simulation_ready
            && finding_absent(readiness, "capacity_runway_acceptable"),
        quorum_ready: readiness_complete && finding_absent(readiness, "quorum_ready"),
        store_recovery_ready: readiness_complete && finding_absent(readiness, "recovery_verified"),
        synthetic_probe_ready: request.synthetic_probe_ready,
        evidence_ids: evidence_ids.into_iter().collect(),
        observed_at,
        valid_until: readiness.expires_at,
    })
}

pub(super) fn validate_observation_phase(
    status: ReleaseStatus,
    phase: ReleaseObservationPhase,
) -> Result<(), ControlPlaneError> {
    let allowed = matches!(
        (status, phase),
        (ReleaseStatus::Ready, ReleaseObservationPhase::Before)
            | (
                ReleaseStatus::CanaryRunning | ReleaseStatus::Verifying,
                ReleaseObservationPhase::During
            )
            | (
                ReleaseStatus::Verifying | ReleaseStatus::RollingBack,
                ReleaseObservationPhase::After
            )
    );
    if !allowed {
        return Err(ControlPlaneError::conflict_code(
            "release_observation_phase_invalid",
            "observation phase does not match the current release state",
        ));
    }
    Ok(())
}

pub(super) fn bounded_release_page_size(limit: Option<u32>) -> u32 {
    limit
        .unwrap_or(DEFAULT_RELEASE_PAGE_SIZE)
        .clamp(1, MAX_RELEASE_PAGE_SIZE)
}

fn validate_release_actions(plan: &ActionPlan) -> Result<(), ControlPlaneError> {
    if plan.steps.is_empty()
        || plan.steps.iter().any(|step| {
            !matches!(
                step.action,
                ExecutionAction::ProxyRolloutImageCanary | ExecutionAction::BrokerRestartOne
            )
        })
    {
        return Err(ControlPlaneError::validation(
            "release_action_unsupported",
            "release escort accepts only Proxy canary or Broker one-by-one actions",
        ));
    }
    Ok(())
}

fn finding_absent(report: &UpgradeReadinessReport, code: &str) -> bool {
    report.findings.iter().all(|finding| !finding.code.starts_with(code))
}
