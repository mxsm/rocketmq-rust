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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ActionPlanId;
use crate::ClusterId;
use crate::CorrelationId;
use crate::EvidenceId;
use crate::ExecutionId;
use crate::FleetId;
use crate::FleetReleaseId;
use crate::IncidentId;
use crate::ReadinessReportId;
use crate::RegionId;
use crate::ReleaseId;
use crate::ReleaseReportId;
use crate::RunbookId;
use crate::SimulationId;
use crate::TenantId;

/// Durable release escort lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseStatus {
    Planned,
    ReadinessChecking,
    Ready,
    CanaryRunning,
    Paused,
    Verifying,
    RollingBack,
    RolledBack,
    Completed,
    ManualTakeover,
    Failed,
}

impl ReleaseStatus {
    /// Returns whether the workflow reached a terminal state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::RolledBack | Self::Completed | Self::ManualTakeover | Self::Failed
        )
    }
}

/// Immutable release gate projection from readiness and what-if checks.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseReadinessSnapshot {
    pub upgrade_readiness_id: ReadinessReportId,
    pub simulation_id: SimulationId,
    pub pdb_ready: bool,
    pub capacity_ready: bool,
    pub quorum_ready: bool,
    pub store_recovery_ready: bool,
    pub synthetic_probe_ready: bool,
    pub evidence_ids: Vec<EvidenceId>,
    pub observed_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
}

impl ReleaseReadinessSnapshot {
    /// Returns whether every deterministic release gate is satisfied.
    #[must_use]
    pub const fn ready(&self) -> bool {
        self.pdb_ready
            && self.capacity_ready
            && self.quorum_ready
            && self.store_recovery_ready
            && self.synthetic_probe_ready
    }
}

/// Phase associated with one SLO/probe observation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseObservationPhase {
    Before,
    During,
    After,
}

/// Bounded observation used for pause, rollback, and release reporting.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseObservation {
    pub phase: ReleaseObservationPhase,
    pub slo_healthy: bool,
    pub synthetic_probe_healthy: bool,
    pub regression_detected: bool,
    pub evidence_ids: Vec<EvidenceId>,
    pub sanitized_summary: String,
    pub observed_at: DateTime<Utc>,
}

/// Persistent release workflow linked to immutable plans and a typed Runbook.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseWorkflow {
    pub schema_version: String,
    pub id: ReleaseId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub correlation_id: CorrelationId,
    pub change_id: String,
    pub release_ref: String,
    pub target_version: String,
    pub runbook_id: RunbookId,
    pub runbook_version: String,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub rollback_plan_id: Option<ActionPlanId>,
    pub rollback_plan_hash: Option<String>,
    pub readiness: Option<ReleaseReadinessSnapshot>,
    pub status: ReleaseStatus,
    pub active_execution_id: Option<ExecutionId>,
    pub regression_detected: bool,
    pub pause_reason: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Immutable before/during/after result of one release workflow.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseReport {
    pub schema_version: String,
    pub id: ReleaseReportId,
    pub release_id: ReleaseId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub change_id: String,
    pub release_ref: String,
    pub final_status: ReleaseStatus,
    pub before: Vec<ReleaseObservation>,
    pub during: Vec<ReleaseObservation>,
    pub after: Vec<ReleaseObservation>,
    pub generated_at: DateTime<Utc>,
}

/// Aggregate lifecycle for a bounded multi-cluster release.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetReleaseStatus {
    Planned,
    ReadinessChecking,
    Ready,
    CanaryRunning,
    BatchRunning,
    Paused,
    Verifying,
    RollingBack,
    RolledBack,
    Completed,
    ManualTakeover,
    Failed,
}

impl FleetReleaseStatus {
    /// Returns whether the Fleet release reached a terminal state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::RolledBack | Self::Completed | Self::ManualTakeover | Self::Failed
        )
    }
}

/// State of one cluster inside a Fleet release.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetReleaseTargetState {
    Pending,
    ReadinessChecking,
    Ready,
    Ineligible,
    CanaryRunning,
    BatchRunning,
    Paused,
    RollingBack,
    RolledBack,
    Completed,
    Skipped,
    Failed,
}

impl FleetReleaseTargetState {
    /// Returns whether the target needs no further scheduling.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Ineligible | Self::RolledBack | Self::Completed | Self::Skipped | Self::Failed
        )
    }
}

/// Deterministic regional batch in a Fleet release.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetReleaseBatch {
    pub sequence: u32,
    pub region_id: RegionId,
    pub cluster_ids: Vec<ClusterId>,
    pub max_concurrency: u32,
    pub canary: bool,
}

/// Durable aggregate that coordinates independent per-cluster release workflows.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetRelease {
    pub schema_version: String,
    pub id: FleetReleaseId,
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub correlation_id: CorrelationId,
    pub release_ref: String,
    pub artifact_digest: String,
    pub target_version: String,
    pub owner: String,
    pub maintenance_window_start: DateTime<Utc>,
    pub maintenance_window_end: DateTime<Utc>,
    pub rollback_artifact_digest: String,
    pub slo_policy_id: String,
    pub status: FleetReleaseStatus,
    pub active_batch: Option<u32>,
    pub batches: Vec<FleetReleaseBatch>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Per-cluster projection linked to an independently approved release workflow.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetReleaseTarget {
    pub fleet_release_id: FleetReleaseId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub region_id: RegionId,
    pub batch_sequence: u32,
    pub canary: bool,
    pub state: FleetReleaseTargetState,
    pub release_id: Option<ReleaseId>,
    pub readiness_reason_codes: Vec<String>,
    pub regression_detected: bool,
    pub sanitized_outcome: Option<String>,
    pub updated_at: DateTime<Utc>,
}

/// Bounded aggregate report for one Fleet release.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetReleaseReport {
    pub schema_version: String,
    pub release: FleetRelease,
    pub targets: Vec<FleetReleaseTarget>,
    pub state_counts: BTreeMap<String, u32>,
    pub skipped_clusters: Vec<ClusterId>,
    pub generated_at: DateTime<Utc>,
}
