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
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ActionItemStatus;
use crate::ClusterId;
use crate::DrActionItemId;
use crate::DrBackupAssetId;
use crate::DrExerciseId;
use crate::DrFindingId;
use crate::DrPlanId;
use crate::EvidenceId;
use crate::FleetId;
use crate::RecoveryCheckpointId;
use crate::RegionId;
use crate::TenantId;

pub const DR_SCHEMA_VERSION: &str = "rocketmq-sre.dr.v1";

/// Recovery subject covered by a plan.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrSubject {
    SreControlPlane,
    RocketMqCluster,
}

/// Deliberately bounded exercise mode. Production cutover is not representable.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrExerciseMode {
    Readiness,
    Tabletop,
    SupervisedTest,
}

/// Runtime boundary enforced for an exercise.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrExecutionBoundary {
    ReadOnly,
    TestClusterSupervised,
}

/// Recovery-point and recovery-time objectives.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RtoRpoTarget {
    pub rto_seconds: u64,
    pub rpo_seconds: u64,
}

/// Backup or rebuild surface tracked by DR Center.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrBackupAssetKind {
    PostgreSql,
    ObjectStorage,
    OidcConfiguration,
    SecretReferences,
    PolicyBundle,
    ObservabilityBackend,
    ControlPlaneRuntime,
    ConnectorRuntime,
    ExecutorRuntime,
    ExecutionAgentRuntime,
    OutboxLedger,
    EffectLedger,
    QuarantineLedger,
    AuditLedger,
    RocketMqRoute,
    RocketMqController,
    RocketMqBrokerHa,
    RocketMqStore,
    RocketMqRocksDb,
    RocketMqTieredStore,
    KubernetesStorage,
}

/// Inventory record for a backup, restore, or deterministic rebuild surface.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DrBackupAsset {
    pub id: DrBackupAssetId,
    pub plan_id: DrPlanId,
    pub kind: DrBackupAssetKind,
    pub owner: String,
    pub access_owner: String,
    pub backup_locator_digest: String,
    pub encrypted: bool,
    pub last_backup_at: Option<DateTime<Utc>>,
    pub restore_verified_at: Option<DateTime<Utc>>,
    pub evidence_ids: Vec<EvidenceId>,
    pub updated_at: DateTime<Utc>,
}

/// One required checkpoint declared by a versioned plan.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RecoveryCheckpointDefinition {
    pub key: String,
    pub title: String,
    pub expected_duration_seconds: u64,
    pub manual_confirmation_required: bool,
    pub cleanup_required: bool,
    pub required_evidence_kinds: Vec<String>,
}

/// Versioned disaster-recovery plan.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DrPlan {
    pub id: DrPlanId,
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_id: RegionId,
    pub cluster_id: Option<ClusterId>,
    pub subject: DrSubject,
    pub name: String,
    pub version: u32,
    pub owner: String,
    pub target: RtoRpoTarget,
    pub allowed_modes: Vec<DrExerciseMode>,
    pub required_sources: Vec<String>,
    pub checkpoints: Vec<RecoveryCheckpointDefinition>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Exercise lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrExerciseState {
    Planned,
    Running,
    AwaitingManualConfirmation,
    Completed,
    Failed,
    Cancelled,
}

impl DrExerciseState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Planned, Self::Running | Self::Cancelled)
                | (
                    Self::Running,
                    Self::AwaitingManualConfirmation | Self::Completed | Self::Failed | Self::Cancelled
                )
                | (
                    Self::AwaitingManualConfirmation,
                    Self::Running | Self::Completed | Self::Failed | Self::Cancelled
                )
        )
    }
}

/// Durable exercise head.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DrExercise {
    pub id: DrExerciseId,
    pub plan_id: DrPlanId,
    pub tenant_id: TenantId,
    pub region_id: RegionId,
    pub cluster_id: Option<ClusterId>,
    pub mode: DrExerciseMode,
    pub boundary: DrExecutionBoundary,
    pub state: DrExerciseState,
    pub target: RtoRpoTarget,
    pub actual_rto_seconds: Option<u64>,
    pub actual_rpo_seconds: Option<u64>,
    pub manual_checkpoint_count: u32,
    pub cleanup_complete: bool,
    pub evidence_ids: Vec<EvidenceId>,
    pub created_by: String,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Result recorded for one recovery checkpoint.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryCheckpointStatus {
    Pending,
    Running,
    Passed,
    Failed,
    ManualConfirmationRequired,
    Skipped,
}

impl RecoveryCheckpointStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Passed | Self::Failed | Self::Skipped)
    }
}

/// Append-only observation for a recovery checkpoint.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RecoveryCheckpoint {
    pub id: RecoveryCheckpointId,
    pub exercise_id: DrExerciseId,
    pub sequence: u32,
    pub key: String,
    pub title: String,
    pub status: RecoveryCheckpointStatus,
    pub expected_duration_seconds: u64,
    pub actual_duration_seconds: Option<u64>,
    pub observed_rpo_seconds: Option<u64>,
    pub manual_confirmation_required: bool,
    pub confirmed_by: Option<String>,
    pub cleanup_required: bool,
    pub cleanup_complete: bool,
    pub evidence_ids: Vec<EvidenceId>,
    pub finding_codes: Vec<String>,
    pub note: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
}

/// Finding severity used by a DR exercise.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrFindingSeverity {
    Info,
    Warning,
    Blocker,
}

/// Lifecycle for a DR finding.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrFindingStatus {
    Open,
    Accepted,
    Resolved,
}

/// Evidence-backed recovery finding.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DrFinding {
    pub id: DrFindingId,
    pub exercise_id: DrExerciseId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub code: String,
    pub severity: DrFindingSeverity,
    pub summary: String,
    pub remediation: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub status: DrFindingStatus,
    pub action_item_id: DrActionItemId,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Follow-up item created transactionally from every DR finding.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DrActionItem {
    pub id: DrActionItemId,
    pub finding_id: DrFindingId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub title: String,
    pub owner: Option<String>,
    pub due_at: Option<DateTime<Utc>>,
    pub status: ActionItemStatus,
    pub verification: Option<String>,
    pub evidence_ids: Vec<EvidenceId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_exercise_cannot_return_to_running() {
        assert!(!DrExerciseState::Completed.can_transition_to(DrExerciseState::Running));
        assert!(DrExerciseState::AwaitingManualConfirmation.can_transition_to(DrExerciseState::Running));
    }

    #[test]
    fn exercise_modes_do_not_include_production_cutover() {
        let modes = [
            DrExerciseMode::Readiness,
            DrExerciseMode::Tabletop,
            DrExerciseMode::SupervisedTest,
        ];
        let encoded = serde_json::to_string(&modes).expect("exercise modes should encode");
        assert!(!encoded.contains("production"));
        assert!(!encoded.contains("cutover"));
    }
}
