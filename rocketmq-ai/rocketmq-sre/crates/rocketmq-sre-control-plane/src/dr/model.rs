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
use rocketmq_sre_contracts::ActionItemStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::DrActionItem;
use rocketmq_sre_contracts::DrBackupAsset;
use rocketmq_sre_contracts::DrBackupAssetKind;
use rocketmq_sre_contracts::DrExercise;
use rocketmq_sre_contracts::DrExerciseMode;
use rocketmq_sre_contracts::DrExerciseState;
use rocketmq_sre_contracts::DrFinding;
use rocketmq_sre_contracts::DrFindingSeverity;
use rocketmq_sre_contracts::DrPlan;
use rocketmq_sre_contracts::DrPlanId;
use rocketmq_sre_contracts::DrSubject;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::RecoveryCheckpoint;
use rocketmq_sre_contracts::RecoveryCheckpointDefinition;
use rocketmq_sre_contracts::RecoveryCheckpointStatus;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::RtoRpoTarget;
use serde::Deserialize;
use serde::Serialize;

pub(crate) const DR_API_SCHEMA_VERSION: &str = "rocketmq-sre.dr-api.v1";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateDrPlanRequest {
    pub(crate) fleet_id: FleetId,
    pub(crate) region_id: RegionId,
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) subject: DrSubject,
    pub(crate) name: String,
    pub(crate) version: u32,
    pub(crate) owner: String,
    pub(crate) target: RtoRpoTarget,
    pub(crate) allowed_modes: Vec<DrExerciseMode>,
    #[serde(default)]
    pub(crate) required_sources: Vec<String>,
    pub(crate) checkpoints: Vec<RecoveryCheckpointDefinition>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DrPlanQuery {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) subject: Option<DrSubject>,
    pub(crate) active: Option<bool>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DrPlanPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<DrPlan>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpsertDrBackupAssetRequest {
    pub(crate) kind: DrBackupAssetKind,
    pub(crate) owner: String,
    pub(crate) access_owner: String,
    pub(crate) backup_locator_digest: String,
    pub(crate) encrypted: bool,
    pub(crate) last_backup_at: Option<DateTime<Utc>>,
    pub(crate) restore_verified_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) evidence_ids: Vec<EvidenceId>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DrBackupAssetPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<DrBackupAsset>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartDrExerciseRequest {
    pub(crate) plan_id: DrPlanId,
    pub(crate) mode: DrExerciseMode,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DrExerciseQuery {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) state: Option<DrExerciseState>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DrExercisePage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<DrExercise>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransitionDrExerciseRequest {
    pub(crate) state: DrExerciseState,
    pub(crate) actual_rto_seconds: Option<u64>,
    pub(crate) actual_rpo_seconds: Option<u64>,
    #[serde(default)]
    pub(crate) evidence_ids: Vec<EvidenceId>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordRecoveryCheckpointRequest {
    pub(crate) sequence: u32,
    pub(crate) key: String,
    pub(crate) title: String,
    pub(crate) status: RecoveryCheckpointStatus,
    pub(crate) expected_duration_seconds: u64,
    pub(crate) actual_duration_seconds: Option<u64>,
    pub(crate) observed_rpo_seconds: Option<u64>,
    #[serde(default)]
    pub(crate) manual_confirmation_required: bool,
    pub(crate) confirmed_by: Option<String>,
    #[serde(default)]
    pub(crate) cleanup_required: bool,
    #[serde(default)]
    pub(crate) cleanup_complete: bool,
    #[serde(default)]
    pub(crate) evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub(crate) finding_codes: Vec<String>,
    pub(crate) note: Option<String>,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RecoveryCheckpointPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<RecoveryCheckpoint>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordDrFindingRequest {
    pub(crate) code: String,
    pub(crate) severity: DrFindingSeverity,
    pub(crate) summary: String,
    pub(crate) remediation: String,
    #[serde(default)]
    pub(crate) evidence_ids: Vec<EvidenceId>,
    pub(crate) owner: Option<String>,
    pub(crate) due_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DrFindingPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<DrFinding>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateDrActionItemRequest {
    pub(crate) status: ActionItemStatus,
    pub(crate) owner: Option<String>,
    pub(crate) due_at: Option<DateTime<Utc>>,
    pub(crate) verification: Option<String>,
    #[serde(default)]
    pub(crate) evidence_ids: Vec<EvidenceId>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DrActionItemQuery {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) status: Option<ActionItemStatus>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DrActionItemPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<DrActionItem>,
    pub(crate) truncated: bool,
}

pub(crate) fn bounded_limit(limit: u16) -> i64 {
    i64::from(limit.clamp(1, 200))
}

const fn default_limit() -> u16 {
    100
}
