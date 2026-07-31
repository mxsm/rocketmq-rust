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

use rocketmq_sre_contracts::ActionItemStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::DrActionItem;
use rocketmq_sre_contracts::DrActionItemId;
use rocketmq_sre_contracts::DrBackupAsset;
use rocketmq_sre_contracts::DrBackupAssetId;
use rocketmq_sre_contracts::DrBackupAssetKind;
use rocketmq_sre_contracts::DrExecutionBoundary;
use rocketmq_sre_contracts::DrExercise;
use rocketmq_sre_contracts::DrExerciseId;
use rocketmq_sre_contracts::DrExerciseMode;
use rocketmq_sre_contracts::DrExerciseState;
use rocketmq_sre_contracts::DrFinding;
use rocketmq_sre_contracts::DrFindingId;
use rocketmq_sre_contracts::DrFindingSeverity;
use rocketmq_sre_contracts::DrFindingStatus;
use rocketmq_sre_contracts::DrPlan;
use rocketmq_sre_contracts::DrPlanId;
use rocketmq_sre_contracts::DrSubject;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::RecoveryCheckpoint;
use rocketmq_sre_contracts::RecoveryCheckpointId;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::RtoRpoTarget;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::ControlPlaneError;

pub(super) fn plan_from_row(row: &PgRow) -> Result<DrPlan, ControlPlaneError> {
    let modes = row
        .try_get::<Vec<String>, _>("allowed_modes")?
        .into_iter()
        .map(|value| exercise_mode(&value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DrPlan {
        id: DrPlanId::from_uuid(row.try_get("id")?),
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        region_id: RegionId::from_uuid(row.try_get("region_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        subject: subject(row.try_get("subject")?)?,
        name: row.try_get("name")?,
        version: u32_value(row.try_get("plan_version")?, "plan version")?,
        owner: row.try_get("owner_name")?,
        target: RtoRpoTarget {
            rto_seconds: u64_value(row.try_get("rto_seconds")?, "RTO")?,
            rpo_seconds: u64_value(row.try_get("rpo_seconds")?, "RPO")?,
        },
        allowed_modes: modes,
        required_sources: row.try_get("required_sources")?,
        checkpoints: serde_json::from_value(row.try_get("checkpoint_definitions")?).map_err(|_| {
            ControlPlaneError::validation("invalid_persisted_dr_plan", "checkpoint definitions are invalid")
        })?,
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn backup_asset_from_row(row: &PgRow) -> Result<DrBackupAsset, ControlPlaneError> {
    Ok(DrBackupAsset {
        id: DrBackupAssetId::from_uuid(row.try_get("id")?),
        plan_id: DrPlanId::from_uuid(row.try_get("plan_id")?),
        kind: backup_kind(row.try_get("asset_kind")?)?,
        owner: row.try_get("owner_name")?,
        access_owner: row.try_get("access_owner")?,
        backup_locator_digest: row.try_get("backup_locator_digest")?,
        encrypted: row.try_get("encrypted")?,
        last_backup_at: row.try_get("last_backup_at")?,
        restore_verified_at: row.try_get("restore_verified_at")?,
        evidence_ids: evidence_ids(row)?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn exercise_from_row(row: &PgRow) -> Result<DrExercise, ControlPlaneError> {
    Ok(DrExercise {
        id: DrExerciseId::from_uuid(row.try_get("id")?),
        plan_id: DrPlanId::from_uuid(row.try_get("plan_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        region_id: RegionId::from_uuid(row.try_get("region_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        mode: exercise_mode(row.try_get("exercise_mode")?)?,
        boundary: execution_boundary(row.try_get("execution_boundary")?)?,
        state: exercise_state(row.try_get("exercise_state")?)?,
        target: RtoRpoTarget {
            rto_seconds: u64_value(row.try_get("target_rto_seconds")?, "target RTO")?,
            rpo_seconds: u64_value(row.try_get("target_rpo_seconds")?, "target RPO")?,
        },
        actual_rto_seconds: optional_u64(row.try_get("actual_rto_seconds")?, "actual RTO")?,
        actual_rpo_seconds: optional_u64(row.try_get("actual_rpo_seconds")?, "actual RPO")?,
        manual_checkpoint_count: u32_value(row.try_get("manual_checkpoint_count")?, "manual checkpoint count")?,
        cleanup_complete: row.try_get("cleanup_complete")?,
        evidence_ids: evidence_ids(row)?,
        created_by: row.try_get("created_by")?,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn checkpoint_from_row(row: &PgRow) -> Result<RecoveryCheckpoint, ControlPlaneError> {
    Ok(RecoveryCheckpoint {
        id: RecoveryCheckpointId::from_uuid(row.try_get("id")?),
        exercise_id: DrExerciseId::from_uuid(row.try_get("exercise_id")?),
        sequence: u32_value(row.try_get("sequence_number")?, "checkpoint sequence")?,
        key: row.try_get("checkpoint_key")?,
        title: row.try_get("title")?,
        status: checkpoint_status(row.try_get("checkpoint_status")?)?,
        expected_duration_seconds: u64_value(
            row.try_get("expected_duration_seconds")?,
            "expected checkpoint duration",
        )?,
        actual_duration_seconds: optional_u64(row.try_get("actual_duration_seconds")?, "actual checkpoint duration")?,
        observed_rpo_seconds: optional_u64(row.try_get("observed_rpo_seconds")?, "observed RPO")?,
        manual_confirmation_required: row.try_get("manual_confirmation_required")?,
        confirmed_by: row.try_get("confirmed_by")?,
        cleanup_required: row.try_get("cleanup_required")?,
        cleanup_complete: row.try_get("cleanup_complete")?,
        evidence_ids: evidence_ids(row)?,
        finding_codes: row.try_get("finding_codes")?,
        note: row.try_get("note")?,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        observed_at: row.try_get("observed_at")?,
    })
}

pub(super) fn finding_from_row(row: &PgRow) -> Result<DrFinding, ControlPlaneError> {
    Ok(DrFinding {
        id: DrFindingId::from_uuid(row.try_get("id")?),
        exercise_id: DrExerciseId::from_uuid(row.try_get("exercise_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        code: row.try_get("finding_code")?,
        severity: finding_severity(row.try_get("severity")?)?,
        summary: row.try_get("summary")?,
        remediation: row.try_get("remediation")?,
        evidence_ids: evidence_ids(row)?,
        status: finding_status(row.try_get("finding_status")?)?,
        action_item_id: DrActionItemId::from_uuid(row.try_get("action_item_id")?),
        created_at: row.try_get("created_at")?,
        resolved_at: row.try_get("resolved_at")?,
    })
}

pub(super) fn action_item_from_row(row: &PgRow) -> Result<DrActionItem, ControlPlaneError> {
    Ok(DrActionItem {
        id: DrActionItemId::from_uuid(row.try_get("id")?),
        finding_id: DrFindingId::from_uuid(row.try_get("finding_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        title: row.try_get("title")?,
        owner: row.try_get("owner_name")?,
        due_at: row.try_get("due_at")?,
        status: action_item_status(row.try_get("action_status")?)?,
        verification: row.try_get("verification")?,
        evidence_ids: evidence_ids(row)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn evidence_ids(row: &PgRow) -> Result<Vec<EvidenceId>, ControlPlaneError> {
    Ok(row
        .try_get::<Vec<Uuid>, _>("evidence_ids")?
        .into_iter()
        .map(EvidenceId::from_uuid)
        .collect())
}

pub(super) const fn subject_name(value: DrSubject) -> &'static str {
    match value {
        DrSubject::SreControlPlane => "sre_control_plane",
        DrSubject::RocketMqCluster => "rocket_mq_cluster",
    }
}

pub(super) const fn exercise_mode_name(value: DrExerciseMode) -> &'static str {
    match value {
        DrExerciseMode::Readiness => "readiness",
        DrExerciseMode::Tabletop => "tabletop",
        DrExerciseMode::SupervisedTest => "supervised_test",
    }
}

pub(super) const fn execution_boundary_name(value: DrExecutionBoundary) -> &'static str {
    match value {
        DrExecutionBoundary::ReadOnly => "read_only",
        DrExecutionBoundary::TestClusterSupervised => "test_cluster_supervised",
    }
}

pub(super) const fn exercise_state_name(value: DrExerciseState) -> &'static str {
    match value {
        DrExerciseState::Planned => "planned",
        DrExerciseState::Running => "running",
        DrExerciseState::AwaitingManualConfirmation => "awaiting_manual_confirmation",
        DrExerciseState::Completed => "completed",
        DrExerciseState::Failed => "failed",
        DrExerciseState::Cancelled => "cancelled",
    }
}

pub(super) const fn checkpoint_status_name(value: rocketmq_sre_contracts::RecoveryCheckpointStatus) -> &'static str {
    match value {
        rocketmq_sre_contracts::RecoveryCheckpointStatus::Pending => "pending",
        rocketmq_sre_contracts::RecoveryCheckpointStatus::Running => "running",
        rocketmq_sre_contracts::RecoveryCheckpointStatus::Passed => "passed",
        rocketmq_sre_contracts::RecoveryCheckpointStatus::Failed => "failed",
        rocketmq_sre_contracts::RecoveryCheckpointStatus::ManualConfirmationRequired => "manual_confirmation_required",
        rocketmq_sre_contracts::RecoveryCheckpointStatus::Skipped => "skipped",
    }
}

pub(super) const fn finding_severity_name(value: DrFindingSeverity) -> &'static str {
    match value {
        DrFindingSeverity::Info => "info",
        DrFindingSeverity::Warning => "warning",
        DrFindingSeverity::Blocker => "blocker",
    }
}

pub(super) const fn action_item_status_name(value: ActionItemStatus) -> &'static str {
    match value {
        ActionItemStatus::Open => "open",
        ActionItemStatus::Assigned => "assigned",
        ActionItemStatus::InProgress => "in_progress",
        ActionItemStatus::Blocked => "blocked",
        ActionItemStatus::Completed => "completed",
        ActionItemStatus::Reopened => "reopened",
        ActionItemStatus::Cancelled => "cancelled",
    }
}

pub(super) const fn backup_kind_name(value: DrBackupAssetKind) -> &'static str {
    match value {
        DrBackupAssetKind::PostgreSql => "postgre_sql",
        DrBackupAssetKind::ObjectStorage => "object_storage",
        DrBackupAssetKind::OidcConfiguration => "oidc_configuration",
        DrBackupAssetKind::SecretReferences => "secret_references",
        DrBackupAssetKind::PolicyBundle => "policy_bundle",
        DrBackupAssetKind::ObservabilityBackend => "observability_backend",
        DrBackupAssetKind::ControlPlaneRuntime => "control_plane_runtime",
        DrBackupAssetKind::ConnectorRuntime => "connector_runtime",
        DrBackupAssetKind::ExecutorRuntime => "executor_runtime",
        DrBackupAssetKind::ExecutionAgentRuntime => "execution_agent_runtime",
        DrBackupAssetKind::OutboxLedger => "outbox_ledger",
        DrBackupAssetKind::EffectLedger => "effect_ledger",
        DrBackupAssetKind::QuarantineLedger => "quarantine_ledger",
        DrBackupAssetKind::AuditLedger => "audit_ledger",
        DrBackupAssetKind::RocketMqRoute => "rocket_mq_route",
        DrBackupAssetKind::RocketMqController => "rocket_mq_controller",
        DrBackupAssetKind::RocketMqBrokerHa => "rocket_mq_broker_ha",
        DrBackupAssetKind::RocketMqStore => "rocket_mq_store",
        DrBackupAssetKind::RocketMqRocksDb => "rocket_mq_rocks_db",
        DrBackupAssetKind::RocketMqTieredStore => "rocket_mq_tiered_store",
        DrBackupAssetKind::KubernetesStorage => "kubernetes_storage",
    }
}

fn subject(value: &str) -> Result<DrSubject, ControlPlaneError> {
    match value {
        "sre_control_plane" => Ok(DrSubject::SreControlPlane),
        "rocket_mq_cluster" => Ok(DrSubject::RocketMqCluster),
        _ => invalid("DR subject"),
    }
}

fn exercise_mode(value: &str) -> Result<DrExerciseMode, ControlPlaneError> {
    match value {
        "readiness" => Ok(DrExerciseMode::Readiness),
        "tabletop" => Ok(DrExerciseMode::Tabletop),
        "supervised_test" => Ok(DrExerciseMode::SupervisedTest),
        _ => invalid("DR exercise mode"),
    }
}

fn execution_boundary(value: &str) -> Result<DrExecutionBoundary, ControlPlaneError> {
    match value {
        "read_only" => Ok(DrExecutionBoundary::ReadOnly),
        "test_cluster_supervised" => Ok(DrExecutionBoundary::TestClusterSupervised),
        _ => invalid("DR execution boundary"),
    }
}

fn exercise_state(value: &str) -> Result<DrExerciseState, ControlPlaneError> {
    match value {
        "planned" => Ok(DrExerciseState::Planned),
        "running" => Ok(DrExerciseState::Running),
        "awaiting_manual_confirmation" => Ok(DrExerciseState::AwaitingManualConfirmation),
        "completed" => Ok(DrExerciseState::Completed),
        "failed" => Ok(DrExerciseState::Failed),
        "cancelled" => Ok(DrExerciseState::Cancelled),
        _ => invalid("DR exercise state"),
    }
}

fn checkpoint_status(value: &str) -> Result<rocketmq_sre_contracts::RecoveryCheckpointStatus, ControlPlaneError> {
    match value {
        "pending" => Ok(rocketmq_sre_contracts::RecoveryCheckpointStatus::Pending),
        "running" => Ok(rocketmq_sre_contracts::RecoveryCheckpointStatus::Running),
        "passed" => Ok(rocketmq_sre_contracts::RecoveryCheckpointStatus::Passed),
        "failed" => Ok(rocketmq_sre_contracts::RecoveryCheckpointStatus::Failed),
        "manual_confirmation_required" => {
            Ok(rocketmq_sre_contracts::RecoveryCheckpointStatus::ManualConfirmationRequired)
        }
        "skipped" => Ok(rocketmq_sre_contracts::RecoveryCheckpointStatus::Skipped),
        _ => invalid("recovery checkpoint status"),
    }
}

fn finding_severity(value: &str) -> Result<DrFindingSeverity, ControlPlaneError> {
    match value {
        "info" => Ok(DrFindingSeverity::Info),
        "warning" => Ok(DrFindingSeverity::Warning),
        "blocker" => Ok(DrFindingSeverity::Blocker),
        _ => invalid("DR finding severity"),
    }
}

fn finding_status(value: &str) -> Result<DrFindingStatus, ControlPlaneError> {
    match value {
        "open" => Ok(DrFindingStatus::Open),
        "accepted" => Ok(DrFindingStatus::Accepted),
        "resolved" => Ok(DrFindingStatus::Resolved),
        _ => invalid("DR finding status"),
    }
}

fn action_item_status(value: &str) -> Result<ActionItemStatus, ControlPlaneError> {
    match value {
        "open" => Ok(ActionItemStatus::Open),
        "assigned" => Ok(ActionItemStatus::Assigned),
        "in_progress" => Ok(ActionItemStatus::InProgress),
        "blocked" => Ok(ActionItemStatus::Blocked),
        "completed" => Ok(ActionItemStatus::Completed),
        "reopened" => Ok(ActionItemStatus::Reopened),
        "cancelled" => Ok(ActionItemStatus::Cancelled),
        _ => invalid("DR action item status"),
    }
}

fn backup_kind(value: &str) -> Result<DrBackupAssetKind, ControlPlaneError> {
    match value {
        "postgre_sql" => Ok(DrBackupAssetKind::PostgreSql),
        "object_storage" => Ok(DrBackupAssetKind::ObjectStorage),
        "oidc_configuration" => Ok(DrBackupAssetKind::OidcConfiguration),
        "secret_references" => Ok(DrBackupAssetKind::SecretReferences),
        "policy_bundle" => Ok(DrBackupAssetKind::PolicyBundle),
        "observability_backend" => Ok(DrBackupAssetKind::ObservabilityBackend),
        "control_plane_runtime" => Ok(DrBackupAssetKind::ControlPlaneRuntime),
        "connector_runtime" => Ok(DrBackupAssetKind::ConnectorRuntime),
        "executor_runtime" => Ok(DrBackupAssetKind::ExecutorRuntime),
        "execution_agent_runtime" => Ok(DrBackupAssetKind::ExecutionAgentRuntime),
        "outbox_ledger" => Ok(DrBackupAssetKind::OutboxLedger),
        "effect_ledger" => Ok(DrBackupAssetKind::EffectLedger),
        "quarantine_ledger" => Ok(DrBackupAssetKind::QuarantineLedger),
        "audit_ledger" => Ok(DrBackupAssetKind::AuditLedger),
        "rocket_mq_route" => Ok(DrBackupAssetKind::RocketMqRoute),
        "rocket_mq_controller" => Ok(DrBackupAssetKind::RocketMqController),
        "rocket_mq_broker_ha" => Ok(DrBackupAssetKind::RocketMqBrokerHa),
        "rocket_mq_store" => Ok(DrBackupAssetKind::RocketMqStore),
        "rocket_mq_rocks_db" => Ok(DrBackupAssetKind::RocketMqRocksDb),
        "rocket_mq_tiered_store" => Ok(DrBackupAssetKind::RocketMqTieredStore),
        "kubernetes_storage" => Ok(DrBackupAssetKind::KubernetesStorage),
        _ => invalid("DR backup asset kind"),
    }
}

fn invalid<T>(name: &str) -> Result<T, ControlPlaneError> {
    Err(ControlPlaneError::validation(
        "invalid_persisted_dr_state",
        format!("{name} is not recognized"),
    ))
}

fn u64_value(value: i64, name: &str) -> Result<u64, ControlPlaneError> {
    u64::try_from(value)
        .map_err(|_| ControlPlaneError::validation("invalid_persisted_dr_state", format!("{name} is negative")))
}

fn optional_u64(value: Option<i64>, name: &str) -> Result<Option<u64>, ControlPlaneError> {
    value.map(|number| u64_value(number, name)).transpose()
}

fn u32_value(value: i32, name: &str) -> Result<u32, ControlPlaneError> {
    u32::try_from(value)
        .map_err(|_| ControlPlaneError::validation("invalid_persisted_dr_state", format!("{name} is negative")))
}
