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
use rocketmq_sre_contracts::DrExercise;
use rocketmq_sre_contracts::DrExerciseId;
use rocketmq_sre_contracts::DrExerciseState;
use rocketmq_sre_contracts::DrFinding;
use rocketmq_sre_contracts::RecoveryCheckpoint;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;

use super::DrRepository;
use super::support::action_item_from_row;
use super::support::action_item_status_name;
use super::support::checkpoint_from_row;
use super::support::checkpoint_status_name;
use super::support::execution_boundary_name;
use super::support::exercise_from_row;
use super::support::exercise_mode_name;
use super::support::exercise_state_name;
use super::support::finding_from_row;
use super::support::finding_severity_name;
use crate::ControlPlaneError;
use crate::dr::model::DrActionItemQuery;
use crate::dr::model::DrExerciseQuery;
use crate::dr::model::bounded_limit;

impl DrRepository {
    pub(super) async fn create_exercise(
        &self,
        exercise: &DrExercise,
    ) -> Result<DrExercise, ControlPlaneError> {
        let row = sqlx::query(
            "INSERT INTO dr_exercises (
                id, plan_id, tenant_id, region_id, cluster_id, exercise_mode,
                execution_boundary, exercise_state, target_rto_seconds,
                target_rpo_seconds, actual_rto_seconds, actual_rpo_seconds,
                manual_checkpoint_count, cleanup_complete, evidence_ids,
                created_by, started_at, completed_at, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, NULL,
                0, FALSE, $11, $12, NULL, NULL, $13, $13
             )
             RETURNING *",
        )
        .bind(exercise.id.as_uuid())
        .bind(exercise.plan_id.as_uuid())
        .bind(exercise.tenant_id.as_uuid())
        .bind(exercise.region_id.as_uuid())
        .bind(exercise.cluster_id.map(ClusterId::as_uuid))
        .bind(exercise_mode_name(exercise.mode))
        .bind(execution_boundary_name(exercise.boundary))
        .bind(exercise_state_name(exercise.state))
        .bind(i64_value(exercise.target.rto_seconds, "target RTO")?)
        .bind(i64_value(exercise.target.rpo_seconds, "target RPO")?)
        .bind(exercise.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(&exercise.created_by)
        .bind(exercise.created_at)
        .fetch_one(&self.pool)
        .await?;
        exercise_from_row(&row)
    }

    pub(super) async fn get_exercise(
        &self,
        tenant_id: TenantId,
        id: DrExerciseId,
    ) -> Result<DrExercise, ControlPlaneError> {
        let row = sqlx::query("SELECT * FROM dr_exercises WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id.as_uuid())
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
        exercise_from_row(&row)
    }

    pub(super) async fn list_exercises(
        &self,
        tenant_id: TenantId,
        query: &DrExerciseQuery,
    ) -> Result<(Vec<DrExercise>, bool), ControlPlaneError> {
        let limit = bounded_limit(query.limit);
        let state = query.state.map(exercise_state_name);
        let rows = sqlx::query(
            "SELECT *
             FROM dr_exercises
             WHERE tenant_id = $1
               AND ($2::UUID IS NULL OR cluster_id = $2)
               AND ($3::TEXT IS NULL OR exercise_state = $3)
             ORDER BY created_at DESC, id
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.cluster_id.map(ClusterId::as_uuid))
        .bind(state)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        rows.into_iter()
            .take(usize::try_from(limit).unwrap_or(200))
            .map(|row| exercise_from_row(&row))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| (items, truncated))
    }

    pub(super) async fn transition_exercise(
        &self,
        current: &DrExercise,
        next_state: DrExerciseState,
        actual_rto_seconds: Option<u64>,
        actual_rpo_seconds: Option<u64>,
        evidence_ids: &[rocketmq_sre_contracts::EvidenceId],
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<DrExercise, ControlPlaneError> {
        let started_at = (next_state == DrExerciseState::Running && current.started_at.is_none()).then_some(now);
        let completed_at = next_state.is_terminal().then_some(now);
        let row = sqlx::query(
            "UPDATE dr_exercises exercise
             SET exercise_state = $4,
                 actual_rto_seconds = $5,
                 actual_rpo_seconds = $6,
                 evidence_ids = $7,
                 started_at = COALESCE(exercise.started_at, $8),
                 completed_at = $9,
                 manual_checkpoint_count = (
                     SELECT COUNT(*)::INTEGER
                     FROM dr_recovery_checkpoints checkpoint
                     WHERE checkpoint.exercise_id = exercise.id
                       AND checkpoint.manual_confirmation_required
                 ),
                 cleanup_complete = NOT EXISTS (
                     SELECT 1
                     FROM dr_recovery_checkpoints checkpoint
                     WHERE checkpoint.exercise_id = exercise.id
                       AND checkpoint.cleanup_required
                       AND NOT checkpoint.cleanup_complete
                 ),
                 updated_at = $10
             WHERE exercise.tenant_id = $1
               AND exercise.id = $2
               AND exercise.exercise_state = $3
             RETURNING exercise.*",
        )
        .bind(current.tenant_id.as_uuid())
        .bind(current.id.as_uuid())
        .bind(exercise_state_name(current.state))
        .bind(exercise_state_name(next_state))
        .bind(optional_i64(actual_rto_seconds, "actual RTO")?)
        .bind(optional_i64(actual_rpo_seconds, "actual RPO")?)
        .bind(evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(started_at)
        .bind(completed_at)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "dr_exercise_state_conflict",
                "exercise state changed before the transition was persisted",
            )
        })?;
        exercise_from_row(&row)
    }

    pub(super) async fn record_checkpoint(
        &self,
        checkpoint: &RecoveryCheckpoint,
    ) -> Result<RecoveryCheckpoint, ControlPlaneError> {
        let row = sqlx::query(
            "INSERT INTO dr_recovery_checkpoints (
                id, exercise_id, sequence_number, checkpoint_key, title,
                checkpoint_status, expected_duration_seconds,
                actual_duration_seconds, observed_rpo_seconds,
                manual_confirmation_required, confirmed_by, cleanup_required,
                cleanup_complete, evidence_ids, finding_codes, note,
                started_at, completed_at, observed_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17, $18, $19
             )
             RETURNING *",
        )
        .bind(checkpoint.id.as_uuid())
        .bind(checkpoint.exercise_id.as_uuid())
        .bind(i32::try_from(checkpoint.sequence).map_err(|_| {
            ControlPlaneError::validation("invalid_recovery_checkpoint", "checkpoint sequence exceeds the supported range")
        })?)
        .bind(&checkpoint.key)
        .bind(&checkpoint.title)
        .bind(checkpoint_status_name(checkpoint.status))
        .bind(i64_value(checkpoint.expected_duration_seconds, "expected checkpoint duration")?)
        .bind(optional_i64(checkpoint.actual_duration_seconds, "actual checkpoint duration")?)
        .bind(optional_i64(checkpoint.observed_rpo_seconds, "observed RPO")?)
        .bind(checkpoint.manual_confirmation_required)
        .bind(&checkpoint.confirmed_by)
        .bind(checkpoint.cleanup_required)
        .bind(checkpoint.cleanup_complete)
        .bind(checkpoint.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(&checkpoint.finding_codes)
        .bind(&checkpoint.note)
        .bind(checkpoint.started_at)
        .bind(checkpoint.completed_at)
        .bind(checkpoint.observed_at)
        .fetch_one(&self.pool)
        .await?;
        checkpoint_from_row(&row)
    }

    pub(super) async fn list_checkpoints(
        &self,
        tenant_id: TenantId,
        exercise_id: DrExerciseId,
    ) -> Result<Vec<RecoveryCheckpoint>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT checkpoint.*
             FROM dr_recovery_checkpoints checkpoint
             JOIN dr_exercises exercise ON exercise.id = checkpoint.exercise_id
             WHERE exercise.tenant_id = $1 AND checkpoint.exercise_id = $2
             ORDER BY checkpoint.sequence_number, checkpoint.observed_at",
        )
        .bind(tenant_id.as_uuid())
        .bind(exercise_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(|row| checkpoint_from_row(&row)).collect()
    }

    pub(super) async fn find_finding_by_code(
        &self,
        tenant_id: TenantId,
        exercise_id: DrExerciseId,
        code: &str,
    ) -> Result<Option<DrFinding>, ControlPlaneError> {
        sqlx::query(
            "SELECT finding.*, action.id AS action_item_id
             FROM dr_findings finding
             JOIN dr_action_items action ON action.finding_id = finding.id
             WHERE finding.tenant_id = $1
               AND finding.exercise_id = $2
               AND finding.finding_code = $3",
        )
        .bind(tenant_id.as_uuid())
        .bind(exercise_id.as_uuid())
        .bind(code)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| finding_from_row(&row))
        .transpose()
    }

    pub(super) async fn create_finding_and_action(
        &self,
        finding: &DrFinding,
        action: &DrActionItem,
    ) -> Result<(DrFinding, DrActionItem), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO dr_findings (
                id, exercise_id, tenant_id, cluster_id, finding_code, severity,
                summary, remediation, evidence_ids, finding_status, created_at, resolved_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, 'open', $10, NULL
             )",
        )
        .bind(finding.id.as_uuid())
        .bind(finding.exercise_id.as_uuid())
        .bind(finding.tenant_id.as_uuid())
        .bind(finding.cluster_id.map(ClusterId::as_uuid))
        .bind(&finding.code)
        .bind(finding_severity_name(finding.severity))
        .bind(&finding.summary)
        .bind(&finding.remediation)
        .bind(finding.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(finding.created_at)
        .execute(&mut *transaction)
        .await?;
        let action_row = sqlx::query(
            "INSERT INTO dr_action_items (
                id, finding_id, tenant_id, cluster_id, title, owner_name,
                due_at, action_status, verification, evidence_ids,
                created_at, updated_at, completed_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, 'open', NULL, $8, $9, $9, NULL
             )
             RETURNING *",
        )
        .bind(action.id.as_uuid())
        .bind(action.finding_id.as_uuid())
        .bind(action.tenant_id.as_uuid())
        .bind(action.cluster_id.map(ClusterId::as_uuid))
        .bind(&action.title)
        .bind(&action.owner)
        .bind(action.due_at)
        .bind(action.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(action.created_at)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok((finding.clone(), action_item_from_row(&action_row)?))
    }

    pub(super) async fn list_findings(
        &self,
        tenant_id: TenantId,
        exercise_id: DrExerciseId,
    ) -> Result<Vec<DrFinding>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT finding.*, action.id AS action_item_id
             FROM dr_findings finding
             JOIN dr_action_items action ON action.finding_id = finding.id
             WHERE finding.tenant_id = $1 AND finding.exercise_id = $2
             ORDER BY finding.created_at, finding.id",
        )
        .bind(tenant_id.as_uuid())
        .bind(exercise_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(|row| finding_from_row(&row)).collect()
    }

    pub(super) async fn list_action_items(
        &self,
        tenant_id: TenantId,
        query: &DrActionItemQuery,
    ) -> Result<(Vec<DrActionItem>, bool), ControlPlaneError> {
        let limit = bounded_limit(query.limit);
        let status = query.status.map(action_item_status_name);
        let rows = sqlx::query(
            "SELECT *
             FROM dr_action_items
             WHERE tenant_id = $1
               AND ($2::UUID IS NULL OR cluster_id = $2)
               AND ($3::TEXT IS NULL OR action_status = $3)
             ORDER BY updated_at DESC, id
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.cluster_id.map(ClusterId::as_uuid))
        .bind(status)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        rows.into_iter()
            .take(usize::try_from(limit).unwrap_or(200))
            .map(|row| action_item_from_row(&row))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| (items, truncated))
    }

    pub(super) async fn get_action_item(
        &self,
        tenant_id: TenantId,
        id: DrActionItemId,
    ) -> Result<DrActionItem, ControlPlaneError> {
        let row = sqlx::query("SELECT * FROM dr_action_items WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id.as_uuid())
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
        action_item_from_row(&row)
    }

    pub(super) async fn update_action_item(
        &self,
        current: &DrActionItem,
        next: &DrActionItem,
    ) -> Result<DrActionItem, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "UPDATE dr_action_items
             SET owner_name = $4,
                 due_at = $5,
                 action_status = $6,
                 verification = $7,
                 evidence_ids = $8,
                 updated_at = $9,
                 completed_at = $10
             WHERE tenant_id = $1 AND id = $2 AND action_status = $3
             RETURNING *",
        )
        .bind(current.tenant_id.as_uuid())
        .bind(current.id.as_uuid())
        .bind(action_item_status_name(current.status))
        .bind(&next.owner)
        .bind(next.due_at)
        .bind(action_item_status_name(next.status))
        .bind(&next.verification)
        .bind(next.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(next.updated_at)
        .bind(next.completed_at)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "dr_action_item_state_conflict",
                "DR action item changed before the update was persisted",
            )
        })?;
        if next.status == ActionItemStatus::Completed {
            sqlx::query(
                "UPDATE dr_findings
                 SET finding_status = 'resolved', resolved_at = $3
                 WHERE tenant_id = $1 AND id = $2",
            )
            .bind(next.tenant_id.as_uuid())
            .bind(next.finding_id.as_uuid())
            .bind(next.updated_at)
            .execute(&mut *transaction)
            .await?;
        } else if current.status == ActionItemStatus::Completed {
            sqlx::query(
                "UPDATE dr_findings
                 SET finding_status = 'open', resolved_at = NULL
                 WHERE tenant_id = $1 AND id = $2",
            )
            .bind(next.tenant_id.as_uuid())
            .bind(next.finding_id.as_uuid())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        action_item_from_row(&row)
    }

    pub(super) async fn unresolved_finding_count(
        &self,
        tenant_id: TenantId,
        exercise_id: DrExerciseId,
    ) -> Result<u64, ControlPlaneError> {
        let count = sqlx::query(
            "SELECT COUNT(*) AS count
             FROM dr_findings
             WHERE tenant_id = $1 AND exercise_id = $2 AND finding_status <> 'resolved'",
        )
        .bind(tenant_id.as_uuid())
        .bind(exercise_id.as_uuid())
        .fetch_one(&self.pool)
        .await?
        .try_get::<i64, _>("count")?;
        u64::try_from(count).map_err(|_| {
            ControlPlaneError::validation("invalid_persisted_dr_state", "unresolved finding count is negative")
        })
    }
}

fn i64_value(value: u64, name: &str) -> Result<i64, ControlPlaneError> {
    i64::try_from(value).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_dr_measurement",
            format!("{name} exceeds the supported range"),
        )
    })
}

fn optional_i64(value: Option<u64>, name: &str) -> Result<Option<i64>, ControlPlaneError> {
    value.map(|number| i64_value(number, name)).transpose()
}
