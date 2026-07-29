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

use chrono::Utc;
use rocketmq_sre_contracts::ActionItemStatus;
use rocketmq_sre_contracts::DrActionItem;
use rocketmq_sre_contracts::DrActionItemId;
use rocketmq_sre_contracts::DrBackupAsset;
use rocketmq_sre_contracts::DrBackupAssetId;
use rocketmq_sre_contracts::DrExercise;
use rocketmq_sre_contracts::DrExerciseId;
use rocketmq_sre_contracts::DrExerciseMode;
use rocketmq_sre_contracts::DrExerciseState;
use rocketmq_sre_contracts::DrFinding;
use rocketmq_sre_contracts::DrFindingId;
use rocketmq_sre_contracts::DrFindingStatus;
use rocketmq_sre_contracts::DrPlan;
use rocketmq_sre_contracts::DrPlanId;
use rocketmq_sre_contracts::RecoveryCheckpoint;
use rocketmq_sre_contracts::RecoveryCheckpointId;
use rocketmq_sre_contracts::is_sha256_digest;

mod support;

use support::action_item_transition_allowed;
use support::bound_evidence;
use support::require_cluster;
use support::require_operator;
use support::require_read;
use support::validate_checkpoint;
use support::validate_checkpoint_transition;
use support::validate_plan_request;
use support::validate_text;

use super::model::CreateDrPlanRequest;
use super::model::DR_API_SCHEMA_VERSION;
use super::model::DrActionItemPage;
use super::model::DrActionItemQuery;
use super::model::DrBackupAssetPage;
use super::model::DrExercisePage;
use super::model::DrExerciseQuery;
use super::model::DrFindingPage;
use super::model::DrPlanPage;
use super::model::DrPlanQuery;
use super::model::RecordDrFindingRequest;
use super::model::RecordRecoveryCheckpointRequest;
use super::model::RecoveryCheckpointPage;
use super::model::StartDrExerciseRequest;
use super::model::TransitionDrExerciseRequest;
use super::model::UpdateDrActionItemRequest;
use super::model::UpsertDrBackupAssetRequest;
use super::repository::DrRepository;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[derive(Clone)]
pub(crate) struct DrService {
    repository: DrRepository,
}

impl DrService {
    pub(crate) fn new(repository: PostgresRepository) -> Self {
        Self {
            repository: DrRepository::new(repository.pool),
        }
    }

    pub(crate) async fn create_plan(
        &self,
        auth: &AuthContext,
        request: &CreateDrPlanRequest,
    ) -> Result<DrPlan, ControlPlaneError> {
        require_operator(auth)?;
        validate_plan_request(request)?;
        require_cluster(auth, request.cluster_id)?;
        if !self
            .repository
            .scope_exists(auth.tenant_id, request.fleet_id, request.region_id, request.cluster_id)
            .await?
        {
            return Err(ControlPlaneError::forbidden(
                "tenant_mismatch",
                "DR plan scope does not belong to the authenticated tenant",
            ));
        }
        let now = Utc::now();
        self.repository
            .create_plan(&DrPlan {
                id: DrPlanId::new(),
                fleet_id: request.fleet_id,
                tenant_id: auth.tenant_id,
                region_id: request.region_id,
                cluster_id: request.cluster_id,
                subject: request.subject,
                name: request.name.trim().to_owned(),
                version: request.version,
                owner: request.owner.trim().to_owned(),
                target: request.target,
                allowed_modes: request.allowed_modes.clone(),
                required_sources: request.required_sources.clone(),
                checkpoints: request.checkpoints.clone(),
                active: true,
                created_at: now,
                updated_at: now,
            })
            .await
    }

    pub(crate) async fn plans(&self, auth: &AuthContext, query: &DrPlanQuery) -> Result<DrPlanPage, ControlPlaneError> {
        require_read(auth)?;
        require_cluster(auth, query.cluster_id)?;
        let (items, truncated) = self.repository.list_plans(auth.tenant_id, query).await?;
        Ok(DrPlanPage {
            schema_version: DR_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn upsert_backup_asset(
        &self,
        auth: &AuthContext,
        plan_id: DrPlanId,
        request: &UpsertDrBackupAssetRequest,
    ) -> Result<DrBackupAsset, ControlPlaneError> {
        require_operator(auth)?;
        validate_text("backup owner", &request.owner, 256)?;
        validate_text("backup access owner", &request.access_owner, 256)?;
        if !is_sha256_digest(&request.backup_locator_digest) {
            return Err(ControlPlaneError::validation(
                "invalid_backup_locator_digest",
                "backup locator must be represented by a SHA-256 digest",
            ));
        }
        bound_evidence(&request.evidence_ids)?;
        let plan = self.repository.get_plan(auth.tenant_id, plan_id).await?;
        require_cluster(auth, plan.cluster_id)?;
        if matches!(
            request.kind,
            rocketmq_sre_contracts::DrBackupAssetKind::PostgreSql
                | rocketmq_sre_contracts::DrBackupAssetKind::ObjectStorage
                | rocketmq_sre_contracts::DrBackupAssetKind::SecretReferences
                | rocketmq_sre_contracts::DrBackupAssetKind::PolicyBundle
                | rocketmq_sre_contracts::DrBackupAssetKind::EffectLedger
                | rocketmq_sre_contracts::DrBackupAssetKind::AuditLedger
        ) && !request.encrypted
        {
            return Err(ControlPlaneError::validation(
                "unencrypted_recovery_asset",
                "sensitive recovery assets must be encrypted",
            ));
        }
        self.repository
            .upsert_backup_asset(
                auth.tenant_id,
                &DrBackupAsset {
                    id: DrBackupAssetId::new(),
                    plan_id,
                    kind: request.kind,
                    owner: request.owner.trim().to_owned(),
                    access_owner: request.access_owner.trim().to_owned(),
                    backup_locator_digest: request.backup_locator_digest.clone(),
                    encrypted: request.encrypted,
                    last_backup_at: request.last_backup_at,
                    restore_verified_at: request.restore_verified_at,
                    evidence_ids: request.evidence_ids.clone(),
                    updated_at: Utc::now(),
                },
            )
            .await
    }

    pub(crate) async fn backup_assets(
        &self,
        auth: &AuthContext,
        plan_id: DrPlanId,
    ) -> Result<DrBackupAssetPage, ControlPlaneError> {
        require_read(auth)?;
        let plan = self.repository.get_plan(auth.tenant_id, plan_id).await?;
        require_cluster(auth, plan.cluster_id)?;
        Ok(DrBackupAssetPage {
            schema_version: DR_API_SCHEMA_VERSION,
            items: self.repository.list_backup_assets(auth.tenant_id, plan_id).await?,
        })
    }

    pub(crate) async fn create_exercise(
        &self,
        auth: &AuthContext,
        request: &StartDrExerciseRequest,
    ) -> Result<DrExercise, ControlPlaneError> {
        require_operator(auth)?;
        let plan = self.repository.get_plan(auth.tenant_id, request.plan_id).await?;
        require_cluster(auth, plan.cluster_id)?;
        if !plan.active || !plan.allowed_modes.contains(&request.mode) {
            return Err(ControlPlaneError::conflict_code(
                "dr_mode_not_allowed",
                "the active DR plan does not allow this exercise mode",
            ));
        }
        let boundary = match request.mode {
            DrExerciseMode::Readiness | DrExerciseMode::Tabletop => {
                rocketmq_sre_contracts::DrExecutionBoundary::ReadOnly
            }
            DrExerciseMode::SupervisedTest => {
                let cluster_id = plan.cluster_id.ok_or_else(|| {
                    ControlPlaneError::validation(
                        "test_cluster_required",
                        "supervised DR exercises require an explicitly scoped test cluster",
                    )
                })?;
                let environment = self.repository.cluster_environment(auth.tenant_id, cluster_id).await?;
                if environment != "test" {
                    return Err(ControlPlaneError::forbidden(
                        "production_dr_cutover_forbidden",
                        "supervised DR exercises are restricted to registered test clusters",
                    ));
                }
                rocketmq_sre_contracts::DrExecutionBoundary::TestClusterSupervised
            }
        };
        let now = Utc::now();
        self.repository
            .create_exercise(&DrExercise {
                id: DrExerciseId::new(),
                plan_id: plan.id,
                tenant_id: auth.tenant_id,
                region_id: plan.region_id,
                cluster_id: plan.cluster_id,
                mode: request.mode,
                boundary,
                state: DrExerciseState::Planned,
                target: plan.target,
                actual_rto_seconds: None,
                actual_rpo_seconds: None,
                manual_checkpoint_count: 0,
                cleanup_complete: false,
                evidence_ids: Vec::new(),
                created_by: auth.subject.clone(),
                started_at: None,
                completed_at: None,
                created_at: now,
                updated_at: now,
            })
            .await
    }

    pub(crate) async fn exercises(
        &self,
        auth: &AuthContext,
        query: &DrExerciseQuery,
    ) -> Result<DrExercisePage, ControlPlaneError> {
        require_read(auth)?;
        require_cluster(auth, query.cluster_id)?;
        let (items, truncated) = self.repository.list_exercises(auth.tenant_id, query).await?;
        Ok(DrExercisePage {
            schema_version: DR_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn transition_exercise(
        &self,
        auth: &AuthContext,
        exercise_id: DrExerciseId,
        request: &TransitionDrExerciseRequest,
    ) -> Result<DrExercise, ControlPlaneError> {
        require_operator(auth)?;
        bound_evidence(&request.evidence_ids)?;
        let current = self.repository.get_exercise(auth.tenant_id, exercise_id).await?;
        require_cluster(auth, current.cluster_id)?;
        if !current.state.can_transition_to(request.state) {
            return Err(ControlPlaneError::conflict_code(
                "invalid_dr_exercise_transition",
                "the requested DR exercise state transition is not allowed",
            ));
        }
        if request.state == DrExerciseState::Completed {
            self.validate_completion(&current, request).await?;
        }
        self.repository
            .transition_exercise(
                &current,
                request.state,
                request.actual_rto_seconds,
                request.actual_rpo_seconds,
                &request.evidence_ids,
                Utc::now(),
            )
            .await
    }

    async fn validate_completion(
        &self,
        exercise: &DrExercise,
        request: &TransitionDrExerciseRequest,
    ) -> Result<(), ControlPlaneError> {
        if request.actual_rto_seconds.is_none() || request.actual_rpo_seconds.is_none() {
            return Err(ControlPlaneError::validation(
                "dr_measurement_required",
                "completed exercises require actual RTO and RPO measurements",
            ));
        }
        if request.evidence_ids.is_empty() {
            return Err(ControlPlaneError::validation(
                "dr_evidence_required",
                "completed exercises require recovery evidence",
            ));
        }
        let plan = self.repository.get_plan(exercise.tenant_id, exercise.plan_id).await?;
        let observations = self
            .repository
            .list_checkpoints(exercise.tenant_id, exercise.id)
            .await?;
        let mut latest = std::collections::BTreeMap::new();
        for observation in observations {
            latest.insert(observation.sequence, observation);
        }
        for (sequence, definition) in plan.checkpoints.iter().enumerate() {
            let sequence = u32::try_from(sequence).map_err(|_| {
                ControlPlaneError::validation("invalid_dr_plan", "checkpoint sequence exceeds the supported range")
            })?;
            let checkpoint = latest.get(&sequence).ok_or_else(|| {
                ControlPlaneError::conflict_code(
                    "dr_checkpoint_incomplete",
                    format!("checkpoint {} has not been recorded", definition.key),
                )
            })?;
            if !checkpoint.status.is_terminal() {
                return Err(ControlPlaneError::conflict_code(
                    "dr_checkpoint_incomplete",
                    format!("checkpoint {} has not reached a terminal state", definition.key),
                ));
            }
            if checkpoint.cleanup_required && !checkpoint.cleanup_complete {
                return Err(ControlPlaneError::conflict_code(
                    "dr_cleanup_incomplete",
                    format!("checkpoint {} still requires cleanup", definition.key),
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn record_checkpoint(
        &self,
        auth: &AuthContext,
        exercise_id: DrExerciseId,
        request: &RecordRecoveryCheckpointRequest,
    ) -> Result<RecoveryCheckpoint, ControlPlaneError> {
        require_operator(auth)?;
        let exercise = self.repository.get_exercise(auth.tenant_id, exercise_id).await?;
        require_cluster(auth, exercise.cluster_id)?;
        if !matches!(
            exercise.state,
            DrExerciseState::Running | DrExerciseState::AwaitingManualConfirmation
        ) {
            return Err(ControlPlaneError::conflict_code(
                "dr_exercise_not_running",
                "recovery checkpoints can only be recorded for a running exercise",
            ));
        }
        let plan = self.repository.get_plan(auth.tenant_id, exercise.plan_id).await?;
        let definition = plan
            .checkpoints
            .get(usize::try_from(request.sequence).map_err(|_| {
                ControlPlaneError::validation(
                    "invalid_recovery_checkpoint",
                    "checkpoint sequence exceeds the supported range",
                )
            })?)
            .ok_or_else(|| {
                ControlPlaneError::validation(
                    "unknown_recovery_checkpoint",
                    "checkpoint sequence is not declared by the active plan",
                )
            })?;
        validate_checkpoint(request, definition)?;
        let prior = self
            .repository
            .list_checkpoints(auth.tenant_id, exercise_id)
            .await?
            .into_iter()
            .filter(|checkpoint| checkpoint.sequence == request.sequence)
            .next_back();
        if let Some(prior) = prior {
            validate_checkpoint_transition(prior.status, request.status)?;
        }
        self.repository
            .record_checkpoint(&RecoveryCheckpoint {
                id: RecoveryCheckpointId::new(),
                exercise_id,
                sequence: request.sequence,
                key: request.key.trim().to_owned(),
                title: request.title.trim().to_owned(),
                status: request.status,
                expected_duration_seconds: request.expected_duration_seconds,
                actual_duration_seconds: request.actual_duration_seconds,
                observed_rpo_seconds: request.observed_rpo_seconds,
                manual_confirmation_required: request.manual_confirmation_required,
                confirmed_by: request.confirmed_by.clone(),
                cleanup_required: request.cleanup_required,
                cleanup_complete: request.cleanup_complete,
                evidence_ids: request.evidence_ids.clone(),
                finding_codes: request.finding_codes.clone(),
                note: request.note.clone(),
                started_at: request.started_at,
                completed_at: request.completed_at,
                observed_at: Utc::now(),
            })
            .await
    }

    pub(crate) async fn checkpoints(
        &self,
        auth: &AuthContext,
        exercise_id: DrExerciseId,
    ) -> Result<RecoveryCheckpointPage, ControlPlaneError> {
        require_read(auth)?;
        let exercise = self.repository.get_exercise(auth.tenant_id, exercise_id).await?;
        require_cluster(auth, exercise.cluster_id)?;
        Ok(RecoveryCheckpointPage {
            schema_version: DR_API_SCHEMA_VERSION,
            items: self.repository.list_checkpoints(auth.tenant_id, exercise_id).await?,
        })
    }

    pub(crate) async fn record_finding(
        &self,
        auth: &AuthContext,
        exercise_id: DrExerciseId,
        request: &RecordDrFindingRequest,
    ) -> Result<DrFinding, ControlPlaneError> {
        require_operator(auth)?;
        validate_text("finding code", &request.code, 128)?;
        validate_text("finding summary", &request.summary, 1_024)?;
        validate_text("finding remediation", &request.remediation, 2_048)?;
        bound_evidence(&request.evidence_ids)?;
        let exercise = self.repository.get_exercise(auth.tenant_id, exercise_id).await?;
        require_cluster(auth, exercise.cluster_id)?;
        if let Some(existing) = self
            .repository
            .find_finding_by_code(auth.tenant_id, exercise_id, request.code.trim())
            .await?
        {
            if existing.severity == request.severity
                && existing.summary == request.summary.trim()
                && existing.remediation == request.remediation.trim()
                && existing.evidence_ids == request.evidence_ids
            {
                return Ok(existing);
            }
            return Err(ControlPlaneError::conflict_code(
                "dr_finding_idempotency_conflict",
                "finding code was already used with different content",
            ));
        }
        let now = Utc::now();
        let finding_id = DrFindingId::new();
        let action_item_id = DrActionItemId::new();
        let finding = DrFinding {
            id: finding_id,
            exercise_id,
            tenant_id: auth.tenant_id,
            cluster_id: exercise.cluster_id,
            code: request.code.trim().to_owned(),
            severity: request.severity,
            summary: request.summary.trim().to_owned(),
            remediation: request.remediation.trim().to_owned(),
            evidence_ids: request.evidence_ids.clone(),
            status: DrFindingStatus::Open,
            action_item_id,
            created_at: now,
            resolved_at: None,
        };
        self.repository
            .create_finding_and_action(
                &finding,
                &DrActionItem {
                    id: action_item_id,
                    finding_id,
                    tenant_id: auth.tenant_id,
                    cluster_id: exercise.cluster_id,
                    title: format!("DR follow-up: {}", request.summary.trim()),
                    owner: request.owner.clone(),
                    due_at: request.due_at,
                    status: ActionItemStatus::Open,
                    verification: None,
                    evidence_ids: request.evidence_ids.clone(),
                    created_at: now,
                    updated_at: now,
                    completed_at: None,
                },
            )
            .await
            .map(|(finding, _)| finding)
    }

    pub(crate) async fn findings(
        &self,
        auth: &AuthContext,
        exercise_id: DrExerciseId,
    ) -> Result<DrFindingPage, ControlPlaneError> {
        require_read(auth)?;
        let exercise = self.repository.get_exercise(auth.tenant_id, exercise_id).await?;
        require_cluster(auth, exercise.cluster_id)?;
        Ok(DrFindingPage {
            schema_version: DR_API_SCHEMA_VERSION,
            items: self.repository.list_findings(auth.tenant_id, exercise_id).await?,
        })
    }

    pub(crate) async fn action_items(
        &self,
        auth: &AuthContext,
        query: &DrActionItemQuery,
    ) -> Result<DrActionItemPage, ControlPlaneError> {
        require_read(auth)?;
        require_cluster(auth, query.cluster_id)?;
        let (items, truncated) = self.repository.list_action_items(auth.tenant_id, query).await?;
        Ok(DrActionItemPage {
            schema_version: DR_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn update_action_item(
        &self,
        auth: &AuthContext,
        id: DrActionItemId,
        request: &UpdateDrActionItemRequest,
    ) -> Result<DrActionItem, ControlPlaneError> {
        require_operator(auth)?;
        bound_evidence(&request.evidence_ids)?;
        if request.status == ActionItemStatus::Completed
            && request.verification.as_deref().is_none_or(str::is_empty)
            && request.evidence_ids.is_empty()
        {
            return Err(ControlPlaneError::validation(
                "dr_action_verification_required",
                "completed DR action items require verification or Evidence",
            ));
        }
        let current = self.repository.get_action_item(auth.tenant_id, id).await?;
        require_cluster(auth, current.cluster_id)?;
        if !action_item_transition_allowed(current.status, request.status) {
            return Err(ControlPlaneError::conflict_code(
                "invalid_dr_action_item_transition",
                "the requested DR action item transition is not allowed",
            ));
        }
        let now = Utc::now();
        let mut next = current.clone();
        next.status = request.status;
        next.owner = request.owner.clone();
        next.due_at = request.due_at;
        next.verification = request.verification.clone();
        next.evidence_ids = request.evidence_ids.clone();
        next.updated_at = now;
        next.completed_at = (request.status == ActionItemStatus::Completed).then_some(now);
        self.repository.update_action_item(&current, &next).await
    }
}
