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
use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::AutonomousExecutionFailure;
use rocketmq_sre_contracts::AutonomyLifecycleState;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyOutcomeClass;
use rocketmq_sre_contracts::AutonomyPolicyDefinition;
use rocketmq_sre_contracts::AutonomyPolicyId;
use rocketmq_sre_contracts::AutonomyQualificationCohort;
use rocketmq_sre_contracts::AutonomyQualificationLevel;
use rocketmq_sre_contracts::AutonomyQualificationSample;
use rocketmq_sre_contracts::AutonomySampleKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CriticReviewId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::DynamicSafetyDecision;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_core::AutonomyActor;
use rocketmq_sre_core::AutonomyStateMachine;
use rocketmq_sre_core::PromotionQualification;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use super::model::AutonomyFreezeView;
use super::model::AutonomyKillSwitchView;
use super::model::AutonomyQualificationView;
use super::model::AutonomyScopeView;
use super::model::ShadowOutcomeRecord;
use super::model::ShadowOutcomeView;
use super::model::SupervisedExecutionQualificationFacts;
use crate::ControlPlaneError;
use crate::PostgresRepository;

impl PostgresRepository {
    pub(super) async fn store_autonomy_policy(
        &self,
        mut definition: AutonomyPolicyDefinition,
        actor: &str,
    ) -> Result<(AutonomyPolicyDefinition, AutonomyLifecycleState), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        lock_scope(
            &mut transaction,
            definition.tenant_id,
            definition.cluster_id,
            definition.action,
            &definition.action_version,
        )
        .await?;
        let existing = sqlx::query(
            "SELECT policy_id, policy_definition_version, mode, previous_mode,
                    owner, pause_reason, lifecycle_revision, updated_by, updated_at
             FROM autonomy_lifecycle_states
             WHERE tenant_id = $1 AND cluster_id = $2
               AND action_id = $3 AND action_version = $4
             FOR UPDATE",
        )
        .bind(definition.tenant_id.as_uuid())
        .bind(definition.cluster_id.as_uuid())
        .bind(definition.action.id())
        .bind(&definition.action_version)
        .fetch_optional(&mut *transaction)
        .await?;
        let (policy_id, next_version, current) = match existing {
            Some(row) => {
                let current = lifecycle_from_row(&row, definition.tenant_id, definition.cluster_id, definition.action)?;
                (
                    AutonomyPolicyId::from_uuid(row.try_get("policy_id")?),
                    u64::try_from(row.try_get::<i64, _>("policy_definition_version")?)
                        .map_err(|_| invalid_persisted("autonomy policy version is negative"))?
                        .checked_add(1)
                        .ok_or_else(|| invalid_persisted("autonomy policy version is exhausted"))?,
                    Some(current),
                )
            }
            None => (definition.id, 1, None),
        };
        definition.id = policy_id;
        definition.definition_version = next_version;
        let definition_snapshot = json_value(&definition)?;
        sqlx::query(
            "INSERT INTO autonomy_policy_definitions (
                id, definition_version, tenant_id, cluster_id, action_id,
                action_version, descriptor_digest, diagnostic_pack_id,
                diagnostic_pack_version, owner,
                minimum_evidence_freshness_seconds, required_evidence_sources,
                min_shadow_samples, min_supervised_successes,
                observation_window_days, max_unresolved_unknown,
                max_recent_rollbacks, max_executions_per_hour,
                cooldown_seconds, max_concurrent_executions,
                stable_window_seconds, definition_snapshot, created_by, created_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10,
                $11, $12,
                $13, $14,
                $15, $16,
                $17, $18,
                $19, $20,
                $21, $22, $23, $24
             )",
        )
        .bind(definition.id.as_uuid())
        .bind(i64::try_from(definition.definition_version).map_err(|_| invalid_request("policy version is too large"))?)
        .bind(definition.tenant_id.as_uuid())
        .bind(definition.cluster_id.as_uuid())
        .bind(definition.action.id())
        .bind(&definition.action_version)
        .bind(&definition.descriptor_digest)
        .bind(&definition.diagnostic_pack_id)
        .bind(&definition.diagnostic_pack_version)
        .bind(&definition.owner)
        .bind(
            i64::try_from(definition.minimum_evidence_freshness_seconds)
                .map_err(|_| invalid_request("evidence freshness is too large"))?,
        )
        .bind(&definition.required_evidence_sources)
        .bind(
            i32::try_from(definition.min_shadow_samples)
                .map_err(|_| invalid_request("shadow sample bound is too large"))?,
        )
        .bind(
            i32::try_from(definition.min_supervised_successes)
                .map_err(|_| invalid_request("supervised success bound is too large"))?,
        )
        .bind(i32::from(definition.observation_window_days))
        .bind(
            i32::try_from(definition.max_unresolved_unknown)
                .map_err(|_| invalid_request("unknown bound is too large"))?,
        )
        .bind(
            i32::try_from(definition.max_recent_rollbacks)
                .map_err(|_| invalid_request("rollback bound is too large"))?,
        )
        .bind(i32::from(definition.max_executions_per_hour))
        .bind(i64::try_from(definition.cooldown_seconds).map_err(|_| invalid_request("cooldown is too large"))?)
        .bind(i32::from(definition.max_concurrent_executions))
        .bind(
            i64::try_from(definition.stable_window_seconds)
                .map_err(|_| invalid_request("stable window is too large"))?,
        )
        .bind(definition_snapshot)
        .bind(actor)
        .bind(definition.created_at)
        .execute(&mut *transaction)
        .await?;

        let next = match current {
            Some(mut lifecycle) => {
                lifecycle.owner = definition.owner.clone();
                lifecycle.lifecycle_revision = lifecycle
                    .lifecycle_revision
                    .checked_add(1)
                    .ok_or_else(|| invalid_persisted("autonomy lifecycle revision is exhausted"))?;
                lifecycle.updated_by = actor.to_owned();
                lifecycle.updated_at = definition.created_at;
                let updated = sqlx::query(
                    "UPDATE autonomy_lifecycle_states
                     SET policy_definition_version = $5,
                         owner = $6,
                         lifecycle_revision = $7,
                         updated_by = $8,
                         updated_at = $9
                     WHERE tenant_id = $1 AND cluster_id = $2
                       AND action_id = $3 AND action_version = $4",
                )
                .bind(definition.tenant_id.as_uuid())
                .bind(definition.cluster_id.as_uuid())
                .bind(definition.action.id())
                .bind(&definition.action_version)
                .bind(
                    i64::try_from(definition.definition_version)
                        .map_err(|_| invalid_request("policy version is too large"))?,
                )
                .bind(&definition.owner)
                .bind(
                    i64::try_from(lifecycle.lifecycle_revision)
                        .map_err(|_| invalid_request("lifecycle revision is too large"))?,
                )
                .bind(actor)
                .bind(definition.created_at)
                .execute(&mut *transaction)
                .await?;
                if updated.rows_affected() != 1 {
                    return Err(ControlPlaneError::conflict_code(
                        "autonomy_state_changed",
                        "autonomy lifecycle changed while the policy version was created",
                    ));
                }
                insert_lifecycle_event(
                    &mut transaction,
                    &lifecycle,
                    &definition.action_version,
                    Some(lifecycle.mode),
                    "policy_definition_changed",
                    actor,
                )
                .await?;
                lifecycle
            }
            None => {
                let lifecycle = AutonomyLifecycleState {
                    tenant_id: definition.tenant_id,
                    cluster_id: definition.cluster_id,
                    action: definition.action,
                    mode: AutonomyMode::Disabled,
                    previous_mode: None,
                    owner: definition.owner.clone(),
                    pause_reason: None,
                    lifecycle_revision: 1,
                    updated_by: actor.to_owned(),
                    updated_at: definition.created_at,
                };
                sqlx::query(
                    "INSERT INTO autonomy_lifecycle_states (
                        tenant_id, cluster_id, action_id, action_version,
                        policy_id, policy_definition_version, mode, previous_mode,
                        owner, owner_confirmed_at, pause_reason,
                        lifecycle_revision, updated_by, updated_at
                     ) VALUES (
                        $1, $2, $3, $4,
                        $5, $6, 'disabled', NULL,
                        $7, NULL, NULL,
                        1, $8, $9
                     )",
                )
                .bind(definition.tenant_id.as_uuid())
                .bind(definition.cluster_id.as_uuid())
                .bind(definition.action.id())
                .bind(&definition.action_version)
                .bind(definition.id.as_uuid())
                .bind(
                    i64::try_from(definition.definition_version)
                        .map_err(|_| invalid_request("policy version is too large"))?,
                )
                .bind(&definition.owner)
                .bind(actor)
                .bind(definition.created_at)
                .execute(&mut *transaction)
                .await?;
                insert_lifecycle_event(
                    &mut transaction,
                    &lifecycle,
                    &definition.action_version,
                    None,
                    "policy_definition_created",
                    actor,
                )
                .await?;
                lifecycle
            }
        };
        transaction.commit().await?;
        Ok((definition, next))
    }

    #[cfg(test)]
    pub(super) async fn autonomy_scope(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        action_version: &str,
    ) -> Result<AutonomyScopeView, ControlPlaneError> {
        self.autonomy_scope_at(tenant_id, cluster_id, action, action_version, Utc::now())
            .await
    }

    pub(super) async fn autonomy_scope_at(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        action_version: &str,
        evaluated_at: DateTime<Utc>,
    ) -> Result<AutonomyScopeView, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT state.*, definition.definition_snapshot
             FROM autonomy_lifecycle_states state
             JOIN autonomy_policy_definitions definition
               ON definition.id = state.policy_id
              AND definition.definition_version = state.policy_definition_version
             WHERE state.tenant_id = $1 AND state.cluster_id = $2
               AND state.action_id = $3 AND state.action_version = $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(action.id())
        .bind(action_version)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let policy: AutonomyPolicyDefinition = from_json(row.try_get("definition_snapshot")?)?;
        let lifecycle = lifecycle_from_row(&row, tenant_id, cluster_id, action)?;
        let qualification = self.autonomy_qualification(&policy, evaluated_at).await?;
        let active_freezes = self
            .autonomy_active_freezes(tenant_id, cluster_id, action, action_version, evaluated_at)
            .await?;
        let kill_switch = self
            .autonomy_kill_switch(tenant_id, cluster_id, action, action_version)
            .await?;
        let recent_outcomes = self
            .autonomy_recent_outcomes(tenant_id, cluster_id, action, action_version, 20)
            .await?;
        let mut reason_codes = Vec::new();
        if lifecycle.mode == AutonomyMode::Disabled {
            reason_codes.push("autonomy_disabled".to_owned());
        }
        if lifecycle.mode == AutonomyMode::Paused {
            reason_codes.push("autonomy_paused".to_owned());
        }
        if !active_freezes.is_empty() {
            reason_codes.push("freeze_active".to_owned());
        }
        if kill_switch.as_ref().is_some_and(|state| state.active) {
            reason_codes.push("kill_switch_active".to_owned());
        }
        Ok(AutonomyScopeView {
            schema_version: rocketmq_sre_contracts::AUTONOMY_SCHEMA_VERSION,
            policy,
            lifecycle,
            qualification,
            active_freezes,
            kill_switch,
            recent_outcomes,
            reason_codes,
        })
    }

    pub(super) async fn autonomy_scopes_at(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        limit: i64,
        evaluated_at: DateTime<Utc>,
    ) -> Result<Vec<AutonomyScopeView>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT action_id, action_version
             FROM autonomy_lifecycle_states
             WHERE tenant_id = $1 AND cluster_id = $2
             ORDER BY action_id, action_version
             LIMIT $3",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut scopes = Vec::with_capacity(rows.len());
        for row in rows {
            let action = parse_action(row.try_get("action_id")?)?;
            let action_version: String = row.try_get("action_version")?;
            scopes.push(
                self.autonomy_scope_at(tenant_id, cluster_id, action, &action_version, evaluated_at)
                    .await?,
            );
        }
        Ok(scopes)
    }

    pub(super) async fn update_autonomy_lifecycle(
        &self,
        current: &AutonomyLifecycleState,
        next: &AutonomyLifecycleState,
        action_version: &str,
        owner_confirmed: bool,
        reason_code: &str,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE autonomy_lifecycle_states
             SET mode = $6,
                 previous_mode = $7,
                 owner_confirmed_at = CASE WHEN $8 THEN $9 ELSE owner_confirmed_at END,
                 pause_reason = $10,
                 lifecycle_revision = $11,
                 updated_by = $12,
                 updated_at = $9
             WHERE tenant_id = $1 AND cluster_id = $2
               AND action_id = $3 AND action_version = $4
               AND lifecycle_revision = $5",
        )
        .bind(current.tenant_id.as_uuid())
        .bind(current.cluster_id.as_uuid())
        .bind(current.action.id())
        .bind(action_version)
        .bind(
            i64::try_from(current.lifecycle_revision)
                .map_err(|_| invalid_request("lifecycle revision is too large"))?,
        )
        .bind(mode_name(next.mode))
        .bind(next.previous_mode.map(mode_name))
        .bind(owner_confirmed)
        .bind(next.updated_at)
        .bind(&next.pause_reason)
        .bind(i64::try_from(next.lifecycle_revision).map_err(|_| invalid_request("lifecycle revision is too large"))?)
        .bind(&next.updated_by)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ControlPlaneError::conflict_code(
                "autonomy_state_changed",
                "autonomy lifecycle changed before this transition completed",
            ));
        }
        insert_lifecycle_event(
            &mut transaction,
            next,
            action_version,
            Some(current.mode),
            reason_code,
            &next.updated_by,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn set_autonomy_freeze(
        &self,
        tenant_id: TenantId,
        cluster_id: Option<ClusterId>,
        action: Option<ExecutionAction>,
        action_version: Option<&str>,
        active: bool,
        reason: &str,
        starts_at: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
        actor: &str,
    ) -> Result<AutonomyFreezeView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let cluster_scope = cluster_id.map_or_else(|| "*".to_owned(), |id| id.to_string());
        let lock_key = format!(
            "autonomy-freeze:{tenant_id}:{cluster_scope}:{}:{}",
            action.map_or("*", ExecutionAction::id),
            action_version.unwrap_or("*")
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_key)
            .execute(&mut *transaction)
            .await?;
        let existing = sqlx::query(
            "SELECT id, revision
             FROM autonomy_freezes
             WHERE tenant_id = $1 AND cluster_id = $2
               AND action_id IS NOT DISTINCT FROM $3
               AND action_version IS NOT DISTINCT FROM $4
             FOR UPDATE",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.map(ClusterId::as_uuid))
        .bind(action.map(ExecutionAction::id))
        .bind(action_version)
        .fetch_optional(&mut *transaction)
        .await?;
        let (id, revision) = match existing {
            Some(row) => {
                let revision = row
                    .try_get::<i64, _>("revision")?
                    .checked_add(1)
                    .ok_or_else(|| invalid_persisted("freeze revision is exhausted"))?;
                let id: Uuid = row.try_get("id")?;
                sqlx::query(
                    "UPDATE autonomy_freezes
                     SET revision = $2, active = $3, reason = $4,
                         starts_at = $5, expires_at = $6,
                         updated_by = $7, updated_at = $8
                     WHERE id = $1",
                )
                .bind(id)
                .bind(revision)
                .bind(active)
                .bind(reason)
                .bind(starts_at)
                .bind(expires_at)
                .bind(actor)
                .bind(Utc::now())
                .execute(&mut *transaction)
                .await?;
                (id, revision)
            }
            None => {
                let id = Uuid::new_v4();
                sqlx::query(
                    "INSERT INTO autonomy_freezes (
                        id, tenant_id, cluster_id, action_id, action_version,
                        revision, active, reason, starts_at, expires_at,
                        updated_by, updated_at
                     ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9, $10, $11)",
                )
                .bind(id)
                .bind(tenant_id.as_uuid())
                .bind(cluster_id.map(ClusterId::as_uuid))
                .bind(action.map(ExecutionAction::id))
                .bind(action_version)
                .bind(active)
                .bind(reason)
                .bind(starts_at)
                .bind(expires_at)
                .bind(actor)
                .bind(Utc::now())
                .execute(&mut *transaction)
                .await?;
                (id, 1)
            }
        };
        transaction.commit().await?;
        Ok(AutonomyFreezeView {
            id,
            cluster_id,
            action,
            action_version: action_version.map(str::to_owned),
            revision: u64::try_from(revision).map_err(|_| invalid_persisted("freeze revision is negative"))?,
            active,
            reason: reason.to_owned(),
            starts_at,
            expires_at,
            updated_by: actor.to_owned(),
            updated_at: Utc::now(),
        })
    }

    pub(super) async fn set_autonomy_kill_switch(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        action_version: &str,
        active: bool,
        reason: &str,
        actor: &str,
    ) -> Result<AutonomyKillSwitchView, ControlPlaneError> {
        let now = Utc::now();
        let row = sqlx::query(
            "INSERT INTO autonomy_kill_switches (
                tenant_id, cluster_id, action_id, action_version,
                revision, active, reason, updated_by, updated_at
             ) VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8)
             ON CONFLICT (tenant_id, cluster_id, action_id, action_version)
             DO UPDATE SET
                revision = autonomy_kill_switches.revision + 1,
                active = EXCLUDED.active,
                reason = EXCLUDED.reason,
                updated_by = EXCLUDED.updated_by,
                updated_at = EXCLUDED.updated_at
             RETURNING revision",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(action.id())
        .bind(action_version)
        .bind(active)
        .bind(reason)
        .bind(actor)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        Ok(AutonomyKillSwitchView {
            cluster_id,
            action,
            action_version: action_version.to_owned(),
            revision: u64::try_from(row.try_get::<i64, _>("revision")?)
                .map_err(|_| invalid_persisted("kill-switch revision is negative"))?,
            active,
            reason: reason.to_owned(),
            updated_by: actor.to_owned(),
            updated_at: now,
        })
    }

    pub(super) async fn store_autonomy_cohort(
        &self,
        policy_id: AutonomyPolicyId,
        cohort: &AutonomyQualificationCohort,
    ) -> Result<AutonomyQualificationCohort, ControlPlaneError> {
        sqlx::query(
            "INSERT INTO autonomy_qualification_cohorts (
                id, level, tenant_id, cluster_id, action_id, action_version,
                policy_id, policy_definition_version, descriptor_digest,
                diagnostic_pack_id, diagnostic_pack_version,
                primary_actual_model_identity_hash,
                critic_actual_model_identity_hash, cohort_hash, created_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9,
                $10, $11,
                $12,
                $13, $14, $15
             )
             ON CONFLICT (cohort_hash) DO NOTHING",
        )
        .bind(cohort.id.as_uuid())
        .bind(qualification_level_name(cohort.level))
        .bind(cohort.tenant_id.as_uuid())
        .bind(cohort.cluster_id.as_uuid())
        .bind(cohort.action.id())
        .bind(&cohort.action_version)
        .bind(policy_id.as_uuid())
        .bind(
            i64::try_from(cohort.policy_definition_version)
                .map_err(|_| invalid_request("cohort policy version is too large"))?,
        )
        .bind(&cohort.descriptor_digest)
        .bind(&cohort.diagnostic_pack_id)
        .bind(&cohort.diagnostic_pack_version)
        .bind(&cohort.primary_actual_model_identity_hash)
        .bind(&cohort.critic_actual_model_identity_hash)
        .bind(&cohort.cohort_hash)
        .bind(cohort.created_at)
        .execute(&self.pool)
        .await?;

        let row = sqlx::query(
            "SELECT *
             FROM autonomy_qualification_cohorts
             WHERE cohort_hash = $1",
        )
        .bind(&cohort.cohort_hash)
        .fetch_one(&self.pool)
        .await?;
        let stored = cohort_from_row(&row)?;
        if stored.level != cohort.level
            || stored.tenant_id != cohort.tenant_id
            || stored.cluster_id != cohort.cluster_id
            || stored.action != cohort.action
            || stored.action_version != cohort.action_version
            || stored.policy_definition_version != cohort.policy_definition_version
            || stored.descriptor_digest != cohort.descriptor_digest
            || stored.diagnostic_pack_id != cohort.diagnostic_pack_id
            || stored.diagnostic_pack_version != cohort.diagnostic_pack_version
            || stored.primary_actual_model_identity_hash != cohort.primary_actual_model_identity_hash
            || stored.critic_actual_model_identity_hash != cohort.critic_actual_model_identity_hash
        {
            return Err(ControlPlaneError::conflict_code(
                "autonomy_cohort_hash_collision",
                "stored autonomy cohort does not match the canonical qualification key",
            ));
        }
        Ok(stored)
    }

    pub(super) async fn autonomy_cohort(
        &self,
        cohort_id: rocketmq_sre_contracts::AutonomyCohortId,
    ) -> Result<AutonomyQualificationCohort, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT *
             FROM autonomy_qualification_cohorts
             WHERE id = $1",
        )
        .bind(cohort_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        cohort_from_row(&row)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the qualification lookup deliberately binds the complete execution scope"
    )]
    pub(super) async fn supervised_execution_qualification(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        incident_id: IncidentId,
        plan_id: ActionPlanId,
        plan_hash: &str,
        execution_id: ExecutionId,
        minimum_stable_window_seconds: u64,
    ) -> Result<SupervisedExecutionQualificationFacts, ControlPlaneError> {
        let minimum_stable_window_seconds =
            i64::try_from(minimum_stable_window_seconds).map_err(|_| invalid_request("stable window is too large"))?;
        let row = sqlx::query(
            "SELECT
                execution.state = 'succeeded' AS succeeded,
                (
                    jsonb_typeof(execution.request_snapshot->'approvals') = 'array'
                    AND jsonb_array_length(execution.request_snapshot->'approvals') > 0
                    AND (
                        execution.request_snapshot->'autonomy_grant' IS NULL
                        OR execution.request_snapshot->'autonomy_grant' = 'null'::JSONB
                    )
                ) AS human_approved,
                (
                    NOT EXISTS (
                        SELECT 1
                        FROM execution_steps step
                        WHERE step.execution_id = execution.id
                          AND step.record_kind = 'intent'
                          AND step.compensation
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM execution_agent_effects effect
                        WHERE effect.execution_id = execution.id
                          AND effect.state = 'unknown'
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM audit_events event
                        WHERE event.resource_kind = 'execution'
                          AND event.resource_id = execution.id::TEXT
                          AND event.event_kind = 'state_changed'
                          AND event.details->>'to' IN (
                              'unknown', 'compensating', 'rolled_back', 'escalated'
                          )
                    )
                ) AS timeline_safe,
                (
                    EXISTS (
                        SELECT 1
                        FROM execution_steps step
                        WHERE step.execution_id = execution.id
                          AND step.record_kind = 'intent'
                          AND NOT step.compensation
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM execution_steps step
                        WHERE step.execution_id = execution.id
                          AND step.record_kind = 'intent'
                          AND NOT step.compensation
                          AND (
                              NOT EXISTS (
                                  SELECT 1
                                  FROM execution_verification_evidence evidence
                                  WHERE evidence.execution_id = execution.id
                                    AND evidence.step_id = step.step_id
                                    AND evidence.attempt = step.attempt
                                    AND evidence.phase = 'pre'
                              )
                              OR NOT EXISTS (
                                  SELECT 1
                                  FROM execution_verification_evidence evidence
                                  WHERE evidence.execution_id = execution.id
                                    AND evidence.step_id = step.step_id
                                    AND evidence.attempt = step.attempt
                                    AND evidence.phase = 'post'
                              )
                          )
                    )
                ) AS evidence_complete,
                (
                    EXISTS (
                        SELECT 1
                        FROM execution_steps step
                        WHERE step.execution_id = execution.id
                          AND step.record_kind = 'intent'
                          AND NOT step.compensation
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM execution_steps step
                        WHERE step.execution_id = execution.id
                          AND step.record_kind = 'intent'
                          AND NOT step.compensation
                          AND NOT EXISTS (
                              SELECT 1
                              FROM execution_verifications verification
                              WHERE verification.execution_id = execution.id
                                AND verification.step_id = step.step_id
                                AND verification.attempt = step.attempt
                                AND NOT verification.compensation
                                AND verification.outcome = 'succeeded'
                                AND (
                                    verification.result_snapshot->>'stable_window_seconds'
                                )::BIGINT >= $8
                                AND verification.completed_at >=
                                    verification.started_at + ($8 * INTERVAL '1 second')
                          )
                    )
                ) AS stable_window_passed,
                COALESCE(execution.completed_at, execution.updated_at) AS observed_at
             FROM executions execution
             JOIN action_plans plan ON plan.id = execution.plan_id
             WHERE execution.id = $1
               AND execution.tenant_id = $2
               AND execution.cluster_id = $3
               AND execution.action_id = $4
               AND execution.plan_id = $5
               AND execution.plan_hash = $6
               AND plan.incident_id = $7",
        )
        .bind(execution_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(action.id())
        .bind(plan_id.as_uuid())
        .bind(plan_hash)
        .bind(incident_id.as_uuid())
        .bind(minimum_stable_window_seconds)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "supervised_execution_scope_mismatch",
                "supervised execution does not match the exact qualification scope",
            )
        })?;

        let critic = sqlx::query(
            "SELECT
                primary_profile.profile_name AS primary_profile,
                primary_invocation.model_family AS primary_model_family,
                primary_invocation.model_revision AS primary_model_revision,
                critic_profile.profile_name AS critic_profile,
                critic_invocation.model_family AS critic_model_family,
                critic_invocation.model_revision AS critic_model_revision
             FROM critic_reviews review
             JOIN action_plans plan ON plan.id = review.plan_id
             JOIN model_invocations primary_invocation
               ON primary_invocation.id = review.primary_invocation_id
             JOIN model_profiles primary_profile
               ON primary_profile.id = primary_invocation.actual_profile_id
             JOIN model_invocations critic_invocation
               ON critic_invocation.id = review.critic_invocation_id
             JOIN model_profiles critic_profile
               ON critic_profile.id = critic_invocation.actual_profile_id
             WHERE review.plan_id = $1
               AND review.plan_hash = $2
               AND review.status = 'valid'
               AND review.conclusion = 'accept'
               AND review.primary_invocation_id = plan.primary_model_invocation_id
             ORDER BY review.created_at DESC, review.id DESC
             LIMIT 1",
        )
        .bind(plan_id.as_uuid())
        .bind(plan_hash)
        .fetch_optional(&self.pool)
        .await?;

        Ok(SupervisedExecutionQualificationFacts {
            succeeded: row.try_get("succeeded")?,
            human_approved: row.try_get("human_approved")?,
            timeline_safe: row.try_get("timeline_safe")?,
            evidence_complete: row.try_get("evidence_complete")?,
            stable_window_passed: row.try_get("stable_window_passed")?,
            observed_at: row.try_get("observed_at")?,
            primary_profile: critic.as_ref().map(|row| row.try_get("primary_profile")).transpose()?,
            primary_model_family: critic
                .as_ref()
                .map(|row| row.try_get("primary_model_family"))
                .transpose()?,
            primary_model_revision: critic
                .as_ref()
                .map(|row| row.try_get("primary_model_revision"))
                .transpose()?,
            critic_profile: critic.as_ref().map(|row| row.try_get("critic_profile")).transpose()?,
            critic_model_family: critic
                .as_ref()
                .map(|row| row.try_get("critic_model_family"))
                .transpose()?,
            critic_model_revision: critic
                .as_ref()
                .map(|row| row.try_get("critic_model_revision"))
                .transpose()?,
        })
    }

    pub(super) async fn store_qualification_sample(
        &self,
        sample: &AutonomyQualificationSample,
    ) -> Result<AutonomyQualificationSample, ControlPlaneError> {
        sample
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_qualification_sample", error.to_string()))?;
        let inserted = sqlx::query(
            "INSERT INTO autonomy_qualification_samples (
                id, cohort_id, sample_kind, incident_id, plan_id, plan_hash,
                execution_id, qualified, reason_codes, human_outcome_linked,
                evidence_complete, stable_window_passed, sample_snapshot,
                observed_at, reconciled_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10,
                $11, $12, $13,
                $14, $15
             )
             ON CONFLICT DO NOTHING",
        )
        .bind(sample.id.as_uuid())
        .bind(sample.cohort_id.as_uuid())
        .bind(sample_kind_name(sample.kind))
        .bind(sample.incident_id.as_uuid())
        .bind(sample.plan_id.as_uuid())
        .bind(&sample.plan_hash)
        .bind(sample.execution_id.map(rocketmq_sre_contracts::ExecutionId::as_uuid))
        .bind(sample.qualified)
        .bind(&sample.reason_codes)
        .bind(sample.human_outcome_linked)
        .bind(sample.evidence_complete)
        .bind(sample.stable_window_passed)
        .bind(json_value(sample)?)
        .bind(sample.observed_at)
        .bind(sample.reconciled_at)
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 1 {
            return Ok(sample.clone());
        }

        let row = match sample.kind {
            AutonomySampleKind::ShadowOutcome => {
                sqlx::query(
                    "SELECT sample_snapshot
                     FROM autonomy_qualification_samples
                     WHERE cohort_id = $1 AND sample_kind = 'shadow_outcome'
                       AND incident_id = $2 AND plan_hash = $3",
                )
                .bind(sample.cohort_id.as_uuid())
                .bind(sample.incident_id.as_uuid())
                .bind(&sample.plan_hash)
                .fetch_one(&self.pool)
                .await?
            }
            AutonomySampleKind::SupervisedSuccess => {
                sqlx::query(
                    "SELECT sample_snapshot
                     FROM autonomy_qualification_samples
                     WHERE cohort_id = $1 AND sample_kind = 'supervised_success'
                       AND execution_id = $2",
                )
                .bind(sample.cohort_id.as_uuid())
                .bind(sample.execution_id.map(ExecutionId::as_uuid))
                .fetch_one(&self.pool)
                .await?
            }
        };
        let stored: AutonomyQualificationSample = from_json(row.try_get("sample_snapshot")?)?;
        if !same_qualification_sample(&stored, sample) {
            return Err(ControlPlaneError::conflict_code(
                "qualification_sample_conflict",
                "qualification sample idempotency key already has different content",
            ));
        }
        Ok(stored)
    }

    pub(super) async fn store_shadow_outcome(
        &self,
        record: &ShadowOutcomeRecord,
        sample: &AutonomyQualificationSample,
    ) -> Result<ShadowOutcomeView, ControlPlaneError> {
        sample
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_qualification_sample", error.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO autonomy_shadow_outcomes (
                id, tenant_id, cluster_id, action_id, action_version,
                incident_id, diagnosis_revision_id, plan_id, plan_hash,
                cohort_id, eligibility_snapshot, expected_effect_snapshot,
                evidence_ids, qualified, reason_codes, human_outcome_snapshot,
                stable_window_snapshot, observed_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12,
                $13, $14, $15, $16,
                $17, $18
             )
             ON CONFLICT (
                tenant_id, cluster_id, action_id, action_version, incident_id, plan_hash
             ) DO NOTHING",
        )
        .bind(record.view.id)
        .bind(record.tenant_id.as_uuid())
        .bind(record.cluster_id.as_uuid())
        .bind(record.action.id())
        .bind(&record.action_version)
        .bind(record.view.incident_id.as_uuid())
        .bind(record.diagnosis_revision_id.as_uuid())
        .bind(record.view.plan_id.as_uuid())
        .bind(&record.view.plan_hash)
        .bind(record.view.cohort_id.as_uuid())
        .bind(json_value(&record.eligibility)?)
        .bind(&record.expected_effect)
        .bind(
            record
                .evidence_ids
                .iter()
                .copied()
                .map(rocketmq_sre_contracts::EvidenceId::as_uuid)
                .collect::<Vec<_>>(),
        )
        .bind(record.view.qualified)
        .bind(&record.view.reason_codes)
        .bind(&record.human_outcome)
        .bind(&record.stable_window)
        .bind(record.view.observed_at)
        .execute(&mut *transaction)
        .await?;
        let stored_view = if inserted.rows_affected() == 1 {
            record.view.clone()
        } else {
            let row = sqlx::query(
                "SELECT id, cohort_id, incident_id, plan_id, plan_hash,
                        qualified, reason_codes, observed_at
                 FROM autonomy_shadow_outcomes
                 WHERE tenant_id = $1 AND cluster_id = $2
                   AND action_id = $3 AND action_version = $4
                   AND incident_id = $5 AND plan_hash = $6",
            )
            .bind(record.tenant_id.as_uuid())
            .bind(record.cluster_id.as_uuid())
            .bind(record.action.id())
            .bind(&record.action_version)
            .bind(record.view.incident_id.as_uuid())
            .bind(&record.view.plan_hash)
            .fetch_one(&mut *transaction)
            .await?;
            shadow_outcome_from_row(&row)?
        };
        if !same_shadow_outcome(&stored_view, &record.view) {
            return Err(ControlPlaneError::conflict_code(
                "shadow_outcome_conflict",
                "Shadow outcome idempotency key already has different content",
            ));
        }

        let sample_inserted = sqlx::query(
            "INSERT INTO autonomy_qualification_samples (
                id, cohort_id, sample_kind, incident_id, plan_id, plan_hash,
                execution_id, qualified, reason_codes, human_outcome_linked,
                evidence_complete, stable_window_passed, sample_snapshot,
                observed_at, reconciled_at
             ) VALUES (
                $1, $2, 'shadow_outcome', $3, $4, $5,
                NULL, $6, $7, $8,
                $9, $10, $11,
                $12, $13
             )
             ON CONFLICT (cohort_id, sample_kind, incident_id, plan_hash)
             DO NOTHING",
        )
        .bind(sample.id.as_uuid())
        .bind(sample.cohort_id.as_uuid())
        .bind(sample.incident_id.as_uuid())
        .bind(sample.plan_id.as_uuid())
        .bind(&sample.plan_hash)
        .bind(sample.qualified)
        .bind(&sample.reason_codes)
        .bind(sample.human_outcome_linked)
        .bind(sample.evidence_complete)
        .bind(sample.stable_window_passed)
        .bind(json_value(sample)?)
        .bind(sample.observed_at)
        .bind(sample.reconciled_at)
        .execute(&mut *transaction)
        .await?;
        if sample_inserted.rows_affected() == 0 {
            let row = sqlx::query(
                "SELECT sample_snapshot
                 FROM autonomy_qualification_samples
                 WHERE cohort_id = $1 AND sample_kind = 'shadow_outcome'
                   AND incident_id = $2 AND plan_hash = $3",
            )
            .bind(sample.cohort_id.as_uuid())
            .bind(sample.incident_id.as_uuid())
            .bind(&sample.plan_hash)
            .fetch_one(&mut *transaction)
            .await?;
            let stored: AutonomyQualificationSample = from_json(row.try_get("sample_snapshot")?)?;
            if !same_qualification_sample(&stored, sample) {
                return Err(ControlPlaneError::conflict_code(
                    "qualification_sample_conflict",
                    "Shadow qualification sample already has different content",
                ));
            }
        }
        transaction.commit().await?;
        Ok(stored_view)
    }

    pub(super) async fn shadow_outcomes(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        action_version: &str,
        limit: i64,
    ) -> Result<Vec<ShadowOutcomeView>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT id, cohort_id, incident_id, plan_id, plan_hash,
                    qualified, reason_codes, observed_at
             FROM autonomy_shadow_outcomes
             WHERE tenant_id = $1 AND cluster_id = $2
               AND action_id = $3 AND action_version = $4
             ORDER BY observed_at DESC, sequence_id DESC
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(action.id())
        .bind(action_version)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(shadow_outcome_from_row).collect()
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact persisted Critic binding is intentionally explicit"
    )]
    pub(super) async fn autonomy_critic_bindings_valid(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        diagnosis_revision_id: DiagnosisRevisionId,
        plan_id: rocketmq_sre_contracts::ActionPlanId,
        plan_hash: &str,
        critic_review_id: CriticReviewId,
        primary_invocation_id: ModelInvocationId,
        critic_invocation_id: ModelInvocationId,
        primary_profile: &str,
        primary_family: &str,
        primary_revision: &str,
        critic_profile: &str,
        critic_family: &str,
        critic_revision: &str,
    ) -> Result<bool, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT
                review.plan_id,
                review.plan_hash,
                review.primary_invocation_id,
                review.critic_invocation_id,
                review.primary_model_family,
                review.critic_model_family,
                review.status,
                review.conclusion,
                primary_invocation.tenant_id AS primary_tenant_id,
                primary_invocation.cluster_id AS primary_cluster_id,
                primary_invocation.diagnosis_revision_id AS primary_diagnosis_revision_id,
                primary_invocation.model_family AS primary_actual_family,
                primary_invocation.model_revision AS primary_actual_revision,
                primary_profile.profile_name AS primary_actual_profile,
                critic_invocation.tenant_id AS critic_tenant_id,
                critic_invocation.cluster_id AS critic_cluster_id,
                critic_invocation.model_family AS critic_actual_family,
                critic_invocation.model_revision AS critic_actual_revision,
                critic_profile.profile_name AS critic_actual_profile
             FROM critic_reviews review
             JOIN model_invocations primary_invocation
               ON primary_invocation.id = review.primary_invocation_id
             JOIN model_profiles primary_profile
               ON primary_profile.id = primary_invocation.actual_profile_id
             JOIN model_invocations critic_invocation
               ON critic_invocation.id = review.critic_invocation_id
             JOIN model_profiles critic_profile
               ON critic_profile.id = critic_invocation.actual_profile_id
             WHERE review.id = $1",
        )
        .bind(critic_review_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        Ok(row.try_get::<Uuid, _>("plan_id")? == plan_id.as_uuid()
            && row.try_get::<String, _>("plan_hash")? == plan_hash
            && row.try_get::<Uuid, _>("primary_invocation_id")? == primary_invocation_id.as_uuid()
            && row.try_get::<Uuid, _>("critic_invocation_id")? == critic_invocation_id.as_uuid()
            && row.try_get::<String, _>("status")? == "valid"
            && row.try_get::<String, _>("conclusion")? == "accept"
            && TenantId::from_uuid(row.try_get("primary_tenant_id")?) == tenant_id
            && ClusterId::from_uuid(row.try_get("primary_cluster_id")?) == cluster_id
            && row
                .try_get::<Option<Uuid>, _>("primary_diagnosis_revision_id")?
                .is_some_and(|id| id == diagnosis_revision_id.as_uuid())
            && TenantId::from_uuid(row.try_get("critic_tenant_id")?) == tenant_id
            && ClusterId::from_uuid(row.try_get("critic_cluster_id")?) == cluster_id
            && row
                .try_get::<String, _>("primary_model_family")?
                .eq_ignore_ascii_case(primary_family)
            && row
                .try_get::<String, _>("critic_model_family")?
                .eq_ignore_ascii_case(critic_family)
            && row
                .try_get::<String, _>("primary_actual_family")?
                .eq_ignore_ascii_case(primary_family)
            && row.try_get::<String, _>("primary_actual_revision")? == primary_revision
            && row.try_get::<String, _>("primary_actual_profile")? == primary_profile
            && row
                .try_get::<String, _>("critic_actual_family")?
                .eq_ignore_ascii_case(critic_family)
            && row.try_get::<String, _>("critic_actual_revision")? == critic_revision
            && row.try_get::<String, _>("critic_actual_profile")? == critic_profile)
    }

    pub(super) async fn store_dynamic_safety_decision(
        &self,
        decision: &DynamicSafetyDecision,
    ) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO autonomy_dynamic_safety_decisions (
                id, tenant_id, cluster_id, action_id, action_version,
                plan_id, plan_hash, execution_id, execution_step_id,
                policy_definition_version, lifecycle_revision,
                error_budget_available, freeze_revision, kill_switch_revision,
                evidence_fresh, allowed, reason_codes, decision_snapshot,
                issued_at, expires_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11,
                $12, $13, $14,
                $15, $16, $17, $18,
                $19, $20
             )
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(decision.id.as_uuid())
        .bind(decision.tenant_id.as_uuid())
        .bind(decision.cluster_id.as_uuid())
        .bind(decision.action.id())
        .bind(&decision.action_version)
        .bind(decision.plan_id.as_uuid())
        .bind(&decision.plan_hash)
        .bind(decision.execution_id.as_uuid())
        .bind(decision.execution_step_id.as_uuid())
        .bind(
            i64::try_from(decision.policy_definition_version)
                .map_err(|_| invalid_request("safety policy version is too large"))?,
        )
        .bind(
            i64::try_from(decision.lifecycle_revision)
                .map_err(|_| invalid_request("safety lifecycle revision is too large"))?,
        )
        .bind(decision.error_budget_available)
        .bind(i64::try_from(decision.freeze_revision).map_err(|_| invalid_request("freeze revision is too large"))?)
        .bind(
            i64::try_from(decision.kill_switch_revision)
                .map_err(|_| invalid_request("kill-switch revision is too large"))?,
        )
        .bind(decision.evidence_fresh)
        .bind(decision.allowed)
        .bind(&decision.reason_codes)
        .bind(json_value(decision)?)
        .bind(decision.issued_at)
        .bind(decision.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn dynamic_safety_decision_is_persisted(
        &self,
        decision: &DynamicSafetyDecision,
    ) -> Result<bool, ControlPlaneError> {
        let snapshot: Option<Value> = sqlx::query_scalar(
            "SELECT decision_snapshot
             FROM autonomy_dynamic_safety_decisions
             WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3
               AND action_id = $4 AND action_version = $5
               AND plan_id = $6 AND plan_hash = $7
               AND execution_id = $8 AND execution_step_id = $9",
        )
        .bind(decision.id.as_uuid())
        .bind(decision.tenant_id.as_uuid())
        .bind(decision.cluster_id.as_uuid())
        .bind(decision.action.id())
        .bind(&decision.action_version)
        .bind(decision.plan_id.as_uuid())
        .bind(&decision.plan_hash)
        .bind(decision.execution_id.as_uuid())
        .bind(decision.execution_step_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        snapshot
            .map(from_json::<DynamicSafetyDecision>)
            .transpose()
            .map(|stored| stored.as_ref() == Some(decision))
    }

    pub(super) async fn plan_evidence_is_current(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        plan_id: ActionPlanId,
        plan_hash: &str,
        action: ExecutionAction,
        action_version: &str,
        maximum_age_seconds: u64,
        required_sources: &[String],
        now: DateTime<Utc>,
    ) -> Result<bool, ControlPlaneError> {
        let snapshot: Option<Value> = sqlx::query_scalar(
            "SELECT plan_snapshot
             FROM action_plans
             WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3
               AND plan_hash = $4",
        )
        .bind(plan_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(plan_hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some(snapshot) = snapshot else {
            return Ok(false);
        };
        let plan: ActionPlan = from_json(snapshot)?;
        if plan.id != plan_id
            || plan.tenant_id != tenant_id
            || plan.cluster_id != cluster_id
            || plan.plan_hash != plan_hash
            || plan.verify_plan_hash().is_err()
            || plan.steps.is_empty()
            || plan
                .steps
                .iter()
                .any(|step| step.action != action || step.descriptor_version != action_version)
        {
            return Ok(false);
        }
        let mut evidence_ids = plan
            .steps
            .iter()
            .flat_map(|step| step.evidence_ids.iter().copied())
            .collect::<Vec<EvidenceId>>();
        evidence_ids.sort_unstable();
        evidence_ids.dedup();
        if evidence_ids.is_empty() {
            return Ok(false);
        }
        let ids = evidence_ids
            .iter()
            .map(|evidence_id| evidence_id.as_uuid())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "SELECT id, source, observed_at, freshness_seconds, coverage,
                    partial, expires_at
             FROM evidence_snapshots
             WHERE tenant_id = $1 AND cluster_id = $2
               AND id = ANY($3)",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;
        if rows.len() != evidence_ids.len() {
            return Ok(false);
        }
        let maximum_age_seconds =
            i64::try_from(maximum_age_seconds).map_err(|_| invalid_request("evidence freshness bound is too large"))?;
        let mut observed_sources = BTreeSet::new();
        for row in rows {
            let observed_at: DateTime<Utc> = row.try_get("observed_at")?;
            let freshness_seconds: i64 = row.try_get("freshness_seconds")?;
            let expires_at: Option<DateTime<Utc>> = row.try_get("expires_at")?;
            let allowed_seconds = freshness_seconds.min(maximum_age_seconds);
            let Some(allowed_age) = TimeDelta::try_seconds(allowed_seconds) else {
                return Ok(false);
            };
            if observed_at > now
                || allowed_seconds <= 0
                || now > observed_at + allowed_age
                || expires_at.is_some_and(|expires_at| expires_at <= now)
                || row.try_get::<bool, _>("partial")?
                || row.try_get::<String, _>("coverage")? != "available"
            {
                return Ok(false);
            }
            observed_sources.insert(row.try_get::<String, _>("source")?);
        }
        Ok(required_sources
            .iter()
            .all(|required| observed_sources.contains(required)))
    }

    pub(super) async fn record_autonomy_outcome(
        &self,
        outcome: &AutonomyOutcome,
        actor: &str,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        lock_scope(
            &mut transaction,
            outcome.tenant_id,
            outcome.cluster_id,
            outcome.action,
            &outcome.action_version,
        )
        .await?;
        let inserted = sqlx::query(
            "INSERT INTO autonomy_outcomes (
                id, tenant_id, cluster_id, action_id, action_version,
                incident_id, plan_id, plan_hash, execution_id, cohort_id,
                outcome_class, failure_code, reason_codes,
                first_positive_intent_persisted, outcome_snapshot,
                occurred_at, reconciled_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11, $12, $13,
                $14, $15,
                $16, $17
             )
             ON CONFLICT (
                tenant_id, cluster_id, action_id, action_version, plan_id
             ) DO NOTHING",
        )
        .bind(outcome.id.as_uuid())
        .bind(outcome.tenant_id.as_uuid())
        .bind(outcome.cluster_id.as_uuid())
        .bind(outcome.action.id())
        .bind(&outcome.action_version)
        .bind(outcome.incident_id.as_uuid())
        .bind(outcome.plan_id.as_uuid())
        .bind(&outcome.plan_hash)
        .bind(outcome.execution_id.map(rocketmq_sre_contracts::ExecutionId::as_uuid))
        .bind(outcome.cohort_id.map(rocketmq_sre_contracts::AutonomyCohortId::as_uuid))
        .bind(outcome_class_name(outcome.class))
        .bind(outcome.failure.map(autonomy_failure_name))
        .bind(&outcome.reason_codes)
        .bind(outcome.first_positive_intent_persisted)
        .bind(json_value(outcome)?)
        .bind(outcome.occurred_at)
        .bind(outcome.reconciled_at)
        .execute(&mut *transaction)
        .await?;
        let effective = if inserted.rows_affected() == 1 {
            outcome.clone()
        } else {
            let snapshot: Value = sqlx::query_scalar(
                "SELECT outcome_snapshot
                 FROM autonomy_outcomes
                 WHERE tenant_id = $1 AND cluster_id = $2
                   AND action_id = $3 AND action_version = $4
                   AND plan_id = $5",
            )
            .bind(outcome.tenant_id.as_uuid())
            .bind(outcome.cluster_id.as_uuid())
            .bind(outcome.action.id())
            .bind(&outcome.action_version)
            .bind(outcome.plan_id.as_uuid())
            .fetch_one(&mut *transaction)
            .await?;
            let stored: AutonomyOutcome = from_json(snapshot)?;
            if !same_autonomy_outcome(&stored, outcome) {
                return Err(ControlPlaneError::conflict_code(
                    "autonomy_outcome_conflict",
                    "autonomy outcome idempotency key already has different content",
                ));
            }
            stored
        };

        if effective.class == AutonomyOutcomeClass::AutonomousExecutionFailure {
            let row = sqlx::query(
                "SELECT *
                 FROM autonomy_lifecycle_states
                 WHERE tenant_id = $1 AND cluster_id = $2
                   AND action_id = $3 AND action_version = $4
                 FOR UPDATE",
            )
            .bind(effective.tenant_id.as_uuid())
            .bind(effective.cluster_id.as_uuid())
            .bind(effective.action.id())
            .bind(&effective.action_version)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
            let current = lifecycle_from_row(&row, effective.tenant_id, effective.cluster_id, effective.action)?;
            if current.mode != AutonomyMode::Paused {
                let reason = effective
                    .failure
                    .map(autonomy_failure_name)
                    .unwrap_or("autonomous_execution_failure");
                let pause_at = effective.reconciled_at.max(current.updated_at);
                let next = AutonomyStateMachine::transition(
                    &current,
                    AutonomyMode::Paused,
                    AutonomyActor::SafetyReconciler,
                    actor,
                    Some(reason),
                    PromotionQualification::default(),
                    pause_at,
                )
                .map_err(|error| ControlPlaneError::conflict_code("autonomy_pause_failed", error.to_string()))?;
                sqlx::query(
                    "UPDATE autonomy_lifecycle_states
                     SET mode = 'paused',
                         previous_mode = $6,
                         pause_reason = $7,
                         lifecycle_revision = $8,
                         updated_by = $9,
                         updated_at = $10
                     WHERE tenant_id = $1 AND cluster_id = $2
                       AND action_id = $3 AND action_version = $4
                       AND lifecycle_revision = $5",
                )
                .bind(effective.tenant_id.as_uuid())
                .bind(effective.cluster_id.as_uuid())
                .bind(effective.action.id())
                .bind(&effective.action_version)
                .bind(
                    i64::try_from(current.lifecycle_revision)
                        .map_err(|_| invalid_request("lifecycle revision is too large"))?,
                )
                .bind(mode_name(current.mode))
                .bind(&next.pause_reason)
                .bind(
                    i64::try_from(next.lifecycle_revision)
                        .map_err(|_| invalid_request("lifecycle revision is too large"))?,
                )
                .bind(actor)
                .bind(effective.reconciled_at)
                .execute(&mut *transaction)
                .await?;
                insert_lifecycle_event(
                    &mut transaction,
                    &next,
                    &effective.action_version,
                    Some(current.mode),
                    reason,
                    actor,
                )
                .await?;
            }
            insert_autonomy_outbox(&mut transaction, &effective, "autonomy_paused", actor).await?;
        } else if effective.class == AutonomyOutcomeClass::Success {
            insert_autonomy_outbox(&mut transaction, &effective, "autonomy_succeeded", actor).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn autonomy_qualification(
        &self,
        policy: &AutonomyPolicyDefinition,
        evaluated_at: DateTime<Utc>,
    ) -> Result<AutonomyQualificationView, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT *
             FROM autonomy_qualification_cohorts
             WHERE tenant_id = $1 AND cluster_id = $2
               AND action_id = $3 AND action_version = $4
               AND policy_definition_version = $5
             ORDER BY created_at DESC",
        )
        .bind(policy.tenant_id.as_uuid())
        .bind(policy.cluster_id.as_uuid())
        .bind(policy.action.id())
        .bind(&policy.action_version)
        .bind(i64::try_from(policy.definition_version).map_err(|_| invalid_request("policy version is too large"))?)
        .fetch_all(&self.pool)
        .await?;
        let mut shadow = None;
        let mut autonomous = None;
        for row in rows {
            let cohort = cohort_from_row(&row)?;
            match cohort.level {
                AutonomyQualificationLevel::Shadow if shadow.is_none() => shadow = Some(cohort),
                AutonomyQualificationLevel::Autonomous if autonomous.is_none() => autonomous = Some(cohort),
                AutonomyQualificationLevel::Shadow | AutonomyQualificationLevel::Autonomous => {}
            }
        }
        let (qualified_shadow, unqualified_shadow, _) = match shadow.as_ref() {
            Some(cohort) => sample_counts(&self.pool, cohort.id, policy.observation_window_days, evaluated_at).await?,
            None => (0, 0, 0),
        };
        let (_, _, supervised_successes) = match autonomous.as_ref() {
            Some(cohort) => sample_counts(&self.pool, cohort.id, policy.observation_window_days, evaluated_at).await?,
            None => (0, 0, 0),
        };
        let (unresolved_unknown, recent_rollbacks) = match autonomous.as_ref() {
            Some(cohort) => outcome_counts(&self.pool, cohort.id, policy.observation_window_days, evaluated_at).await?,
            None => (0, 0),
        };
        let shadow_observation_window_met = shadow.as_ref().is_some_and(|cohort| {
            observation_window_elapsed(cohort.created_at, policy.observation_window_days, evaluated_at)
        });
        let autonomous_observation_window_met = autonomous.as_ref().is_some_and(|cohort| {
            observation_window_elapsed(cohort.created_at, policy.observation_window_days, evaluated_at)
        });
        Ok(AutonomyQualificationView {
            shadow_cohort: shadow,
            autonomous_cohort: autonomous,
            qualified_shadow_samples: count_u32(qualified_shadow)?,
            unqualified_shadow_samples: count_u32(unqualified_shadow)?,
            qualified_supervised_successes: count_u32(supervised_successes)?,
            unresolved_unknown: count_u32(unresolved_unknown)?,
            recent_rollbacks: count_u32(recent_rollbacks)?,
            shadow_observation_window_met,
            autonomous_observation_window_met,
        })
    }

    async fn autonomy_active_freezes(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        action_version: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<AutonomyFreezeView>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT *
             FROM autonomy_freezes
             WHERE tenant_id = $1 AND active
               AND (cluster_id IS NULL OR cluster_id = $2)
               AND (
                    action_id IS NULL
                    OR (action_id = $3 AND action_version = $4)
               )
               AND starts_at <= $5
               AND (expires_at IS NULL OR expires_at > $5)
             ORDER BY cluster_id NULLS FIRST, action_id NULLS FIRST, revision DESC
             LIMIT 16",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(action.id())
        .bind(action_version)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(freeze_from_row).collect()
    }

    pub(super) async fn autonomy_freeze_state(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        action_version: &str,
        now: DateTime<Utc>,
    ) -> Result<(u64, bool), ControlPlaneError> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(revision), 0) AS revision,
                    COALESCE(BOOL_OR(
                        active AND starts_at <= $5
                        AND (expires_at IS NULL OR expires_at > $5)
                    ), FALSE) AS active
             FROM autonomy_freezes
             WHERE tenant_id = $1
               AND (cluster_id IS NULL OR cluster_id = $2)
               AND (
                    action_id IS NULL
                    OR (action_id = $3 AND action_version = $4)
               )",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(action.id())
        .bind(action_version)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        Ok((
            u64::try_from(row.try_get::<i64, _>("revision")?)
                .map_err(|_| invalid_persisted("freeze revision is negative"))?,
            row.try_get("active")?,
        ))
    }

    async fn autonomy_kill_switch(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        action_version: &str,
    ) -> Result<Option<AutonomyKillSwitchView>, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT *
             FROM autonomy_kill_switches
             WHERE tenant_id = $1 AND cluster_id = $2
               AND action_id = $3 AND action_version = $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(action.id())
        .bind(action_version)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(kill_switch_from_row).transpose()
    }

    async fn autonomy_recent_outcomes(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        action_version: &str,
        limit: i64,
    ) -> Result<Vec<AutonomyOutcome>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT outcome_snapshot
             FROM autonomy_outcomes
             WHERE tenant_id = $1 AND cluster_id = $2
               AND action_id = $3 AND action_version = $4
             ORDER BY occurred_at DESC, sequence_id DESC
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(action.id())
        .bind(action_version)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| from_json(row.try_get("outcome_snapshot")?))
            .collect()
    }
}

async fn lock_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    action: ExecutionAction,
    action_version: &str,
) -> Result<(), ControlPlaneError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("{tenant_id}:{cluster_id}:{}:{action_version}", action.id()))
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn insert_lifecycle_event(
    transaction: &mut Transaction<'_, Postgres>,
    state: &AutonomyLifecycleState,
    action_version: &str,
    from_mode: Option<AutonomyMode>,
    reason_code: &str,
    actor: &str,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO autonomy_lifecycle_events (
            event_id, tenant_id, cluster_id, action_id, action_version,
            from_mode, to_mode, previous_mode, lifecycle_revision,
            reason_code, actor_subject, event_snapshot, occurred_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13
         )",
    )
    .bind(Uuid::new_v4())
    .bind(state.tenant_id.as_uuid())
    .bind(state.cluster_id.as_uuid())
    .bind(state.action.id())
    .bind(action_version)
    .bind(from_mode.map(mode_name))
    .bind(mode_name(state.mode))
    .bind(state.previous_mode.map(mode_name))
    .bind(i64::try_from(state.lifecycle_revision).map_err(|_| invalid_request("lifecycle revision is too large"))?)
    .bind(reason_code)
    .bind(actor)
    .bind(json_value(state)?)
    .bind(state.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_autonomy_outbox(
    transaction: &mut Transaction<'_, Postgres>,
    outcome: &AutonomyOutcome,
    event_kind: &'static str,
    actor: &str,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO autonomy_outbox (
            id, tenant_id, cluster_id, action_id, action_version,
            outcome_id, event_kind, idempotency_key, status,
            event_snapshot, attempt_count, next_attempt_at,
            created_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, 'pending',
            $9, 0, $10,
            $10
         )
         ON CONFLICT (tenant_id, idempotency_key) DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(outcome.tenant_id.as_uuid())
    .bind(outcome.cluster_id.as_uuid())
    .bind(outcome.action.id())
    .bind(&outcome.action_version)
    .bind(outcome.id.as_uuid())
    .bind(event_kind)
    .bind(format!("autonomy-outcome:{}:{event_kind}", outcome.id))
    .bind(serde_json::json!({
        "schema_version": rocketmq_sre_contracts::AUTONOMY_SCHEMA_VERSION,
        "outcome_id": outcome.id,
        "event_kind": event_kind,
        "actor": actor,
    }))
    .bind(outcome.reconciled_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn outcome_counts(
    pool: &sqlx::PgPool,
    cohort_id: rocketmq_sre_contracts::AutonomyCohortId,
    observation_window_days: u16,
    evaluated_at: DateTime<Utc>,
) -> Result<(i64, i64), ControlPlaneError> {
    let row = sqlx::query(
        "SELECT
            COUNT(*) FILTER (
                WHERE outcome.outcome_class = 'autonomous_execution_failure'
                  AND outcome.failure_code = 'unknown_effect'
                  AND (
                      outcome.execution_id IS NULL
                      OR execution.state IN ('unknown', 'reconciling')
                  )
            ) AS unresolved_unknown,
            COUNT(*) FILTER (
                WHERE outcome.outcome_class = 'autonomous_execution_failure'
                  AND (
                      execution.state = 'rolled_back'
                      OR (
                          outcome.execution_id IS NULL
                          AND outcome.failure_code = 'rolled_back'
                      )
                  )
            ) AS recent_rollbacks
         FROM autonomy_outcomes outcome
         LEFT JOIN executions execution ON execution.id = outcome.execution_id
         WHERE outcome.cohort_id = $1
           AND outcome.occurred_at >= $3 - ($2::int * INTERVAL '1 day')
           AND outcome.occurred_at <= $3",
    )
    .bind(cohort_id.as_uuid())
    .bind(i32::from(observation_window_days))
    .bind(evaluated_at)
    .fetch_one(pool)
    .await?;
    Ok((row.try_get("unresolved_unknown")?, row.try_get("recent_rollbacks")?))
}

async fn sample_counts(
    pool: &sqlx::PgPool,
    cohort_id: rocketmq_sre_contracts::AutonomyCohortId,
    observation_window_days: u16,
    evaluated_at: DateTime<Utc>,
) -> Result<(i64, i64, i64), ControlPlaneError> {
    let row = sqlx::query(
        "SELECT
            COUNT(*) FILTER (
                WHERE sample_kind = 'shadow_outcome' AND qualified
            ) AS qualified_shadow,
            COUNT(*) FILTER (
                WHERE sample_kind = 'shadow_outcome' AND NOT qualified
            ) AS unqualified_shadow,
            COUNT(*) FILTER (
                WHERE sample_kind = 'supervised_success' AND qualified
            ) AS supervised_successes
         FROM autonomy_qualification_samples
         WHERE cohort_id = $1
           AND observed_at >= $3 - ($2::int * INTERVAL '1 day')
           AND observed_at <= $3
           AND reconciled_at <= $3",
    )
    .bind(cohort_id.as_uuid())
    .bind(i32::from(observation_window_days))
    .bind(evaluated_at)
    .fetch_one(pool)
    .await?;
    Ok((
        row.try_get("qualified_shadow")?,
        row.try_get("unqualified_shadow")?,
        row.try_get("supervised_successes")?,
    ))
}

fn observation_window_elapsed(
    cohort_created_at: DateTime<Utc>,
    observation_window_days: u16,
    evaluated_at: DateTime<Utc>,
) -> bool {
    cohort_created_at
        .checked_add_signed(TimeDelta::days(i64::from(observation_window_days)))
        .is_some_and(|window_end| window_end <= evaluated_at)
}

fn same_qualification_sample(stored: &AutonomyQualificationSample, candidate: &AutonomyQualificationSample) -> bool {
    stored.cohort_id == candidate.cohort_id
        && stored.kind == candidate.kind
        && stored.incident_id == candidate.incident_id
        && stored.plan_id == candidate.plan_id
        && stored.plan_hash == candidate.plan_hash
        && stored.execution_id == candidate.execution_id
        && stored.qualified == candidate.qualified
        && stored.reason_codes == candidate.reason_codes
        && stored.human_outcome_linked == candidate.human_outcome_linked
        && stored.evidence_complete == candidate.evidence_complete
        && stored.stable_window_passed == candidate.stable_window_passed
        && stored.observed_at == candidate.observed_at
        && stored.reconciled_at <= candidate.reconciled_at
}

fn same_autonomy_outcome(stored: &AutonomyOutcome, candidate: &AutonomyOutcome) -> bool {
    stored.tenant_id == candidate.tenant_id
        && stored.cluster_id == candidate.cluster_id
        && stored.action == candidate.action
        && stored.action_version == candidate.action_version
        && stored.incident_id == candidate.incident_id
        && stored.plan_id == candidate.plan_id
        && stored.plan_hash == candidate.plan_hash
        && stored.execution_id == candidate.execution_id
        && stored.cohort_id == candidate.cohort_id
        && stored.class == candidate.class
        && stored.failure == candidate.failure
        && stored.reason_codes == candidate.reason_codes
        && stored.first_positive_intent_persisted == candidate.first_positive_intent_persisted
        && stored.occurred_at == candidate.occurred_at
        && stored.reconciled_at == candidate.reconciled_at
}

fn shadow_outcome_from_row(row: &sqlx::postgres::PgRow) -> Result<ShadowOutcomeView, ControlPlaneError> {
    Ok(ShadowOutcomeView {
        id: row.try_get("id")?,
        cohort_id: rocketmq_sre_contracts::AutonomyCohortId::from_uuid(row.try_get("cohort_id")?),
        incident_id: rocketmq_sre_contracts::IncidentId::from_uuid(row.try_get("incident_id")?),
        plan_id: rocketmq_sre_contracts::ActionPlanId::from_uuid(row.try_get("plan_id")?),
        plan_hash: row.try_get("plan_hash")?,
        qualified: row.try_get("qualified")?,
        reason_codes: row.try_get("reason_codes")?,
        observed_at: row.try_get("observed_at")?,
    })
}

fn same_shadow_outcome(stored: &ShadowOutcomeView, candidate: &ShadowOutcomeView) -> bool {
    stored.cohort_id == candidate.cohort_id
        && stored.incident_id == candidate.incident_id
        && stored.plan_id == candidate.plan_id
        && stored.plan_hash == candidate.plan_hash
        && stored.qualified == candidate.qualified
        && stored.reason_codes == candidate.reason_codes
        && stored.observed_at == candidate.observed_at
}

fn lifecycle_from_row(
    row: &sqlx::postgres::PgRow,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    action: ExecutionAction,
) -> Result<AutonomyLifecycleState, ControlPlaneError> {
    Ok(AutonomyLifecycleState {
        tenant_id,
        cluster_id,
        action,
        mode: parse_mode(row.try_get("mode")?)?,
        previous_mode: row
            .try_get::<Option<String>, _>("previous_mode")?
            .as_deref()
            .map(parse_mode)
            .transpose()?,
        owner: row.try_get("owner")?,
        pause_reason: row.try_get("pause_reason")?,
        lifecycle_revision: u64::try_from(row.try_get::<i64, _>("lifecycle_revision")?)
            .map_err(|_| invalid_persisted("lifecycle revision is negative"))?,
        updated_by: row.try_get("updated_by")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn cohort_from_row(row: &sqlx::postgres::PgRow) -> Result<AutonomyQualificationCohort, ControlPlaneError> {
    let level = match row.try_get::<&str, _>("level")? {
        "shadow" => AutonomyQualificationLevel::Shadow,
        "autonomous" => AutonomyQualificationLevel::Autonomous,
        _ => return Err(invalid_persisted("qualification cohort level is unknown")),
    };
    Ok(AutonomyQualificationCohort {
        id: rocketmq_sre_contracts::AutonomyCohortId::from_uuid(row.try_get("id")?),
        level,
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        action: parse_action(row.try_get("action_id")?)?,
        action_version: row.try_get("action_version")?,
        policy_definition_version: u64::try_from(row.try_get::<i64, _>("policy_definition_version")?)
            .map_err(|_| invalid_persisted("cohort policy version is negative"))?,
        descriptor_digest: row.try_get("descriptor_digest")?,
        diagnostic_pack_id: row.try_get("diagnostic_pack_id")?,
        diagnostic_pack_version: row.try_get("diagnostic_pack_version")?,
        primary_actual_model_identity_hash: row.try_get("primary_actual_model_identity_hash")?,
        critic_actual_model_identity_hash: row.try_get("critic_actual_model_identity_hash")?,
        cohort_hash: row.try_get("cohort_hash")?,
        created_at: row.try_get("created_at")?,
    })
}

fn freeze_from_row(row: &sqlx::postgres::PgRow) -> Result<AutonomyFreezeView, ControlPlaneError> {
    Ok(AutonomyFreezeView {
        id: row.try_get("id")?,
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        action: row
            .try_get::<Option<String>, _>("action_id")?
            .as_deref()
            .map(parse_action)
            .transpose()?,
        action_version: row.try_get("action_version")?,
        revision: u64::try_from(row.try_get::<i64, _>("revision")?)
            .map_err(|_| invalid_persisted("freeze revision is negative"))?,
        active: row.try_get("active")?,
        reason: row.try_get("reason")?,
        starts_at: row.try_get("starts_at")?,
        expires_at: row.try_get("expires_at")?,
        updated_by: row.try_get("updated_by")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn kill_switch_from_row(row: &sqlx::postgres::PgRow) -> Result<AutonomyKillSwitchView, ControlPlaneError> {
    Ok(AutonomyKillSwitchView {
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        action: parse_action(row.try_get("action_id")?)?,
        action_version: row.try_get("action_version")?,
        revision: u64::try_from(row.try_get::<i64, _>("revision")?)
            .map_err(|_| invalid_persisted("kill-switch revision is negative"))?,
        active: row.try_get("active")?,
        reason: row.try_get("reason")?,
        updated_by: row.try_get("updated_by")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn parse_action(value: &str) -> Result<ExecutionAction, ControlPlaneError> {
    ExecutionAction::from_id(value).ok_or_else(|| invalid_persisted("autonomy action identifier is unknown"))
}

const fn mode_name(mode: AutonomyMode) -> &'static str {
    match mode {
        AutonomyMode::Disabled => "disabled",
        AutonomyMode::Shadow => "shadow",
        AutonomyMode::Supervised => "supervised",
        AutonomyMode::Autonomous => "autonomous",
        AutonomyMode::Paused => "paused",
    }
}

const fn qualification_level_name(level: AutonomyQualificationLevel) -> &'static str {
    match level {
        AutonomyQualificationLevel::Shadow => "shadow",
        AutonomyQualificationLevel::Autonomous => "autonomous",
    }
}

const fn sample_kind_name(kind: AutonomySampleKind) -> &'static str {
    match kind {
        AutonomySampleKind::ShadowOutcome => "shadow_outcome",
        AutonomySampleKind::SupervisedSuccess => "supervised_success",
    }
}

const fn outcome_class_name(class: AutonomyOutcomeClass) -> &'static str {
    match class {
        AutonomyOutcomeClass::ExpectedDeny => "expected_deny",
        AutonomyOutcomeClass::Success => "success",
        AutonomyOutcomeClass::AutonomousExecutionFailure => "autonomous_execution_failure",
    }
}

const fn autonomy_failure_name(failure: AutonomousExecutionFailure) -> &'static str {
    match failure {
        AutonomousExecutionFailure::ApplyFailed => "apply_failed",
        AutonomousExecutionFailure::VerificationFailed => "verification_failed",
        AutonomousExecutionFailure::UnknownEffect => "unknown_effect",
        AutonomousExecutionFailure::CompensationStarted => "compensation_started",
        AutonomousExecutionFailure::RolledBack => "rolled_back",
        AutonomousExecutionFailure::Escalated => "escalated",
        AutonomousExecutionFailure::SafetyInvalidatedDuringExecution => "safety_invalidated_during_execution",
        AutonomousExecutionFailure::OperatorStopped => "operator_stopped",
        AutonomousExecutionFailure::CriticUnavailable => "critic_unavailable",
        AutonomousExecutionFailure::CriticInvalid => "critic_invalid",
        AutonomousExecutionFailure::CriticConflict => "critic_conflict",
        AutonomousExecutionFailure::EvidenceDegraded => "evidence_degraded",
    }
}

fn parse_mode(value: &str) -> Result<AutonomyMode, ControlPlaneError> {
    match value {
        "disabled" => Ok(AutonomyMode::Disabled),
        "shadow" => Ok(AutonomyMode::Shadow),
        "supervised" => Ok(AutonomyMode::Supervised),
        "autonomous" => Ok(AutonomyMode::Autonomous),
        "paused" => Ok(AutonomyMode::Paused),
        _ => Err(invalid_persisted("autonomy mode is unknown")),
    }
}

fn count_u32(value: i64) -> Result<u32, ControlPlaneError> {
    u32::try_from(value).map_err(|_| invalid_persisted("autonomy sample count is outside the contract bound"))
}

fn json_value<T: serde::Serialize>(value: &T) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|_| ControlPlaneError::validation("invalid_request", "value cannot be represented as JSON"))
}

fn from_json<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value)
        .map_err(|_| invalid_persisted("stored autonomy JSON is incompatible with the current contract"))
}

fn invalid_request(detail: &'static str) -> ControlPlaneError {
    ControlPlaneError::validation("invalid_autonomy_request", detail)
}

fn invalid_persisted(detail: &'static str) -> ControlPlaneError {
    ControlPlaneError::validation("invalid_persisted_autonomy", detail)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::observation_window_elapsed;

    #[test]
    fn observation_window_is_independent_from_sample_count() {
        let created_at = chrono::Utc
            .with_ymd_and_hms(2026, 7, 1, 0, 0, 0)
            .single()
            .expect("valid timestamp");

        assert!(!observation_window_elapsed(
            created_at,
            7,
            created_at + chrono::Duration::days(6),
        ));
        assert!(observation_window_elapsed(
            created_at,
            7,
            created_at + chrono::Duration::days(7),
        ));
    }
}
