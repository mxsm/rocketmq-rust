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
use rocketmq_sre_contracts::AutonomyLifecycleState;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyPolicyDefinition;
use rocketmq_sre_contracts::AutonomyPolicyId;
use rocketmq_sre_contracts::AutonomyQualificationCohort;
use rocketmq_sre_contracts::AutonomyQualificationLevel;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::TenantId;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use super::model::AutonomyFreezeView;
use super::model::AutonomyKillSwitchView;
use super::model::AutonomyQualificationView;
use super::model::AutonomyScopeView;
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
                let current = lifecycle_from_row(
                    &row,
                    definition.tenant_id,
                    definition.cluster_id,
                    definition.action,
                )?;
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
        .bind(i32::try_from(definition.min_shadow_samples).map_err(|_| invalid_request("shadow sample bound is too large"))?)
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

    pub(super) async fn autonomy_scope(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        action_version: &str,
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
        let qualification = self.autonomy_qualification(&policy).await?;
        let active_freezes = self
            .autonomy_active_freezes(tenant_id, cluster_id, action, action_version, Utc::now())
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

    pub(super) async fn autonomy_scopes(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        limit: i64,
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
                self.autonomy_scope(tenant_id, cluster_id, action, &action_version)
                    .await?,
            );
        }
        Ok(scopes)
    }

    pub(super) async fn update_autonomy_lifecycle(
        &self,
        current: &AutonomyLifecycleState,
        next: &AutonomyLifecycleState,
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
        .bind("1.0.0")
        .bind(
            i64::try_from(current.lifecycle_revision)
                .map_err(|_| invalid_request("lifecycle revision is too large"))?,
        )
        .bind(mode_name(next.mode))
        .bind(next.previous_mode.map(mode_name))
        .bind(owner_confirmed)
        .bind(next.updated_at)
        .bind(&next.pause_reason)
        .bind(
            i64::try_from(next.lifecycle_revision)
                .map_err(|_| invalid_request("lifecycle revision is too large"))?,
        )
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
        cluster_id: ClusterId,
        action: Option<ExecutionAction>,
        action_version: Option<&str>,
        active: bool,
        reason: &str,
        starts_at: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
        actor: &str,
    ) -> Result<AutonomyFreezeView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let lock_key = format!(
            "autonomy-freeze:{tenant_id}:{cluster_id}:{}:{}",
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
        .bind(cluster_id.as_uuid())
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
                .bind(cluster_id.as_uuid())
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
            cluster_id: Some(cluster_id),
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

    async fn autonomy_qualification(
        &self,
        policy: &AutonomyPolicyDefinition,
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
        .bind(
            i64::try_from(policy.definition_version)
                .map_err(|_| invalid_request("policy version is too large"))?,
        )
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
        let counts = match autonomous.as_ref().or(shadow.as_ref()) {
            Some(cohort) => sqlx::query(
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
                   AND observed_at >= NOW() - ($2::int * INTERVAL '1 day')",
            )
            .bind(cohort.id.as_uuid())
            .bind(i32::from(policy.observation_window_days))
            .fetch_one(&self.pool)
            .await?
            .try_get::<i64, _>("qualified_shadow")
            .and_then(|shadow_count| {
                Ok((
                    shadow_count,
                    0_i64,
                    0_i64,
                ))
            })?,
            None => (0, 0, 0),
        };
        let (qualified_shadow, unqualified_shadow, supervised_successes) = if let Some(cohort) = autonomous.as_ref() {
            sample_counts(&self.pool, cohort.id, policy.observation_window_days).await?
        } else if let Some(cohort) = shadow.as_ref() {
            sample_counts(&self.pool, cohort.id, policy.observation_window_days).await?
        } else {
            counts
        };
        Ok(AutonomyQualificationView {
            shadow_cohort: shadow,
            autonomous_cohort: autonomous,
            qualified_shadow_samples: count_u32(qualified_shadow)?,
            unqualified_shadow_samples: count_u32(unqualified_shadow)?,
            qualified_supervised_successes: count_u32(supervised_successes)?,
            unresolved_unknown: 0,
            recent_rollbacks: 0,
            shadow_observation_window_met: count_u32(qualified_shadow)? >= policy.min_shadow_samples,
            autonomous_observation_window_met: count_u32(supervised_successes)? >= policy.min_supervised_successes,
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
            $1, $2, $3, $4, '1.0.0',
            $5, $6, $7, $8,
            $9, $10, $11, $12
         )",
    )
    .bind(Uuid::new_v4())
    .bind(state.tenant_id.as_uuid())
    .bind(state.cluster_id.as_uuid())
    .bind(state.action.id())
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

async fn sample_counts(
    pool: &sqlx::PgPool,
    cohort_id: rocketmq_sre_contracts::AutonomyCohortId,
    observation_window_days: u16,
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
           AND observed_at >= NOW() - ($2::int * INTERVAL '1 day')",
    )
    .bind(cohort_id.as_uuid())
    .bind(i32::from(observation_window_days))
    .fetch_one(pool)
    .await?;
    Ok((
        row.try_get("qualified_shadow")?,
        row.try_get("unqualified_shadow")?,
        row.try_get("supervised_successes")?,
    ))
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
        cluster_id: row
            .try_get::<Option<Uuid>, _>("cluster_id")?
            .map(ClusterId::from_uuid),
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
