// Copyright 2023 The RocketMQ Rust Authors
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

use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::ModelProfileId;
use rocketmq_sre_contracts::TenantId;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::lifecycle::ModelProfileLifecycleState;
use super::lifecycle::ModelProfileLifecycleTransitionRequest;
use super::lifecycle::ModelProfileLifecycleView;
use super::lifecycle::ProviderSmokeResultView;
use crate::ControlPlaneError;
use crate::PostgresRepository;

impl PostgresRepository {
    pub(super) async fn ensure_model_profile_lifecycles(&self, tenant_id: TenantId) -> Result<(), ControlPlaneError> {
        let profile_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT profile.id
             FROM model_profiles profile
             LEFT JOIN model_profile_lifecycle lifecycle
               ON lifecycle.profile_id = profile.id
             WHERE profile.tenant_id = $1
               AND lifecycle.profile_id IS NULL
             ORDER BY profile.id",
        )
        .bind(tenant_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        if profile_ids.is_empty() {
            return Ok(());
        }

        let mut transaction = self.pool.begin().await?;
        for profile_id in profile_ids {
            let inserted = sqlx::query(
                "INSERT INTO model_profile_lifecycle (
                    profile_id, tenant_id, state, revision, rollback_profile_id,
                    reason_code, operator_confirmed, updated_by, updated_at
                 )
                 VALUES ($1, $2, 'draft', 1, NULL, 'profile_registered', FALSE, 'system', NOW())
                 ON CONFLICT (profile_id) DO NOTHING",
            )
            .bind(profile_id)
            .bind(tenant_id.as_uuid())
            .execute(&mut *transaction)
            .await?;
            if inserted.rows_affected() == 1 {
                append_lifecycle_event(
                    &mut transaction,
                    tenant_id,
                    ModelProfileId::from_uuid(profile_id),
                    None,
                    ModelProfileLifecycleState::Draft,
                    1,
                    None,
                    "profile_registered",
                    false,
                    "system",
                    CorrelationId::new(),
                )
                .await?;
            }
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn model_profile_lifecycles(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<ModelProfileLifecycleView>, ControlPlaneError> {
        self.ensure_model_profile_lifecycles(tenant_id).await?;
        let rows = sqlx::query(lifecycle_projection_query(false))
            .bind(tenant_id.as_uuid())
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(lifecycle_from_row).collect()
    }

    pub(super) async fn model_profile_lifecycle(
        &self,
        tenant_id: TenantId,
        profile_id: ModelProfileId,
    ) -> Result<ModelProfileLifecycleView, ControlPlaneError> {
        self.ensure_model_profile_lifecycles(tenant_id).await?;
        let row = sqlx::query(lifecycle_projection_query(true))
            .bind(tenant_id.as_uuid())
            .bind(profile_id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
        lifecycle_from_row(&row)
    }

    pub(super) async fn transition_model_profile_lifecycle(
        &self,
        tenant_id: TenantId,
        profile_id: ModelProfileId,
        request: &ModelProfileLifecycleTransitionRequest,
        changed_by: &str,
        correlation_id: CorrelationId,
    ) -> Result<(), ControlPlaneError> {
        self.ensure_model_profile_lifecycles(tenant_id).await?;
        let mut transaction = self.pool.begin().await?;
        let current = lock_lifecycle(&mut transaction, tenant_id, profile_id).await?;
        ensure_expected_revision(current.revision, request.expected_revision)?;
        if current.state == request.target_state {
            transaction.commit().await?;
            return Ok(());
        }
        if !current.state.permits_operator_transition_to(request.target_state) {
            return Err(ControlPlaneError::conflict_code(
                "invalid_model_lifecycle_transition",
                "model profile lifecycle transition is not allowed",
            ));
        }
        if request.rollback_profile_id.is_some() && request.target_state != ModelProfileLifecycleState::Promoted {
            return Err(ControlPlaneError::validation(
                "invalid_model_lifecycle_transition",
                "rollback_profile_id is only accepted when promoting a profile",
            ));
        }
        if matches!(
            request.target_state,
            ModelProfileLifecycleState::Certified | ModelProfileLifecycleState::Promoted
        ) && !latest_smoke_passed(&mut transaction, tenant_id, profile_id).await?
        {
            return Err(ControlPlaneError::conflict_code(
                "provider_smoke_required",
                "a passing provider smoke result is required for certification or promotion",
            ));
        }

        if let Some(rollback_profile_id) = request.rollback_profile_id {
            validate_and_prepare_rollback_target(
                &mut transaction,
                tenant_id,
                profile_id,
                rollback_profile_id,
                changed_by,
                correlation_id,
            )
            .await?;
        }

        let next_revision = current.revision.checked_add(1).ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "model_lifecycle_revision_exhausted",
                "model profile lifecycle revision cannot advance",
            )
        })?;
        update_lifecycle(
            &mut transaction,
            tenant_id,
            profile_id,
            request.target_state,
            next_revision,
            request.rollback_profile_id,
            &request.reason_code,
            true,
            changed_by,
        )
        .await?;
        update_profile_routing(&mut transaction, tenant_id, profile_id, request.target_state).await?;
        append_lifecycle_event(
            &mut transaction,
            tenant_id,
            profile_id,
            Some(current.state),
            request.target_state,
            next_revision,
            request.rollback_profile_id,
            &request.reason_code,
            true,
            changed_by,
            correlation_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn rollback_model_profile(
        &self,
        tenant_id: TenantId,
        profile_id: ModelProfileId,
        expected_revision: u64,
        reason_code: &str,
        changed_by: &str,
        correlation_id: CorrelationId,
    ) -> Result<ModelProfileId, ControlPlaneError> {
        self.ensure_model_profile_lifecycles(tenant_id).await?;
        let mut transaction = self.pool.begin().await?;
        let current = lock_lifecycle(&mut transaction, tenant_id, profile_id).await?;
        ensure_expected_revision(current.revision, expected_revision)?;
        if current.state != ModelProfileLifecycleState::Promoted {
            return Err(ControlPlaneError::conflict_code(
                "invalid_model_lifecycle_transition",
                "only a promoted model profile can be rolled back",
            ));
        }
        let rollback_profile_id = current.rollback_profile_id.ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "model_rollback_target_missing",
                "the promoted model profile has no rollback target",
            )
        })?;
        let rollback = lock_lifecycle(&mut transaction, tenant_id, rollback_profile_id).await?;
        if !matches!(
            rollback.state,
            ModelProfileLifecycleState::Certified | ModelProfileLifecycleState::Promoted
        ) || !latest_smoke_passed(&mut transaction, tenant_id, rollback_profile_id).await?
        {
            return Err(ControlPlaneError::conflict_code(
                "model_rollback_target_unavailable",
                "the rollback model profile is not certified by a current passing smoke result",
            ));
        }

        let current_revision = next_revision(current.revision)?;
        update_lifecycle(
            &mut transaction,
            tenant_id,
            profile_id,
            ModelProfileLifecycleState::Quarantined,
            current_revision,
            Some(rollback_profile_id),
            reason_code,
            true,
            changed_by,
        )
        .await?;
        update_profile_routing(
            &mut transaction,
            tenant_id,
            profile_id,
            ModelProfileLifecycleState::Quarantined,
        )
        .await?;
        append_lifecycle_event(
            &mut transaction,
            tenant_id,
            profile_id,
            Some(current.state),
            ModelProfileLifecycleState::Quarantined,
            current_revision,
            Some(rollback_profile_id),
            reason_code,
            true,
            changed_by,
            correlation_id,
        )
        .await?;

        let rollback_revision = next_revision(rollback.revision)?;
        update_lifecycle(
            &mut transaction,
            tenant_id,
            rollback_profile_id,
            ModelProfileLifecycleState::Promoted,
            rollback_revision,
            Some(profile_id),
            reason_code,
            true,
            changed_by,
        )
        .await?;
        update_profile_routing(
            &mut transaction,
            tenant_id,
            rollback_profile_id,
            ModelProfileLifecycleState::Promoted,
        )
        .await?;
        append_lifecycle_event(
            &mut transaction,
            tenant_id,
            rollback_profile_id,
            Some(rollback.state),
            ModelProfileLifecycleState::Promoted,
            rollback_revision,
            Some(profile_id),
            reason_code,
            true,
            changed_by,
            correlation_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(rollback_profile_id)
    }
}

#[derive(Clone, Copy)]
struct LockedLifecycle {
    state: ModelProfileLifecycleState,
    revision: u64,
    rollback_profile_id: Option<ModelProfileId>,
}

async fn lock_lifecycle(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    profile_id: ModelProfileId,
) -> Result<LockedLifecycle, ControlPlaneError> {
    let row = sqlx::query(
        "SELECT state, revision, rollback_profile_id
         FROM model_profile_lifecycle
         WHERE tenant_id = $1 AND profile_id = $2
         FOR UPDATE",
    )
    .bind(tenant_id.as_uuid())
    .bind(profile_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ControlPlaneError::NotFound)?;
    Ok(LockedLifecycle {
        state: parse_state(row.try_get("state")?)?,
        revision: positive_revision(row.try_get("revision")?)?,
        rollback_profile_id: row
            .try_get::<Option<Uuid>, _>("rollback_profile_id")?
            .map(ModelProfileId::from_uuid),
    })
}

async fn validate_and_prepare_rollback_target(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    promoted_profile_id: ModelProfileId,
    rollback_profile_id: ModelProfileId,
    changed_by: &str,
    correlation_id: CorrelationId,
) -> Result<(), ControlPlaneError> {
    if promoted_profile_id == rollback_profile_id {
        return Err(ControlPlaneError::validation(
            "invalid_model_rollback_target",
            "a model profile cannot use itself as a rollback target",
        ));
    }
    let rollback = lock_lifecycle(transaction, tenant_id, rollback_profile_id).await?;
    if !matches!(
        rollback.state,
        ModelProfileLifecycleState::Certified | ModelProfileLifecycleState::Promoted
    ) || !latest_smoke_passed(transaction, tenant_id, rollback_profile_id).await?
    {
        return Err(ControlPlaneError::conflict_code(
            "model_rollback_target_unavailable",
            "rollback target must be certified or promoted with a passing smoke result",
        ));
    }
    if rollback.state == ModelProfileLifecycleState::Promoted {
        let revision = next_revision(rollback.revision)?;
        update_lifecycle(
            transaction,
            tenant_id,
            rollback_profile_id,
            ModelProfileLifecycleState::Certified,
            revision,
            rollback.rollback_profile_id,
            "superseded_by_profile",
            true,
            changed_by,
        )
        .await?;
        append_lifecycle_event(
            transaction,
            tenant_id,
            rollback_profile_id,
            Some(ModelProfileLifecycleState::Promoted),
            ModelProfileLifecycleState::Certified,
            revision,
            rollback.rollback_profile_id,
            "superseded_by_profile",
            true,
            changed_by,
            correlation_id,
        )
        .await?;
    }
    Ok(())
}

async fn latest_smoke_passed(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    profile_id: ModelProfileId,
) -> Result<bool, ControlPlaneError> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT connectivity_ok
                AND structured_output_ok
                AND tool_arguments_ok
                AND evidence_citation_ok
         FROM provider_smoke_results
         WHERE tenant_id = $1 AND profile_id = $2
         ORDER BY observed_at DESC, sequence_id DESC
         LIMIT 1",
    )
    .bind(tenant_id.as_uuid())
    .bind(profile_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await?
    .unwrap_or(false))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the lifecycle projection mirrors one audited database row"
)]
async fn update_lifecycle(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    profile_id: ModelProfileId,
    state: ModelProfileLifecycleState,
    revision: u64,
    rollback_profile_id: Option<ModelProfileId>,
    reason_code: &str,
    operator_confirmed: bool,
    changed_by: &str,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "UPDATE model_profile_lifecycle
         SET state = $1, revision = $2, rollback_profile_id = $3,
             reason_code = $4, operator_confirmed = $5,
             updated_by = $6, updated_at = NOW()
         WHERE tenant_id = $7 AND profile_id = $8",
    )
    .bind(state.as_str())
    .bind(revision_i64(revision)?)
    .bind(rollback_profile_id.map(|id| id.as_uuid()))
    .bind(reason_code)
    .bind(operator_confirmed)
    .bind(changed_by)
    .bind(tenant_id.as_uuid())
    .bind(profile_id.as_uuid())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_profile_routing(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    profile_id: ModelProfileId,
    state: ModelProfileLifecycleState,
) -> Result<(), ControlPlaneError> {
    let (enabled, health) = match state {
        ModelProfileLifecycleState::Draft => (true, "unknown"),
        ModelProfileLifecycleState::Certified | ModelProfileLifecycleState::Promoted => (true, "healthy"),
        ModelProfileLifecycleState::Quarantined => (true, "quarantined"),
        ModelProfileLifecycleState::Retired => (false, "disabled"),
    };
    sqlx::query(
        "UPDATE model_profiles
         SET enabled = $1, health = $2, updated_at = NOW()
         WHERE tenant_id = $3 AND id = $4",
    )
    .bind(enabled)
    .bind(health)
    .bind(tenant_id.as_uuid())
    .bind(profile_id.as_uuid())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the append-only event records the complete lifecycle transition"
)]
async fn append_lifecycle_event(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    profile_id: ModelProfileId,
    from_state: Option<ModelProfileLifecycleState>,
    to_state: ModelProfileLifecycleState,
    revision: u64,
    rollback_profile_id: Option<ModelProfileId>,
    reason_code: &str,
    operator_confirmed: bool,
    changed_by: &str,
    correlation_id: CorrelationId,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO model_profile_lifecycle_events (
            id, tenant_id, profile_id, from_state, to_state, revision,
            rollback_profile_id, reason_code, operator_confirmed,
            changed_by, correlation_id, observed_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())",
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id.as_uuid())
    .bind(profile_id.as_uuid())
    .bind(from_state.map(ModelProfileLifecycleState::as_str))
    .bind(to_state.as_str())
    .bind(revision_i64(revision)?)
    .bind(rollback_profile_id.map(|id| id.as_uuid()))
    .bind(reason_code)
    .bind(operator_confirmed)
    .bind(changed_by)
    .bind(correlation_id.as_uuid())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn lifecycle_projection_query(filter_by_profile: bool) -> sqlx::AssertSqlSafe<String> {
    let profile_filter = if filter_by_profile { " AND profile.id = $2" } else { "" };
    // The optional clause is selected from static SQL; request values remain bind parameters.
    sqlx::AssertSqlSafe(format!(
        "SELECT profile.id, profile.profile_name, profile.provider_family,
                profile.model_family, profile.model_revision, profile.health,
                lifecycle.state, lifecycle.revision, lifecycle.rollback_profile_id,
                lifecycle.reason_code, lifecycle.operator_confirmed,
                lifecycle.updated_by, lifecycle.updated_at,
                smoke.id AS smoke_id,
                smoke.connectivity_ok, smoke.structured_output_ok,
                smoke.tool_arguments_ok, smoke.evidence_citation_ok,
                smoke.latency_ms, smoke.result_snapshot, smoke.observed_at
         FROM model_profiles profile
         JOIN model_profile_lifecycle lifecycle
           ON lifecycle.profile_id = profile.id
          AND lifecycle.tenant_id = profile.tenant_id
         LEFT JOIN LATERAL (
            SELECT id, connectivity_ok, structured_output_ok,
                   tool_arguments_ok, evidence_citation_ok,
                   latency_ms, result_snapshot, observed_at
            FROM provider_smoke_results
            WHERE tenant_id = profile.tenant_id
              AND profile_id = profile.id
            ORDER BY observed_at DESC, sequence_id DESC
            LIMIT 1
         ) smoke ON TRUE
         WHERE profile.tenant_id = $1{profile_filter}
         ORDER BY profile.priority, profile.profile_name"
    ))
}

fn lifecycle_from_row(row: &PgRow) -> Result<ModelProfileLifecycleView, ControlPlaneError> {
    let profile_id = ModelProfileId::from_uuid(row.try_get("id")?);
    let state = parse_state(row.try_get("state")?)?;
    let latest_smoke = smoke_from_row(row, profile_id)?;
    let health: String = row.try_get("health")?;
    let automation_eligible = state == ModelProfileLifecycleState::Promoted
        && latest_smoke.as_ref().is_some_and(|smoke| smoke.overall_ok)
        && matches!(health.as_str(), "healthy" | "degraded");
    Ok(ModelProfileLifecycleView {
        profile_id,
        profile_name: row.try_get("profile_name")?,
        provider_family: row.try_get("provider_family")?,
        model_family: row.try_get("model_family")?,
        model_revision: row.try_get("model_revision")?,
        state,
        revision: positive_revision(row.try_get("revision")?)?,
        rollback_profile_id: row
            .try_get::<Option<Uuid>, _>("rollback_profile_id")?
            .map(ModelProfileId::from_uuid),
        reason_code: row.try_get("reason_code")?,
        operator_confirmed: row.try_get("operator_confirmed")?,
        updated_by: row.try_get("updated_by")?,
        updated_at: row.try_get("updated_at")?,
        latest_smoke,
        automation_eligible,
    })
}

fn smoke_from_row(
    row: &PgRow,
    profile_id: ModelProfileId,
) -> Result<Option<ProviderSmokeResultView>, ControlPlaneError> {
    let Some(id) = row.try_get::<Option<Uuid>, _>("smoke_id")? else {
        return Ok(None);
    };
    let connectivity_ok = row.try_get("connectivity_ok")?;
    let structured_output_ok = row.try_get("structured_output_ok")?;
    let tool_arguments_ok = row.try_get("tool_arguments_ok")?;
    let evidence_citation_ok = row.try_get("evidence_citation_ok")?;
    let result_snapshot: Value = row.try_get("result_snapshot")?;
    Ok(Some(ProviderSmokeResultView {
        id,
        profile_id,
        connectivity_ok,
        structured_output_ok,
        tool_arguments_ok,
        evidence_citation_ok,
        overall_ok: connectivity_ok && structured_output_ok && tool_arguments_ok && evidence_citation_ok,
        latency_ms: row
            .try_get::<Option<i64>, _>("latency_ms")?
            .map(non_negative_u64)
            .transpose()?,
        failure_codes: result_snapshot
            .get("failure_codes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .take(16)
            .map(str::to_owned)
            .collect(),
        result_snapshot,
        observed_at: row.try_get("observed_at")?,
    }))
}

fn ensure_expected_revision(actual: u64, expected: u64) -> Result<(), ControlPlaneError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ControlPlaneError::conflict_code(
            "model_lifecycle_revision_mismatch",
            "model profile lifecycle changed while the operation was being confirmed",
        ))
    }
}

fn parse_state(value: &str) -> Result<ModelProfileLifecycleState, ControlPlaneError> {
    ModelProfileLifecycleState::parse(value).map_err(ControlPlaneError::configuration)
}

fn positive_revision(value: i64) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| ControlPlaneError::configuration("stored model lifecycle revision is invalid"))
}

fn revision_i64(value: u64) -> Result<i64, ControlPlaneError> {
    i64::try_from(value).map_err(|_| {
        ControlPlaneError::conflict_code(
            "model_lifecycle_revision_exhausted",
            "model profile lifecycle revision exceeds PostgreSQL bounds",
        )
    })
}

fn next_revision(value: u64) -> Result<u64, ControlPlaneError> {
    value.checked_add(1).ok_or_else(|| {
        ControlPlaneError::conflict_code(
            "model_lifecycle_revision_exhausted",
            "model profile lifecycle revision cannot advance",
        )
    })
}

fn non_negative_u64(value: i64) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| ControlPlaneError::configuration("stored provider smoke latency is invalid"))
}
