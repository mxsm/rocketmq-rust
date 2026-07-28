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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::ModelProfileId;
use rocketmq_sre_contracts::TenantId;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use super::lifecycle::ModelProfileLifecycleState;
use super::lifecycle::ProviderSmokeResultView;
use crate::ControlPlaneError;
use crate::PostgresRepository;

const MAX_SMOKE_SNAPSHOT_BYTES: usize = 64 * 1024;
const MAX_FAILURE_CODES: usize = 16;

pub(super) struct PersistProviderSmokeResult {
    pub(super) connectivity_ok: bool,
    pub(super) structured_output_ok: bool,
    pub(super) tool_arguments_ok: bool,
    pub(super) evidence_citation_ok: bool,
    pub(super) latency_ms: Option<u64>,
    pub(super) failure_codes: Vec<String>,
    pub(super) result_snapshot: Value,
    pub(super) observed_at: DateTime<Utc>,
}

impl PersistProviderSmokeResult {
    pub(super) const fn overall_ok(&self) -> bool {
        self.connectivity_ok && self.structured_output_ok && self.tool_arguments_ok && self.evidence_citation_ok
    }

    fn validate(&self) -> Result<(), ControlPlaneError> {
        if self.failure_codes.len() > MAX_FAILURE_CODES
            || self.failure_codes.iter().any(|code| {
                code.is_empty()
                    || code.len() > 128
                    || !code
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
            })
        {
            return Err(ControlPlaneError::validation(
                "invalid_provider_smoke_result",
                "provider smoke failure codes must be bounded safe identifiers",
            ));
        }
        let snapshot_bytes = serde_json::to_vec(&self.result_snapshot)
            .map_err(|_| ControlPlaneError::validation("invalid_provider_smoke_result", "smoke snapshot is invalid"))?;
        if snapshot_bytes.len() > MAX_SMOKE_SNAPSHOT_BYTES {
            return Err(ControlPlaneError::validation(
                "invalid_provider_smoke_result",
                "provider smoke snapshot exceeds the 64 KiB bound",
            ));
        }
        Ok(())
    }
}

impl PostgresRepository {
    pub(super) async fn model_profile_tenants(&self) -> Result<Vec<TenantId>, ControlPlaneError> {
        let rows = sqlx::query_scalar::<_, Uuid>(
            "SELECT DISTINCT tenant_id
             FROM model_profiles
             ORDER BY tenant_id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(TenantId::from_uuid).collect())
    }

    pub(super) async fn model_profiles_due_smoke(
        &self,
        tenant_id: TenantId,
        due_before: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ModelProfileId>, ControlPlaneError> {
        self.ensure_model_profile_lifecycles(tenant_id).await?;
        let rows = sqlx::query_scalar::<_, Uuid>(
            "SELECT profile.id
             FROM model_profiles profile
             JOIN model_profile_lifecycle lifecycle
               ON lifecycle.profile_id = profile.id
              AND lifecycle.tenant_id = profile.tenant_id
             LEFT JOIN LATERAL (
                SELECT observed_at
                FROM provider_smoke_results
                WHERE tenant_id = profile.tenant_id
                  AND profile_id = profile.id
                ORDER BY observed_at DESC, sequence_id DESC
                LIMIT 1
             ) smoke ON TRUE
             WHERE profile.tenant_id = $1
               AND profile.enabled = TRUE
               AND lifecycle.state <> 'retired'
               AND (smoke.observed_at IS NULL OR smoke.observed_at <= $2)
             ORDER BY smoke.observed_at NULLS FIRST, profile.priority, profile.id
             LIMIT $3",
        )
        .bind(tenant_id.as_uuid())
        .bind(due_before)
        .bind(i64::from(limit.clamp(1, 64)))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ModelProfileId::from_uuid).collect())
    }

    pub(super) async fn persist_provider_smoke_result(
        &self,
        tenant_id: TenantId,
        profile_id: ModelProfileId,
        result: &PersistProviderSmokeResult,
        changed_by: &str,
        correlation_id: CorrelationId,
    ) -> Result<ProviderSmokeResultView, ControlPlaneError> {
        result.validate()?;
        self.ensure_model_profile_lifecycles(tenant_id).await?;
        let id = Uuid::new_v4();
        let mut transaction = self.pool.begin().await?;
        let profile_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM model_profiles
                WHERE tenant_id = $1 AND id = $2
            )",
        )
        .bind(tenant_id.as_uuid())
        .bind(profile_id.as_uuid())
        .fetch_one(&mut *transaction)
        .await?;
        if !profile_exists {
            return Err(ControlPlaneError::NotFound);
        }
        sqlx::query(
            "INSERT INTO provider_smoke_results (
                id, tenant_id, profile_id, connectivity_ok,
                structured_output_ok, tool_arguments_ok,
                evidence_citation_ok, latency_ms,
                result_snapshot, observed_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(id)
        .bind(tenant_id.as_uuid())
        .bind(profile_id.as_uuid())
        .bind(result.connectivity_ok)
        .bind(result.structured_output_ok)
        .bind(result.tool_arguments_ok)
        .bind(result.evidence_citation_ok)
        .bind(result.latency_ms.map(i64::try_from).transpose().map_err(|_| {
            ControlPlaneError::validation("invalid_provider_smoke_result", "provider smoke latency exceeds bounds")
        })?)
        .bind(&result.result_snapshot)
        .bind(result.observed_at)
        .execute(&mut *transaction)
        .await?;

        if result.overall_ok() {
            sqlx::query(
                "UPDATE model_profiles profile
                 SET health = 'healthy', updated_at = NOW()
                 FROM model_profile_lifecycle lifecycle
                 WHERE profile.id = lifecycle.profile_id
                   AND profile.tenant_id = lifecycle.tenant_id
                   AND profile.tenant_id = $1
                   AND profile.id = $2
                   AND lifecycle.state IN ('draft', 'certified', 'promoted')",
            )
            .bind(tenant_id.as_uuid())
            .bind(profile_id.as_uuid())
            .execute(&mut *transaction)
            .await?;
        } else {
            sqlx::query(
                "UPDATE model_profiles
                 SET health = 'quarantined', updated_at = NOW()
                 WHERE tenant_id = $1 AND id = $2 AND enabled = TRUE",
            )
            .bind(tenant_id.as_uuid())
            .bind(profile_id.as_uuid())
            .execute(&mut *transaction)
            .await?;
            auto_quarantine_lifecycle(&mut transaction, tenant_id, profile_id, changed_by, correlation_id).await?;
        }
        transaction.commit().await?;

        Ok(ProviderSmokeResultView {
            id,
            profile_id,
            connectivity_ok: result.connectivity_ok,
            structured_output_ok: result.structured_output_ok,
            tool_arguments_ok: result.tool_arguments_ok,
            evidence_citation_ok: result.evidence_citation_ok,
            overall_ok: result.overall_ok(),
            latency_ms: result.latency_ms,
            failure_codes: result.failure_codes.clone(),
            result_snapshot: result.result_snapshot.clone(),
            observed_at: result.observed_at,
        })
    }
}

async fn auto_quarantine_lifecycle(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: TenantId,
    profile_id: ModelProfileId,
    changed_by: &str,
    correlation_id: CorrelationId,
) -> Result<(), ControlPlaneError> {
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
    let state = ModelProfileLifecycleState::parse(row.try_get("state")?).map_err(ControlPlaneError::configuration)?;
    if matches!(
        state,
        ModelProfileLifecycleState::Quarantined | ModelProfileLifecycleState::Retired
    ) {
        return Ok(());
    }
    let revision = u64::try_from(row.try_get::<i64, _>("revision")?)
        .ok()
        .and_then(|revision| revision.checked_add(1))
        .ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "model_lifecycle_revision_exhausted",
                "model profile lifecycle revision cannot advance",
            )
        })?;
    let rollback_profile_id = row.try_get::<Option<Uuid>, _>("rollback_profile_id")?;
    sqlx::query(
        "UPDATE model_profile_lifecycle
         SET state = 'quarantined', revision = $1,
             reason_code = 'provider_smoke_failed',
             operator_confirmed = FALSE, updated_by = $2, updated_at = NOW()
         WHERE tenant_id = $3 AND profile_id = $4",
    )
    .bind(i64::try_from(revision).map_err(|_| {
        ControlPlaneError::conflict_code(
            "model_lifecycle_revision_exhausted",
            "model profile lifecycle revision exceeds PostgreSQL bounds",
        )
    })?)
    .bind(changed_by)
    .bind(tenant_id.as_uuid())
    .bind(profile_id.as_uuid())
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO model_profile_lifecycle_events (
            id, tenant_id, profile_id, from_state, to_state, revision,
            rollback_profile_id, reason_code, operator_confirmed,
            changed_by, correlation_id, observed_at
         )
         VALUES (
            $1, $2, $3, $4, 'quarantined', $5, $6,
            'provider_smoke_failed', FALSE, $7, $8, NOW()
         )",
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id.as_uuid())
    .bind(profile_id.as_uuid())
    .bind(state.as_str())
    .bind(i64::try_from(revision).map_err(|_| {
        ControlPlaneError::conflict_code(
            "model_lifecycle_revision_exhausted",
            "model profile lifecycle revision exceeds PostgreSQL bounds",
        )
    })?)
    .bind(rollback_profile_id)
    .bind(changed_by)
    .bind(correlation_id.as_uuid())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_smoke_result_bounds_failure_codes_and_snapshot() {
        let valid = PersistProviderSmokeResult {
            connectivity_ok: false,
            structured_output_ok: false,
            tool_arguments_ok: false,
            evidence_citation_ok: false,
            latency_ms: Some(12),
            failure_codes: vec!["provider_timeout".to_owned()],
            result_snapshot: serde_json::json!({"failure_codes": ["provider_timeout"]}),
            observed_at: Utc::now(),
        };
        assert!(valid.validate().is_ok());

        let mut invalid = valid;
        invalid.failure_codes = vec!["contains whitespace".to_owned()];
        assert!(invalid.validate().is_err());
    }
}
