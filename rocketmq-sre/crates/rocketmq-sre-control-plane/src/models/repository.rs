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

use chrono::Utc;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::ModelInvocationPurpose;
use rocketmq_sre_contracts::ModelInvocationRecord;
use rocketmq_sre_contracts::ModelProfileId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_model_gateway::ProviderHealth;
use rocketmq_sre_model_gateway::ProviderProfile;
use serde::Serialize;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::model::ModelInvocationListQuery;
use super::model::ModelInvocationPage;
use super::model::ModelInvocationView;
use super::model::ModelProfileStatus;
use super::model::PersistInvocation;
use super::model::RuntimeModelProfile;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::observability::DependencyStatus;
use crate::observability::HealthReasonCode;
use crate::observability::ProviderFamilyLabel;
use crate::observability::ProviderHealthSample;

impl PostgresRepository {
    pub(super) async fn ensure_model_profiles(
        &self,
        tenant_id: TenantId,
        profiles: &[ProviderProfile],
    ) -> Result<Vec<RuntimeModelProfile>, ControlPlaneError> {
        let mut configured = Vec::with_capacity(profiles.len());
        for profile in profiles {
            let id = ModelProfileId::new();
            let credential_ref = profile
                .credential_ref
                .as_ref()
                .map_or_else(String::new, |reference| reference.as_reference_uri());
            let row = sqlx::query(
                "INSERT INTO model_profiles (
                    id, tenant_id, profile_name, provider_family, protocol_family,
                    model_family, model_name, model_revision, endpoint_instance,
                    region, data_residency, data_classes, capabilities, priority,
                    credential_ref, credential_owner, enabled, health,
                    endpoint_url, dialect, allowed_data_classes,
                    estimated_cost_microusd_per_1k_tokens,
                    preserve_reasoning_content, kimi_mfjs_enabled,
                    created_at, updated_at
                 ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, $11, $12,
                    $13, $14, 'gateway', TRUE, $15, $16, $17, $11, $18, $19,
                    $20, NOW(), NOW()
                 )
                 ON CONFLICT (tenant_id, profile_name)
                 DO UPDATE SET
                    provider_family = EXCLUDED.provider_family,
                    protocol_family = EXCLUDED.protocol_family,
                    model_family = EXCLUDED.model_family,
                    model_name = EXCLUDED.model_name,
                    model_revision = EXCLUDED.model_revision,
                    endpoint_instance = EXCLUDED.endpoint_instance,
                    region = EXCLUDED.region,
                    data_residency = EXCLUDED.data_residency,
                    data_classes = EXCLUDED.data_classes,
                    capabilities = EXCLUDED.capabilities,
                    priority = EXCLUDED.priority,
                    credential_ref = EXCLUDED.credential_ref,
                    credential_owner = EXCLUDED.credential_owner,
                    enabled = TRUE,
                    endpoint_url = EXCLUDED.endpoint_url,
                    dialect = EXCLUDED.dialect,
                    allowed_data_classes = EXCLUDED.allowed_data_classes,
                    estimated_cost_microusd_per_1k_tokens =
                        EXCLUDED.estimated_cost_microusd_per_1k_tokens,
                    preserve_reasoning_content = EXCLUDED.preserve_reasoning_content,
                    kimi_mfjs_enabled = EXCLUDED.kimi_mfjs_enabled,
                    health = CASE
                        WHEN model_profiles.credential_ref IS DISTINCT FROM EXCLUDED.credential_ref
                          OR model_profiles.endpoint_url IS DISTINCT FROM EXCLUDED.endpoint_url
                          OR model_profiles.model_revision IS DISTINCT FROM EXCLUDED.model_revision
                        THEN 'unknown'
                        ELSE model_profiles.health
                    END,
                    updated_at = NOW()
                 RETURNING id, health",
            )
            .bind(id.as_uuid())
            .bind(tenant_id.as_uuid())
            .bind(&profile.id)
            .bind(enum_name(profile.provider_family)?)
            .bind(enum_name(profile.dialect)?)
            .bind(&profile.model_family)
            .bind(&profile.model)
            .bind(&profile.model_revision)
            .bind(&profile.endpoint_instance)
            .bind(&profile.region)
            .bind(
                serde_json::to_value(&profile.allowed_data_classes)
                    .map_err(|_| ControlPlaneError::configuration("model profile data classes cannot be serialized"))?,
            )
            .bind(
                serde_json::to_value(&profile.capabilities)
                    .map_err(|_| ControlPlaneError::configuration("model profile capabilities cannot be serialized"))?,
            )
            .bind(i32::from(profile.priority))
            .bind(credential_ref)
            .bind(health_name(ProviderHealth::Unknown))
            .bind(&profile.endpoint)
            .bind(enum_name(profile.dialect)?)
            .bind(
                profile
                    .estimated_cost_microusd_per_1k_tokens
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| ControlPlaneError::configuration("model profile cost exceeds PostgreSQL bounds"))?,
            )
            .bind(profile.preserve_reasoning_content)
            .bind(profile.kimi_mfjs_enabled)
            .fetch_one(&self.pool)
            .await?;
            let mut runtime_profile = profile.clone();
            runtime_profile.health = parse_health(row.try_get("health")?)?;
            configured.push(RuntimeModelProfile {
                id: ModelProfileId::from_uuid(row.try_get("id")?),
                profile: runtime_profile,
            });
        }
        Ok(configured)
    }

    pub(super) async fn record_model_health(
        &self,
        tenant_id: TenantId,
        profile: &RuntimeModelProfile,
        health: ProviderHealth,
        credential_version_fingerprint: Option<&str>,
    ) -> Result<(), ControlPlaneError> {
        let health = health_name(health);
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "UPDATE model_profiles
             SET health = $1, credential_version_fingerprint = $2, updated_at = NOW()
             WHERE id = $3 AND tenant_id = $4",
        )
        .bind(health)
        .bind(credential_version_fingerprint)
        .bind(profile.id.as_uuid())
        .bind(tenant_id.as_uuid())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO provider_health_events (
                tenant_id, profile_id, health, capability,
                credential_version_fingerprint, observed_at
             ) VALUES ($1, $2, $3, $4, $5, NOW())",
        )
        .bind(tenant_id.as_uuid())
        .bind(profile.id.as_uuid())
        .bind(health)
        .bind(
            serde_json::to_value(&profile.profile.capabilities)
                .map_err(|_| ControlPlaneError::configuration("model capabilities cannot be serialized"))?,
        )
        .bind(credential_version_fingerprint)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn persist_model_invocation(
        &self,
        invocation: &PersistInvocation,
    ) -> Result<(), ControlPlaneError> {
        let fallback_chain = invocation
            .fallback_chain
            .iter()
            .map(|profile_id| profile_id.as_uuid())
            .collect::<Vec<_>>();
        sqlx::query(
            "INSERT INTO model_invocations (
                id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
                parent_invocation_id, purpose, requested_profile_id,
                actual_profile_id, provider_family, model_family, actual_model,
                model_revision, endpoint_instance, fallback_chain,
                prompt_version, schema_version, input_tokens, output_tokens,
                cost_micros, rationale, error_code, correlation_id,
                started_at, completed_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
                $25
             )",
        )
        .bind(invocation.id.as_uuid())
        .bind(invocation.tenant_id.as_uuid())
        .bind(invocation.cluster_id.as_uuid())
        .bind(invocation.incident_id.as_uuid())
        .bind(invocation.diagnosis_revision_id.map(DiagnosisRevisionId::as_uuid))
        .bind(invocation.parent_invocation_id.map(ModelInvocationId::as_uuid))
        .bind(invocation.purpose)
        .bind(invocation.requested_profile_id.as_uuid())
        .bind(invocation.actual_profile_id.as_uuid())
        .bind(&invocation.provider_family)
        .bind(&invocation.model_family)
        .bind(&invocation.actual_model)
        .bind(&invocation.model_revision)
        .bind(&invocation.endpoint_instance)
        .bind(fallback_chain)
        .bind(invocation.prompt_version)
        .bind(invocation.schema_version)
        .bind(
            invocation
                .input_tokens
                .map(i32::try_from)
                .transpose()
                .map_err(|_| ControlPlaneError::configuration("model input token usage exceeds PostgreSQL bounds"))?,
        )
        .bind(
            invocation
                .output_tokens
                .map(i32::try_from)
                .transpose()
                .map_err(|_| ControlPlaneError::configuration("model output token usage exceeds PostgreSQL bounds"))?,
        )
        .bind(
            invocation
                .cost_micros
                .map(i64::try_from)
                .transpose()
                .map_err(|_| ControlPlaneError::configuration("model invocation cost exceeds PostgreSQL bounds"))?,
        )
        .bind(&invocation.rationale)
        .bind(invocation.error_code.as_deref())
        .bind(invocation.correlation_id.as_uuid())
        .bind(invocation.started_at)
        .bind(invocation.completed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn model_profile_statuses(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<ModelProfileStatus>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT p.id, p.profile_name, p.provider_family, p.protocol_family,
                    p.model_family, p.model_name, p.model_revision,
                    p.endpoint_instance, p.region, p.capabilities, p.priority,
                    p.credential_ref, p.credential_owner, p.health,
                    health.observed_at AS last_health_observed_at
             FROM model_profiles p
             LEFT JOIN LATERAL (
                SELECT observed_at
                FROM provider_health_events
                WHERE tenant_id = p.tenant_id AND profile_id = p.id
                ORDER BY observed_at DESC, sequence_id DESC
                LIMIT 1
             ) health ON TRUE
             WHERE p.tenant_id = $1 AND p.enabled = TRUE
             ORDER BY p.priority, p.profile_name",
        )
        .bind(tenant_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(model_profile_status_from_row).collect()
    }

    pub(super) async fn model_health_samples(
        &self,
        limit: u32,
    ) -> Result<Vec<ProviderHealthSample>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT profile_name, provider_family, health
             FROM model_profiles
             WHERE enabled = TRUE
             ORDER BY updated_at DESC, id
             LIMIT $1",
        )
        .bind(i64::from(limit.clamp(1, 256)))
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let profile_name: String = row.try_get("profile_name")?;
                let provider_family: String = row.try_get("provider_family")?;
                let health: String = row.try_get("health")?;
                let (status, reason) = dependency_health(&health)?;
                Ok(ProviderHealthSample::new(
                    provider_label(&profile_name, &provider_family),
                    status,
                    None,
                    reason,
                ))
            })
            .collect()
    }

    pub(super) async fn list_model_invocations(
        &self,
        auth: &AuthContext,
        query: &ModelInvocationListQuery,
    ) -> Result<ModelInvocationPage, ControlPlaneError> {
        if !auth.clusters.contains(&query.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "model invocation cluster is outside the caller scope",
            ));
        }
        let limit = query.bounded_limit();
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
                    parent_invocation_id, purpose, requested_profile_id, actual_profile_id,
                    provider_family, model_family, actual_model, model_revision,
                    endpoint_instance, fallback_chain, prompt_version,
                    schema_version, input_tokens, output_tokens, cost_micros,
                    rationale, error_code, correlation_id, started_at, completed_at
             FROM model_invocations
             WHERE tenant_id = $1 AND cluster_id = $2
               AND ($3::UUID IS NULL OR incident_id = $3)
             ORDER BY started_at DESC, id DESC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(query.incident_id.map(IncidentId::as_uuid))
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let partial = rows.len() > limit as usize;
        let items = rows
            .iter()
            .take(limit as usize)
            .map(model_invocation_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ModelInvocationPage {
            schema_version: "rocketmq-sre.model-invocations.v1",
            items,
            partial,
            observed_at: Utc::now(),
        })
    }

    pub(crate) async fn exact_primary_model_invocation(
        &self,
        auth: &AuthContext,
        plan: &rocketmq_sre_contracts::ActionPlan,
    ) -> Result<ModelInvocationRecord, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT m.id, m.tenant_id, m.cluster_id, m.incident_id,
                    m.diagnosis_revision_id, m.parent_invocation_id,
                    m.requested_profile_id, m.actual_profile_id,
                    m.provider_family, m.model_family, m.model_revision,
                    m.endpoint_instance, m.fallback_chain, m.prompt_version,
                    m.schema_version, m.input_tokens, m.output_tokens,
                    m.cost_micros, m.rationale, m.started_at, m.completed_at
             FROM model_invocations m
             JOIN diagnosis_revisions d
               ON d.id = m.diagnosis_revision_id
              AND d.incident_id = m.incident_id
              AND d.primary_model_invocation_id = m.id
             JOIN action_plans p
               ON p.id = $1
              AND p.diagnosis_revision_id = d.id
              AND p.primary_model_invocation_id = m.id
             WHERE m.id = $2
               AND m.tenant_id = $3
               AND m.cluster_id = $4
               AND m.incident_id = $5
               AND m.diagnosis_revision_id = $6
               AND m.purpose = 'primary_diagnosis'
               AND m.error_code IS NULL",
        )
        .bind(plan.id.as_uuid())
        .bind(plan.primary_model_invocation_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(plan.cluster_id.as_uuid())
        .bind(plan.incident_id.as_uuid())
        .bind(plan.diagnosis_revision.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "primary_invocation_mismatch",
                "plan primary invocation does not match the exact confirmed diagnosis revision",
            )
        })?;
        contract_model_invocation_from_row(&row)
    }
}

fn enum_name(value: impl Serialize) -> Result<String, ControlPlaneError> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or_else(|| ControlPlaneError::configuration("model profile enum cannot be serialized"))
}

const fn health_name(health: ProviderHealth) -> &'static str {
    match health {
        ProviderHealth::Unknown => "unknown",
        ProviderHealth::Healthy => "healthy",
        ProviderHealth::Degraded => "degraded",
        ProviderHealth::Unavailable => "unavailable",
        ProviderHealth::Quarantined => "quarantined",
    }
}

fn parse_health(value: &str) -> Result<ProviderHealth, ControlPlaneError> {
    match value {
        "unknown" => Ok(ProviderHealth::Unknown),
        "healthy" => Ok(ProviderHealth::Healthy),
        "degraded" => Ok(ProviderHealth::Degraded),
        "unavailable" | "disabled" => Ok(ProviderHealth::Unavailable),
        "quarantined" => Ok(ProviderHealth::Quarantined),
        _ => Err(ControlPlaneError::configuration(
            "stored model provider health is invalid",
        )),
    }
}

fn dependency_health(value: &str) -> Result<(DependencyStatus, Option<HealthReasonCode>), ControlPlaneError> {
    match value {
        "healthy" => Ok((DependencyStatus::Healthy, None)),
        "unknown" => Ok((DependencyStatus::Unknown, Some(HealthReasonCode::Unknown))),
        "degraded" => Ok((DependencyStatus::Degraded, Some(HealthReasonCode::Unknown))),
        "unavailable" | "disabled" => Ok((DependencyStatus::Unavailable, Some(HealthReasonCode::ConnectionFailed))),
        "quarantined" => Ok((
            DependencyStatus::Unavailable,
            Some(HealthReasonCode::AuthenticationFailed),
        )),
        _ => Err(ControlPlaneError::configuration(
            "stored model provider health is invalid",
        )),
    }
}

pub(super) fn provider_label(profile_name: &str, provider_family: &str) -> ProviderFamilyLabel {
    let normalized = profile_name.to_ascii_lowercase();
    if normalized.contains("deepseek") {
        ProviderFamilyLabel::DeepSeek
    } else if normalized.contains("zhipu") || normalized.contains("glm") {
        ProviderFamilyLabel::ZhipuGlm
    } else if normalized.contains("kimi") || normalized.contains("moonshot") {
        ProviderFamilyLabel::MoonshotKimi
    } else if ["vllm", "ollama", "llama", "sglang", "local"]
        .iter()
        .any(|marker| normalized.contains(marker))
    {
        ProviderFamilyLabel::Local
    } else {
        match provider_family {
            "open_ai_compatible" => ProviderFamilyLabel::OpenAiCompatible,
            "anthropic" => ProviderFamilyLabel::Anthropic,
            "gemini" => ProviderFamilyLabel::Gemini,
            "bedrock" => ProviderFamilyLabel::Bedrock,
            "provider_spi" => ProviderFamilyLabel::Spi,
            _ => ProviderFamilyLabel::Other,
        }
    }
}

fn model_profile_status_from_row(row: &PgRow) -> Result<ModelProfileStatus, ControlPlaneError> {
    let credential_ref: String = row.try_get("credential_ref")?;
    Ok(ModelProfileStatus {
        id: ModelProfileId::from_uuid(row.try_get("id")?),
        profile_name: row.try_get("profile_name")?,
        provider_family: row.try_get("provider_family")?,
        protocol_family: row.try_get("protocol_family")?,
        model_family: row.try_get("model_family")?,
        model_name: row.try_get("model_name")?,
        model_revision: row.try_get("model_revision")?,
        endpoint_instance: row.try_get("endpoint_instance")?,
        region: row.try_get("region")?,
        capabilities: row.try_get("capabilities")?,
        priority: u16::try_from(row.try_get::<i32, _>("priority")?)
            .map_err(|_| ControlPlaneError::configuration("stored model priority is invalid"))?,
        credential_configured: !credential_ref.is_empty(),
        credential_owner: row.try_get("credential_owner")?,
        health: row.try_get("health")?,
        last_health_observed_at: row.try_get("last_health_observed_at")?,
    })
}

fn model_invocation_from_row(row: &PgRow) -> Result<ModelInvocationView, ControlPlaneError> {
    let fallback_chain = row
        .try_get::<Vec<Uuid>, _>("fallback_chain")?
        .into_iter()
        .map(ModelProfileId::from_uuid)
        .collect();
    Ok(ModelInvocationView {
        id: ModelInvocationId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: rocketmq_sre_contracts::ClusterId::from_uuid(row.try_get("cluster_id")?),
        incident_id: row
            .try_get::<Option<Uuid>, _>("incident_id")?
            .map(IncidentId::from_uuid),
        diagnosis_revision_id: row
            .try_get::<Option<Uuid>, _>("diagnosis_revision_id")?
            .map(DiagnosisRevisionId::from_uuid),
        parent_invocation_id: row
            .try_get::<Option<Uuid>, _>("parent_invocation_id")?
            .map(ModelInvocationId::from_uuid),
        purpose: row.try_get("purpose")?,
        requested_profile_id: ModelProfileId::from_uuid(row.try_get("requested_profile_id")?),
        actual_profile_id: ModelProfileId::from_uuid(row.try_get("actual_profile_id")?),
        provider_family: row.try_get("provider_family")?,
        model_family: row.try_get("model_family")?,
        actual_model: row.try_get("actual_model")?,
        model_revision: row.try_get("model_revision")?,
        endpoint_instance: row.try_get("endpoint_instance")?,
        fallback_chain,
        prompt_version: row.try_get("prompt_version")?,
        schema_version: row.try_get("schema_version")?,
        input_tokens: row
            .try_get::<Option<i32>, _>("input_tokens")?
            .map(u32::try_from)
            .transpose()
            .map_err(|_| ControlPlaneError::configuration("stored input token usage is invalid"))?,
        output_tokens: row
            .try_get::<Option<i32>, _>("output_tokens")?
            .map(u32::try_from)
            .transpose()
            .map_err(|_| ControlPlaneError::configuration("stored output token usage is invalid"))?,
        cost_micros: row
            .try_get::<Option<i64>, _>("cost_micros")?
            .map(u64::try_from)
            .transpose()
            .map_err(|_| ControlPlaneError::configuration("stored model cost is invalid"))?,
        rationale: row.try_get("rationale")?,
        error_code: row.try_get("error_code")?,
        correlation_id: row
            .try_get::<Option<Uuid>, _>("correlation_id")?
            .map(CorrelationId::from_uuid),
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn contract_model_invocation_from_row(row: &PgRow) -> Result<ModelInvocationRecord, ControlPlaneError> {
    Ok(ModelInvocationRecord {
        id: ModelInvocationId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: rocketmq_sre_contracts::ClusterId::from_uuid(row.try_get("cluster_id")?),
        incident_id: row
            .try_get::<Option<Uuid>, _>("incident_id")?
            .map(IncidentId::from_uuid),
        diagnosis_revision_id: row
            .try_get::<Option<Uuid>, _>("diagnosis_revision_id")?
            .map(DiagnosisRevisionId::from_uuid),
        parent_invocation_id: row
            .try_get::<Option<Uuid>, _>("parent_invocation_id")?
            .map(ModelInvocationId::from_uuid),
        purpose: ModelInvocationPurpose::PrimaryDiagnosis,
        requested_profile_id: ModelProfileId::from_uuid(row.try_get("requested_profile_id")?),
        actual_profile_id: ModelProfileId::from_uuid(row.try_get("actual_profile_id")?),
        provider_family: row.try_get("provider_family")?,
        model_family: row.try_get("model_family")?,
        model_revision: row.try_get("model_revision")?,
        endpoint_instance: row.try_get("endpoint_instance")?,
        fallback_chain: row
            .try_get::<Vec<Uuid>, _>("fallback_chain")?
            .into_iter()
            .map(ModelProfileId::from_uuid)
            .collect(),
        prompt_version: row.try_get("prompt_version")?,
        schema_version: row.try_get("schema_version")?,
        input_tokens: bounded_u32(row.try_get("input_tokens")?, "input token usage")?,
        output_tokens: bounded_u32(row.try_get("output_tokens")?, "output token usage")?,
        cost_micros: bounded_u64(row.try_get("cost_micros")?, "model cost")?,
        rationale: row.try_get("rationale")?,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn bounded_u32(value: Option<i32>, field: &'static str) -> Result<Option<u32>, ControlPlaneError> {
    value
        .map(u32::try_from)
        .transpose()
        .map_err(|_| ControlPlaneError::configuration(format!("stored {field} is invalid")))
}

fn bounded_u64(value: Option<i64>, field: &'static str) -> Result<Option<u64>, ControlPlaneError> {
    value
        .map(u64::try_from)
        .transpose()
        .map_err(|_| ControlPlaneError::configuration(format!("stored {field} is invalid")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_model_gateway::SecretReference;

    use super::*;
    use crate::models::model::DIAGNOSIS_OUTPUT_SCHEMA_VERSION;
    use crate::models::model::DIAGNOSIS_PROMPT_VERSION;
    use crate::models::model::DIAGNOSIS_REPAIR_PROMPT_VERSION;

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn postgres_profiles_and_invocations_store_references_and_provenance() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 2)
            .await
            .expect("database and migrations");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let incident_id = IncidentId::new();
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', 'local', '5.3.0', 'test', 'model-test',
                'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(tenant_id.to_string())
        .bind(format!("model-{cluster_id}"))
        .execute(&repository.pool)
        .await
        .expect("test cluster");
        sqlx::query(
            "INSERT INTO sre_incidents (
                id, tenant_id, cluster_id, title, symptom_family, fingerprint,
                status, created_by_subject, created_at, updated_at
             ) VALUES (
                $1, $2, $3, 'Model integration test', 'broker-health',
                'model-test-fingerprint', 'diagnosing', 'model-test', NOW(), NOW()
             )",
        )
        .bind(incident_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .execute(&repository.pool)
        .await
        .expect("test incident");

        let mut profile = rocketmq_sre_model_gateway::builtin_provider_profiles()
            .into_iter()
            .find(|profile| profile.id == "vllm")
            .expect("local profile fixture");
        profile.id = "postgres-model-test".to_owned();
        profile.endpoint = "http://127.0.0.1:18080/v1".to_owned();
        profile.credential_ref = Some(SecretReference::parse("env://ROCKETMQ_SRE_MODEL_TEST_KEY").expect("reference"));
        let configured = repository
            .ensure_model_profiles(tenant_id, &[profile])
            .await
            .expect("configured profile");
        let invocation_id = ModelInvocationId::new();
        let correlation_id = CorrelationId::new();
        repository
            .persist_model_invocation(&PersistInvocation {
                id: invocation_id,
                tenant_id,
                cluster_id,
                incident_id,
                diagnosis_revision_id: None,
                parent_invocation_id: None,
                purpose: "primary_diagnosis",
                requested_profile_id: configured[0].id,
                actual_profile_id: configured[0].id,
                provider_family: "open_ai_compatible".to_owned(),
                model_family: "local".to_owned(),
                actual_model: "served-model".to_owned(),
                model_revision: "served".to_owned(),
                endpoint_instance: "postgres-model-test:local".to_owned(),
                fallback_chain: Vec::new(),
                prompt_version: DIAGNOSIS_PROMPT_VERSION,
                schema_version: DIAGNOSIS_OUTPUT_SCHEMA_VERSION,
                input_tokens: Some(10),
                output_tokens: Some(5),
                cost_micros: Some(1),
                rationale: "bounded test rationale".to_owned(),
                error_code: None,
                correlation_id,
                started_at: Utc::now(),
                completed_at: Utc::now(),
            })
            .await
            .expect("model invocation");
        let auth = AuthContext {
            tenant_id,
            subject: "model-test".to_owned(),
            clusters: BTreeSet::from([cluster_id]),
            roles: BTreeSet::new(),
        };
        let revision = repository
            .persist_diagnosis_revision(
                &auth,
                incident_id,
                rocketmq_sre_contracts::IncidentStatus::Monitoring,
                serde_json::json!({"mode": "model_assisted"}),
                serde_json::json!([]),
                Vec::new(),
                false,
                Some(invocation_id),
                "model_assisted",
                correlation_id,
            )
            .await
            .expect("model-assisted diagnosis revision");
        let page = repository
            .list_model_invocations(
                &auth,
                &ModelInvocationListQuery {
                    cluster_id,
                    incident_id: Some(incident_id),
                    limit: Some(10),
                },
            )
            .await
            .expect("invocation page");
        let credential_ref: String = sqlx::query_scalar("SELECT credential_ref FROM model_profiles WHERE id = $1")
            .bind(configured[0].id.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .expect("credential reference");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].id, invocation_id);
        assert_eq!(page.items[0].diagnosis_revision_id, Some(revision.id));
        assert_eq!(revision.primary_model_invocation_id, Some(invocation_id));
        assert!(!revision.execution_eligible);
        assert_eq!(credential_ref, "env://ROCKETMQ_SRE_MODEL_TEST_KEY");
        assert!(!credential_ref.contains("secret-value"));

        let failed_incident_id = IncidentId::new();
        sqlx::query(
            "INSERT INTO sre_incidents (
                id, tenant_id, cluster_id, title, symptom_family, fingerprint,
                status, created_by_subject, created_at, updated_at
             ) VALUES (
                $1, $2, $3, 'Rules-only model test', 'broker-health',
                'rules-only-model-test-fingerprint', 'diagnosing', 'model-test',
                NOW(), NOW()
             )",
        )
        .bind(failed_incident_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .execute(&repository.pool)
        .await
        .expect("rules-only incident");
        let failed_correlation_id = CorrelationId::new();
        let failed_invocation_id = ModelInvocationId::new();
        repository
            .persist_model_invocation(&PersistInvocation {
                id: failed_invocation_id,
                tenant_id,
                cluster_id,
                incident_id: failed_incident_id,
                diagnosis_revision_id: None,
                parent_invocation_id: None,
                purpose: "primary_diagnosis",
                requested_profile_id: configured[0].id,
                actual_profile_id: configured[0].id,
                provider_family: "open_ai_compatible".to_owned(),
                model_family: "local".to_owned(),
                actual_model: "served-model".to_owned(),
                model_revision: "served".to_owned(),
                endpoint_instance: "postgres-model-test:local".to_owned(),
                fallback_chain: Vec::new(),
                prompt_version: DIAGNOSIS_PROMPT_VERSION,
                schema_version: DIAGNOSIS_OUTPUT_SCHEMA_VERSION,
                input_tokens: None,
                output_tokens: None,
                cost_micros: None,
                rationale: "provider attempt failed with rate_limited".to_owned(),
                error_code: Some("rate_limited".to_owned()),
                correlation_id: failed_correlation_id,
                started_at: Utc::now(),
                completed_at: Utc::now(),
            })
            .await
            .expect("failed model attempt");
        let repair_invocation_id = ModelInvocationId::new();
        repository
            .persist_model_invocation(&PersistInvocation {
                id: repair_invocation_id,
                tenant_id,
                cluster_id,
                incident_id: failed_incident_id,
                diagnosis_revision_id: None,
                parent_invocation_id: Some(failed_invocation_id),
                purpose: "schema_repair",
                requested_profile_id: configured[0].id,
                actual_profile_id: configured[0].id,
                provider_family: "open_ai_compatible".to_owned(),
                model_family: "local".to_owned(),
                actual_model: "served-model".to_owned(),
                model_revision: "served".to_owned(),
                endpoint_instance: "postgres-model-test:local".to_owned(),
                fallback_chain: Vec::new(),
                prompt_version: DIAGNOSIS_REPAIR_PROMPT_VERSION,
                schema_version: DIAGNOSIS_OUTPUT_SCHEMA_VERSION,
                input_tokens: Some(12),
                output_tokens: Some(4),
                cost_micros: Some(1),
                rationale: "schema repair still invalid".to_owned(),
                error_code: Some("schema_validation_failed".to_owned()),
                correlation_id: failed_correlation_id,
                started_at: Utc::now(),
                completed_at: Utc::now(),
            })
            .await
            .expect("failed schema repair attempt");
        let rules_only = repository
            .persist_diagnosis_revision(
                &auth,
                failed_incident_id,
                rocketmq_sre_contracts::IncidentStatus::Monitoring,
                serde_json::json!({"mode": "rules_only"}),
                serde_json::json!([]),
                Vec::new(),
                true,
                None,
                "rules_only",
                failed_correlation_id,
            )
            .await
            .expect("rules-only diagnosis revision");
        let failed_page = repository
            .list_model_invocations(
                &auth,
                &ModelInvocationListQuery {
                    cluster_id,
                    incident_id: Some(failed_incident_id),
                    limit: Some(10),
                },
            )
            .await
            .expect("failed invocation page");
        assert!(rules_only.primary_model_invocation_id.is_none());
        assert!(!rules_only.execution_eligible);
        assert_eq!(failed_page.items.len(), 2);
        assert!(
            failed_page
                .items
                .iter()
                .all(|item| item.diagnosis_revision_id == Some(rules_only.id))
        );
        let repair = failed_page
            .items
            .iter()
            .find(|item| item.id == repair_invocation_id)
            .expect("repair invocation");
        assert_eq!(repair.parent_invocation_id, Some(failed_invocation_id));
        assert_eq!(repair.purpose, "schema_repair");
        assert_eq!(repair.error_code.as_deref(), Some("schema_validation_failed"));
    }
}
