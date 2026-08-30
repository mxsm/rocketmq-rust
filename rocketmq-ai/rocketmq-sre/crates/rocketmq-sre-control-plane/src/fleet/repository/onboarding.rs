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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ClusterRegistration;
use rocketmq_sre_contracts::FleetOnboardingAssessment;
use rocketmq_sre_contracts::FleetQuotaDecisionId;
use rocketmq_sre_contracts::FleetQuotaDecisionRecord;
use rocketmq_sre_contracts::FleetQuotaResource;
use rocketmq_sre_contracts::FleetQuotaWorkKind;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;

use super::FleetRepository;
use super::support::environment_name;
use crate::ControlPlaneError;
use crate::fleet::model::FleetOnboardingRequest;
use crate::fleet::model::FleetQuotaDecisionQuery;
use crate::fleet::model::bounded_limit;

impl FleetRepository {
    pub(in crate::fleet) async fn onboarding_scope_exists(
        &self,
        tenant_id: TenantId,
        fleet_id: rocketmq_sre_contracts::FleetId,
        region_id: rocketmq_sre_contracts::RegionId,
    ) -> Result<bool, ControlPlaneError> {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM fleet_tenants tenant
                JOIN fleet_regions region
                  ON region.fleet_id = tenant.fleet_id
                WHERE tenant.id = $1
                  AND tenant.fleet_id = $2
                  AND region.id = $3
                  AND tenant.active = TRUE
                  AND region.active = TRUE
             )",
        )
        .bind(tenant_id.as_uuid())
        .bind(fleet_id.as_uuid())
        .bind(region_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    pub(in crate::fleet) async fn store_onboarding_assessment(
        &self,
        assessment: &FleetOnboardingAssessment,
    ) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO fleet_onboarding_assessments (
                id, fleet_id, tenant_id, region_id, cluster_id,
                requested_access, effective_access, connector_tls_verified,
                schema_compatible, missing_capabilities, signal_gaps,
                excessive_scopes, incompatibilities, eligible, observed_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10, $11,
                $12, $13, $14, $15
             )",
        )
        .bind(assessment.id.as_uuid())
        .bind(assessment.fleet_id.as_uuid())
        .bind(assessment.tenant_id.as_uuid())
        .bind(assessment.region_id.as_uuid())
        .bind(assessment.cluster_id.as_uuid())
        .bind(access_profile_name(assessment.requested_access))
        .bind(access_profile_name(assessment.effective_access))
        .bind(assessment.connector_tls_verified)
        .bind(assessment.schema_compatible)
        .bind(assessment.missing_capabilities.iter().cloned().collect::<Vec<_>>())
        .bind(assessment.signal_gaps.iter().cloned().collect::<Vec<_>>())
        .bind(assessment.excessive_scopes.iter().cloned().collect::<Vec<_>>())
        .bind(assessment.incompatibilities.iter().cloned().collect::<Vec<_>>())
        .bind(assessment.eligible)
        .bind(assessment.observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(in crate::fleet) async fn upsert_cluster_registration(
        &self,
        tenant_id: TenantId,
        request: &FleetOnboardingRequest,
        degraded: bool,
    ) -> Result<ClusterRegistration, ControlPlaneError> {
        let state = if degraded { "read_only_degraded" } else { "active" };
        let residency_tags = serde_json::to_value(&request.residency_tags)
            .map_err(|_| ControlPlaneError::validation("invalid_request", "residency tags are invalid"))?;
        let row = sqlx::query(
            "INSERT INTO fleet_cluster_registrations (
                cluster_id, fleet_id, tenant_id, region_id, environment,
                owner_name, lifecycle_state, residency_tags, lifecycle_revision,
                created_at, updated_at
             )
             SELECT cluster.id, $3, $2::UUID, $4, $5,
                    $6, $7, $8, 1, NOW(), NOW()
             FROM clusters cluster
             WHERE cluster.id = $1 AND cluster.tenant_id = $2
               AND cluster.onboarding_state <> 'offboarded'
             ON CONFLICT (cluster_id) DO UPDATE SET
                region_id = EXCLUDED.region_id,
                environment = EXCLUDED.environment,
                owner_name = EXCLUDED.owner_name,
                lifecycle_state = EXCLUDED.lifecycle_state,
                residency_tags = EXCLUDED.residency_tags,
                lifecycle_revision = fleet_cluster_registrations.lifecycle_revision + 1,
                updated_at = NOW()
             WHERE fleet_cluster_registrations.fleet_id = EXCLUDED.fleet_id
               AND fleet_cluster_registrations.tenant_id = EXCLUDED.tenant_id
               AND fleet_cluster_registrations.lifecycle_state NOT IN ('offboarding', 'retired')
             RETURNING cluster_id",
        )
        .bind(request.cluster_id.as_uuid())
        .bind(tenant_id.to_string())
        .bind(request.fleet_id.as_uuid())
        .bind(request.region_id.as_uuid())
        .bind(environment_name(request.environment))
        .bind(request.owner.trim())
        .bind(state)
        .bind(residency_tags)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "fleet_registration_conflict",
                "cluster cannot be registered outside its immutable Fleet and tenant scope",
            )
        })?;
        let cluster_id = ClusterId::from_uuid(row.try_get("cluster_id")?);
        self.cluster_registration(tenant_id, cluster_id).await
    }

    pub(in crate::fleet) async fn begin_offboarding(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
    ) -> Result<(), ControlPlaneError> {
        let pending = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM executions
             WHERE tenant_id = $1 AND cluster_id = $2
               AND state NOT IN ('succeeded', 'rolled_back', 'escalated')",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        if pending > 0 {
            return Err(ControlPlaneError::conflict_code(
                "offboarding_pending_execution",
                "cluster has an execution that must be reconciled before offboarding",
            ));
        }
        let updated = sqlx::query(
            "UPDATE fleet_cluster_registrations
             SET lifecycle_state = 'offboarding',
                 lifecycle_revision = lifecycle_revision + 1,
                 updated_at = NOW()
             WHERE tenant_id = $1 AND cluster_id = $2
               AND lifecycle_state IN ('pending', 'onboarding', 'active', 'read_only_degraded')",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ControlPlaneError::conflict_code(
                "fleet_registration_terminal",
                "Fleet registration is already offboarding or retired",
            ));
        }
        Ok(())
    }

    pub(in crate::fleet) async fn retire_registration(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
    ) -> Result<ClusterRegistration, ControlPlaneError> {
        sqlx::query(
            "UPDATE fleet_cluster_registrations
             SET lifecycle_state = 'retired',
                 lifecycle_revision = lifecycle_revision + 1,
                 updated_at = NOW()
             WHERE tenant_id = $1 AND cluster_id = $2
               AND lifecycle_state = 'offboarding'",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .execute(&self.pool)
        .await?;
        self.cluster_registration(tenant_id, cluster_id).await
    }

    pub(in crate::fleet) async fn store_quota_decision(
        &self,
        decision: &FleetQuotaDecisionRecord,
    ) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO fleet_quota_decisions (
                id, policy_id, tenant_id, cluster_id, work_kind,
                resource_kind, amount, allowed, reason_code, observed,
                quota_limit, occurred_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11, $12
             )",
        )
        .bind(decision.id.as_uuid())
        .bind(decision.policy_id.as_uuid())
        .bind(decision.tenant_id.as_uuid())
        .bind(decision.cluster_id.map(ClusterId::as_uuid))
        .bind(work_kind_name(decision.work_kind))
        .bind(quota_resource_name(decision.resource))
        .bind(i64::try_from(decision.amount).unwrap_or(i64::MAX))
        .bind(decision.allowed)
        .bind(&decision.reason)
        .bind(i64::try_from(decision.observed).unwrap_or(i64::MAX))
        .bind(i64::try_from(decision.limit).unwrap_or(i64::MAX))
        .bind(decision.occurred_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(in crate::fleet) async fn quota_decisions(
        &self,
        tenant_id: TenantId,
        query: &FleetQuotaDecisionQuery,
    ) -> Result<(Vec<FleetQuotaDecisionRecord>, bool), ControlPlaneError> {
        let requested = i64::from(bounded_limit(query.limit));
        let rows = sqlx::query(
            "SELECT id, policy_id, tenant_id, cluster_id, work_kind,
                    resource_kind, amount, allowed, reason_code, observed,
                    quota_limit, occurred_at
             FROM fleet_quota_decisions
             WHERE tenant_id = $1
               AND ($2::UUID IS NULL OR cluster_id = $2)
               AND ($3::BOOLEAN IS NULL OR allowed = $3)
             ORDER BY occurred_at DESC, id
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.cluster_id.map(ClusterId::as_uuid))
        .bind(query.allowed)
        .bind(requested + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > requested;
        let items = rows
            .iter()
            .take(usize::try_from(requested).unwrap_or(usize::MAX))
            .map(quota_decision_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((items, truncated))
    }
}

fn quota_decision_from_row(row: &sqlx::postgres::PgRow) -> Result<FleetQuotaDecisionRecord, ControlPlaneError> {
    Ok(FleetQuotaDecisionRecord {
        id: FleetQuotaDecisionId::from_uuid(row.try_get("id")?),
        policy_id: rocketmq_sre_contracts::QuotaPolicyId::from_uuid(row.try_get("policy_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: row
            .try_get::<Option<uuid::Uuid>, _>("cluster_id")?
            .map(ClusterId::from_uuid),
        work_kind: parse_work_kind(row.try_get("work_kind")?)?,
        resource: parse_quota_resource(row.try_get("resource_kind")?)?,
        amount: unsigned(row.try_get("amount")?, "quota amount")?,
        allowed: row.try_get("allowed")?,
        reason: row.try_get("reason_code")?,
        observed: unsigned(row.try_get("observed")?, "quota observed value")?,
        limit: unsigned(row.try_get("quota_limit")?, "quota limit")?,
        occurred_at: row.try_get("occurred_at")?,
    })
}

fn access_profile_name(profile: rocketmq_sre_contracts::FleetAccessProfile) -> &'static str {
    match profile {
        rocketmq_sre_contracts::FleetAccessProfile::ReadOnly => "read_only",
        rocketmq_sre_contracts::FleetAccessProfile::Supervised => "supervised",
        rocketmq_sre_contracts::FleetAccessProfile::BoundedAutonomy => "bounded_autonomy",
    }
}

fn work_kind_name(kind: FleetQuotaWorkKind) -> &'static str {
    match kind {
        FleetQuotaWorkKind::ActiveIncident => "active_incident",
        FleetQuotaWorkKind::Verification => "verification",
        FleetQuotaWorkKind::Rollback => "rollback",
        FleetQuotaWorkKind::Audit => "audit",
        FleetQuotaWorkKind::InteractiveQuery => "interactive_query",
        FleetQuotaWorkKind::Workflow => "workflow",
        FleetQuotaWorkKind::Inspection => "inspection",
        FleetQuotaWorkKind::ModelExplanation => "model_explanation",
        FleetQuotaWorkKind::Notification => "notification",
        FleetQuotaWorkKind::AutomaticAction => "automatic_action",
    }
}

fn parse_work_kind(value: String) -> Result<FleetQuotaWorkKind, ControlPlaneError> {
    match value.as_str() {
        "active_incident" => Ok(FleetQuotaWorkKind::ActiveIncident),
        "verification" => Ok(FleetQuotaWorkKind::Verification),
        "rollback" => Ok(FleetQuotaWorkKind::Rollback),
        "audit" => Ok(FleetQuotaWorkKind::Audit),
        "interactive_query" => Ok(FleetQuotaWorkKind::InteractiveQuery),
        "workflow" => Ok(FleetQuotaWorkKind::Workflow),
        "inspection" => Ok(FleetQuotaWorkKind::Inspection),
        "model_explanation" => Ok(FleetQuotaWorkKind::ModelExplanation),
        "notification" => Ok(FleetQuotaWorkKind::Notification),
        "automatic_action" => Ok(FleetQuotaWorkKind::AutomaticAction),
        _ => Err(ControlPlaneError::configuration(
            "Fleet quota work kind contains an invalid persisted value",
        )),
    }
}

fn quota_resource_name(resource: FleetQuotaResource) -> &'static str {
    match resource {
        FleetQuotaResource::Query => "query",
        FleetQuotaResource::ModelToken => "model_token",
        FleetQuotaResource::ConcurrentWorkflow => "concurrent_workflow",
        FleetQuotaResource::ConcurrentInspection => "concurrent_inspection",
        FleetQuotaResource::EvidenceByte => "evidence_byte",
        FleetQuotaResource::Notification => "notification",
        FleetQuotaResource::AutomaticAction => "automatic_action",
    }
}

fn parse_quota_resource(value: String) -> Result<FleetQuotaResource, ControlPlaneError> {
    match value.as_str() {
        "query" => Ok(FleetQuotaResource::Query),
        "model_token" => Ok(FleetQuotaResource::ModelToken),
        "concurrent_workflow" => Ok(FleetQuotaResource::ConcurrentWorkflow),
        "concurrent_inspection" => Ok(FleetQuotaResource::ConcurrentInspection),
        "evidence_byte" => Ok(FleetQuotaResource::EvidenceByte),
        "notification" => Ok(FleetQuotaResource::Notification),
        "automatic_action" => Ok(FleetQuotaResource::AutomaticAction),
        _ => Err(ControlPlaneError::configuration(
            "Fleet quota resource contains an invalid persisted value",
        )),
    }
}

fn unsigned(value: i64, field: &str) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| ControlPlaneError::configuration(format!("{field} is invalid")))
}
