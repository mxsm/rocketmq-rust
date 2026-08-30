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

use std::collections::BTreeMap;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::GovernanceAdmission;
use rocketmq_sre_contracts::GovernanceArtifactId;
use rocketmq_sre_contracts::GovernanceImpact;
use rocketmq_sre_contracts::GovernanceVersionId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;

use super::GovernanceRepository;
use super::support::access_path_name;
use super::support::admission_from_row;
use super::support::event_from_row;
use super::support::impact_from_row;
use super::support::impact_kind_name;
use crate::ControlPlaneError;
use crate::governance::model::GOVERNANCE_API_SCHEMA_VERSION;
use crate::governance::model::GovernanceAuditExport;
use crate::governance::model::GovernanceAuditQuery;
use crate::governance::model::GovernanceComplianceReport;
use crate::governance::model::GovernanceImpactQuery;
use crate::governance::model::bounded_export_limit;
use crate::governance::model::bounded_limit;

impl GovernanceRepository {
    pub(in crate::governance) async fn cluster_in_tenant(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
    ) -> Result<bool, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM fleet_cluster_registrations
                WHERE tenant_id = $1
                  AND cluster_id = $2
                  AND lifecycle_state IN ('active', 'read_only_degraded')
             ) AS present",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("present")?)
    }

    pub(in crate::governance) async fn record_impact(
        &self,
        impact: &GovernanceImpact,
    ) -> Result<GovernanceImpact, ControlPlaneError> {
        let row = sqlx::query(
            "INSERT INTO governance_impacts (
                version_id, tenant_id, cluster_id, impact_kind,
                reference_id, label, observed_at
             )
             SELECT version.id, version.tenant_id, $3, $4, $5, $6, $7
             FROM governance_versions version
             WHERE version.id = $1 AND version.tenant_id = $2
             ON CONFLICT (version_id, impact_kind, reference_id) DO UPDATE SET
                cluster_id = EXCLUDED.cluster_id,
                label = EXCLUDED.label,
                observed_at = EXCLUDED.observed_at
             RETURNING *",
        )
        .bind(impact.version_id.as_uuid())
        .bind(impact.tenant_id.as_uuid())
        .bind(impact.cluster_id.map(ClusterId::as_uuid))
        .bind(impact_kind_name(impact.kind))
        .bind(&impact.reference_id)
        .bind(&impact.label)
        .bind(impact.observed_at)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        impact_from_row(&row)
    }

    pub(in crate::governance) async fn list_impacts(
        &self,
        tenant_id: TenantId,
        version_id: GovernanceVersionId,
        query: &GovernanceImpactQuery,
    ) -> Result<(Vec<GovernanceImpact>, bool), ControlPlaneError> {
        let limit = bounded_limit(query.limit);
        let kind = query.kind.map(impact_kind_name);
        let rows = sqlx::query(
            "SELECT *
             FROM governance_impacts
             WHERE tenant_id = $1
               AND version_id = $2
               AND ($3::UUID IS NULL OR cluster_id = $3)
               AND ($4::TEXT IS NULL OR impact_kind = $4)
             ORDER BY impact_kind, reference_id
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(version_id.as_uuid())
        .bind(query.cluster_id.map(ClusterId::as_uuid))
        .bind(kind)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        rows.into_iter()
            .take(usize::try_from(limit).unwrap_or(200))
            .map(|row| impact_from_row(&row))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| (items, truncated))
    }

    pub(in crate::governance) async fn record_admission(
        &self,
        admission: &GovernanceAdmission,
    ) -> Result<GovernanceAdmission, ControlPlaneError> {
        let row = sqlx::query(
            "INSERT INTO governance_admissions (
                id, tenant_id, cluster_id, access_path, required_version_ids,
                allowed, degraded, reason_codes, evaluated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             RETURNING *",
        )
        .bind(admission.id.as_uuid())
        .bind(admission.tenant_id.as_uuid())
        .bind(admission.cluster_id.map(ClusterId::as_uuid))
        .bind(access_path_name(admission.access_path))
        .bind(
            admission
                .required_version_ids
                .iter()
                .map(|id| id.as_uuid())
                .collect::<Vec<_>>(),
        )
        .bind(admission.allowed)
        .bind(admission.degraded)
        .bind(&admission.reason_codes)
        .bind(admission.evaluated_at)
        .fetch_one(&self.pool)
        .await?;
        admission_from_row(&row)
    }

    pub(in crate::governance) async fn audit_export(
        &self,
        tenant_id: TenantId,
        query: &GovernanceAuditQuery,
    ) -> Result<GovernanceAuditExport, ControlPlaneError> {
        let limit = bounded_export_limit(query.limit);
        let rows = sqlx::query(
            "SELECT *
             FROM governance_events
             WHERE tenant_id = $1
               AND ($2::UUID IS NULL OR artifact_id = $2)
               AND ($3::UUID IS NULL OR version_id = $3)
               AND ($4::TIMESTAMPTZ IS NULL OR occurred_at >= $4)
               AND ($5::TIMESTAMPTZ IS NULL OR occurred_at < $5)
             ORDER BY occurred_at, sequence_id
             LIMIT $6",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.artifact_id.map(GovernanceArtifactId::as_uuid))
        .bind(query.version_id.map(GovernanceVersionId::as_uuid))
        .bind(query.from)
        .bind(query.to)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        let items = rows
            .into_iter()
            .take(usize::try_from(limit).unwrap_or(1_000))
            .map(|row| event_from_row(&row))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GovernanceAuditExport {
            schema_version: GOVERNANCE_API_SCHEMA_VERSION,
            items,
            truncated,
            exported_at: Utc::now(),
        })
    }

    pub(in crate::governance) async fn compliance_report(
        &self,
        tenant_id: TenantId,
        now: DateTime<Utc>,
    ) -> Result<GovernanceComplianceReport, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT lifecycle_state, COUNT(*) AS count
             FROM governance_versions
             WHERE tenant_id = $1
             GROUP BY lifecycle_state",
        )
        .bind(tenant_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        let mut state_counts = BTreeMap::new();
        for row in rows {
            let count = u64::try_from(row.try_get::<i64, _>("count")?).map_err(|_| {
                ControlPlaneError::validation("invalid_persisted_governance_state", "governance count is negative")
            })?;
            state_counts.insert(row.try_get("lifecycle_state")?, count);
        }
        let row = sqlx::query(
            "SELECT
                COUNT(*) FILTER (
                    WHERE lifecycle_state = 'active'
                      AND (
                          signature_algorithm IS NULL
                          OR signing_key_id IS NULL
                          OR signature_value IS NULL
                      )
                ) AS unsigned_active,
                COUNT(*) FILTER (
                    WHERE lifecycle_state = 'active'
                      AND expires_at IS NOT NULL
                      AND expires_at <= $2
                ) AS expired_active,
                COUNT(*) FILTER (
                    WHERE lifecycle_state = 'active' AND review_due_at <= $2
                ) AS overdue_review,
                COUNT(*) FILTER (WHERE lifecycle_state = 'quarantined') AS quarantined
             FROM governance_versions
             WHERE tenant_id = $1",
        )
        .bind(tenant_id.as_uuid())
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        let unsigned_active = count(&row, "unsigned_active")?;
        let expired_active = count(&row, "expired_active")?;
        let overdue_review = count(&row, "overdue_review")?;
        let quarantined = count(&row, "quarantined")?;
        Ok(GovernanceComplianceReport {
            schema_version: GOVERNANCE_API_SCHEMA_VERSION,
            state_counts,
            unsigned_active,
            expired_active,
            overdue_review,
            quarantined,
            compliant: unsigned_active == 0 && expired_active == 0 && overdue_review == 0,
            observed_at: now,
        })
    }
}

fn count(row: &sqlx::postgres::PgRow, column: &str) -> Result<u64, ControlPlaneError> {
    u64::try_from(row.try_get::<i64, _>(column)?).map_err(|_| {
        ControlPlaneError::validation("invalid_persisted_governance_state", "governance count is negative")
    })
}
