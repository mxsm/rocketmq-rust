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
use rocketmq_sre_contracts::ClusterHealthReport;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::HealthDataQuality;
use rocketmq_sre_contracts::HealthOperationalState;
use rocketmq_sre_contracts::HealthStatus;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::TimelineEventKind;
use serde_json::Value;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Row;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[derive(Clone, Debug)]
pub(crate) struct FleetHealthRecord {
    pub(crate) cluster_id: ClusterId,
    pub(crate) external_cluster_key: String,
    pub(crate) region: String,
    pub(crate) report: Option<ClusterHealthReport>,
}

impl PostgresRepository {
    pub(crate) async fn store_health_snapshot(
        &self,
        auth: &AuthContext,
        report: &ClusterHealthReport,
    ) -> Result<ClusterHealthReport, ControlPlaneError> {
        enforce_health_scope(auth, report.tenant_id, report.cluster_id)?;
        let report_json = serde_json::to_value(report).map_err(|_| {
            ControlPlaneError::validation("invalid_health_snapshot", "health report cannot be serialized")
        })?;
        sqlx::query(
            "INSERT INTO cluster_health_snapshots (
                id, tenant_id, cluster_id, score, status, data_quality,
                operational_state, algorithm_version, evidence_ids, report, observed_at
             )
             SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
             WHERE EXISTS (
                 SELECT 1 FROM clusters
                 WHERE id = $3 AND tenant_id = $12 AND onboarding_state <> 'offboarded'
             )
             ON CONFLICT (tenant_id, cluster_id, observed_at, algorithm_version)
             DO NOTHING",
        )
        .bind(report.id.as_uuid())
        .bind(report.tenant_id.as_uuid())
        .bind(report.cluster_id.as_uuid())
        .bind(report.score.map(i16::from))
        .bind(health_status_name(report.status))
        .bind(data_quality_name(report.data_quality))
        .bind(operational_state_name(report.operational_state))
        .bind(&report.algorithm_version)
        .bind(report.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(report_json)
        .bind(report.observed_at)
        .bind(report.tenant_id.to_string())
        .execute(&self.pool)
        .await?;
        self.latest_health_snapshot(auth, report.cluster_id)
            .await?
            .ok_or_else(|| {
                ControlPlaneError::forbidden(
                    "cluster_not_allowed",
                    "health snapshot cluster is offboarded or outside the authenticated scope",
                )
            })
    }

    pub(crate) async fn latest_health_snapshot(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<Option<ClusterHealthReport>, ControlPlaneError> {
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "health snapshot cluster is outside the authenticated scope",
            ));
        }
        let report = sqlx::query_scalar::<_, Value>(
            "SELECT report
             FROM cluster_health_snapshots
             WHERE tenant_id = $1 AND cluster_id = $2
             ORDER BY observed_at DESC, id DESC
             LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        report.map(parse_health_report).transpose()
    }

    pub(crate) async fn fleet_health_records(
        &self,
        auth: &AuthContext,
        region: Option<&str>,
    ) -> Result<Vec<FleetHealthRecord>, ControlPlaneError> {
        let clusters = auth
            .clusters
            .iter()
            .map(|cluster_id| cluster_id.as_uuid())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "SELECT c.id, c.external_cluster_key, c.region, latest.report
             FROM clusters c
             LEFT JOIN LATERAL (
                 SELECT h.report
                 FROM cluster_health_snapshots h
                 WHERE h.tenant_id = $1 AND h.cluster_id = c.id
                 ORDER BY h.observed_at DESC, h.id DESC
                 LIMIT 1
             ) latest ON TRUE
             WHERE c.tenant_id = $2
               AND c.id = ANY($3)
               AND c.onboarding_state <> 'offboarded'
               AND ($4::TEXT IS NULL OR c.region = $4)
             ORDER BY c.region, c.external_cluster_key, c.id",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(auth.tenant_id.to_string())
        .bind(clusters)
        .bind(region)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let report = row
                    .try_get::<Option<Value>, _>("report")?
                    .map(parse_health_report)
                    .transpose()?;
                Ok(FleetHealthRecord {
                    cluster_id: ClusterId::from_uuid(row.try_get("id")?),
                    external_cluster_key: row.try_get("external_cluster_key")?,
                    region: row.try_get("region")?,
                    report,
                })
            })
            .collect()
    }

    pub(crate) async fn append_latest_health_to_incident(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        incident_id: IncidentId,
        correlation_id: CorrelationId,
    ) -> Result<(), ControlPlaneError> {
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "incident cluster is outside the authenticated scope",
            ));
        }
        let Some(report) = self.latest_health_snapshot(auth, cluster_id).await? else {
            return Ok(());
        };
        let event_id = deterministic_uuid(&format!("incident-health:{incident_id}:{}", report.id));
        let details = json!({
            "health_snapshot_id": report.id,
            "score": report.score,
            "status": report.status,
            "data_quality": report.data_quality,
            "operational_state": report.operational_state,
            "triggered_sli_ids": report.triggered_sli_ids,
            "evidence_ids": report.evidence_ids,
            "algorithm_version": report.algorithm_version,
        });
        sqlx::query(
            "INSERT INTO incident_timeline (
                event_id, tenant_id, cluster_id, incident_id, event_type, summary,
                details, correlation_id, actor_subject, occurred_at
             )
             SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
             WHERE EXISTS (
                 SELECT 1 FROM sre_incidents
                 WHERE id = $4 AND tenant_id = $2 AND cluster_id = $3
             )
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event_id)
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(TimelineEventKind::HealthSnapshot.as_str())
        .bind(format!(
            "Deterministic cluster health is {} with score {}",
            health_status_name(report.status),
            report
                .score
                .map_or_else(|| "unknown".to_owned(), |score| score.to_string())
        ))
        .bind(details)
        .bind(correlation_id.as_uuid())
        .bind("rocketmq-sre-health-engine")
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn enforce_health_scope(
    auth: &AuthContext,
    tenant_id: rocketmq_sre_contracts::TenantId,
    cluster_id: ClusterId,
) -> Result<(), ControlPlaneError> {
    if tenant_id != auth.tenant_id || !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "health snapshot scope differs from the authenticated scope",
        ));
    }
    Ok(())
}

fn parse_health_report(value: Value) -> Result<ClusterHealthReport, ControlPlaneError> {
    serde_json::from_value(value)
        .map_err(|_| ControlPlaneError::configuration("database contains an invalid cluster health report"))
}

const fn health_status_name(status: HealthStatus) -> &'static str {
    match status {
        HealthStatus::Healthy => "healthy",
        HealthStatus::Degraded => "degraded",
        HealthStatus::Critical => "critical",
        HealthStatus::Unknown => "unknown",
    }
}

const fn data_quality_name(quality: HealthDataQuality) -> &'static str {
    match quality {
        HealthDataQuality::Complete => "complete",
        HealthDataQuality::Partial => "partial",
        HealthDataQuality::Stale => "stale",
        HealthDataQuality::Missing => "missing",
    }
}

const fn operational_state_name(state: HealthOperationalState) -> &'static str {
    match state {
        HealthOperationalState::Normal => "normal",
        HealthOperationalState::Maintenance => "maintenance",
        HealthOperationalState::FaultDrill => "fault_drill",
    }
}

fn deterministic_uuid(material: &str) -> Uuid {
    let digest = Sha256::digest(material.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::HealthSnapshotId;
    use rocketmq_sre_contracts::IncidentHealthSummary;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn health_snapshots_round_trip_into_fleet_and_incident_timeline() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 2)
            .await
            .expect("database and migrations");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let incident_id = IncidentId::new();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', 'slo-test', 'test', 'test', 'slo-test',
                'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(tenant_id.to_string())
        .bind(format!("slo-test-{cluster_id}"))
        .execute(&repository.pool)
        .await
        .expect("test cluster");
        sqlx::query(
            "INSERT INTO sre_incidents (
                id, tenant_id, cluster_id, title, symptom_family, fingerprint,
                status, created_by_subject, created_at, updated_at
             ) VALUES (
                $1, $2, $3, 'SLO integration', 'health', $4,
                'new', 'slo-test', $5, $5
             )",
        )
        .bind(incident_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(format!("slo-test-{incident_id}"))
        .bind(now)
        .execute(&repository.pool)
        .await
        .expect("test incident");
        let auth = AuthContext {
            tenant_id,
            subject: "slo-test".to_owned(),
            clusters: BTreeSet::from([cluster_id]),
            roles: BTreeSet::from(["diagnose".to_owned()]),
        };
        let report = ClusterHealthReport {
            schema_version: "rocketmq-sre.cluster-health.v1".to_owned(),
            id: HealthSnapshotId::new(),
            tenant_id,
            cluster_id,
            score: Some(42),
            status: HealthStatus::Critical,
            data_quality: HealthDataQuality::Complete,
            operational_state: HealthOperationalState::Normal,
            dimensions: Vec::new(),
            slis: Vec::new(),
            incident_summary: IncidentHealthSummary {
                active_incidents: 1,
                critical_incidents: 1,
                unassigned_incidents: 1,
                last_alert_at: Some(now),
            },
            triggered_sli_ids: vec!["broker_runtime".to_owned()],
            evidence_ids: Vec::new(),
            recent_changes: Vec::new(),
            algorithm_version: "rocketmq-sre.health-score.v1".to_owned(),
            model_adjustment_supported: false,
            execution_eligible: false,
            observed_at: now,
        };

        let stored = repository
            .store_health_snapshot(&auth, &report)
            .await
            .expect("store health");
        assert_eq!(stored, report);
        let fleet = repository
            .fleet_health_records(&auth, Some("slo-test"))
            .await
            .expect("fleet rows");
        assert_eq!(fleet.len(), 1);
        assert_eq!(fleet[0].report.as_ref().and_then(|item| item.score), Some(42));

        repository
            .append_latest_health_to_incident(&auth, cluster_id, incident_id, CorrelationId::new())
            .await
            .expect("append health timeline");
        repository
            .append_latest_health_to_incident(&auth, cluster_id, incident_id, CorrelationId::new())
            .await
            .expect("idempotent health timeline");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM incident_timeline
             WHERE incident_id = $1 AND event_type = 'health_snapshot'",
        )
        .bind(incident_id.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .expect("timeline count");
        assert_eq!(count, 1);
    }
}
