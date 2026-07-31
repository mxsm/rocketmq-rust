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
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceReference;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::InvestigationId;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::SchemaVersion;
use rocketmq_sre_contracts::Sensitivity;
use rocketmq_sre_contracts::TimeRange;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::EvidenceListQuery;
use super::EvidencePage;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const EVIDENCE_RETENTION_DAYS: i64 = 30;

impl PostgresRepository {
    pub(crate) async fn message_journey_evidence(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        trace_fingerprint: &str,
    ) -> Result<Vec<EvidenceSnapshot>, ControlPlaneError> {
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "message journey cluster is outside the authenticated scope",
            ));
        }
        let rows = sqlx::query(&format!(
            "{EVIDENCE_COLUMNS}
             WHERE e.tenant_id = $1
               AND e.cluster_id = $2
               AND (e.expires_at IS NULL OR e.expires_at > NOW())
               AND e.resource LIKE ('%' || $3 || '%')
               AND e.source IN ('admin-query', 'tempo', 'mcp', 'rocketmq-mcp')
             ORDER BY e.observed_at ASC, e.id ASC
             LIMIT 64"
        ))
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(trace_fingerprint)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(evidence_from_row).collect()
    }

    pub(crate) async fn persist_evidence(
        &self,
        auth: &AuthContext,
        snapshot: &EvidenceSnapshot,
        investigation_id: Option<InvestigationId>,
        incident_id: Option<IncidentId>,
        content_digest: &str,
    ) -> Result<EvidenceSnapshot, ControlPlaneError> {
        enforce_evidence_scope(auth, snapshot)?;
        let query_hash = query_hash(snapshot);
        let mut transaction = self.pool.begin().await?;
        let existing = sqlx::query(&format!(
            "{EVIDENCE_COLUMNS}
             WHERE e.tenant_id = $1 AND e.cluster_id = $2 AND e.query_hash = $3
               AND e.time_range_start = $4 AND e.time_range_end = $5 AND e.content_hash = $6
               AND (e.expires_at IS NULL OR e.expires_at > NOW())
             ORDER BY e.collected_at DESC LIMIT 1"
        ))
        .bind(auth.tenant_id.as_uuid())
        .bind(snapshot.cluster_id.as_uuid())
        .bind(&query_hash)
        .bind(snapshot.time_range.start)
        .bind(snapshot.time_range.end)
        .bind(snapshot.content_hash.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        let persisted = if let Some(row) = existing {
            evidence_from_row(&row)?
        } else {
            let (inline_content, reference) = match &snapshot.content {
                EvidenceContent::Inline(content) => (Some(content.clone()), None),
                EvidenceContent::Reference(reference) => (None, Some(reference)),
            };
            let collected_at = Utc::now();
            let expires_at = evidence_expires_at(collected_at);
            sqlx::query(
                "INSERT INTO evidence_snapshots (
                    id, query_id, correlation_id, tenant_id, cluster_id,
                    investigation_id, incident_id, schema_family, schema_major, schema_minor,
                    source, resource, time_range_start, time_range_end, observed_at, collected_at,
                    freshness_seconds, coverage, sensitivity, exposure, partial, warnings, inline_content,
                    content_uri, content_size_bytes, content_hash, content_digest, query_hash, expires_at
                 ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
                    $23, $24, $25, $26, $27, $28, $29
                 )",
            )
            .bind(snapshot.evidence_id.as_uuid())
            .bind(snapshot.query_id.as_uuid())
            .bind(snapshot.correlation_id.as_uuid())
            .bind(snapshot.tenant_id.as_uuid())
            .bind(snapshot.cluster_id.as_uuid())
            .bind(investigation_id.map(InvestigationId::as_uuid))
            .bind(incident_id.map(IncidentId::as_uuid))
            .bind(&snapshot.schema.family)
            .bind(i32::from(snapshot.schema.major))
            .bind(i32::from(snapshot.schema.minor))
            .bind(&snapshot.source)
            .bind(&snapshot.resource)
            .bind(snapshot.time_range.start)
            .bind(snapshot.time_range.end)
            .bind(snapshot.observed_at)
            .bind(collected_at)
            .bind(i64::try_from(snapshot.freshness_seconds).map_err(|_| {
                ControlPlaneError::validation("invalid_request", "evidence freshness exceeds the supported range")
            })?)
            .bind(coverage_name(snapshot.coverage))
            .bind(sensitivity_name(snapshot.sensitivity))
            .bind(exposure_name(snapshot.exposure))
            .bind(snapshot.partial)
            .bind(serde_json::to_value(&snapshot.warnings).map_err(|_| {
                ControlPlaneError::validation("invalid_request", "evidence warnings cannot be serialized")
            })?)
            .bind(inline_content)
            .bind(reference.map(|value| value.uri.as_str()))
            .bind(
                reference
                    .map(|value| i64::try_from(value.size_bytes))
                    .transpose()
                    .map_err(|_| {
                        ControlPlaneError::validation(
                            "output_too_large",
                            "evidence content size exceeds the supported range",
                        )
                    })?,
            )
            .bind(&snapshot.content_hash)
            .bind(content_digest)
            .bind(&query_hash)
            .bind(expires_at)
            .execute(&mut *transaction)
            .await?;
            snapshot.clone()
        };

        if investigation_id.is_some() || incident_id.is_some() {
            sqlx::query(
                "INSERT INTO evidence_links (
                    id, evidence_id, investigation_id, incident_id, linked_at
                 ) VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT DO NOTHING",
            )
            .bind(Uuid::new_v4())
            .bind(persisted.evidence_id.as_uuid())
            .bind(investigation_id.map(InvestigationId::as_uuid))
            .bind(incident_id.map(IncidentId::as_uuid))
            .bind(Utc::now())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(persisted)
    }

    pub(crate) async fn latest_cluster_source_evidence(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        source: &str,
        resource: &str,
    ) -> Result<Option<EvidenceSnapshot>, ControlPlaneError> {
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "evidence cluster is outside the authenticated scope",
            ));
        }
        let row = sqlx::query(&format!(
            "{EVIDENCE_COLUMNS}
             WHERE e.tenant_id = $1 AND e.cluster_id = $2
               AND e.source = $3 AND e.resource = $4
               AND (e.expires_at IS NULL OR e.expires_at > NOW())
             ORDER BY e.observed_at DESC, e.collected_at DESC, e.id DESC
             LIMIT 1"
        ))
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(source)
        .bind(resource)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(evidence_from_row).transpose()
    }

    pub(crate) async fn evidence(
        &self,
        auth: &AuthContext,
        id: EvidenceId,
    ) -> Result<EvidenceSnapshot, ControlPlaneError> {
        let row = sqlx::query(&format!(
            "{EVIDENCE_COLUMNS}
             WHERE e.id = $1 AND e.tenant_id = $2
               AND (e.expires_at IS NULL OR e.expires_at > NOW())"
        ))
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let snapshot = evidence_from_row(&row)?;
        enforce_evidence_scope(auth, &snapshot)?;
        Ok(snapshot)
    }

    pub(crate) async fn evidence_content_digest(
        &self,
        auth: &AuthContext,
        id: EvidenceId,
    ) -> Result<String, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT cluster_id, content_digest
             FROM evidence_snapshots
             WHERE id = $1 AND tenant_id = $2
               AND (expires_at IS NULL OR expires_at > NOW())",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let cluster = rocketmq_sre_contracts::ClusterId::from_uuid(row.try_get("cluster_id")?);
        if !auth.clusters.contains(&cluster) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "evidence cluster is outside the authenticated scope",
            ));
        }
        row.try_get("content_digest").map_err(ControlPlaneError::from)
    }

    pub(crate) async fn list_evidence(
        &self,
        auth: &AuthContext,
        query: &EvidenceListQuery,
    ) -> Result<EvidencePage, ControlPlaneError> {
        if !auth.clusters.contains(&query.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "evidence cluster is outside the authenticated scope",
            ));
        }
        let limit = query.bounded_limit()?;
        let cursor = query
            .cursor
            .as_deref()
            .map(|value| {
                value
                    .parse::<Uuid>()
                    .map_err(|_| ControlPlaneError::validation("invalid_request", "evidence cursor must be a UUID"))
            })
            .transpose()?;
        let rows = sqlx::query(&format!(
            "{EVIDENCE_COLUMNS}
             WHERE e.tenant_id = $1 AND e.cluster_id = $2
               AND (e.expires_at IS NULL OR e.expires_at > NOW())
               AND ($3::UUID IS NULL OR EXISTS (
                   SELECT 1 FROM evidence_links l
                   WHERE l.evidence_id = e.id AND l.incident_id = $3
               ))
               AND ($4::TEXT IS NULL OR e.source = $4)
               AND ($5::UUID IS NULL OR e.id < $5)
             ORDER BY e.id DESC LIMIT $6"
        ))
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(query.incident_id.map(IncidentId::as_uuid))
        .bind(query.source.as_deref())
        .bind(cursor)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > limit as usize;
        let mut items = rows
            .iter()
            .take(limit as usize)
            .map(evidence_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = has_more
            .then(|| items.last().map(|item| item.evidence_id.to_string()))
            .flatten();
        let partial = items
            .iter()
            .any(|item| item.partial || item.coverage != CoverageStatus::Available);
        items.shrink_to_fit();
        Ok(EvidencePage {
            items,
            next_cursor,
            partial,
        })
    }
}

fn evidence_expires_at(collected_at: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    collected_at + chrono::Duration::days(EVIDENCE_RETENTION_DAYS)
}

fn evidence_from_row(row: &PgRow) -> Result<EvidenceSnapshot, ControlPlaneError> {
    let family: String = row.try_get("schema_family")?;
    let major =
        u16::try_from(row.try_get::<i32, _>("schema_major")?).map_err(|_| invalid_stored_evidence("schema major"))?;
    let minor =
        u16::try_from(row.try_get::<i32, _>("schema_minor")?).map_err(|_| invalid_stored_evidence("schema minor"))?;
    let inline: Option<Value> = row.try_get("inline_content")?;
    let uri: Option<String> = row.try_get("content_uri")?;
    let digest: String = row.try_get("content_digest")?;
    let content = match (inline, uri) {
        (Some(value), None) => EvidenceContent::Inline(value),
        (None, Some(uri)) => EvidenceContent::Reference(EvidenceReference {
            uri,
            digest,
            media_type: "application/json".to_owned(),
            size_bytes: u64::try_from(row.try_get::<i64, _>("content_size_bytes")?)
                .map_err(|_| invalid_stored_evidence("content size"))?,
        }),
        _ => return Err(invalid_stored_evidence("content storage")),
    };
    let warnings: Vec<String> =
        serde_json::from_value(row.try_get("warnings")?).map_err(|_| invalid_stored_evidence("warnings"))?;
    let snapshot = EvidenceSnapshot {
        schema: SchemaVersion::new(family, major, minor),
        evidence_id: EvidenceId::from_uuid(row.try_get("id")?),
        query_id: QueryId::from_uuid(row.try_get("query_id")?),
        correlation_id: rocketmq_sre_contracts::CorrelationId::from_uuid(row.try_get("correlation_id")?),
        tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: rocketmq_sre_contracts::ClusterId::from_uuid(row.try_get("cluster_id")?),
        source: row.try_get("source")?,
        resource: row.try_get("resource")?,
        time_range: TimeRange::new(row.try_get("time_range_start")?, row.try_get("time_range_end")?)
            .map_err(|_| invalid_stored_evidence("time range"))?,
        observed_at: row.try_get("observed_at")?,
        freshness_seconds: u64::try_from(row.try_get::<i64, _>("freshness_seconds")?)
            .map_err(|_| invalid_stored_evidence("freshness"))?,
        partial: row.try_get("partial")?,
        warnings,
        sensitivity: parse_sensitivity(row.try_get("sensitivity")?)?,
        coverage: parse_coverage(row.try_get("coverage")?)?,
        exposure: parse_exposure(row.try_get("exposure")?)?,
        content,
        content_hash: row.try_get("content_hash")?,
    };
    snapshot
        .verify_content_hash()
        .map_err(|_| invalid_stored_evidence("content hash"))?;
    Ok(snapshot)
}

fn enforce_evidence_scope(auth: &AuthContext, snapshot: &EvidenceSnapshot) -> Result<(), ControlPlaneError> {
    if snapshot.tenant_id != auth.tenant_id {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "evidence tenant differs from the authenticated tenant",
        ));
    }
    if !auth.clusters.contains(&snapshot.cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "evidence cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

fn query_hash(snapshot: &EvidenceSnapshot) -> String {
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}",
        snapshot.cluster_id,
        snapshot.source,
        snapshot.resource,
        snapshot.time_range.start.to_rfc3339(),
        snapshot.time_range.end.to_rfc3339()
    );
    format!("sha256:{:x}", Sha256::digest(canonical.as_bytes()))
}

fn coverage_name(value: CoverageStatus) -> &'static str {
    match value {
        CoverageStatus::Available => "available",
        CoverageStatus::Partial => "partial",
        CoverageStatus::Missing => "missing",
        CoverageStatus::NotProductionVerified => "not_production_verified",
    }
}

fn parse_coverage(value: &str) -> Result<CoverageStatus, ControlPlaneError> {
    match value {
        "available" => Ok(CoverageStatus::Available),
        "partial" => Ok(CoverageStatus::Partial),
        "missing" => Ok(CoverageStatus::Missing),
        "not_production_verified" => Ok(CoverageStatus::NotProductionVerified),
        _ => Err(invalid_stored_evidence("coverage")),
    }
}

fn sensitivity_name(value: Sensitivity) -> &'static str {
    match value {
        Sensitivity::Public => "public",
        Sensitivity::Internal => "internal",
        Sensitivity::Confidential => "confidential",
        Sensitivity::Restricted => "restricted",
    }
}

fn parse_sensitivity(value: &str) -> Result<Sensitivity, ControlPlaneError> {
    match value {
        "public" => Ok(Sensitivity::Public),
        "internal" => Ok(Sensitivity::Internal),
        "confidential" => Ok(Sensitivity::Confidential),
        "restricted" => Ok(Sensitivity::Restricted),
        _ => Err(invalid_stored_evidence("sensitivity")),
    }
}

const fn exposure_name(exposure: EvidenceExposure) -> &'static str {
    match exposure {
        EvidenceExposure::Unknown => "unknown",
        EvidenceExposure::McpTool => "mcp_tool",
        EvidenceExposure::McpResource => "mcp_resource",
        EvidenceExposure::AdminRpc => "admin_rpc",
        EvidenceExposure::PrometheusApi => "prometheus_api",
        EvidenceExposure::AlertmanagerApi => "alertmanager_api",
        EvidenceExposure::LokiApi => "loki_api",
        EvidenceExposure::TempoApi => "tempo_api",
        EvidenceExposure::KubernetesApi => "kubernetes_api",
        EvidenceExposure::RuntimeDiagnostics => "runtime_diagnostics",
        EvidenceExposure::ExecutionAgentApi => "execution_agent_api",
        EvidenceExposure::RequiredSignals => "required_signals",
        EvidenceExposure::Synthetic => "synthetic",
        EvidenceExposure::Unsupported => "unsupported",
    }
}

fn parse_exposure(value: &str) -> Result<EvidenceExposure, ControlPlaneError> {
    match value {
        "unknown" => Ok(EvidenceExposure::Unknown),
        "mcp_tool" => Ok(EvidenceExposure::McpTool),
        "mcp_resource" => Ok(EvidenceExposure::McpResource),
        "admin_rpc" => Ok(EvidenceExposure::AdminRpc),
        "prometheus_api" => Ok(EvidenceExposure::PrometheusApi),
        "alertmanager_api" => Ok(EvidenceExposure::AlertmanagerApi),
        "loki_api" => Ok(EvidenceExposure::LokiApi),
        "tempo_api" => Ok(EvidenceExposure::TempoApi),
        "kubernetes_api" => Ok(EvidenceExposure::KubernetesApi),
        "runtime_diagnostics" => Ok(EvidenceExposure::RuntimeDiagnostics),
        "execution_agent_api" => Ok(EvidenceExposure::ExecutionAgentApi),
        "required_signals" => Ok(EvidenceExposure::RequiredSignals),
        "synthetic" => Ok(EvidenceExposure::Synthetic),
        "unsupported" => Ok(EvidenceExposure::Unsupported),
        _ => Err(invalid_stored_evidence("exposure")),
    }
}

fn invalid_stored_evidence(field: &str) -> ControlPlaneError {
    ControlPlaneError::validation("source_unavailable", format!("stored evidence {field} is invalid"))
}

const EVIDENCE_COLUMNS: &str = "SELECT e.id, e.query_id, e.correlation_id, e.tenant_id, e.cluster_id,
    e.schema_family, e.schema_major, e.schema_minor, e.source, e.resource,
    e.time_range_start, e.time_range_end, e.observed_at, e.freshness_seconds,
    e.coverage, e.sensitivity, e.exposure, e.partial, e.warnings, e.inline_content,
    e.content_uri, e.content_size_bytes, e.content_hash, e.content_digest
    FROM evidence_snapshots e";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_retention_is_bounded_and_deterministic() {
        let collected_at = chrono::DateTime::parse_from_rfc3339("2026-07-27T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        assert_eq!(
            evidence_expires_at(collected_at),
            collected_at + chrono::Duration::days(30)
        );
    }

    #[test]
    fn required_signal_exposure_round_trips_through_storage() {
        let stored = exposure_name(EvidenceExposure::RequiredSignals);

        assert_eq!(stored, "required_signals");
        assert_eq!(
            parse_exposure(stored).expect("required signal exposure should remain readable"),
            EvidenceExposure::RequiredSignals
        );
    }

    #[test]
    fn execution_agent_exposure_round_trips_through_storage() {
        let stored = exposure_name(EvidenceExposure::ExecutionAgentApi);

        assert_eq!(stored, "execution_agent_api");
        assert_eq!(
            parse_exposure(stored).expect("Execution Agent exposure should remain readable"),
            EvidenceExposure::ExecutionAgentApi
        );
    }
}
